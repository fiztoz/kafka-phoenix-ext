// Package poller owns the Kafka poll loop and the in-memory snapshot the
// HTTP layer reads. Failed polls never wipe topics; they mark poll_ok=false
// and keep the last good sample.
package poller

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

// Source is the subset of the Kafka client the poller needs.
type Source interface {
	Describe(ctx context.Context, o kafka.DescribeOptions) (*kafka.ClusterSnapshot, error)
}

// TopicView is one topic as shown to the HTTP layer, enriched with growth,
// skew and threshold state.
type TopicView struct {
	Name         string
	Partitions   int
	Replicas     int
	StorageBytes int64
	LeaderBytes  int64
	IsInternal   bool
	UnderRep     int
	Offline      int
	Parts        []kafka.PartitionUse

	PrevBytes     int64     // previous good sample
	PrevPolledAt  time.Time // zero when this is the first sample
	GrowthPerHour int64     // over the rolling window; 0 until two samples exist

	HoursToLimit  *int64  // forecast at current growth; nil when not applicable
	PartitionSkew float64 // max/avg leader partition size; 0 = unknown or uniform

	Stale          bool
	ThresholdBytes *int64 // operator-set size limit
	WarnBytes      *int64 // optional; below ThresholdBytes
	OverStreak     int    // consecutive polls at/over threshold
	ConfirmedOver  bool   // OverStreak >= confirmSamples

	GrowthThreshold *int64 // operator-set bytes/hour limit (optional)
	GrowthStreak    int    // consecutive polls at/over growth threshold
	GrowthConfirmed bool   // GrowthStreak >= confirmSamples

	RepStreak        int  // consecutive polls with under-rep/offline partitions
	ReplicaConfirmed bool // RepStreak >= confirmSamples

	// ReplicaUnavailable means this sample cannot prove replica health.
	ReplicaUnavailable bool
	// SizeIncomplete means this topic's byte total omitted a known replica.
	SizeIncomplete bool
}

// Snapshot is the poller's last observation.
type Snapshot struct {
	ClusterID    string
	ControllerID int32
	PolledAt     time.Time // zero until the first successful poll
	PollOK       bool
	LastError    string
	TotalBytes   int64
	Topics       []TopicView
	Brokers      []kafka.BrokerUse
	BrokerErrors []string

	SkewConfirmed bool    // hottest broker confirmed over the skew policy
	SkewStreak    int     // consecutive polls over the skew policy
	SkewBrokerID  int32   // hottest broker in the last sample (-1 = unknown)
	SkewSharePct  float64 // hottest broker's share of broker-reported bytes

	// SizeIncomplete means the visible byte totals omit a failed broker or
	// directory. Alarms and the growth baseline stay on the last complete sample.
	SizeIncomplete bool
	// MetadataUnavailable means replica health is unknown, not healthy.
	MetadataUnavailable bool
	// StorageError is a failed durability write. Alert state still advances.
	StorageError string
}

// SkewPolicy is the broker disk-skew alert policy. SharePct is the share of
// cluster bytes one broker may hold before it counts as skew (needs >= 2
// brokers); MaxBytes is an absolute per-broker cap. Zero disables each arm.
type SkewPolicy struct {
	SharePct int
	MaxBytes int64
}

// confirmSamples is the hysteresis: a condition must hold across this many
// consecutive polls before it is treated as confirmed.
const confirmSamples = 2

// windowLen caps the in-memory growth window per topic. A short window keeps
// a single compaction dip from flipping growth alerts while staying cheap;
// long-term history belongs to Prometheus, not this process.
const windowLen = 8

type prevSample struct {
	bytes int64
	at    time.Time
}

// Poller polls Kafka log dirs on an interval and maintains hysteresis state.
type Poller struct {
	source   Source
	store    store.Store
	opts     kafka.DescribeOptions
	skew     SkewPolicy
	interval time.Duration
	log      *slog.Logger

	// now is injectable for tests.
	now func() time.Time

	mu          sync.RWMutex
	snap        Snapshot
	prev        map[string]prevSample
	windows     map[string][]prevSample
	streaks     map[string]int
	repStreaks  map[string]int
	growStreaks map[string]int
	skewStreak  int
	thresholds  map[string]store.ThresholdRow
}

// New builds a Poller. The snapshot is seeded from durable store state so a
// restart does not forget a confirmed over-threshold or the growth baseline.
func New(ctx context.Context, source Source, st store.Store, opts kafka.DescribeOptions, skew SkewPolicy, interval time.Duration, log *slog.Logger) *Poller {
	p := &Poller{
		source:      source,
		store:       st,
		opts:        opts,
		skew:        skew,
		interval:    interval,
		log:         log,
		now:         time.Now,
		snap:        Snapshot{PollOK: false, ControllerID: -1, SkewBrokerID: -1},
		prev:        map[string]prevSample{},
		windows:     map[string][]prevSample{},
		streaks:     map[string]int{},
		repStreaks:  map[string]int{},
		growStreaks: map[string]int{},
		thresholds:  map[string]store.ThresholdRow{},
	}
	if rows, err := st.States(ctx); err == nil {
		p.seedFromState(rows)
	} else {
		log.Warn("poller: could not seed state from store", "err", err)
	}
	if th, err := st.Thresholds(ctx); err == nil {
		p.thresholds = th
	} else {
		log.Warn("poller: could not seed thresholds from store", "err", err)
	}
	p.applyThresholdsLocked()
	return p
}

func (p *Poller) seedFromState(rows []store.StateRow) {
	for _, r := range rows {
		p.prev[r.Topic] = prevSample{bytes: r.StorageBytes, at: r.PolledAt}
		p.streaks[r.Topic] = r.OverStreak
		win := []prevSample{}
		if r.PrevPolledAt != nil && !r.PrevPolledAt.IsZero() {
			win = append(win, prevSample{bytes: r.PrevBytes, at: *r.PrevPolledAt})
		}
		win = append(win, prevSample{bytes: r.StorageBytes, at: r.PolledAt})
		p.windows[r.Topic] = win
		tv := topicViewFromState(r)
		p.snap.Topics = append(p.snap.Topics, tv)
		p.snap.TotalBytes += r.StorageBytes
		if r.PolledAt.After(p.snap.PolledAt) {
			p.snap.PolledAt = r.PolledAt
		}
	}
}

func topicViewFromState(r store.StateRow) TopicView {
	tv := TopicView{
		Name:          r.Topic,
		Partitions:    r.Partitions,
		StorageBytes:  r.StorageBytes,
		PrevBytes:     r.PrevBytes,
		OverStreak:    r.OverStreak,
		ConfirmedOver: r.ConfirmedOver,
		Stale:         true, // until a fresh sample proves otherwise
	}
	if r.PrevPolledAt != nil {
		tv.PrevPolledAt = *r.PrevPolledAt
	}
	return tv
}

// Run polls immediately, then every interval until ctx is done.
func (p *Poller) Run(ctx context.Context) {
	p.tick(ctx)
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

// Snapshot returns an immutable copy of the last observation. Callers may
// read it after this method returns; later polls and threshold refreshes do
// not mutate the returned value.
func (p *Poller) Snapshot() Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cloneSnapshot(p.snap)
}

// PollOnce runs one observation. Run calls it on each interval.
func (p *Poller) PollOnce(ctx context.Context) { p.tick(ctx) }

// StaleThreshold is how old a sample must be before the UI flags it stale.
func (p *Poller) StaleThreshold() time.Duration { return 3 * p.interval }

func (p *Poller) tick(ctx context.Context) {
	now := p.now()
	described, err := p.source.Describe(ctx, p.opts)
	if err != nil {
		p.mu.Lock()
		p.snap.PollOK = false
		p.snap.LastError = fmt.Sprintf("describe: %v", err)
		p.mu.Unlock()
		p.log.Warn("poller: describe failed; keeping last good sample", "err", err)
		return
	}

	// Thresholds are reloaded every tick so UI mutations become visible
	// without waiting for a restart or a rollback.
	if th, thErr := p.store.Thresholds(ctx); thErr == nil {
		p.mu.Lock()
		p.thresholds = th
		p.mu.Unlock()
	} else {
		p.log.Warn("poller: could not reload thresholds", "err", thErr)
	}

	p.mu.Lock()
	rows := p.observeLocked(described, now)
	p.mu.Unlock()

	if described.SizeIncomplete {
		p.log.Warn("poller: incomplete size observation; preserving alarms and growth baseline",
			"errors", described.BrokerErrors)
		return
	}
	if err := p.store.UpsertStates(ctx, rows); err != nil {
		// In-memory alert state already advanced. Durability is retried from
		// the next observation; this sample is not applied twice.
		p.mu.Lock()
		p.snap.StorageError = fmt.Sprintf("persist: %v", err)
		p.mu.Unlock()
		p.log.Error("poller: could not persist states", "err", err)
		return
	}
	p.mu.Lock()
	p.snap.StorageError = ""
	p.mu.Unlock()

	p.log.Debug("poller: tick complete", "topics", len(described.Topics),
		"total_bytes", described.TotalBytes, "brokers", len(described.Brokers))
}

// RefreshThresholds reloads the threshold map and recomputes the visible
// flags so a UI mutation is reflected without waiting for the next tick.
// Streaks are never incremented here: the 2-sample rule belongs to ticks,
// but a topic now under its (raised) limit clears immediately.
func (p *Poller) RefreshThresholds(ctx context.Context) {
	th, err := p.store.Thresholds(ctx)
	if err != nil {
		p.log.Warn("poller: refresh thresholds failed", "err", err)
		return
	}
	p.mu.Lock()
	p.thresholds = th
	p.mu.Unlock()
	p.applyThresholdsLocked()
}

// applyThresholdsLocked recomputes streak/confirmed display state only;
// callers must not hold the lock (it takes it itself).
func (p *Poller) applyThresholdsLocked() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.snap.Topics {
		tv := &p.snap.Topics[i]
		thr, ok := p.thresholds[tv.Name]
		if !ok {
			tv.ThresholdBytes = nil
			tv.WarnBytes = nil
			tv.OverStreak = 0
			tv.ConfirmedOver = false
			tv.GrowthThreshold = nil
			tv.GrowthStreak = 0
			tv.GrowthConfirmed = false
			continue
		}
		tv.ThresholdBytes = &thr.ThresholdBytes
		tv.WarnBytes = thr.WarnBytes
		if tv.StorageBytes < thr.ThresholdBytes {
			tv.OverStreak = 0
			tv.ConfirmedOver = false
		}
		tv.GrowthThreshold = thr.GrowthPerHour
		if tv.GrowthThreshold == nil || tv.GrowthPerHour < *tv.GrowthThreshold {
			tv.GrowthStreak = 0
			tv.GrowthConfirmed = false
		}
	}
}

// growthFromWindow derives the previous sample and the growth rate over the
// whole window. With only one sample there is no baseline yet.
func growthFromWindow(win []prevSample) (prevBytes int64, prevAt time.Time, perHour int64) {
	if len(win) < 2 {
		return 0, time.Time{}, 0
	}
	first, last := win[0], win[len(win)-1]
	prev := win[len(win)-2]
	if hours := last.at.Sub(first.at).Hours(); hours > 0 {
		perHour = int64(float64(last.bytes-first.bytes) / hours)
	}
	return prev.bytes, prev.at, perHour
}

// hoursToLimit forecasts how long the topic can keep growing at its current
// rate before it crosses its size threshold. Nil when there is no threshold,
// growth is flat or negative, or the topic is already over.
func hoursToLimit(threshold *int64, storage, growthPerHour int64) *int64 {
	if threshold == nil || growthPerHour <= 0 || storage >= *threshold {
		return nil
	}
	h := int64(math.Ceil(float64(*threshold-storage) / float64(growthPerHour)))
	return &h
}

// partitionSkew is max/avg over leader partition sizes: a cheap hot-key /
// bad-partitioner signal. 0 means unknown (fewer than 2 sized partitions).
func partitionSkew(parts []kafka.PartitionUse) float64 {
	var total, max int64
	n := 0
	for _, pu := range parts {
		if pu.SizeBytes <= 0 {
			continue
		}
		total += pu.SizeBytes
		if pu.SizeBytes > max {
			max = pu.SizeBytes
		}
		n++
	}
	if n < 2 || total == 0 {
		return 0
	}
	avg := float64(total) / float64(n)
	return float64(max) / avg
}

// evaluateSkew finds the hottest broker and reports whether it violates the
// policy. Share is measured against broker-reported bytes (which include
// topics filtered out of the UI), not against the visible total.
func evaluateSkew(brokers []kafka.BrokerUse, pol SkewPolicy) (hotID int32, sharePct float64, over bool) {
	if len(brokers) == 0 {
		return -1, 0, false
	}
	hot := brokers[0]
	var total int64
	for _, b := range brokers {
		total += b.Bytes
		if b.Bytes > hot.Bytes {
			hot = b
		}
	}
	if total <= 0 {
		return hot.ID, 0, false
	}
	sharePct = float64(hot.Bytes) / float64(total) * 100
	if pol.MaxBytes > 0 && hot.Bytes > pol.MaxBytes {
		over = true
	}
	if len(brokers) >= 2 && pol.SharePct > 0 && sharePct > float64(pol.SharePct) {
		over = true
	}
	return hot.ID, sharePct, over
}
