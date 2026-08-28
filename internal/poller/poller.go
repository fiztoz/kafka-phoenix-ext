// Package poller owns the Kafka poll loop and the in-memory snapshot the
// HTTP layer reads. Failed polls never wipe topics; they mark poll_ok=false
// and keep the last good sample.
package poller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

// Source is the subset of the Kafka client the poller needs.
type Source interface {
	Describe(ctx context.Context, o kafka.DescribeOptions) (*kafka.ClusterSnapshot, error)
}

// TopicView is one topic as shown to the HTTP layer, enriched with growth
// and threshold state.
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
	GrowthPerHour int64     // (storage-prev)/hours; 0 when no prev sample

	Stale          bool
	ThresholdBytes *int64 // operator-set limit
	WarnBytes      *int64 // optional; below ThresholdBytes
	OverStreak     int    // consecutive polls at/over threshold
	ConfirmedOver  bool   // OverStreak >= confirmSamples
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
}

// confirmSamples is the hysteresis: a threshold must hold across this many
// consecutive polls before it is treated as confirmed.
const confirmSamples = 2

type prevSample struct {
	bytes int64
	at    time.Time
}

// Poller polls Kafka log dirs on an interval and maintains hysteresis state.
type Poller struct {
	source   Source
	store    store.Store
	opts     kafka.DescribeOptions
	interval time.Duration
	log      *slog.Logger

	// now is injectable for tests.
	now func() time.Time

	mu         sync.RWMutex
	snap       Snapshot
	prev       map[string]prevSample
	streaks    map[string]int
	thresholds map[string]store.ThresholdRow
}

// New builds a Poller. The snapshot is seeded from durable store state so a
// restart does not forget a confirmed over-threshold or the growth baseline.
func New(ctx context.Context, source Source, st store.Store, opts kafka.DescribeOptions, interval time.Duration, log *slog.Logger) *Poller {
	p := &Poller{
		source:     source,
		store:      st,
		opts:       opts,
		interval:   interval,
		log:        log,
		now:        time.Now,
		snap:       Snapshot{PollOK: false, ControllerID: -1},
		prev:       map[string]prevSample{},
		streaks:    map[string]int{},
		thresholds: map[string]store.ThresholdRow{},
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
		tv := topicViewFromState(r)
		p.snap.Topics = append(p.snap.Topics, tv)
		p.snap.TotalBytes += r.StorageBytes
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

// Snapshot returns the last observation (safe for concurrent readers).
func (p *Poller) Snapshot() Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.snap
}

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
	rows := make([]store.StateRow, 0, len(described.Topics))
	for _, tu := range described.Topics {
		ps, hasPrev := p.prev[tu.Name]
		var prevAt *time.Time
		if hasPrev && !ps.at.IsZero() {
			prevAt = &ps.at
		}
		thr, hasThr := p.thresholds[tu.Name]
		streak := 0
		if hasThr && tu.StorageBytes >= thr.ThresholdBytes {
			streak = p.streaks[tu.Name] + 1
		}
		confirmed := streak >= confirmSamples
		rows = append(rows, store.StateRow{
			Topic:         tu.Name,
			Partitions:    tu.Partitions,
			StorageBytes:  tu.StorageBytes,
			PrevBytes:     ps.bytes,
			PolledAt:      now,
			PrevPolledAt:  prevAt,
			OverStreak:    streak,
			ConfirmedOver: confirmed,
		})
	}

	// Build the fresh snapshot from the described cluster plus hysteresis.
	p.snap = Snapshot{
		ClusterID:    described.ClusterID,
		ControllerID: described.ControllerID,
		PolledAt:     now,
		PollOK:       true,
		LastError:    "",
		TotalBytes:   described.TotalBytes,
		Brokers:      described.Brokers,
		BrokerErrors: described.BrokerErrors,
		Topics:       make([]TopicView, 0, len(described.Topics)),
	}
	for i := range described.Topics {
		tu := described.Topics[i]
		tv := TopicView{
			Name:         tu.Name,
			Partitions:   tu.Partitions,
			Replicas:     tu.Replicas,
			StorageBytes: tu.StorageBytes,
			LeaderBytes:  tu.LeaderBytes,
			IsInternal:   tu.IsInternal,
			UnderRep:     tu.UnderRep,
			Offline:      tu.Offline,
			Parts:        tu.Parts,
		}
		if ps, ok := p.prev[tu.Name]; ok {
			tv.PrevBytes = ps.bytes
			if !ps.at.IsZero() {
				tv.PrevPolledAt = ps.at
				if hours := now.Sub(ps.at).Hours(); hours > 0 {
					tv.GrowthPerHour = int64(float64(tu.StorageBytes-ps.bytes) / hours)
				}
			}
		}
		if thr, ok := p.thresholds[tu.Name]; ok {
			tv.ThresholdBytes = &thr.ThresholdBytes
			tv.WarnBytes = thr.WarnBytes
			tv.OverStreak = rows[i].OverStreak
			tv.ConfirmedOver = rows[i].ConfirmedOver
		}
		p.snap.Topics = append(p.snap.Topics, tv)
	}
	p.mu.Unlock()

	if err := p.store.UpsertStates(ctx, rows); err != nil {
		// Data is still fresh; only the durable state is a casualty. Do not
		// advance prev/streaks so the next tick retries with the same
		// baseline and hysteresis counters.
		p.log.Error("poller: could not persist states", "err", err)
		return
	}
	p.mu.Lock()
	for _, r := range rows {
		p.prev[r.Topic] = prevSample{bytes: r.StorageBytes, at: r.PolledAt}
		p.streaks[r.Topic] = r.OverStreak
	}
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
			continue
		}
		tv.ThresholdBytes = &thr.ThresholdBytes
		tv.WarnBytes = thr.WarnBytes
		if tv.StorageBytes < thr.ThresholdBytes {
			tv.OverStreak = 0
			tv.ConfirmedOver = false
		}
	}
}
