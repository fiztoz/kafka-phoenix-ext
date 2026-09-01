package poller

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

type fakeSource struct {
	mu   sync.Mutex
	next *kafka.ClusterSnapshot
	err  error
}

func (f *fakeSource) set(s *kafka.ClusterSnapshot) { f.mu.Lock(); f.next = s; f.mu.Unlock() }
func (f *fakeSource) fail(err error)               { f.mu.Lock(); f.err = err; f.mu.Unlock() }
func (f *fakeSource) Describe(context.Context, kafka.DescribeOptions) (*kafka.ClusterSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.next, nil
}

type fakeStore struct {
	mu         sync.Mutex
	states     map[string]store.StateRow
	thresholds map[string]store.ThresholdRow
	failWrites bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		states:     map[string]store.StateRow{},
		thresholds: map[string]store.ThresholdRow{},
	}
}

func (f *fakeStore) UpsertStates(_ context.Context, rows []store.StateRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWrites {
		return errors.New("store down")
	}
	for _, r := range rows {
		f.states[r.Topic] = r
	}
	return nil
}

func (f *fakeStore) States(context.Context) ([]store.StateRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.StateRow
	for _, r := range f.states {
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeStore) SetThreshold(_ context.Context, topic string, limit int64, warn, growth *int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thresholds[topic] = store.ThresholdRow{Topic: topic, ThresholdBytes: limit, WarnBytes: warn, GrowthPerHour: growth, UpdatedAt: time.Now()}
	return nil
}

func (f *fakeStore) DeleteThreshold(_ context.Context, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.thresholds, topic)
	return nil
}

func (f *fakeStore) Thresholds(context.Context) (map[string]store.ThresholdRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]store.ThresholdRow, len(f.thresholds))
	for k, v := range f.thresholds {
		out[k] = v
	}
	return out, nil
}

func (f *fakeStore) setThreshold(topic string, limit int64) error {
	return f.SetThreshold(context.Background(), topic, limit, nil, nil)
}

func (f *fakeStore) setGrowthThreshold(topic string, limit int64, growth int64) error {
	return f.SetThreshold(context.Background(), topic, limit, nil, &growth)
}

func (f *fakeStore) Migrate(context.Context) error { return nil }
func (f *fakeStore) Close() error                  { return nil }

func discardLog() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func cluster(topic string, bytes int64) *kafka.ClusterSnapshot {
	return &kafka.ClusterSnapshot{
		ClusterID:    "c1",
		ControllerID: 1,
		Brokers:      []kafka.BrokerUse{{ID: 1, Host: "kafka-1", Bytes: bytes, Partitions: 1}},
		Topics: []kafka.TopicUse{
			{
				Name: topic, Partitions: 1, Replicas: 3,
				StorageBytes: bytes, LeaderBytes: bytes / 3,
				Parts: []kafka.PartitionUse{{Partition: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}, SizeBytes: bytes / 3}},
			},
		},
		TotalBytes: bytes,
	}
}

func newTestPoller(ctx context.Context, t *testing.T, src *fakeSource, st *fakeStore) *Poller {
	t.Helper()
	p := New(ctx, src, st, kafka.DescribeOptions{}, SkewPolicy{SharePct: 60}, time.Minute, discardLog())
	p.now = func() time.Time { return testNow }
	return p
}

var testNow = time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)

func TestTickSnapshotsTopicsAndGrowth(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	p := newTestPoller(ctx, t, src, st)

	src.set(cluster("orders", 1000))
	p.tick(ctx)
	snap := p.Snapshot()
	if !snap.PollOK || len(snap.Topics) != 1 || snap.Topics[0].StorageBytes != 1000 {
		t.Fatalf("first tick: %+v", snap)
	}
	if snap.ClusterID != "c1" || snap.ControllerID != 1 {
		t.Fatalf("cluster meta: %+v", snap)
	}

	// 6 hours later the topic grew by 1200 bytes → 200/hour.
	testNow = testNow.Add(6 * time.Hour)
	src.set(cluster("orders", 2200))
	p.tick(ctx)
	tv := p.Snapshot().Topics[0]
	if tv.GrowthPerHour != 200 {
		t.Errorf("growth/hour = %d, want 200", tv.GrowthPerHour)
	}
	if tv.PrevBytes != 1000 || tv.PrevPolledAt.IsZero() {
		t.Errorf("prev sample: %+v", tv)
	}

	// Durable state records the baseline for a restart.
	if st.states["orders"].PrevBytes != 1000 {
		t.Errorf("store prev_bytes = %d, want 1000", st.states["orders"].PrevBytes)
	}
}

func TestTwoSampleHysteresis(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	if err := st.setThreshold("orders", 1500); err != nil {
		t.Fatal(err)
	}
	p := newTestPoller(ctx, t, src, st)

	src.set(cluster("orders", 2000))
	p.tick(ctx) // sample 1 over: streak 1, not confirmed
	tv := p.Snapshot().Topics[0]
	if tv.OverStreak != 1 || tv.ConfirmedOver {
		t.Fatalf("tick1: streak=%d confirmed=%v", tv.OverStreak, tv.ConfirmedOver)
	}

	p.tick(ctx) // sample 2 over: confirmed
	tv = p.Snapshot().Topics[0]
	if tv.OverStreak != 2 || !tv.ConfirmedOver {
		t.Fatalf("tick2: streak=%d confirmed=%v", tv.OverStreak, tv.ConfirmedOver)
	}

	src.set(cluster("orders", 1000))
	p.tick(ctx) // recovered: reset
	tv = p.Snapshot().Topics[0]
	if tv.OverStreak != 0 || tv.ConfirmedOver {
		t.Fatalf("tick3: streak=%d confirmed=%v", tv.OverStreak, tv.ConfirmedOver)
	}
}

func TestDescribeFailureKeepsLastGood(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	p := newTestPoller(ctx, t, src, st)

	src.set(cluster("orders", 1000))
	p.tick(ctx)

	src.fail(errors.New("cluster unreachable"))
	p.tick(ctx)
	snap := p.Snapshot()
	if snap.PollOK {
		t.Fatal("PollOK must be false after a failed describe")
	}
	if snap.LastError == "" {
		t.Fatal("LastError must be set")
	}
	if len(snap.Topics) != 1 || snap.Topics[0].StorageBytes != 1000 {
		t.Fatalf("last good topics lost: %+v", snap.Topics)
	}
}

func TestRefreshThresholdsRecomputesWithoutTick(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	if err := st.setThreshold("orders", 1500); err != nil {
		t.Fatal(err)
	}
	p := newTestPoller(ctx, t, src, st)

	src.set(cluster("orders", 2000))
	p.tick(ctx)
	p.tick(ctx)
	if !p.Snapshot().Topics[0].ConfirmedOver {
		t.Fatal("expected confirmed over-threshold after two ticks")
	}

	// Operator raises the limit above current usage: clears immediately,
	// without waiting for the next poll.
	if err := st.setThreshold("orders", 3000); err != nil {
		t.Fatal(err)
	}
	p.RefreshThresholds(ctx)
	tv := p.Snapshot().Topics[0]
	if tv.ConfirmedOver || tv.OverStreak != 0 {
		t.Fatalf("after refresh: streak=%d confirmed=%v", tv.OverStreak, tv.ConfirmedOver)
	}
	if tv.ThresholdBytes == nil || *tv.ThresholdBytes != 3000 {
		t.Fatalf("threshold not updated: %+v", tv)
	}
}

func TestSeedsFromStore(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	past := testNow.Add(-time.Hour)
	st.states["ghost"] = store.StateRow{
		Topic: "ghost", Partitions: 2, StorageBytes: 500, PrevBytes: 400,
		PolledAt: past, PrevPolledAt: &past, OverStreak: 2, ConfirmedOver: true,
	}
	// The threshold that the streak was earned against must still exist,
	// otherwise the seed is correctly discarded as stale.
	st.thresholds["ghost"] = store.ThresholdRow{Topic: "ghost", ThresholdBytes: 400, UpdatedAt: past}
	p := newTestPoller(ctx, t, src, st)
	snap := p.Snapshot()
	if len(snap.Topics) != 1 || !snap.Topics[0].Stale {
		t.Fatalf("seed from store: %+v", snap.Topics)
	}
	if !snap.Topics[0].ConfirmedOver {
		t.Fatal("confirmed_over lost across restart")
	}
}

func clusterUnderRep(topic string, bytes int64) *kafka.ClusterSnapshot {
	c := cluster(topic, bytes)
	c.Topics[0].UnderRep = 1
	c.Topics[0].Parts[0].UnderRep = true
	c.Topics[0].Parts[0].ISR = []int32{1}
	return c
}

func TestReplicaHysteresis(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	p := newTestPoller(ctx, t, src, st)

	src.set(clusterUnderRep("orders", 1000))
	p.tick(ctx)
	tv := p.Snapshot().Topics[0]
	if tv.RepStreak != 1 || tv.ReplicaConfirmed {
		t.Fatalf("tick1: streak=%d confirmed=%v", tv.RepStreak, tv.ReplicaConfirmed)
	}

	p.tick(ctx)
	tv = p.Snapshot().Topics[0]
	if tv.RepStreak != 2 || !tv.ReplicaConfirmed {
		t.Fatalf("tick2: streak=%d confirmed=%v", tv.RepStreak, tv.ReplicaConfirmed)
	}

	src.set(cluster("orders", 1000))
	p.tick(ctx)
	tv = p.Snapshot().Topics[0]
	if tv.RepStreak != 0 || tv.ReplicaConfirmed {
		t.Fatalf("tick3: streak=%d confirmed=%v", tv.RepStreak, tv.ReplicaConfirmed)
	}
}

func TestGrowthThresholdHysteresisAndWindow(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	if err := st.setGrowthThreshold("orders", 1_000_000, 150); err != nil {
		t.Fatal(err)
	}
	p := newTestPoller(ctx, t, src, st)

	src.set(cluster("orders", 1000))
	p.tick(ctx) // one sample: no growth yet
	tv := p.Snapshot().Topics[0]
	if tv.GrowthPerHour != 0 || tv.GrowthStreak != 0 || tv.GrowthConfirmed {
		t.Fatalf("tick1: %+v", tv)
	}
	if tv.GrowthThreshold == nil || *tv.GrowthThreshold != 150 {
		t.Fatalf("growth threshold not surfaced: %+v", tv.GrowthThreshold)
	}

	testNow = testNow.Add(time.Hour)
	src.set(cluster("orders", 1200))
	p.tick(ctx) // growth 200/hr >= 150: streak 1, not confirmed
	tv = p.Snapshot().Topics[0]
	if tv.GrowthPerHour != 200 || tv.GrowthStreak != 1 || tv.GrowthConfirmed {
		t.Fatalf("tick2: growth=%d streak=%d confirmed=%v", tv.GrowthPerHour, tv.GrowthStreak, tv.GrowthConfirmed)
	}

	testNow = testNow.Add(time.Hour)
	src.set(cluster("orders", 1500))
	p.tick(ctx) // window growth (1500-1000)/2h = 250: confirmed
	tv = p.Snapshot().Topics[0]
	if tv.GrowthPerHour != 250 || !tv.GrowthConfirmed {
		t.Fatalf("tick3: growth=%d confirmed=%v", tv.GrowthPerHour, tv.GrowthConfirmed)
	}

	testNow = testNow.Add(time.Hour)
	src.set(cluster("orders", 1300))
	p.tick(ctx) // dip: window growth (1300-1000)/3h = 100 < 150: clears
	tv = p.Snapshot().Topics[0]
	if tv.GrowthPerHour != 100 || tv.GrowthConfirmed || tv.GrowthStreak != 0 {
		t.Fatalf("tick4: growth=%d streak=%d confirmed=%v", tv.GrowthPerHour, tv.GrowthStreak, tv.GrowthConfirmed)
	}
}

func TestHoursToLimitForecast(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	if err := st.setThreshold("orders", 10000); err != nil {
		t.Fatal(err)
	}
	p := newTestPoller(ctx, t, src, st)

	src.set(cluster("orders", 2000))
	p.tick(ctx)
	testNow = testNow.Add(time.Hour)
	src.set(cluster("orders", 4000))
	p.tick(ctx) // growth 2000/hr, 6000 bytes of headroom: 3h to limit
	tv := p.Snapshot().Topics[0]
	if tv.HoursToLimit == nil || *tv.HoursToLimit != 3 {
		t.Fatalf("hours to limit: %+v", tv.HoursToLimit)
	}

	// Negative growth hides the forecast instead of extrapolating backwards.
	testNow = testNow.Add(time.Hour)
	src.set(cluster("orders", 1000))
	p.tick(ctx)
	tv = p.Snapshot().Topics[0]
	if tv.HoursToLimit != nil {
		t.Fatalf("negative growth should hide the forecast, got %+v", *tv.HoursToLimit)
	}
}

func TestPartitionSkew(t *testing.T) {
	c := cluster("orders", 1200)
	c.Topics[0].Parts = []kafka.PartitionUse{
		{Partition: 0, Leader: 1, SizeBytes: 900},
		{Partition: 1, Leader: 1, SizeBytes: 100},
		{Partition: 2, Leader: 1, SizeBytes: 200},
	}
	if got := partitionSkew(c.Topics[0].Parts); got < 2.24 || got > 2.26 {
		t.Fatalf("partition skew = %f, want ~2.25 (900/400)", got)
	}
	if got := partitionSkew(c.Topics[0].Parts[:1]); got != 0 {
		t.Fatalf("single partition skew = %f, want 0", got)
	}
}

func clusterSkewed() *kafka.ClusterSnapshot {
	return &kafka.ClusterSnapshot{
		ClusterID:    "c1",
		ControllerID: 1,
		Brokers: []kafka.BrokerUse{
			{ID: 1, Host: "kafka-1", Bytes: 900, Partitions: 3},
			{ID: 2, Host: "kafka-2", Bytes: 100, Partitions: 3},
		},
		Topics: []kafka.TopicUse{
			{
				Name: "orders", Partitions: 1, Replicas: 2,
				StorageBytes: 1000, LeaderBytes: 500,
				Parts: []kafka.PartitionUse{{Partition: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, SizeBytes: 500}},
			},
		},
		TotalBytes: 1000,
	}
}

func TestBrokerSkewHysteresis(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	p := newTestPoller(ctx, t, src, st) // policy: 60% share

	src.set(clusterSkewed())
	p.tick(ctx)
	snap := p.Snapshot()
	if snap.SkewStreak != 1 || snap.SkewConfirmed {
		t.Fatalf("tick1: streak=%d confirmed=%v", snap.SkewStreak, snap.SkewConfirmed)
	}
	if snap.SkewBrokerID != 1 || snap.SkewSharePct < 89.9 || snap.SkewSharePct > 90.1 {
		t.Fatalf("hot broker: id=%d share=%.1f", snap.SkewBrokerID, snap.SkewSharePct)
	}

	p.tick(ctx)
	if snap = p.Snapshot(); !snap.SkewConfirmed {
		t.Fatalf("tick2: skew should be confirmed, streak=%d", snap.SkewStreak)
	}

	// Balanced cluster clears it.
	balanced := clusterSkewed()
	balanced.Brokers[0].Bytes = 500
	balanced.Brokers[1].Bytes = 500
	src.set(balanced)
	p.tick(ctx)
	if snap = p.Snapshot(); snap.SkewConfirmed || snap.SkewStreak != 0 {
		t.Fatalf("tick3: streak=%d confirmed=%v", snap.SkewStreak, snap.SkewConfirmed)
	}
}
