package poller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
)

func TestIncompleteSizeDoesNotClearAlarmOrBaseline(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	if err := st.setThreshold("orders", 1500); err != nil {
		t.Fatal(err)
	}
	testNow = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	p := newTestPoller(ctx, t, src, st)

	src.set(cluster("orders", 2000))
	p.tick(ctx)
	testNow = testNow.Add(time.Hour)
	p.tick(ctx)
	if tv := p.Snapshot().Topics[0]; !tv.ConfirmedOver || tv.StorageBytes != 2000 {
		t.Fatalf("complete confirmation: %+v", tv)
	}
	if st.states["orders"].StorageBytes != 2000 {
		t.Fatalf("baseline = %d", st.states["orders"].StorageBytes)
	}

	partial := cluster("orders", 1000)
	partial.SizeIncomplete = true
	partial.TotalBytes = 1000
	partial.BrokerErrors = []string{"broker 1: connection refused"}
	src.set(partial)
	testNow = testNow.Add(time.Hour)
	p.tick(ctx)

	snap := p.Snapshot()
	if !snap.PollOK || !snap.SizeIncomplete {
		t.Fatalf("partial poll: ok=%v incomplete=%v err=%q", snap.PollOK, snap.SizeIncomplete, snap.LastError)
	}
	if len(snap.Topics) != 1 || !snap.Topics[0].ConfirmedOver || snap.Topics[0].StorageBytes != 1000 {
		t.Fatalf("partial display: %+v", snap.Topics)
	}
	if st.states["orders"].StorageBytes != 2000 || st.states["orders"].PrevBytes != 2000 {
		t.Fatalf("partial poll replaced baseline: %+v", st.states["orders"])
	}

	testNow = testNow.Add(time.Hour)
	src.set(cluster("orders", 2000))
	p.tick(ctx)
	tv := p.Snapshot().Topics[0]
	if tv.GrowthPerHour != 0 {
		t.Fatalf("recovery fabricated growth %d/h", tv.GrowthPerHour)
	}
	if !tv.ConfirmedOver || st.states["orders"].StorageBytes != 2000 || st.states["orders"].PrevBytes != 2000 {
		t.Fatalf("after recovery: view=%+v store=%+v", tv, st.states["orders"])
	}
}

func TestMetadataUnavailableDoesNotClearReplicaAlarm(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	testNow = time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	p := newTestPoller(ctx, t, src, st)

	src.set(clusterUnderRep("orders", 1000))
	p.tick(ctx)
	p.tick(ctx)
	if !p.Snapshot().Topics[0].ReplicaConfirmed {
		t.Fatal("expected confirmed replica fault")
	}

	unknown := cluster("orders", 1000)
	unknown.MetadataUnavailable = true
	unknown.Topics[0].UnderRep = 0
	unknown.Topics[0].Offline = 0
	unknown.Topics[0].ReplicaUnavailable = true
	src.set(unknown)
	p.tick(ctx)
	tv := p.Snapshot().Topics[0]
	if !tv.ReplicaConfirmed || !p.Snapshot().MetadataUnavailable {
		t.Fatalf("unknown metadata cleared replica alarm: %+v", tv)
	}

	src.set(cluster("orders", 1000))
	p.tick(ctx)
	tv = p.Snapshot().Topics[0]
	if tv.ReplicaConfirmed || tv.RepStreak != 0 || p.Snapshot().MetadataUnavailable {
		t.Fatalf("healthy metadata did not clear: %+v unavailable=%v", tv, p.Snapshot().MetadataUnavailable)
	}
}

func TestPersistFailureStillConfirmsAlerts(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	if err := st.setGrowthThreshold("orders", 1500, 1); err != nil {
		t.Fatal(err)
	}
	testNow = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	p := newTestPoller(ctx, t, src, st)
	st.failWrites = true

	for i := 0; i < 5; i++ {
		obs := clusterUnderRep("orders", int64(2000+i*1000))
		obs.Brokers = []kafka.BrokerUse{
			{ID: 1, Host: "kafka-1", Bytes: 900, Partitions: 1},
			{ID: 2, Host: "kafka-2", Bytes: 100, Partitions: 1},
		}
		src.set(obs)
		p.tick(ctx)
		testNow = testNow.Add(time.Hour)
	}
	snap := p.Snapshot()
	tv := snap.Topics[0]
	if !snap.PollOK || snap.StorageError == "" {
		t.Fatalf("persist error not visible: ok=%v storage=%q last=%q", snap.PollOK, snap.StorageError, snap.LastError)
	}
	if tv.OverStreak != 5 || !tv.ConfirmedOver {
		t.Fatalf("capacity streak = %d confirmed=%v", tv.OverStreak, tv.ConfirmedOver)
	}
	if tv.RepStreak != 5 || !tv.ReplicaConfirmed {
		t.Fatalf("replica streak = %d confirmed=%v", tv.RepStreak, tv.ReplicaConfirmed)
	}
	if !tv.GrowthConfirmed || tv.GrowthStreak < 2 {
		t.Fatalf("growth streak = %d confirmed=%v rate=%d", tv.GrowthStreak, tv.GrowthConfirmed, tv.GrowthPerHour)
	}
	if snap.SkewStreak != 5 || !snap.SkewConfirmed {
		t.Fatalf("skew streak = %d confirmed=%v", snap.SkewStreak, snap.SkewConfirmed)
	}
	if len(st.states) != 0 {
		t.Fatalf("failed writes were stored: %+v", st.states)
	}

	st.failWrites = false
	src.set(clusterUnderRep("orders", 8000))
	p.tick(ctx)
	snap = p.Snapshot()
	if snap.StorageError != "" {
		t.Fatalf("storage error stuck after recovery: %s", snap.StorageError)
	}
	if snap.Topics[0].OverStreak != 6 {
		t.Fatalf("recovery duplicated or reset streak: %d", snap.Topics[0].OverStreak)
	}
	if len(st.states) != 1 || st.states["orders"].OverStreak != 6 {
		t.Fatalf("durable state = %+v", st.states)
	}
}

func TestSnapshotUnchangedAfterRefresh(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	if err := st.setThreshold("orders", 500); err != nil {
		t.Fatal(err)
	}
	testNow = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	p := newTestPoller(ctx, t, src, st)
	src.set(cluster("orders", 2000))
	p.tick(ctx)
	p.tick(ctx)
	before := p.Snapshot()
	if !before.Topics[0].ConfirmedOver || before.Topics[0].ThresholdBytes == nil || *before.Topics[0].ThresholdBytes != 500 {
		t.Fatalf("setup: %+v", before.Topics[0])
	}

	if err := st.setThreshold("orders", 3000); err != nil {
		t.Fatal(err)
	}
	p.RefreshThresholds(ctx)
	if !before.Topics[0].ConfirmedOver || *before.Topics[0].ThresholdBytes != 500 {
		t.Fatalf("returned snapshot mutated: confirmed=%v threshold=%v", before.Topics[0].ConfirmedOver, before.Topics[0].ThresholdBytes)
	}
	after := p.Snapshot()
	if after.Topics[0].ConfirmedOver || after.Topics[0].ThresholdBytes == nil || *after.Topics[0].ThresholdBytes != 3000 {
		t.Fatalf("live snapshot not refreshed: %+v", after.Topics[0])
	}
}

func TestSnapshotRefreshRace(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	if err := st.setThreshold("orders", 500); err != nil {
		t.Fatal(err)
	}
	testNow = time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	p := newTestPoller(ctx, t, src, st)
	src.set(cluster("orders", 2000))
	p.tick(ctx)
	p.tick(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	stop := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap := p.Snapshot()
			if len(snap.Topics) == 0 || snap.Topics[0].ThresholdBytes == nil {
				continue
			}
			_ = snap.Topics[0].ConfirmedOver
			_ = *snap.Topics[0].ThresholdBytes
			_ = snap.Topics[0].Parts
		}
	}()
	for i := 0; i < 200; i++ {
		limit := int64(500 + i)
		if err := st.setThreshold("orders", limit); err != nil {
			close(stop)
			wg.Wait()
			t.Fatal(err)
		}
		p.RefreshThresholds(ctx)
	}
	close(stop)
	wg.Wait()
}

func TestDescribeErrorDoesNotPublishZeroByteSuccess(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{}
	st := newFakeStore()
	if err := st.setThreshold("orders", 1500); err != nil {
		t.Fatal(err)
	}
	testNow = time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	p := newTestPoller(ctx, t, src, st)
	src.set(cluster("orders", 2000))
	p.tick(ctx)
	p.tick(ctx)
	src.fail(errors.New("kafka: describe log dirs: no usable directory data"))
	p.tick(ctx)
	snap := p.Snapshot()
	if snap.PollOK || snap.LastError == "" {
		t.Fatalf("all-directory failure looked successful: %+v", snap)
	}
	if len(snap.Topics) != 1 || snap.Topics[0].StorageBytes != 2000 || !snap.Topics[0].ConfirmedOver {
		t.Fatalf("last complete sample lost: %+v", snap.Topics)
	}
}
