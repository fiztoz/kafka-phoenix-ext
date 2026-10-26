package poller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
)

// newManagerPoller builds one poller for cluster id over its own source and
// store, so a test can give each cluster a different observation.
func newManagerPoller(t *testing.T, id string, bytes int64) (*Poller, *fakeSource) {
	t.Helper()
	src := &fakeSource{}
	src.set(cluster(t.Name(), bytes))
	p := New(context.Background(), id, src, newFakeStore(),
		kafka.DescribeOptions{}, SkewPolicy{SharePct: 60}, time.Minute, discardLog())
	p.now = func() time.Time { return testNow }
	return p, src
}

func TestManagerKeepsClustersSeparate(t *testing.T) {
	harbor, _ := newManagerPoller(t, "harbor", 1000)
	summit, _ := newManagerPoller(t, "summit", 9000)

	m := NewManager(time.Minute, harbor, summit)
	if got := m.Clusters(); len(got) != 2 || got[0] != "harbor" || got[1] != "summit" {
		t.Fatalf("clusters = %v, want [harbor summit] in declaration order", got)
	}
	if m.Len() != 2 || !m.Has("harbor") || m.Has("nope") {
		t.Fatal("Has/Len disagree with the configured set")
	}

	m.PollOnce(context.Background())

	harborSnap, ok := m.Snapshot("harbor")
	if !ok {
		t.Fatal("harbor snapshot missing")
	}
	summitSnap, ok := m.Snapshot("summit")
	if !ok {
		t.Fatal("summit snapshot missing")
	}
	if harborSnap.Cluster != "harbor" || summitSnap.Cluster != "summit" {
		t.Fatalf("snapshot cluster ids = %q/%q", harborSnap.Cluster, summitSnap.Cluster)
	}
	if harborSnap.TotalBytes != 1000 || summitSnap.TotalBytes != 9000 {
		t.Fatalf("byte totals crossed clusters: harbor=%d summit=%d",
			harborSnap.TotalBytes, summitSnap.TotalBytes)
	}
	if !harborSnap.PollOK || !summitSnap.PollOK {
		t.Fatalf("both clusters should have polled: harbor=%v summit=%v", harborSnap.PollOK, summitSnap.PollOK)
	}
	if _, ok := m.Snapshot("staging"); ok {
		t.Fatal("unknown cluster must report false")
	}
}

// One cluster failing must never damage the other cluster's snapshot: the
// two Kafka clusters are independent dependencies.
func TestManagerIsolatesClusterFailure(t *testing.T) {
	harbor, _ := newManagerPoller(t, "harbor", 1000)
	summit, _ := newManagerPoller(t, "summit", 9000)

	m := NewManager(time.Minute, harbor, summit)
	m.PollOnce(context.Background())
	before, _ := m.Snapshot("summit")

	harborSrcErr := errors.New("describe failed")
	if p := m.byID["harbor"]; p != nil {
		p.source.(*fakeSource).fail(harborSrcErr)
	}
	m.PollOnce(context.Background())

	harborSnap, _ := m.Snapshot("harbor")
	summitSnap, _ := m.Snapshot("summit")
	if harborSnap.PollOK {
		t.Fatal("harbor should be marked failed")
	}
	if summitSnap.PollOK != before.PollOK || summitSnap.TotalBytes != before.TotalBytes {
		t.Fatal("summit snapshot changed because harbor failed")
	}
	if summitSnap.LastError != "" {
		t.Fatalf("summit inherited harbor's error: %q", summitSnap.LastError)
	}
}

func TestManagerSnapshotsAreOrdered(t *testing.T) {
	harbor, _ := newManagerPoller(t, "harbor", 10)
	summit, _ := newManagerPoller(t, "summit", 20)
	m := NewManager(2*time.Minute, harbor, summit)
	m.PollOnce(context.Background())

	snaps := m.Snapshots()
	if len(snaps) != 2 || snaps[0].Cluster != "harbor" || snaps[1].Cluster != "summit" {
		t.Fatalf("snapshots = %+v", snaps)
	}
	if m.StaleThreshold() != 6*time.Minute {
		t.Fatalf("stale threshold = %s, want 6m", m.StaleThreshold())
	}
}

func TestManagerRunStopsOnContextCancel(t *testing.T) {
	harbor, _ := newManagerPoller(t, "harbor", 10)
	m := NewManager(time.Minute, harbor)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Run(ctx)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
