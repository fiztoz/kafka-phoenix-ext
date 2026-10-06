package http

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/poller"
	"github.com/twmb/franz-go/pkg/kadm"
)

type aggSource struct {
	snap *kafka.ClusterSnapshot
}

func (a aggSource) Describe(context.Context, kafka.DescribeOptions) (*kafka.ClusterSnapshot, error) {
	return a.snap, nil
}

func TestMetadataOnlyOfflineReplicasConfirmedByHandler(t *testing.T) {
	meta := kadm.Metadata{Topics: kadm.TopicDetails{
		"orders": {
			Topic: "orders",
			Partitions: kadm.PartitionDetails{
				0: {
					Topic: "orders", Partition: 0, Leader: -1,
					Replicas: []int32{0, 1}, ISR: []int32{}, OfflineReplicas: []int32{1},
				},
			},
		},
	}}
	described := kafka.Aggregate(kadm.DescribedAllLogDirs{
		0: {"/data": {Broker: 0, Dir: "/data"}},
	}, &meta, kafka.DescribeOptions{})
	if described.Topics[0].Partitions != 1 || len(described.Topics[0].Parts) != 1 || described.Topics[0].Offline != 1 {
		t.Fatalf("aggregate dropped metadata-only fault: %+v", described.Topics[0])
	}

	st := newFakeStore()
	p := poller.New(context.Background(), aggSource{snap: described}, st, kafka.DescribeOptions{}, poller.SkewPolicy{}, time.Minute, slog.New(slog.DiscardHandler))
	for i := 0; i < 3; i++ {
		p.PollOnce(context.Background())
	}
	snap := p.Snapshot()
	if len(snap.Topics) != 1 || len(snap.Topics[0].Parts) != 1 || !snap.Topics[0].ReplicaConfirmed {
		t.Fatalf("poller snapshot: %+v", snap.Topics)
	}

	srv, err := New(Deps{
		BasePath:  "/kafka",
		Snapshots: p,
		Store:     st,
		Log:       slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	w := do(t, srv.Handler(), http.MethodGet, "/kafka/health/replicas", nil, nil)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "orders") {
		t.Fatalf("replicas health = %d %q", w.Code, w.Body.String())
	}
}

func TestMissingMetadataIsNotHealthyReplicaObservation(t *testing.T) {
	snap := sampleSnapshot()
	snap.MetadataUnavailable = true
	snap.Topics[0].ReplicaConfirmed = false
	srv, _, _ := newTestServer(t, snap, "")
	w := do(t, srv.Handler(), http.MethodGet, "/kafka/health/replicas", nil, nil)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "unavailable") {
		t.Fatalf("missing metadata looked healthy: %d %q", w.Code, w.Body.String())
	}
}
