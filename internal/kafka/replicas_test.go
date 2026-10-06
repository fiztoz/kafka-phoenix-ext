package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestAggregateMetadataOnlyOfflinePartition(t *testing.T) {
	meta := kadm.Metadata{
		Cluster:    "c",
		Controller: 0,
		Topics: kadm.TopicDetails{
			"orders": {
				Topic: "orders",
				Partitions: kadm.PartitionDetails{
					0: {
						Topic: "orders", Partition: 0, Leader: -1,
						Replicas: []int32{0, 1}, ISR: nil, OfflineReplicas: []int32{0, 1},
					},
				},
			},
		},
	}
	snap := Aggregate(kadm.DescribedAllLogDirs{}, &meta, DescribeOptions{})
	if len(snap.Topics) != 1 {
		t.Fatalf("topics = %+v", snap.Topics)
	}
	orders := snap.Topics[0]
	if orders.Partitions != 1 || len(orders.Parts) != 1 {
		t.Fatalf("metadata-only partition dropped: partitions=%d parts=%d", orders.Partitions, len(orders.Parts))
	}
	if orders.UnderRep != 1 || orders.Offline != 1 {
		t.Fatalf("underrep/offline = %d/%d, want 1/1", orders.UnderRep, orders.Offline)
	}
	part := orders.Parts[0]
	if part.SizeKnown || !orders.SizeIncomplete {
		t.Fatal("metadata-only partition must not look like a measured empty log")
	}
	if !part.ReplicaKnown || !part.Offline || !part.UnderRep {
		t.Fatalf("partition = %+v", part)
	}
}

func TestAggregateLeaderlessEmptyISRIsUnhealthy(t *testing.T) {
	described := kadm.DescribedAllLogDirs{
		0: {
			"/data": {
				Broker: 0, Dir: "/data",
				Topics: kadm.DescribedLogDirTopics{
					"orders": {0: {Broker: 0, Dir: "/data", Topic: "orders", Partition: 0, Size: 1000}},
				},
			},
		},
	}
	meta := kadm.Metadata{Topics: kadm.TopicDetails{
		"orders": {
			Topic: "orders",
			Partitions: kadm.PartitionDetails{
				0: {Topic: "orders", Partition: 0, Leader: -1, Replicas: []int32{0, 1}, ISR: []int32{}},
			},
		},
	}}
	snap := Aggregate(described, &meta, DescribeOptions{})
	if len(snap.Topics) != 1 || len(snap.Topics[0].Parts) != 1 {
		t.Fatalf("snapshot = %+v", snap.Topics)
	}
	part := snap.Topics[0].Parts[0]
	if !part.Offline || !part.UnderRep {
		t.Fatalf("leaderless empty ISR treated as healthy: %+v", part)
	}
	if snap.Topics[0].Offline != 1 || snap.Topics[0].UnderRep != 1 {
		t.Fatalf("topic faults = under %d offline %d", snap.Topics[0].UnderRep, snap.Topics[0].Offline)
	}
}

func TestAggregateMissingMetadataIsUnavailable(t *testing.T) {
	described, _ := fixture()
	snap := Aggregate(described, nil, DescribeOptions{})
	if !snap.MetadataUnavailable {
		t.Fatal("nil metadata must be explicitly unavailable")
	}
	for _, topic := range snap.Topics {
		if !topic.ReplicaUnavailable {
			t.Fatalf("topic %s replica state looks known without metadata", topic.Name)
		}
		if topic.UnderRep != 0 || topic.Offline != 0 {
			t.Fatalf("unknown replica state counted as a known fault: %+v", topic)
		}
	}
}

func TestAggregateHealthyMetadataClearsFault(t *testing.T) {
	meta := kadm.Metadata{Topics: kadm.TopicDetails{
		"orders": {
			Topic: "orders",
			Partitions: kadm.PartitionDetails{
				0: {Topic: "orders", Partition: 0, Leader: 0, Replicas: []int32{0, 1}, ISR: []int32{0, 1}},
			},
		},
	}}
	snap := Aggregate(kadm.DescribedAllLogDirs{}, &meta, DescribeOptions{})
	if snap.MetadataUnavailable || snap.Topics[0].ReplicaUnavailable || snap.Topics[0].Offline != 0 || snap.Topics[0].UnderRep != 0 {
		t.Fatalf("healthy metadata still faulty: %+v", snap.Topics[0])
	}
}
