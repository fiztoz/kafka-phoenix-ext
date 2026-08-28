package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

// fixture models a 2-broker cluster: "orders" RF2, "events" RF2, internal
// topic excluded by default, one broker dir error, one future replica that
// must be ignored.
func fixture() (kadm.DescribedAllLogDirs, kadm.Metadata) {
	mk := func(broker int32, dir, topic string, part int32, size int64) kadm.DescribedLogDirPartition {
		return kadm.DescribedLogDirPartition{
			Broker: broker, Dir: dir, Topic: topic, Partition: part, Size: size,
		}
	}
	described := kadm.DescribedAllLogDirs{
		0: {
			"/var/lib/kafka": {
				Broker: 0, Dir: "/var/lib/kafka",
				Topics: kadm.DescribedLogDirTopics{
					"orders":             {0: mk(0, "/var/lib/kafka", "orders", 0, 1024), 1: mk(0, "/var/lib/kafka", "orders", 1, 2048)},
					"__consumer_offsets": {0: mk(0, "/var/lib/kafka", "__consumer_offsets", 0, 8192)},
					"events":             {0: mk(0, "/var/lib/kafka", "events", 0, 512)},
				},
			},
			"/var/lib/kafka2": {
				Broker: 0, Dir: "/var/lib/kafka2",
				Err: errors.New("dir not found"), // partitions skipped, error surfaced
			},
		},
		1: {
			"/var/lib/kafka": {
				Broker: 1, Dir: "/var/lib/kafka",
				Topics: kadm.DescribedLogDirTopics{
					"orders":             {0: mk(1, "/var/lib/kafka", "orders", 0, 1024), 1: mk(1, "/var/lib/kafka", "orders", 1, 4096)},
					"events":             {0: mk(1, "/var/lib/kafka", "events", 0, 512)},
					"__consumer_offsets": {0: mk(1, "/var/lib/kafka", "__consumer_offsets", 0, 8192)},
				},
			},
			// A future (not-yet-materialized) replica copy of orders p1 that
			// must be ignored: it is concurrently being moved into this dir.
			"/var/lib/kafka-future": {
				Broker: 1, Dir: "/var/lib/kafka-future",
				Topics: kadm.DescribedLogDirTopics{
					"orders": {1: mk(1, "/var/lib/kafka-future", "orders", 1, 999999)},
				},
			},
		},
	}
	f := mk(1, "/var/lib/kafka-future", "orders", 1, 999999)
	f.IsFuture = true
	described[1]["/var/lib/kafka-future"].Topics["orders"][1] = f

	rack := "rack-a"
	meta := kadm.Metadata{
		Cluster:    "test-cluster",
		Controller: 0,
		Brokers: kadm.BrokerDetails{
			{NodeID: 0, Host: "kafka-0", Port: 9092, Rack: &rack},
			{NodeID: 1, Host: "kafka-1", Port: 9092, Rack: &rack},
		},
		Topics: kadm.TopicDetails{
			"orders": {
				Topic:      "orders",
				IsInternal: false,
				Partitions: kadm.PartitionDetails{
					0: {Topic: "orders", Partition: 0, Leader: 0, Replicas: []int32{0, 1}, ISR: []int32{0, 1}},
					1: {Topic: "orders", Partition: 1, Leader: 1, Replicas: []int32{0, 1}, ISR: []int32{0}, OfflineReplicas: []int32{1}},
				},
			},
			"events": {
				Topic:      "events",
				IsInternal: false,
				Partitions: kadm.PartitionDetails{
					0: {Topic: "events", Partition: 0, Leader: 0, Replicas: []int32{0, 1}, ISR: []int32{0, 1}},
				},
			},
			"__consumer_offsets": {
				Topic:      "__consumer_offsets",
				IsInternal: true,
				Partitions: kadm.PartitionDetails{
					0: {Topic: "__consumer_offsets", Partition: 0, Leader: 0, Replicas: []int32{0, 1}, ISR: []int32{0, 1}},
				},
			},
		},
	}
	return described, meta
}

func TestAggregateStorageSums(t *testing.T) {
	described, meta := fixture()
	snap := Aggregate(described, &meta, DescribeOptions{})

	if snap.ClusterID != "test-cluster" || snap.ControllerID != 0 {
		t.Fatalf("cluster meta: %+v", snap)
	}
	if len(snap.Topics) != 2 { // __consumer_offsets filtered
		t.Fatalf("topics = %d, want 2: %+v", len(snap.Topics), snap.Topics)
	}
	if snap.Topics[0].Name != "orders" { // sorted by bytes desc
		t.Fatalf("top topic = %q, want orders", snap.Topics[0].Name)
	}

	orders := snap.Topics[0]
	// broker0: 1024+2048; broker1: 1024+4096 (future 999999 ignored).
	if want := int64(1024 + 2048 + 1024 + 4096); orders.StorageBytes != want {
		t.Errorf("orders storage = %d, want %d", orders.StorageBytes, want)
	}
	// leader p0=broker0(1024), leader p1=broker1(4096).
	if want := int64(1024 + 4096); orders.LeaderBytes != want {
		t.Errorf("orders leader bytes = %d, want %d", orders.LeaderBytes, want)
	}
	if orders.Partitions != 2 || orders.Replicas != 4 {
		t.Errorf("orders partitions/replicas = %d/%d, want 2/4", orders.Partitions, orders.Replicas)
	}
	if orders.UnderRep != 1 || orders.Offline != 1 {
		t.Errorf("orders underrep/offline = %d/%d, want 1/1", orders.UnderRep, orders.Offline)
	}

	events := snap.Topics[1]
	if events.StorageBytes != 1024 || events.LeaderBytes != 512 {
		t.Errorf("events storage/leader = %d/%d, want 1024/512", events.StorageBytes, events.LeaderBytes)
	}

	if snap.TotalBytes != orders.StorageBytes+events.StorageBytes {
		t.Errorf("total = %d", snap.TotalBytes)
	}

	// Broker totals include internal-topic partitions hosted on the broker.
	if b0 := snap.Brokers[0]; b0.Bytes != 1024+2048+8192+512 {
		t.Errorf("broker0 bytes = %d", b0.Bytes)
	}
	if snap.Brokers[1].Bytes != 1024+4096+512+8192 {
		t.Errorf("broker1 bytes = %d", snap.Brokers[1].Bytes)
	}
	// Host comes from metadata.
	if snap.Brokers[0].Host != "kafka-0" {
		t.Errorf("broker0 host = %q", snap.Brokers[0].Host)
	}

	if len(snap.BrokerErrors) != 1 {
		t.Errorf("broker errors = %v, want 1 entry", snap.BrokerErrors)
	}

	// Partition detail: p1 leader is broker 1 → size 4096, underrep, offline.
	var p1 *PartitionUse
	for i := range orders.Parts {
		if orders.Parts[i].Partition == 1 {
			p1 = &orders.Parts[i]
		}
	}
	if p1 == nil || p1.SizeBytes != 4096 || !p1.UnderRep || !p1.Offline {
		t.Fatalf("orders p1 detail = %+v", p1)
	}
}

func TestAggregateIncludeInternalAndFilter(t *testing.T) {
	described, meta := fixture()
	snap := Aggregate(described, &meta, DescribeOptions{IncludeInternal: true})
	if len(snap.Topics) != 3 {
		t.Fatalf("with internals: %d topics, want 3", len(snap.Topics))
	}

	filtered := Aggregate(described, &meta, DescribeOptions{
		TopicAllowed: func(name string) bool { return name == "events" },
	})
	if len(filtered.Topics) != 1 || filtered.Topics[0].Name != "events" {
		t.Fatalf("filtered topics = %+v", filtered.Topics)
	}
}

func TestAggregateWithoutMetadata(t *testing.T) {
	described, _ := fixture()
	snap := Aggregate(described, nil, DescribeOptions{})
	if snap.ClusterID != "" || snap.ControllerID != -1 {
		t.Fatalf("nil meta: cluster=%q controller=%d", snap.ClusterID, snap.ControllerID)
	}
	if len(snap.Topics) != 2 {
		t.Fatalf("topics = %+v", snap.Topics)
	}
	// Sizes preserved without metadata; leader bytes degrade to 1-copy fallback.
	if snap.Topics[0].StorageBytes == 0 {
		t.Fatal("storage lost without metadata")
	}
	// orders has 2 replicas per partition → partitions derived from max p+1.
	if snap.Topics[0].Partitions != 2 {
		t.Errorf("derived partitions = %d, want 2", snap.Topics[0].Partitions)
	}
}
