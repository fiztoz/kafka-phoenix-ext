package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

type fakeAdmin struct {
	dirs    kadm.DescribedAllLogDirs
	dirErr  error
	meta    kadm.Metadata
	metaErr error
}

func (f fakeAdmin) DescribeAllLogDirs(context.Context, kadm.TopicsSet) (kadm.DescribedAllLogDirs, error) {
	return f.dirs, f.dirErr
}

func (f fakeAdmin) Metadata(context.Context, ...string) (kadm.Metadata, error) {
	return f.meta, f.metaErr
}

func dirWithSize(broker int32, topic string, size int64) kadm.DescribedLogDirs {
	return kadm.DescribedLogDirs{
		"/var/lib/kafka": {
			Broker: broker,
			Dir:    "/var/lib/kafka",
			Topics: kadm.DescribedLogDirTopics{
				topic: {
					0: {Broker: broker, Dir: "/var/lib/kafka", Topic: topic, Partition: 0, Size: size},
				},
			},
		},
	}
}

func TestDescribeKeepsShardErrorsAndMarksSizeIncomplete(t *testing.T) {
	admin := fakeAdmin{
		dirs: kadm.DescribedAllLogDirs{0: dirWithSize(0, "orders", 1000)},
		dirErr: &kadm.ShardErrors{Errs: []kadm.ShardError{{
			Broker: kadm.BrokerDetail{NodeID: 1},
			Err:    errors.New("connection refused"),
		}}},
		meta: kadm.Metadata{Cluster: "c", Controller: 0},
	}
	snap, err := describeCluster(context.Background(), admin, time.Second, DescribeOptions{})
	if err != nil {
		t.Fatalf("partial describe: %v", err)
	}
	if !snap.SizeIncomplete {
		t.Fatal("missing broker must not count as a complete size observation")
	}
	if snap.TotalBytes != 1000 {
		t.Fatalf("partial bytes = %d, want 1000", snap.TotalBytes)
	}
	joined := strings.Join(snap.BrokerErrors, "\n")
	if !strings.Contains(joined, "broker 1: connection refused") {
		t.Fatalf("shard error dropped: %v", snap.BrokerErrors)
	}
}

func TestDescribeRejectsAllDirectoryFailures(t *testing.T) {
	admin := fakeAdmin{
		dirs: kadm.DescribedAllLogDirs{
			0: {"/data": {Broker: 0, Dir: "/data", Err: errors.New("KAFKA_STORAGE_ERROR")}},
			1: {"/data": {Broker: 1, Dir: "/data", Err: errors.New("KAFKA_STORAGE_ERROR")}},
		},
		meta: kadm.Metadata{Cluster: "c"},
	}
	snap, err := describeCluster(context.Background(), admin, time.Second, DescribeOptions{})
	if err == nil {
		t.Fatalf("all-directory failure published as success: %+v", snap)
	}
	if snap != nil && snap.TotalBytes == 0 && snap.PollOK() {
		t.Fatal("zero-byte success")
	}
	if !strings.Contains(err.Error(), "no usable directory data") || !strings.Contains(err.Error(), "KAFKA_STORAGE_ERROR") {
		t.Fatalf("error = %v", err)
	}
}

func TestDescribeRetainsMetadataRequestAndTopicErrors(t *testing.T) {
	admin := fakeAdmin{
		dirs:    kadm.DescribedAllLogDirs{0: dirWithSize(0, "orders", 1000)},
		metaErr: errors.New("metadata request failed"),
	}
	snap, err := describeCluster(context.Background(), admin, time.Second, DescribeOptions{})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if !snap.MetadataUnavailable {
		t.Fatal("metadata request failure must be unavailable, not healthy")
	}
	if !strings.Contains(strings.Join(snap.BrokerErrors, "\n"), "metadata: metadata request failed") {
		t.Fatalf("metadata request error dropped: %v", snap.BrokerErrors)
	}

	topicErr := errors.New("UNKNOWN_TOPIC_OR_PARTITION")
	admin = fakeAdmin{
		dirs: kadm.DescribedAllLogDirs{
			0: {
				"/missing": {Broker: 0, Dir: "/missing", Err: errors.New("dir not found")},
				"/var/lib/kafka": {
					Broker: 0, Dir: "/var/lib/kafka",
					Topics: kadm.DescribedLogDirTopics{
						"orders": {0: {Broker: 0, Topic: "orders", Partition: 0, Size: 1000}},
					},
				},
			},
		},
		meta: kadm.Metadata{Topics: kadm.TopicDetails{
			"orders": {Topic: "orders", Err: topicErr, Partitions: kadm.PartitionDetails{}},
		}},
	}
	snap, err = describeCluster(context.Background(), admin, time.Second, DescribeOptions{})
	if err != nil {
		t.Fatalf("describe with topic error: %v", err)
	}
	joined := strings.Join(snap.BrokerErrors, "\n")
	if !strings.Contains(joined, "topic orders metadata: UNKNOWN_TOPIC_OR_PARTITION") {
		t.Fatalf("topic metadata error dropped: %v", snap.BrokerErrors)
	}
	if !strings.Contains(joined, "dir not found") {
		t.Fatalf("dir error dropped when topic error is present: %v", snap.BrokerErrors)
	}
	if len(snap.Topics) != 1 || !snap.Topics[0].ReplicaUnavailable {
		t.Fatalf("topic replica state = %+v", snap.Topics)
	}
}

// PollOK is not a field on ClusterSnapshot; the helper keeps the assertion
// above from depending on a success-shaped zero snapshot.
func (s *ClusterSnapshot) PollOK() bool {
	return s != nil && !s.SizeIncomplete && s.UsableDirectories > 0
}
