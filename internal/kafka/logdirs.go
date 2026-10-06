// Aggregation of DescribeLogDirs + Metadata into a storage usage snapshot.
// StorageBytes counts every replica copy reported by brokers (each broker
// reports the partitions it hosts), i.e. the cluster-wide disk footprint.
// LeaderBytes counts only each partition's leader copy (logical data size).
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// PartitionUse is one partition as shown in the topic drill-down.
type PartitionUse struct {
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
	SizeBytes int64
	OffsetLag int64
	UnderRep  bool
	Offline   bool

	// SizeKnown is false when no directory reported a size. A zero SizeBytes
	// with SizeKnown false is an unavailable measurement, not an empty log.
	SizeKnown bool
	// ReplicaKnown is false when Metadata did not describe this partition.
	// Unknown replica state must not be treated as healthy.
	ReplicaKnown bool
}

// TopicUse is one topic aggregated across all brokers.
type TopicUse struct {
	Name         string
	Partitions   int
	Replicas     int   // replica copies actually reported by DescribeLogDirs
	StorageBytes int64 // all replicas across the cluster (disk footprint)
	LeaderBytes  int64 // leader copies only (logical data size)
	IsInternal   bool
	UnderRep     int // partitions with ISR short of the assigned replicas, including an empty ISR
	Offline      int // partitions with an offline replica, no leader, or a partition metadata error

	// ReplicaUnavailable is true when this topic's replica health cannot be
	// decided from Metadata. Callers must not clear a replica alarm from it.
	ReplicaUnavailable bool
	// SizeIncomplete is true when a known replica has no directory measurement.
	// StorageBytes is then a lower bound, not a new growth baseline.
	SizeIncomplete bool

	Parts []PartitionUse `json:"-"`
}

// BrokerUse is one broker's reported log-dir usage.
type BrokerUse struct {
	ID         int32
	Host       string
	Bytes      int64
	Partitions int // replica copies hosted
}

// ClusterSnapshot is the aggregated view of one poll.
type ClusterSnapshot struct {
	ClusterID    string
	ControllerID int32
	Brokers      []BrokerUse // sorted by ID
	Topics       []TopicUse  // sorted by StorageBytes desc
	BrokerErrors []string    // per-broker/dir/metadata issues that did not fail the poll
	TotalBytes   int64       // sum of TopicUse.StorageBytes (incl. filtered-out topics)

	// SizeIncomplete is true when a broker or directory failure omitted bytes.
	// The totals stay visible, but they are not a complete growth baseline.
	SizeIncomplete bool
	// MetadataUnavailable is true when topic metadata could not be loaded.
	// Replica health is then unknown, not healthy.
	MetadataUnavailable bool
	// UsableDirectories counts log directories that returned without error.
	UsableDirectories int
}

// DescribeOptions carries the topic filter applied by Describe.
type DescribeOptions struct {
	IncludeInternal bool
	TopicAllowed    func(string) bool // nil = allow all
}

type partitionEntry struct {
	broker int32
	size   int64
	lag    int64
}

type topicAgg struct {
	storage  int64
	entries  map[int32][]partitionEntry // partition -> entries (one per replica copy)
	internal bool
	metaPart kadm.PartitionDetails
	metaErr  error
}

// clusterAdmin is the kadm surface Describe needs. *kadm.Client implements it.
type clusterAdmin interface {
	DescribeAllLogDirs(ctx context.Context, s kadm.TopicsSet) (kadm.DescribedAllLogDirs, error)
	Metadata(ctx context.Context, topics ...string) (kadm.Metadata, error)
}

// Describe gathers cluster-wide log-dir usage and topic metadata. A partial
// response (some brokers unreachable) is usable data, not an error: failed
// brokers land in BrokerErrors and SizeIncomplete is set. It is an error
// when no usable directory came back, including when every directory reports
// a storage error.
func (c *Client) Describe(ctx context.Context, o DescribeOptions) (*ClusterSnapshot, error) {
	return describeCluster(ctx, c.adm, c.opts.Timeout, o)
}

func describeCluster(ctx context.Context, adm clusterAdmin, timeout time.Duration, o DescribeOptions) (*ClusterSnapshot, error) {
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	described, err := adm.DescribeAllLogDirs(ctx, nil)
	var shardErrs *kadm.ShardErrors
	if err != nil && !errors.As(err, &shardErrs) {
		return nil, fmt.Errorf("kafka: describe log dirs: %w", err)
	}
	if shardErrs != nil && (shardErrs.AllFailed || len(described) == 0) {
		return nil, fmt.Errorf("kafka: describe log dirs: %w", err)
	}

	var meta *kadm.Metadata
	var metaErr error
	if m, err := adm.Metadata(ctx); err != nil {
		metaErr = err
	} else {
		meta = &m
	}
	snap := Aggregate(described, meta, o)
	if shardErrs != nil {
		snap.SizeIncomplete = true
		for _, se := range shardErrs.Errs {
			snap.BrokerErrors = append(snap.BrokerErrors, fmt.Sprintf("broker %d: %v", se.Broker.NodeID, se.Err))
		}
	}
	if metaErr != nil {
		snap.MetadataUnavailable = true
		snap.BrokerErrors = append(snap.BrokerErrors, fmt.Sprintf("metadata: %v", metaErr))
	}
	sort.Strings(snap.BrokerErrors)
	if snap.UsableDirectories == 0 {
		detail := "no usable directory data"
		if len(snap.BrokerErrors) > 0 {
			detail += ": " + strings.Join(snap.BrokerErrors, "; ")
		}
		return nil, fmt.Errorf("kafka: describe log dirs: %s", detail)
	}
	return snap, nil
}

// Aggregate is the pure merge of a log-dirs response and topic metadata into
// a ClusterSnapshot. meta may be nil (partition detail degraded, sizes kept).
func Aggregate(described kadm.DescribedAllLogDirs, meta *kadm.Metadata, o DescribeOptions) *ClusterSnapshot {
	snap := &ClusterSnapshot{ControllerID: -1, MetadataUnavailable: meta == nil}
	if meta != nil {
		snap.ClusterID = meta.Cluster
		snap.ControllerID = meta.Controller
	}

	aggs := map[string]*topicAgg{}
	brokerAgg := map[int32]*BrokerUse{}
	var brokerErrs []string
	usableDirs := 0
	failedDirs := 0

	hostByID := map[int32]string{}
	if meta != nil {
		for _, b := range meta.Brokers {
			hostByID[b.NodeID] = b.Host
		}
	}

	for brokerID, dirs := range described {
		bu := brokerAgg[brokerID]
		if bu == nil {
			bu = &BrokerUse{ID: brokerID, Host: hostByID[brokerID]}
			brokerAgg[brokerID] = bu
		}
		for dir, d := range dirs {
			if d.Err != nil {
				failedDirs++
				brokerErrs = append(brokerErrs, fmt.Sprintf("broker %d dir %s: %v", brokerID, dir, d.Err))
				continue
			}
			usableDirs++
			for topic, parts := range d.Topics {
				ta := aggs[topic]
				if ta == nil {
					ta = &topicAgg{entries: map[int32][]partitionEntry{}}
					aggs[topic] = ta
				}
				for p, dp := range parts {
					if dp.IsFuture {
						continue // future replica: not materialized yet
					}
					ta.storage += dp.Size
					ta.entries[p] = append(ta.entries[p], partitionEntry{broker: brokerID, size: dp.Size, lag: dp.OffsetLag})
					bu.Bytes += dp.Size
					bu.Partitions++
				}
			}
		}
	}
	snap.UsableDirectories = usableDirs
	snap.SizeIncomplete = failedDirs > 0

	if meta != nil {
		for name, td := range meta.Topics {
			ta := aggs[name]
			if ta == nil {
				// topic with no log dir reports yet (empty, fresh): still listed.
				ta = &topicAgg{entries: map[int32][]partitionEntry{}}
				aggs[name] = ta
			}
			ta.internal = td.IsInternal
			ta.metaPart = td.Partitions
			if td.Err != nil {
				ta.metaErr = td.Err
			}
		}
	}

	for name, ta := range aggs {
		// Metadata is the authority for IsInternal; when it is unavailable,
		// fall back on Kafka's reserved "__" topic prefix convention.
		if ta.metaPart == nil && strings.HasPrefix(name, "__") {
			ta.internal = true
		}
		if ta.metaErr != nil {
			// Captured before the filter continue, and assigned to the snapshot
			// only after this loop. Appending after the slice is published drops
			// the diagnostic when the slice header's length is already fixed.
			brokerErrs = append(brokerErrs, fmt.Sprintf("topic %s metadata: %v", name, ta.metaErr))
		}
		allowed := o.TopicAllowed == nil || o.TopicAllowed(name)
		if ta.internal && !o.IncludeInternal || !allowed {
			continue
		}
		tu := TopicUse{
			Name: name, IsInternal: ta.internal, StorageBytes: ta.storage,
			ReplicaUnavailable: ta.metaPart == nil || ta.metaErr != nil,
		}
		if ta.metaPart != nil {
			tu.Partitions = len(ta.metaPart)
		}
		if tu.Partitions == 0 && len(ta.entries) > 0 {
			// Metadata missing/unavailable: partition count from log dirs.
			for p := range ta.entries {
				if int(p)+1 > tu.Partitions {
					tu.Partitions = int(p) + 1
				}
			}
		}
		pids := partitionIDs(ta)
		for _, p := range pids {
			entries := ta.entries[p]
			pu := PartitionUse{Partition: p, Leader: -1}
			if d, ok := ta.metaPart[p]; ok {
				pu.ReplicaKnown = true
				pu.Leader = d.Leader
				pu.Replicas = d.Replicas
				pu.ISR = d.ISR
				pu.UnderRep, pu.Offline = replicaFaults(d)
			} else {
				tu.ReplicaUnavailable = true
			}
			if len(entries) > 0 {
				pu.SizeKnown = true
			}
			for _, e := range entries {
				tu.Replicas++
				if e.broker == pu.Leader {
					pu.SizeBytes = e.size
					pu.OffsetLag = e.lag
				}
			}
			if pu.SizeBytes == 0 && len(entries) == 1 {
				pu.SizeBytes = entries[0].size // 1 copy: it is the leader
				pu.OffsetLag = entries[0].lag
			}
			if pu.UnderRep {
				tu.UnderRep++
			}
			if pu.Offline {
				tu.Offline++
			}
			tu.LeaderBytes += pu.SizeBytes
			tu.Parts = append(tu.Parts, pu)
		}
		if len(pids) > tu.Partitions {
			tu.Partitions = len(pids)
		}
		if tu.Partitions == 0 {
			tu.Partitions = len(tu.Parts)
		}
		for _, pu := range tu.Parts {
			if pu.ReplicaKnown && len(pu.Replicas) > 0 && !pu.SizeKnown {
				tu.SizeIncomplete = true
				break
			}
		}
		snap.Topics = append(snap.Topics, tu)
	}

	for _, bu := range brokerAgg {
		snap.Brokers = append(snap.Brokers, *bu)
	}
	sort.Slice(snap.Brokers, func(i, j int) bool { return snap.Brokers[i].ID < snap.Brokers[j].ID })

	// BrokerErrors order varies between responses; keep it stable.
	// Assign only after topic-level metadata errors have been appended.
	snap.BrokerErrors = append([]string(nil), brokerErrs...)
	sort.Strings(snap.BrokerErrors)

	sort.SliceStable(snap.Topics, func(i, j int) bool {
		if snap.Topics[i].StorageBytes != snap.Topics[j].StorageBytes {
			return snap.Topics[i].StorageBytes > snap.Topics[j].StorageBytes
		}
		return snap.Topics[i].Name < snap.Topics[j].Name
	})
	for i := range snap.Topics {
		snap.TotalBytes += snap.Topics[i].StorageBytes
	}
	return snap
}

func partitionIDs(ta *topicAgg) []int32 {
	set := map[int32]struct{}{}
	for p := range ta.entries {
		set[p] = struct{}{}
	}
	for p := range ta.metaPart {
		set[p] = struct{}{}
	}
	pids := make([]int32, 0, len(set))
	for p := range set {
		pids = append(pids, p)
	}
	sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })
	return pids
}

// replicaFaults reports known replica failure. An empty ISR with assigned
// replicas is under-replicated. A missing leader is offline even when
// OfflineReplicas was not populated.
func replicaFaults(d kadm.PartitionDetail) (under, offline bool) {
	if d.Err != nil || d.Leader < 0 || len(d.OfflineReplicas) > 0 {
		offline = true
	}
	if len(d.Replicas) > 0 && len(d.ISR) < len(d.Replicas) {
		under = true
	}
	return under, offline
}
