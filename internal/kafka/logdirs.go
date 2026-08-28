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
}

// TopicUse is one topic aggregated across all brokers.
type TopicUse struct {
	Name         string
	Partitions   int
	Replicas     int   // replica copies actually reported by DescribeLogDirs
	StorageBytes int64 // all replicas across the cluster (disk footprint)
	LeaderBytes  int64 // leader copies only (logical data size)
	IsInternal   bool
	UnderRep     int // partitions with len(ISR) < len(Replicas)
	Offline      int // partitions with an offline replica

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
	BrokerErrors []string    // per-broker/dir issues that did not fail the poll
	TotalBytes   int64       // sum of TopicUse.StorageBytes (incl. filtered-out topics)
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

// Describe gathers cluster-wide log-dir usage and topic metadata. A partial
// response (some brokers unreachable) is usable data, not an error: failed
// brokers land in BrokerErrors. It is only an error when no usable data
// came back at all.
func (c *Client) Describe(ctx context.Context, o DescribeOptions) (*ClusterSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
	defer cancel()

	described, err := c.adm.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		// *ShardErrors still carries data; only fail when nothing came back.
		var se *kadm.ShardErrors
		if !errors.As(err, &se) || len(described) == 0 {
			return nil, fmt.Errorf("kafka: describe log dirs: %w", err)
		}
	}

	var meta *kadm.Metadata
	if m, metaErr := c.adm.Metadata(ctx); metaErr == nil {
		meta = &m
	}
	return Aggregate(described, meta, o), nil
}

// Aggregate is the pure merge of a log-dirs response and topic metadata into
// a ClusterSnapshot. meta may be nil (partition detail degraded, sizes kept).
func Aggregate(described kadm.DescribedAllLogDirs, meta *kadm.Metadata, o DescribeOptions) *ClusterSnapshot {
	snap := &ClusterSnapshot{ControllerID: -1}
	if meta != nil {
		snap.ClusterID = meta.Cluster
		snap.ControllerID = meta.Controller
	}

	aggs := map[string]*topicAgg{}
	brokerAgg := map[int32]*BrokerUse{}
	var brokerErrs []string

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
				brokerErrs = append(brokerErrs, fmt.Sprintf("broker %d dir %s: %v", brokerID, dir, d.Err))
				continue
			}
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
	snap.BrokerErrors = brokerErrs

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
		allowed := o.TopicAllowed == nil || o.TopicAllowed(name)
		if ta.internal && !o.IncludeInternal || !allowed {
			continue
		}
		tu := TopicUse{Name: name, IsInternal: ta.internal, StorageBytes: ta.storage}
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
		pids := make([]int32, 0, len(ta.entries))
		for p := range ta.entries {
			pids = append(pids, p)
		}
		sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })

		for _, p := range pids {
			entries := ta.entries[p]
			leader, replicas := int32(-1), []int32{}
			var isr []int32
			var offline []int32
			if d, ok := ta.metaPart[p]; ok {
				leader = d.Leader
				replicas = d.Replicas
				isr = d.ISR
				offline = d.OfflineReplicas
			}
			pu := PartitionUse{Partition: p, Leader: leader, Replicas: replicas, ISR: isr}
			for _, e := range entries {
				tu.Replicas++
				if e.broker == leader {
					pu.SizeBytes = e.size
					pu.OffsetLag = e.lag
				}
			}
			if pu.SizeBytes == 0 && len(entries) == 1 {
				pu.SizeBytes = entries[0].size // 1 copy: it is the leader
				pu.OffsetLag = entries[0].lag
			}
			if len(isr) > 0 && len(isr) < len(replicas) {
				pu.UnderRep = true
			}
			if len(offline) > 0 {
				pu.Offline = true
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
		if ta.metaErr != nil {
			brokerErrs = append(brokerErrs, fmt.Sprintf("topic %s metadata: %v", name, ta.metaErr))
		}
		if tu.Partitions == 0 {
			tu.Partitions = len(tu.Parts)
		}
		snap.Topics = append(snap.Topics, tu)
	}

	for _, bu := range brokerAgg {
		snap.Brokers = append(snap.Brokers, *bu)
	}
	sort.Slice(snap.Brokers, func(i, j int) bool { return snap.Brokers[i].ID < snap.Brokers[j].ID })

	// BrokerErrors order varies between responses; keep it stable.
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
