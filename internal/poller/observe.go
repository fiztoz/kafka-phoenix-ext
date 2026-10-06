package poller

import (
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

// observeLocked merges one Kafka observation into the published snapshot.
// The caller holds p.mu. A size-incomplete observation is visible but does
// not move capacity, growth, or skew counters and must not be persisted.
// Replica counters still advance when Metadata is authoritative.
//
// Returned rows are the durable state for a complete observation. They are
// nil when the byte totals are incomplete.
func (p *Poller) observeLocked(described *kafka.ClusterSnapshot, now time.Time) []store.StateRow {
	oldTopics := p.snap.Topics
	prevByName := make(map[string]TopicView, len(oldTopics))
	for _, tv := range oldTopics {
		prevByName[tv.Name] = tv
	}
	storageErr := p.snap.StorageError

	p.snap.ClusterID = described.ClusterID
	p.snap.ControllerID = described.ControllerID
	p.snap.PolledAt = now
	p.snap.PollOK = true
	p.snap.LastError = ""
	p.snap.TotalBytes = described.TotalBytes
	p.snap.Brokers = append([]kafka.BrokerUse(nil), described.Brokers...)
	p.snap.BrokerErrors = append([]string(nil), described.BrokerErrors...)
	p.snap.SizeIncomplete = described.SizeIncomplete
	p.snap.MetadataUnavailable = described.MetadataUnavailable
	p.snap.StorageError = storageErr

	topics := make([]TopicView, 0, len(described.Topics)+len(prevByName))
	seen := map[string]bool{}
	var rows []store.StateRow
	if !described.SizeIncomplete {
		rows = make([]store.StateRow, 0, len(described.Topics))
	}

	for _, tu := range described.Topics {
		seen[tu.Name] = true
		old := prevByName[tu.Name]
		tv, row, persist := p.foldTopic(tu, old, now, described)
		topics = append(topics, tv)
		if persist && !described.SizeIncomplete {
			rows = append(rows, row)
		}
	}
	if described.SizeIncomplete || described.MetadataUnavailable {
		for _, old := range oldTopics {
			if seen[old.Name] {
				continue
			}
			kept := cloneTopicView(old)
			if described.MetadataUnavailable {
				kept.ReplicaUnavailable = true
			}
			topics = append(topics, kept)
		}
	}
	p.snap.Topics = topics

	if described.SizeIncomplete {
		// Partial broker bytes are visible above, but they must not create
		// or clear a skew alarm.
		p.snap.SkewStreak = p.skewStreak
		p.snap.SkewConfirmed = p.skewStreak >= confirmSamples
		return nil
	}

	hotID, share, over := evaluateSkew(described.Brokers, p.skew)
	skewStreak := 0
	if over {
		skewStreak = p.skewStreak + 1
	}
	p.skewStreak = skewStreak
	p.snap.SkewBrokerID = hotID
	p.snap.SkewSharePct = share
	p.snap.SkewStreak = skewStreak
	p.snap.SkewConfirmed = skewStreak >= confirmSamples
	return rows
}

func (p *Poller) foldTopic(tu kafka.TopicUse, old TopicView, now time.Time, described *kafka.ClusterSnapshot) (TopicView, store.StateRow, bool) {
	tv := TopicView{
		Name:               tu.Name,
		Partitions:         tu.Partitions,
		Replicas:           tu.Replicas,
		StorageBytes:       tu.StorageBytes,
		LeaderBytes:        tu.LeaderBytes,
		IsInternal:         tu.IsInternal,
		UnderRep:           tu.UnderRep,
		Offline:            tu.Offline,
		Parts:              cloneParts(tu.Parts),
		PartitionSkew:      partitionSkew(tu.Parts),
		ReplicaUnavailable: tu.ReplicaUnavailable || described.MetadataUnavailable,
		SizeIncomplete:     described.SizeIncomplete || tu.SizeIncomplete,
	}
	var row store.StateRow
	persist := false

	if described.SizeIncomplete || tu.SizeIncomplete {
		tv.PrevBytes = old.PrevBytes
		tv.PrevPolledAt = old.PrevPolledAt
		tv.GrowthPerHour = old.GrowthPerHour
		tv.HoursToLimit = cloneI64(old.HoursToLimit)
		tv.OverStreak = old.OverStreak
		tv.ConfirmedOver = old.ConfirmedOver
		tv.GrowthStreak = old.GrowthStreak
		tv.GrowthConfirmed = old.GrowthConfirmed
		if thr, ok := p.thresholds[tu.Name]; ok {
			tv.ThresholdBytes = cloneI64(&thr.ThresholdBytes)
			tv.WarnBytes = cloneI64(thr.WarnBytes)
			tv.GrowthThreshold = cloneI64(thr.GrowthPerHour)
		}
	} else {
		ps, hasPrev := p.prev[tu.Name]
		var prevAt *time.Time
		if hasPrev && !ps.at.IsZero() {
			t := ps.at
			prevAt = &t
		}
		thr, hasThr := p.thresholds[tu.Name]
		streak := 0
		if hasThr && tu.StorageBytes >= thr.ThresholdBytes {
			streak = p.streaks[tu.Name] + 1
		}
		p.streaks[tu.Name] = streak
		p.prev[tu.Name] = prevSample{bytes: tu.StorageBytes, at: now}

		win := append(append([]prevSample{}, p.windows[tu.Name]...), prevSample{bytes: tu.StorageBytes, at: now})
		if len(win) > windowLen {
			win = win[len(win)-windowLen:]
		}
		p.windows[tu.Name] = win
		tv.PrevBytes, tv.PrevPolledAt, tv.GrowthPerHour = growthFromWindow(win)
		tv.OverStreak = streak
		tv.ConfirmedOver = hasThr && streak >= confirmSamples
		if hasThr {
			tv.ThresholdBytes = cloneI64(&thr.ThresholdBytes)
			tv.WarnBytes = cloneI64(thr.WarnBytes)
			tv.GrowthThreshold = cloneI64(thr.GrowthPerHour)
			gStreak := 0
			if thr.GrowthPerHour != nil && tv.GrowthPerHour >= *thr.GrowthPerHour {
				gStreak = p.growStreaks[tu.Name] + 1
			}
			p.growStreaks[tu.Name] = gStreak
			tv.GrowthStreak = gStreak
			tv.GrowthConfirmed = gStreak >= confirmSamples
		}
		tv.HoursToLimit = hoursToLimit(tv.ThresholdBytes, tv.StorageBytes, tv.GrowthPerHour)
		row = store.StateRow{
			Topic:         tu.Name,
			Partitions:    tu.Partitions,
			StorageBytes:  tu.StorageBytes,
			PrevBytes:     ps.bytes,
			PolledAt:      now,
			PrevPolledAt:  prevAt,
			OverStreak:    streak,
			ConfirmedOver: tv.ConfirmedOver,
		}
		persist = true
	}

	if described.MetadataUnavailable || tu.ReplicaUnavailable {
		tv.RepStreak = old.RepStreak
		tv.ReplicaConfirmed = old.ReplicaConfirmed
		tv.ReplicaUnavailable = true
		if described.MetadataUnavailable {
			if old.UnderRep > tv.UnderRep {
				tv.UnderRep = old.UnderRep
			}
			if old.Offline > tv.Offline {
				tv.Offline = old.Offline
			}
		}
		return tv, row, persist
	}
	if tu.UnderRep > 0 || tu.Offline > 0 {
		streak := p.repStreaks[tu.Name] + 1
		p.repStreaks[tu.Name] = streak
		tv.RepStreak = streak
		tv.ReplicaConfirmed = streak >= confirmSamples
		return tv, row, persist
	}
	p.repStreaks[tu.Name] = 0
	tv.RepStreak = 0
	tv.ReplicaConfirmed = false
	return tv, row, persist
}

func cloneSnapshot(s Snapshot) Snapshot {
	out := s
	if s.Topics != nil {
		out.Topics = make([]TopicView, len(s.Topics))
		for i := range s.Topics {
			out.Topics[i] = cloneTopicView(s.Topics[i])
		}
	}
	out.Brokers = append([]kafka.BrokerUse(nil), s.Brokers...)
	out.BrokerErrors = append([]string(nil), s.BrokerErrors...)
	return out
}

func cloneTopicView(tv TopicView) TopicView {
	tv.Parts = cloneParts(tv.Parts)
	tv.ThresholdBytes = cloneI64(tv.ThresholdBytes)
	tv.WarnBytes = cloneI64(tv.WarnBytes)
	tv.GrowthThreshold = cloneI64(tv.GrowthThreshold)
	tv.HoursToLimit = cloneI64(tv.HoursToLimit)
	return tv
}

func cloneParts(in []kafka.PartitionUse) []kafka.PartitionUse {
	if in == nil {
		return nil
	}
	out := make([]kafka.PartitionUse, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Replicas = append([]int32(nil), in[i].Replicas...)
		out[i].ISR = append([]int32(nil), in[i].ISR...)
	}
	return out
}

func cloneI64(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
