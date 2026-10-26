// Package store persists per-topic state, hysteresis counters and
// operator-set thresholds in MariaDB or SQLite. All timestamps are
// normalised to UTC in Go before binding.
//
// Every row is keyed by (cluster, topic): one shared schema backs every
// polled Kafka cluster, and two clusters with a topic of the same name never
// collide. Store is the engine handle (migrations, lifecycle); Scoped is the
// per-cluster surface the poller and HTTP layers use.
package store

import (
	"context"
	"time"
)

// StateRow is one durable topic observation plus hysteresis counters.
// PrevBytes/PrevPolledAt hold the previous good sample so growth rates
// survive a restart without a full history table.
type StateRow struct {
	Cluster       string // set by Scoped reads; ignored by writes
	Topic         string
	Partitions    int
	StorageBytes  int64
	PrevBytes     int64
	PolledAt      time.Time
	PrevPolledAt  *time.Time // nil when this is the first sample
	OverStreak    int
	ConfirmedOver bool
	LastError     string
}

// ThresholdRow is one operator-set threshold bundle. WarnBytes is optional
// and must be below ThresholdBytes; GrowthPerHour is an optional bytes/hour
// growth limit evaluated against the poller's rolling growth window.
type ThresholdRow struct {
	Cluster        string
	Topic          string
	ThresholdBytes int64
	WarnBytes      *int64
	GrowthPerHour  *int64
	UpdatedAt      time.Time
}

// Scoped is the per-cluster persistence surface used by the poller and the
// HTTP layer. Implementations are bound to one cluster id at construction.
type Scoped interface {
	// Cluster reports the cluster id this scope writes to.
	Cluster() string
	// UpsertStates replaces the durable observation rows for one poll.
	// It must not touch threshold rows.
	UpsertStates(ctx context.Context, rows []StateRow) error
	// States returns this cluster's durable state rows (used to seed
	// hysteresis and growth history after a restart).
	States(ctx context.Context) ([]StateRow, error)
	// SetThreshold inserts or replaces a topic threshold bundle. warnBytes
	// and growthPerHour are optional (nil clears them).
	SetThreshold(ctx context.Context, topic string, thresholdBytes int64, warnBytes, growthPerHour *int64) error
	// DeleteThreshold removes a threshold row. Missing rows are not an error.
	DeleteThreshold(ctx context.Context, topic string) error
	// Thresholds returns all of this cluster's thresholds keyed by topic.
	Thresholds(ctx context.Context) (map[string]ThresholdRow, error)
}

// Store is the engine handle: it owns migrations and the connection pool and
// hands out one Scoped view per cluster.
type Store interface {
	// Scoped returns the persistence view bound to one cluster id.
	Scoped(cluster string) Scoped
	// Migrate applies this repo's SQL on start.
	Migrate(ctx context.Context) error
	Close() error
}
