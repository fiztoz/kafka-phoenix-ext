// Package store persists per-topic state, hysteresis counters and
// operator-set thresholds in MariaDB or SQLite. All timestamps are
// normalised to UTC in Go before binding.
package store

import (
	"context"
	"time"
)

// StateRow is one durable topic observation plus hysteresis counters.
// PrevBytes/PrevPolledAt hold the previous good sample so growth rates
// survive a restart without a full history table.
type StateRow struct {
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

// ThresholdRow is one operator-set size threshold. WarnBytes is optional
// and must be below ThresholdBytes.
type ThresholdRow struct {
	Topic          string
	ThresholdBytes int64
	WarnBytes      *int64
	UpdatedAt      time.Time
}

// Store is the persistence surface used by poller and HTTP layers.
type Store interface {
	// UpsertStates replaces the durable observation rows for one poll.
	// It must not touch threshold rows.
	UpsertStates(ctx context.Context, rows []StateRow) error
	// States returns all durable state rows (used to seed hysteresis and
	// growth history after a restart).
	States(ctx context.Context) ([]StateRow, error)
	// SetThreshold inserts or replaces a topic size threshold.
	SetThreshold(ctx context.Context, topic string, thresholdBytes int64, warnBytes *int64) error
	// DeleteThreshold removes a threshold row. Missing rows are not an error.
	DeleteThreshold(ctx context.Context, topic string) error
	// Thresholds returns all thresholds keyed by topic.
	Thresholds(ctx context.Context) (map[string]ThresholdRow, error)
	// Migrate applies this repo's SQL on start.
	Migrate(ctx context.Context) error
	Close() error
}
