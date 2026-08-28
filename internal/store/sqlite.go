package store

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"strings"

	_ "modernc.org/sqlite"
)

// SQLite is the Store implementation backed by modernc.org/sqlite (CGO off);
// used for local development when no MariaDB is available.
type SQLite struct {
	db         *sql.DB
	migrations fs.FS
}

// OpenSQLite opens a file: DSN with sane pragmas appended.
func OpenSQLite(dsn string, migrations fs.FS) (*SQLite, error) {
	dsn = ensureSQLiteParams(dsn)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open sqlite: %w", err)
	}
	// Single writer keeps SQLite honest; kafka-phoenix-ext is a single replica.
	db.SetMaxOpenConns(1)
	return &SQLite{db: db, migrations: migrations}, nil
}

func ensureSQLiteParams(dsn string) string {
	var add []string
	if !strings.Contains(dsn, "_pragma=journal_mode") {
		add = append(add, "_pragma=journal_mode(WAL)")
	}
	if !strings.Contains(dsn, "_pragma=busy_timeout") {
		add = append(add, "_pragma=busy_timeout(5000)")
	}
	if len(add) == 0 {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + strings.Join(add, "&")
}

func (s *SQLite) Migrate(ctx context.Context) error {
	return Migrate(ctx, s.db, s.migrations)
}

const upsertStateSQLite = `
INSERT INTO ext_kafka_usage_topic_state
  (topic, partitions, storage_bytes, prev_bytes,
   polled_at, prev_polled_at, over_streak, confirmed_over, last_error)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(topic) DO UPDATE SET
  partitions = excluded.partitions,
  storage_bytes = excluded.storage_bytes,
  prev_bytes = excluded.prev_bytes,
  polled_at = excluded.polled_at,
  prev_polled_at = excluded.prev_polled_at,
  over_streak = excluded.over_streak,
  confirmed_over = excluded.confirmed_over,
  last_error = excluded.last_error`

func (s *SQLite) UpsertStates(ctx context.Context, rows []StateRow) error {
	for _, r := range rows {
		if _, err := s.db.ExecContext(ctx, upsertStateSQLite,
			r.Topic, r.Partitions, r.StorageBytes, r.PrevBytes,
			bindUTC(r.PolledAt), bindNullUTC(r.PrevPolledAt),
			r.OverStreak, r.ConfirmedOver, nullString(r.LastError),
		); err != nil {
			return fmt.Errorf("store: upsert state %s: %w", r.Topic, err)
		}
	}
	return nil
}

func (s *SQLite) States(ctx context.Context) ([]StateRow, error) {
	return queryStates(ctx, s.db)
}

func (s *SQLite) SetThreshold(ctx context.Context, topic string, thresholdBytes int64, warnBytes *int64) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO ext_kafka_usage_thresholds (topic, threshold_bytes, warn_bytes, updated_at)
VALUES (?, ?, ?, datetime('now'))
ON CONFLICT(topic) DO UPDATE SET
  threshold_bytes = excluded.threshold_bytes,
  warn_bytes = excluded.warn_bytes,
  updated_at = excluded.updated_at`,
		topic, thresholdBytes, warnBytes)
	if err != nil {
		return fmt.Errorf("store: set threshold %s: %w", topic, err)
	}
	return nil
}

func (s *SQLite) DeleteThreshold(ctx context.Context, topic string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM ext_kafka_usage_thresholds WHERE topic = ?`, topic)
	if err != nil {
		return fmt.Errorf("store: delete threshold %s: %w", topic, err)
	}
	return nil
}

func (s *SQLite) Thresholds(ctx context.Context) (map[string]ThresholdRow, error) {
	return queryThresholds(ctx, s.db)
}

func (s *SQLite) Close() error { return s.db.Close() }
