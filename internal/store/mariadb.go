package store

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MariaDB is the Store implementation backed by go-sql-driver/mysql.
type MariaDB struct {
	db         *sql.DB
	migrations fs.FS
}

// OpenMariaDB opens the DSN, adding parseTime/multiStatements when absent.
func OpenMariaDB(dsn string, migrations fs.FS) (*MariaDB, error) {
	dsn = ensureMariaParams(dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open mariadb: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	return &MariaDB{db: db, migrations: migrations}, nil
}

func ensureMariaParams(dsn string) string {
	var add []string
	if !strings.Contains(dsn, "parseTime=") {
		add = append(add, "parseTime=true")
	}
	if !strings.Contains(dsn, "multiStatements=") {
		add = append(add, "multiStatements=true")
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

func (m *MariaDB) Migrate(ctx context.Context) error {
	return Migrate(ctx, m.db, m.migrations)
}

const upsertStateMaria = `
INSERT INTO ext_kafka_usage_topic_state
  (topic, partitions, storage_bytes, prev_bytes,
   polled_at, prev_polled_at, over_streak, confirmed_over, last_error)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  partitions = VALUES(partitions),
  storage_bytes = VALUES(storage_bytes),
  prev_bytes = VALUES(prev_bytes),
  polled_at = VALUES(polled_at),
  prev_polled_at = VALUES(prev_polled_at),
  over_streak = VALUES(over_streak),
  confirmed_over = VALUES(confirmed_over),
  last_error = VALUES(last_error)`

func (m *MariaDB) UpsertStates(ctx context.Context, rows []StateRow) error {
	for _, r := range rows {
		if _, err := m.db.ExecContext(ctx, upsertStateMaria,
			r.Topic, r.Partitions, r.StorageBytes, r.PrevBytes,
			bindUTC(r.PolledAt), bindNullUTC(r.PrevPolledAt),
			r.OverStreak, r.ConfirmedOver, nullString(r.LastError),
		); err != nil {
			return fmt.Errorf("store: upsert state %s: %w", r.Topic, err)
		}
	}
	return nil
}

const selectStates = `
SELECT topic, partitions, storage_bytes, prev_bytes,
       polled_at, prev_polled_at, over_streak, confirmed_over,
       COALESCE(last_error, '')
FROM ext_kafka_usage_topic_state`

func (m *MariaDB) States(ctx context.Context) ([]StateRow, error) {
	return queryStates(ctx, m.db)
}

func (m *MariaDB) SetThreshold(ctx context.Context, topic string, thresholdBytes int64, warnBytes *int64) error {
	_, err := m.db.ExecContext(ctx, `
INSERT INTO ext_kafka_usage_thresholds (topic, threshold_bytes, warn_bytes, updated_at)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  threshold_bytes = VALUES(threshold_bytes),
  warn_bytes = VALUES(warn_bytes),
  updated_at = VALUES(updated_at)`,
		topic, thresholdBytes, warnBytes, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("store: set threshold %s: %w", topic, err)
	}
	return nil
}

func (m *MariaDB) DeleteThreshold(ctx context.Context, topic string) error {
	_, err := m.db.ExecContext(ctx,
		`DELETE FROM ext_kafka_usage_thresholds WHERE topic = ?`, topic)
	if err != nil {
		return fmt.Errorf("store: delete threshold %s: %w", topic, err)
	}
	return nil
}

func (m *MariaDB) Thresholds(ctx context.Context) (map[string]ThresholdRow, error) {
	return queryThresholds(ctx, m.db)
}

func (m *MariaDB) Close() error { return m.db.Close() }

// --- shared query helpers ---

func queryStates(ctx context.Context, db *sql.DB) ([]StateRow, error) {
	rows, err := db.QueryContext(ctx, selectStates)
	if err != nil {
		return nil, fmt.Errorf("store: query states: %w", err)
	}
	defer rows.Close()
	var out []StateRow
	for rows.Next() {
		var r StateRow
		var polled, prevPolled any
		var confirmed int
		if err := rows.Scan(&r.Topic, &r.Partitions, &r.StorageBytes, &r.PrevBytes,
			&polled, &prevPolled, &r.OverStreak, &confirmed, &r.LastError); err != nil {
			return nil, fmt.Errorf("store: scan state: %w", err)
		}
		if r.PrevPolledAt, err = scanTime(prevPolled); err != nil {
			return nil, err
		}
		pt, err := scanTime(polled)
		if err != nil {
			return nil, err
		}
		if pt != nil {
			r.PolledAt = *pt
		}
		r.ConfirmedOver = confirmed != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

func queryThresholds(ctx context.Context, db *sql.DB) (map[string]ThresholdRow, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT topic, threshold_bytes, warn_bytes, updated_at FROM ext_kafka_usage_thresholds`)
	if err != nil {
		return nil, fmt.Errorf("store: query thresholds: %w", err)
	}
	defer rows.Close()
	out := make(map[string]ThresholdRow)
	for rows.Next() {
		var q ThresholdRow
		var warn any
		var updated any
		if err := rows.Scan(&q.Topic, &q.ThresholdBytes, &warn, &updated); err != nil {
			return nil, fmt.Errorf("store: scan threshold: %w", err)
		}
		switch x := warn.(type) {
		case int64:
			v := x
			q.WarnBytes = &v
		case []byte:
			q.WarnBytes = parseWarn(x)
		case nil:
		}
		if ut, err := scanTime(updated); err == nil && ut != nil {
			q.UpdatedAt = *ut
		}
		out[q.Topic] = q
	}
	return out, rows.Err()
}

func parseWarn(b []byte) *int64 {
	var v int64
	s := string(b)
	if s == "" {
		return nil
	}
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
		return nil
	}
	return &v
}
