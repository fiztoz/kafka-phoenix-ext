package store

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	kafkaext "github.com/fiztoz/kafka-phoenix-ext"
)

// Create an actual version-3 database, before either cluster migration.
func legacySQLite(t *testing.T) *SQLite {
	t.Helper()
	s, err := OpenSQLite("file:"+filepath.Join(t.TempDir(), "legacy.db"), kafkaext.Migrations)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	for _, name := range []string{"migrations/001_init.up.sql", "migrations/002_growth_threshold.up.sql"} {
		body, err := fs.ReadFile(kafkaext.Migrations, name)
		if err != nil {
			t.Fatal(err)
		}
		for _, stmt := range splitStatements(string(body)) {
			if _, err := s.db.ExecContext(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, stmt := range []string{
		`CREATE TABLE ext_kafka_usage_schema_migrations (version INT NOT NULL PRIMARY KEY, applied_at TIMESTAMP NOT NULL)`,
		`INSERT INTO ext_kafka_usage_schema_migrations VALUES (1, '2026-01-01'), (2, '2026-01-01'), (3, '2026-01-01')`,
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	for _, topic := range []string{"Orders", "orders"} {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO ext_kafka_usage_topic_state
		(topic, partitions, storage_bytes, prev_bytes, polled_at, prev_polled_at, over_streak, confirmed_over, last_error)
		VALUES (?, 3, 2048, 1024, '2026-01-02 03:04:05', '2026-01-02 02:04:05', 2, 1, 'last error')`, topic); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO ext_kafka_usage_thresholds
		(topic, threshold_bytes, warn_bytes, growth_bytes_per_hour, updated_at)
		VALUES (?, 1500, 1200, 512, '2026-01-02 03:04:05')`, topic); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func assertLegacyMigrated(t *testing.T, s *SQLite) {
	t.Helper()
	ctx := context.Background()
	states, err := s.Scoped("default").States(ctx)
	if err != nil {
		t.Fatal(err)
	}
	thresholds, err := s.Scoped("default").Thresholds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || len(thresholds) != 2 {
		t.Fatalf("legacy rows lost: states=%+v thresholds=%+v", states, thresholds)
	}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	prev := now.Add(-time.Hour)
	for _, row := range states {
		want := StateRow{Cluster: "default", Topic: row.Topic, Partitions: 3,
			StorageBytes: 2048, PrevBytes: 1024, PolledAt: now, PrevPolledAt: &prev,
			OverStreak: 2, ConfirmedOver: true, LastError: "last error"}
		if !reflect.DeepEqual(row, want) {
			t.Fatalf("legacy state changed: got %+v want %+v", row, want)
		}
		th, ok := thresholds[row.Topic]
		if !ok || th.Cluster != "default" || th.ThresholdBytes != 1500 || th.WarnBytes == nil || *th.WarnBytes != 1200 ||
			th.GrowthPerHour == nil || *th.GrowthPerHour != 512 || !th.UpdatedAt.Equal(now) {
			t.Fatalf("legacy threshold changed: %+v", th)
		}
	}
	for _, id := range []string{"harbor", "summit"} {
		rows, err := s.Scoped(id).States(ctx)
		if err != nil || len(rows) != 0 {
			t.Fatalf("legacy states copied to %s: %+v, %v", id, rows, err)
		}
		th, err := s.Scoped(id).Thresholds(ctx)
		if err != nil || len(th) != 0 {
			t.Fatalf("legacy thresholds copied to %s: %+v, %v", id, th, err)
		}
	}
}

func TestSQLiteMigrationRollbackAndRetry(t *testing.T) {
	const path = "migrations/005_cluster_scope_sqlite.up.sql"
	body, err := fs.ReadFile(kafkaext.Migrations, path)
	if err != nil {
		t.Fatal(err)
	}
	stmts := splitStatements(string(body))
	for i := 0; i <= len(stmts); i++ {
		t.Run(fmt.Sprintf("after_%d_statements", i), func(t *testing.T) {
			s := legacySQLite(t)
			ctx := context.Background()
			// Earlier migrations aren't read: this fixture starts at version 3.
			broken := append([]string(nil), stmts[:i]...)
			broken = append(broken, "SELECT * FROM deliberately_missing_migration_table")
			broken = append(broken, stmts[i:]...)
			s.migrations = fstest.MapFS{path: &fstest.MapFile{Data: []byte(strings.Join(broken, ";\n"))}}
			if err := s.Migrate(ctx); err == nil {
				t.Fatal("injected SQL failure was ignored")
			}
			var version, rows, leftovers, clusterColumns int
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('ext_kafka_usage_topic_state') WHERE name = 'cluster'`).Scan(&clusterColumns); err != nil || clusterColumns != 0 {
				t.Fatalf("original schema changed: cluster columns=%d, %v", clusterColumns, err)
			}
			if err := s.db.QueryRowContext(ctx, `SELECT MAX(version) FROM ext_kafka_usage_schema_migrations`).Scan(&version); err != nil || version != 4 {
				t.Fatalf("failed rebuild recorded version: %d, %v", version, err)
			}
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ext_kafka_usage_topic_state WHERE confirmed_over = 1 AND over_streak = 2`).Scan(&rows); err != nil || rows != 2 {
				t.Fatalf("original state lost: %d, %v", rows, err)
			}
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%_rebuild'`).Scan(&leftovers); err != nil || leftovers != 0 {
				t.Fatalf("partial rebuild left behind: %d, %v", leftovers, err)
			}
			s.migrations = kafkaext.Migrations
			if err := s.Migrate(ctx); err != nil {
				t.Fatalf("retry after rollback: %v", err)
			}
			assertLegacyMigrated(t, s)
		})
	}
}

func TestSQLiteMigrationVersionRecordIsAtomic(t *testing.T) {
	s := legacySQLite(t)
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_version BEFORE INSERT ON ext_kafka_usage_schema_migrations
	WHEN NEW.version = 5 BEGIN SELECT RAISE(ABORT, 'injected version failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err == nil {
		t.Fatal("version insertion failure ignored")
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('ext_kafka_usage_topic_state') WHERE name = 'cluster'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("schema committed without version: cluster columns=%d err=%v", count, err)
	}
	if _, err := s.db.ExecContext(ctx, `DROP TRIGGER reject_version`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("retry version failure: %v", err)
	}
	assertLegacyMigrated(t, s)
}
