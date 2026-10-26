package store

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	kafkaext "github.com/fiztoz/kafka-phoenix-ext"
)

func TestMariaDBClusterMigrationRetryFromEveryLegacyBoundary(t *testing.T) {
	admin := os.Getenv("TEST_MARIADB_ADMIN_DSN")
	if admin == "" {
		t.Skip("TEST_MARIADB_ADMIN_DSN unset")
	}
	ctx := context.Background()
	db := openAdmin(t, admin)
	// These were the four independently committed statements in the first
	// implementation. Retry must handle every possible committed prefix,
	// including complete DDL with no version record.
	oldStatements := []string{
		`ALTER TABLE ext_kafka_usage_topic_state ADD COLUMN cluster VARCHAR(64) NOT NULL DEFAULT 'default' AFTER topic`,
		`ALTER TABLE ext_kafka_usage_topic_state DROP PRIMARY KEY, ADD PRIMARY KEY (cluster, topic)`,
		`ALTER TABLE ext_kafka_usage_thresholds ADD COLUMN cluster VARCHAR(64) NOT NULL DEFAULT 'default' AFTER topic`,
		`ALTER TABLE ext_kafka_usage_thresholds DROP PRIMARY KEY, ADD PRIMARY KEY (cluster, topic)`,
	}
	for boundary := 0; boundary <= len(oldStatements); boundary++ {
		t.Run(fmt.Sprintf("after_%d_statements", boundary), func(t *testing.T) {
			name := fmt.Sprintf("kpe_recovery_%d_%d", time.Now().UnixNano(), boundary)
			if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name+" CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := db.ExecContext(context.Background(), "DROP DATABASE "+name); err != nil {
					t.Errorf("cleanup owned test schema: %v", err)
				}
			})
			s, err := OpenMariaDB(dsnWithDatabase(admin, name), kafkaext.Migrations)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			for _, path := range []string{"migrations/001_init.up.sql", "migrations/002_growth_threshold.up.sql", "migrations/003_topic_collation.up.sql"} {
				body, err := fs.ReadFile(kafkaext.Migrations, path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.ExecContext(ctx, string(body)); err != nil {
					t.Fatal(err)
				}
			}
			for _, stmt := range []string{
				`CREATE TABLE ext_kafka_usage_schema_migrations (version INT NOT NULL PRIMARY KEY, applied_at TIMESTAMP NOT NULL)`,
				`INSERT INTO ext_kafka_usage_schema_migrations VALUES (1, '2026-01-01'), (2, '2026-01-01'), (3, '2026-01-01')`,
				`INSERT INTO ext_kafka_usage_topic_state (topic, storage_bytes, polled_at, over_streak, confirmed_over) VALUES ('orders', 2048, '2026-01-02', 2, 1)`,
				`INSERT INTO ext_kafka_usage_thresholds (topic, threshold_bytes, growth_bytes_per_hour, updated_at) VALUES ('orders', 1500, 512, '2026-01-02')`,
			} {
				if _, err := s.db.ExecContext(ctx, stmt); err != nil {
					t.Fatal(err)
				}
			}
			for _, stmt := range oldStatements[:boundary] {
				if _, err := s.db.ExecContext(ctx, stmt); err != nil {
					t.Fatal(err)
				}
			}
			if boundary > 0 {
				// Negative control: the original SQL cannot recover this state.
				s.migrations = fstest.MapFS{"migrations/004_cluster_scope.up.sql": &fstest.MapFile{Data: []byte(strings.Join(oldStatements, ";\n"))}}
				if err := s.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "Duplicate column") {
					t.Fatalf("original migration should fail on replay: %v", err)
				}
				s.migrations = kafkaext.Migrations
			}
			if err := s.Migrate(ctx); err != nil {
				t.Fatalf("resume partial migration: %v", err)
			}
			if err := s.Migrate(ctx); err != nil {
				t.Fatalf("repeat completed migration: %v", err)
			}
			rows, err := s.Scoped("default").States(ctx)
			if err != nil || len(rows) != 1 || rows[0].StorageBytes != 2048 || rows[0].OverStreak != 2 || !rows[0].ConfirmedOver {
				t.Fatalf("legacy state changed: %+v, %v", rows, err)
			}
			th, err := s.Scoped("default").Thresholds(ctx)
			if err != nil || len(th) != 1 || th["orders"].ThresholdBytes != 1500 || th["orders"].GrowthPerHour == nil || *th["orders"].GrowthPerHour != 512 {
				t.Fatalf("legacy thresholds changed: %+v, %v", th, err)
			}
			for _, id := range []string{"harbor", "summit"} {
				if err := s.Scoped(id).SetThreshold(ctx, "orders", 5000, nil, nil); err != nil {
					t.Fatalf("composite threshold key missing: %v", err)
				}
				if err := s.Scoped(id).UpsertStates(ctx, []StateRow{{Topic: "orders", StorageBytes: 9999, PolledAt: time.Now()}}); err != nil {
					t.Fatalf("composite state key missing: %v", err)
				}
			}
			rows, err = s.Scoped("default").States(ctx)
			if err != nil || len(rows) != 1 || rows[0].StorageBytes != 2048 {
				t.Fatalf("cross-cluster state collision: %+v, %v", rows, err)
			}
			th, err = s.Scoped("default").Thresholds(ctx)
			if err != nil || th["orders"].ThresholdBytes != 1500 {
				t.Fatalf("cross-cluster threshold collision: %+v, %v", th, err)
			}
		})
	}
}
