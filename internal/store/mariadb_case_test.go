package store

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	kafkaext "github.com/fiztoz/kafka-phoenix-ext"
)

func TestMariaDBCaseDistinctTopics(t *testing.T) {
	admin := os.Getenv("TEST_MARIADB_ADMIN_DSN")
	if admin == "" {
		t.Skip("TEST_MARIADB_ADMIN_DSN unset")
	}
	ctx := context.Background()
	db := openAdmin(t, admin)
	const name = "kpe_case_ci"
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+name+" CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name)
	})

	s, err := OpenMariaDB(dsnWithDatabase(admin, name), kafkaext.Migrations)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var collation string
	err = s.db.QueryRowContext(ctx, `
SELECT COLLATION_NAME FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ext_kafka_usage_thresholds' AND COLUMN_NAME = 'topic'`, name).Scan(&collation)
	if err != nil {
		t.Fatal(err)
	}
	if collation != "utf8mb4_bin" {
		t.Fatalf("topic collation = %s, want utf8mb4_bin", collation)
	}

	now := time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC)
	if err := s.Scoped("default").UpsertStates(ctx, []StateRow{
		{Topic: "Orders", Partitions: 1, StorageBytes: 11, PolledAt: now},
		{Topic: "orders", Partitions: 1, StorageBytes: 22, PolledAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Scoped("default").SetThreshold(ctx, "Orders", 100, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Scoped("default").SetThreshold(ctx, "orders", 200, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Scoped("default").SetThreshold(ctx, "orders", 250, nil, nil); err != nil {
		t.Fatal(err)
	}
	states, err := s.Scoped("default").States(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 {
		t.Fatalf("states = %d, want 2", len(states))
	}
	th, err := s.Scoped("default").Thresholds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if th["Orders"].ThresholdBytes != 100 || th["orders"].ThresholdBytes != 250 {
		t.Fatalf("thresholds collided: %+v", th)
	}
	if err := s.Scoped("default").DeleteThreshold(ctx, "Orders"); err != nil {
		t.Fatal(err)
	}
	th, err = s.Scoped("default").Thresholds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := th["Orders"]; ok {
		t.Fatal("deleted Orders still present")
	}
	if th["orders"].ThresholdBytes != 250 {
		t.Fatalf("orders threshold lost: %+v", th["orders"])
	}
}

func openAdmin(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", ensureMariaParams(dsn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func dsnWithDatabase(dsn, database string) string {
	q := ""
	if i := strings.Index(dsn, "?"); i >= 0 {
		q = dsn[i:]
		dsn = dsn[:i]
	}
	if i := strings.LastIndex(dsn, "/"); i >= 0 {
		dsn = dsn[:i]
	}
	return dsn + "/" + database + q
}

// TestMariaDBClustersAreIsolated proves the composite (cluster, topic) key
// works through the MariaDB ON DUPLICATE KEY UPDATE path, not just SQLite's
// ON CONFLICT, and that migration 004 is safe to re-run.
func TestMariaDBClustersAreIsolated(t *testing.T) {
	admin := os.Getenv("TEST_MARIADB_ADMIN_DSN")
	if admin == "" {
		t.Skip("TEST_MARIADB_ADMIN_DSN unset")
	}
	ctx := context.Background()
	db := openAdmin(t, admin)
	const name = "kpe_multi_cluster"
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+name+" CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name)
	})

	s, err := OpenMariaDB(dsnWithDatabase(admin, name), kafkaext.Migrations)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("re-run migrate: %v", err)
	}

	harbor, summit := s.Scoped("harbor"), s.Scoped("summit")
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if err := harbor.UpsertStates(ctx, []StateRow{
		{Topic: "orders", Partitions: 3, StorageBytes: 111, PolledAt: now, OverStreak: 2, ConfirmedOver: true},
	}); err != nil {
		t.Fatal(err)
	}
	// Same topic name, different cluster: a separate row, no ON DUPLICATE
	// KEY UPDATE collision.
	if err := summit.UpsertStates(ctx, []StateRow{
		{Topic: "orders", Partitions: 9, StorageBytes: 999, PolledAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	if err := harbor.SetThreshold(ctx, "orders", 100, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := summit.SetThreshold(ctx, "orders", 5000, nil, nil); err != nil {
		t.Fatal(err)
	}

	harborStates, err := harbor.States(ctx)
	if err != nil {
		t.Fatal(err)
	}
	summitStates, err := summit.States(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(harborStates) != 1 || harborStates[0].StorageBytes != 111 || !harborStates[0].ConfirmedOver {
		t.Fatalf("harbor states = %+v", harborStates)
	}
	if len(summitStates) != 1 || summitStates[0].StorageBytes != 999 || summitStates[0].ConfirmedOver {
		t.Fatalf("summit states = %+v", summitStates)
	}

	harborTh, _ := harbor.Thresholds(ctx)
	summitTh, _ := summit.Thresholds(ctx)
	if harborTh["orders"].ThresholdBytes != 100 || summitTh["orders"].ThresholdBytes != 5000 {
		t.Fatalf("thresholds collided across clusters: harbor=%+v summit=%+v", harborTh, summitTh)
	}
	if err := harbor.DeleteThreshold(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if got, _ := summit.Thresholds(ctx); len(got) != 1 {
		t.Fatal("harbor delete removed summit's threshold")
	}
}
