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
	if err := s.UpsertStates(ctx, []StateRow{
		{Topic: "Orders", Partitions: 1, StorageBytes: 11, PolledAt: now},
		{Topic: "orders", Partitions: 1, StorageBytes: 22, PolledAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetThreshold(ctx, "Orders", 100, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SetThreshold(ctx, "orders", 200, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SetThreshold(ctx, "orders", 250, nil, nil); err != nil {
		t.Fatal(err)
	}
	states, err := s.States(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 {
		t.Fatalf("states = %d, want 2", len(states))
	}
	th, err := s.Thresholds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if th["Orders"].ThresholdBytes != 100 || th["orders"].ThresholdBytes != 250 {
		t.Fatalf("thresholds collided: %+v", th)
	}
	if err := s.DeleteThreshold(ctx, "Orders"); err != nil {
		t.Fatal(err)
	}
	th, err = s.Thresholds(ctx)
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
