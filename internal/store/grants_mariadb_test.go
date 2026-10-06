package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	kafkaext "github.com/fiztoz/kafka-phoenix-ext"
)

func TestGrantsProvisionRestrictedUser(t *testing.T) {
	adminDSN := os.Getenv("TEST_MARIADB_ADMIN_DSN")
	if adminDSN == "" {
		t.Skip("TEST_MARIADB_ADMIN_DSN unset")
	}
	ctx := context.Background()
	admin := openAdmin(t, adminDSN)
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS phoenix"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS phoenix.app_monitor (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "INSERT IGNORE INTO phoenix.app_monitor (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "DROP USER IF EXISTS 'kafka_usage'@'%'"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS kafka_usage"); err != nil {
		t.Fatal(err)
	}

	script := readGrants(t)
	cred := fmt.Sprintf("kpe-test-%d", time.Now().UnixNano())
	script = strings.ReplaceAll(script, "REPLACE_BEFORE_RUN", cred)
	if _, err := admin.ExecContext(ctx, script); err != nil {
		t.Fatalf("grants script: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS 'kafka_usage'@'%'")
		_, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS kafka_usage")
	})

	restrictedDSN := dsnWithDatabase(swapUser(adminDSN, "kafka_usage", cred), "kafka_usage")
	s, err := OpenMariaDB(restrictedDSN, kafkaext.Migrations)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("restricted migrate: %v", err)
	}
	now := time.Now().UTC()
	if err := s.UpsertStates(ctx, []StateRow{{Topic: "orders", Partitions: 1, StorageBytes: 5, PolledAt: now}}); err != nil {
		t.Fatalf("restricted state write: %v", err)
	}
	if err := s.SetThreshold(ctx, "orders", 9, nil, nil); err != nil {
		t.Fatalf("restricted threshold write: %v", err)
	}

	raw, err := sql.Open("mysql", ensureMariaParams(restrictedDSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	var id int
	err = raw.QueryRowContext(ctx, "SELECT id FROM phoenix.app_monitor").Scan(&id)
	if err == nil {
		t.Fatal("restricted user read a Phoenix application table")
	}
	if _, err := raw.ExecContext(ctx, "DELETE FROM phoenix.app_monitor"); err == nil {
		t.Fatal("restricted user modified a Phoenix application table")
	}
}

func readGrants(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "deploy", "grants.sql"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func swapUser(dsn, user, cred string) string {
	// user:cred@tcp(host)/db
	at := strings.LastIndex(dsn, "@")
	if at < 0 {
		return dsn
	}
	return user + ":" + cred + dsn[at:]
}
