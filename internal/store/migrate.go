package store

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"strings"
	"time"
)

const schemaTable = "ext_kafka_usage_schema_migrations"

// Migrate applies the embedded migrations up to the latest version. The
// extension owns its schema; Phoenix migrations never grow these tables.
func Migrate(ctx context.Context, db *sql.DB, migrations fs.FS) error {
	boot := `CREATE TABLE IF NOT EXISTS ` + schemaTable + ` (
	  version INT NOT NULL PRIMARY KEY,
	  applied_at TIMESTAMP NOT NULL
	)`
	if _, err := db.ExecContext(ctx, boot); err != nil {
		return fmt.Errorf("store: create schema_migrations table: %w", err)
	}

	var current int
	row := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM `+schemaTable)
	if err := row.Scan(&current); err != nil {
		return fmt.Errorf("store: read schema version: %w", err)
	}

	// migrations are sequential: 001_init.up.sql, 002_*.up.sql, ...
	// engine restricts a file to one driver. SQLite still records the version
	// so both engines stay on the same schema number.
	type migration struct {
		version int
		file    string
		engine  string
	}
	all := []migration{
		{version: 1, file: "migrations/001_init.up.sql"},
		{version: 2, file: "migrations/002_growth_threshold.up.sql"},
		{version: 3, file: "migrations/003_topic_collation.up.sql", engine: "mariadb"},
		{version: 4, file: "migrations/004_cluster_scope.up.sql", engine: "mariadb"},
		{version: 5, file: "migrations/005_cluster_scope_sqlite.up.sql", engine: "sqlite"},
	}
	engine := schemaEngine(ctx, db)
	for _, m := range all {
		if current >= m.version {
			continue
		}
		var body string
		if m.engine == "" || m.engine == engine {
			sqlBytes, err := fs.ReadFile(migrations, m.file)
			if err != nil {
				return fmt.Errorf("store: read %s: %w", m.file, err)
			}
			body = string(sqlBytes)
		}
		if err := applyMigration(ctx, db, body, engine == "sqlite", m.version); err != nil {
			return fmt.Errorf("store: apply %s: %w", m.file, err)
		}
	}
	return nil
}

// SQLite DDL and its version record commit together. A failure (including
// cancellation or a failed version INSERT) rolls back the entire rebuild.
// MariaDB DDL implicitly commits, so those SQL files must be replay-safe.
func applyMigration(ctx context.Context, db *sql.DB, body string, sqlite bool, version int) error {
	if sqlite {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, stmt := range splitStatements(body) {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		if err := recordMigration(ctx, tx, version); err != nil {
			return err
		}
		return tx.Commit()
	}
	if body != "" {
		if _, err := db.ExecContext(ctx, body); err != nil {
			return err
		}
	}
	return recordMigration(ctx, db, version)
}

func recordMigration(ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, version int) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO `+schemaTable+` (version, applied_at) VALUES (?, ?)`,
		version, time.Now().UTC())
	return err
}

// splitStatements breaks a migration file into executable statements. It
// drops full-line SQL comments and trailing semicolons. These files carry no
// string literal containing a semicolon, so a plain split is faithful here.
func splitStatements(body string) []string {
	var kept []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}
	var out []string
	for _, stmt := range strings.Split(strings.Join(kept, "\n"), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func schemaEngine(ctx context.Context, db *sql.DB) string {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err == nil && version != "" {
		return "sqlite"
	}
	return "mariadb"
}

func bindUTC(t time.Time) time.Time { return t.UTC() }

// bindNullUTC maps nil/zero time to SQL NULL (MariaDB zero dates break
// parseTime=true).
func bindNullUTC(t *time.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return t.UTC()
}

func scanTime(v any) (*time.Time, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case time.Time:
		t := x.UTC()
		return &t, nil
	case []byte:
		return parseScanTime(string(x))
	case string:
		return parseScanTime(x)
	default:
		return nil, fmt.Errorf("store: unexpected time column type %T", v)
	}
}

func parseScanTime(s string) (*time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" || strings.HasPrefix(s, "0000-00-00") {
		return nil, nil // legacy zero-date rows mean "no timestamp"
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			t = t.UTC()
			return &t, nil
		}
	}
	return nil, fmt.Errorf("store: unparseable time column %q", s)
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
