package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	kafkaext "github.com/fiztoz/kafka-phoenix-ext"
)

func openTestSQLite(t *testing.T) *SQLite {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "test.db")
	s, err := OpenSQLite(dsn, kafkaext.Migrations)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

// scopedDefault is the cluster the single-cluster layout uses.
func scopedDefault(s *SQLite) Scoped { return s.Scoped("default") }

func TestSQLiteStateAndThresholdRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openTestSQLite(t)
	sc := scopedDefault(s)

	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	prev := now.Add(-time.Hour)
	rows := []StateRow{
		{
			Topic: "orders", Partitions: 2, StorageBytes: 2048, PrevBytes: 1024,
			PolledAt: now, PrevPolledAt: &prev, OverStreak: 1, ConfirmedOver: false,
			LastError: "transient",
		},
		{Topic: "events", Partitions: 1, StorageBytes: 512, PolledAt: now},
	}
	if err := sc.UpsertStates(ctx, rows); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := sc.States(ctx)
	if err != nil {
		t.Fatalf("states: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("states = %d, want 2", len(got))
	}
	byTopic := map[string]StateRow{}
	for _, r := range got {
		if r.Cluster != "default" {
			t.Errorf("row cluster = %q, want default", r.Cluster)
		}
		byTopic[r.Topic] = r
	}
	o := byTopic["orders"]
	if o.StorageBytes != 2048 || o.PrevBytes != 1024 {
		t.Errorf("orders row = %+v", o)
	}
	if o.PrevPolledAt == nil || !o.PrevPolledAt.Equal(prev.UTC()) {
		t.Errorf("prev_polled_at = %v, want %v", o.PrevPolledAt, prev)
	}
	if o.LastError != "transient" {
		t.Errorf("last_error = %q", o.LastError)
	}
	if o.PolledAt.IsZero() {
		t.Error("polled_at lost")
	}

	// Upsert replaces: streak advances, prev shifts to the earlier sample.
	now2 := now.Add(2 * time.Hour)
	if err := sc.UpsertStates(ctx, []StateRow{{
		Topic: "orders", Partitions: 2, StorageBytes: 3072, PrevBytes: 2048,
		PolledAt: now2, PrevPolledAt: &now, OverStreak: 2, ConfirmedOver: true,
	}}); err != nil {
		t.Fatal(err)
	}
	got, _ = sc.States(ctx)
	byTopic = map[string]StateRow{}
	for _, r := range got {
		byTopic[r.Topic] = r
	}
	if !byTopic["orders"].ConfirmedOver || byTopic["orders"].OverStreak != 2 {
		t.Errorf("after second upsert: %+v", byTopic["orders"])
	}
	if byTopic["orders"].LastError != "" {
		t.Errorf("last_error should reset: %q", byTopic["orders"].LastError)
	}

	// Thresholds round-trip incl. NULL warn and growth columns.
	warn := int64(1024)
	growth := int64(512)
	if err := sc.SetThreshold(ctx, "orders", 2048, &warn, &growth); err != nil {
		t.Fatal(err)
	}
	if err := sc.SetThreshold(ctx, "events", 4096, nil, nil); err != nil {
		t.Fatal(err)
	}
	th, err := sc.Thresholds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if th["orders"].WarnBytes == nil || *th["orders"].WarnBytes != 1024 {
		t.Errorf("orders threshold = %+v", th["orders"])
	}
	if th["orders"].GrowthPerHour == nil || *th["orders"].GrowthPerHour != 512 {
		t.Errorf("orders growth threshold = %+v", th["orders"])
	}
	if th["events"].WarnBytes != nil || th["events"].GrowthPerHour != nil {
		t.Errorf("events optional columns should be nil: %+v", th["events"])
	}
	if th["orders"].UpdatedAt.IsZero() {
		t.Error("updated_at lost")
	}

	if err := sc.DeleteThreshold(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	th, _ = sc.Thresholds(ctx)
	if _, ok := th["orders"]; ok {
		t.Error("threshold not deleted")
	}
	if len(th) != 1 {
		t.Errorf("thresholds = %d, want 1", len(th))
	}
}

func TestSQLiteMigrateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := openTestSQLite(t)
	for i := 0; i < 2; i++ {
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("migrate run %d: %v", i, err)
		}
	}
	var version int
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(version) FROM ext_kafka_usage_schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 5 {
		t.Fatalf("schema version = %d, want 5", version)
	}
}

func TestSQLiteCaseDistinctTopics(t *testing.T) {
	ctx := context.Background()
	s := openTestSQLite(t)
	sc := scopedDefault(s)
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	if err := sc.UpsertStates(ctx, []StateRow{
		{Topic: "Orders", Partitions: 1, StorageBytes: 10, PolledAt: now},
		{Topic: "orders", Partitions: 1, StorageBytes: 20, PolledAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	if err := sc.SetThreshold(ctx, "Orders", 100, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := sc.SetThreshold(ctx, "orders", 200, nil, nil); err != nil {
		t.Fatal(err)
	}
	states, err := sc.States(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 {
		t.Fatalf("states = %d, want 2 distinct spellings", len(states))
	}
	th, err := sc.Thresholds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if th["Orders"].ThresholdBytes != 100 || th["orders"].ThresholdBytes != 200 {
		t.Fatalf("thresholds collided: %+v", th)
	}
	if err := sc.DeleteThreshold(ctx, "Orders"); err != nil {
		t.Fatal(err)
	}
	th, err = sc.Thresholds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := th["Orders"]; ok {
		t.Fatal("deleted Orders still present")
	}
	if th["orders"].ThresholdBytes != 200 {
		t.Fatalf("orders threshold lost: %+v", th["orders"])
	}
}

// Two clusters that both have a topic called "orders" must never see each
// other's size, hysteresis or threshold — that is the whole point of the
// (cluster, topic) key.
func TestSQLiteClustersAreIsolated(t *testing.T) {
	ctx := context.Background()
	s := openTestSQLite(t)
	harbor := s.Scoped("harbor")
	summit := s.Scoped("summit")
	now := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)

	if err := harbor.UpsertStates(ctx, []StateRow{
		{Topic: "orders", Partitions: 3, StorageBytes: 111, PolledAt: now, OverStreak: 2, ConfirmedOver: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := summit.UpsertStates(ctx, []StateRow{
		{Topic: "orders", Partitions: 9, StorageBytes: 999, PolledAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	if err := harbor.SetThreshold(ctx, "orders", 100, nil, nil); err != nil {
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
	if len(harborStates) != 1 || len(summitStates) != 1 {
		t.Fatalf("harbor=%d summit=%d states, want 1 each", len(harborStates), len(summitStates))
	}
	if harborStates[0].StorageBytes != 111 || harborStates[0].Partitions != 3 || !harborStates[0].ConfirmedOver {
		t.Fatalf("harbor row bled into another cluster: %+v", harborStates[0])
	}
	if summitStates[0].StorageBytes != 999 || summitStates[0].Partitions != 9 || summitStates[0].ConfirmedOver {
		t.Fatalf("summit row bled into another cluster: %+v", summitStates[0])
	}

	summitTh, err := summit.Thresholds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summitTh) != 0 {
		t.Fatalf("summit inherited harbor's threshold: %+v", summitTh)
	}

	// A delete in one cluster must not touch the other.
	if err := harbor.DeleteThreshold(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := harbor.UpsertStates(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := summit.States(ctx); len(got) != 1 {
		t.Fatalf("summit states = %d after harbor delete", len(got))
	}
}

// Legacy rows written before multi-cluster support must survive the 004/005
// migration as cluster "default", not be dropped by the table rebuild.
func TestSQLiteClusterMigrationPreservesLegacyRows(t *testing.T) {
	ctx := context.Background()
	s := legacySQLite(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	assertLegacyMigrated(t, s)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	assertLegacyMigrated(t, s)
	var pk string
	if err := s.db.QueryRowContext(ctx, `
SELECT group_concat(name) FROM pragma_index_info('sqlite_autoindex_ext_kafka_usage_topic_state_1')`,
	).Scan(&pk); err != nil {
		t.Fatalf("read primary key: %v", err)
	}
	if pk != "cluster,topic" {
		t.Fatalf("primary key = %q, want cluster,topic", pk)
	}
	if got := s.Scoped("other"); got.Cluster() != "other" {
		t.Fatalf("scoped cluster = %q", got.Cluster())
	}
}
