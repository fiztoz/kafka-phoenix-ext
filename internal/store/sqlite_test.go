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

func TestSQLiteStateAndThresholdRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openTestSQLite(t)

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
	if err := s.UpsertStates(ctx, rows); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := s.States(ctx)
	if err != nil {
		t.Fatalf("states: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("states = %d, want 2", len(got))
	}
	byTopic := map[string]StateRow{}
	for _, r := range got {
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
	if err := s.UpsertStates(ctx, []StateRow{{
		Topic: "orders", Partitions: 2, StorageBytes: 3072, PrevBytes: 2048,
		PolledAt: now2, PrevPolledAt: &now, OverStreak: 2, ConfirmedOver: true,
	}}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.States(ctx)
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
	if err := s.SetThreshold(ctx, "orders", 2048, &warn, &growth); err != nil {
		t.Fatal(err)
	}
	if err := s.SetThreshold(ctx, "events", 4096, nil, nil); err != nil {
		t.Fatal(err)
	}
	th, err := s.Thresholds(ctx)
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

	if err := s.DeleteThreshold(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	th, _ = s.Thresholds(ctx)
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
}
