package main

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kafkaext "github.com/fiztoz/kafka-phoenix-ext"
	"github.com/fiztoz/kafka-phoenix-ext/internal/config"
	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/poller"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

// This source must never be contacted: constructors load durable state only.
type noDescribe struct{}

func (noDescribe) Describe(context.Context, kafka.DescribeOptions) (*kafka.ClusterSnapshot, error) {
	return nil, errors.New("unexpected Kafka I/O")
}

func TestLegacyScopeUpgradePolicy(t *testing.T) {
	for _, content := range []string{"empty", "state", "threshold", "both"} {
		t.Run(content, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.OpenSQLite("file:"+filepath.Join(t.TempDir(), "upgrade.db"), kafkaext.Migrations)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			if err := st.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			old := st.Scoped("default")
			if content == "state" || content == "both" {
				if err := old.UpsertStates(ctx, []store.StateRow{{Topic: "orders", StorageBytes: 2000, PolledAt: time.Now(), OverStreak: 2, ConfirmedOver: true}}); err != nil {
					t.Fatal(err)
				}
			}
			if content == "threshold" || content == "both" {
				if err := old.SetThreshold(ctx, "orders", 1000, nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			cfg := &config.Config{Clusters: []config.Cluster{{ID: "harbor"}, {ID: "summit"}}, LegacyCluster: "harbor"}
			err = validateLegacyScope(ctx, cfg, st)
			if content == "empty" {
				if err != nil {
					t.Fatalf("fresh installation rejected: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "KAFKA_DEFAULT_BROKERS") {
				t.Fatalf("orphaned data was not rejected with recovery guidance: %v", err)
			}
			cfg.Clusters = []config.Cluster{{ID: "default"}, {ID: "summit"}}
			cfg.LegacyCluster = "default"
			if err := validateLegacyScope(ctx, cfg, st); err != nil {
				t.Fatalf("documented upgrade rejected: %v", err)
			}
			if content == "both" {
				p := poller.New(ctx, "default", noDescribe{}, st.Scoped("default"), kafka.DescribeOptions{}, poller.SkewPolicy{}, time.Minute, slog.New(slog.DiscardHandler))
				snap := p.Snapshot()
				if len(snap.Topics) != 1 || !snap.Topics[0].ConfirmedOver || snap.Topics[0].ThresholdBytes == nil || *snap.Topics[0].ThresholdBytes != 1000 {
					t.Fatalf("documented upgrade lost active threshold/hysteresis: %+v", snap.Topics)
				}
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.Clusters = []config.Cluster{{ID: "harbor"}}
			if err := validateLegacyScope(ctx, cfg, st); err == nil {
				t.Fatal("unreadable legacy data must fail closed")
			}
		})
	}
}

func TestRunRejectsOrphanedLegacyDataBeforeKafkaOrHTTP(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "orphan.db")
	st, err := store.OpenSQLite(dsn, kafkaext.Migrations)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.Scoped("default").SetThreshold(ctx, "orders", 1000, nil, nil); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_DSN", dsn)
	t.Setenv("DATABASE_ENGINE", "sqlite")
	t.Setenv("KAFKA_CLUSTERS", "harbor,summit")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_HARBOR_BROKERS", "127.0.0.1:1")
	t.Setenv("KAFKA_SUMMIT_BROKERS", "127.0.0.1:1")
	t.Setenv("KAFKA_SASL_MECHANISM", "none")
	t.Setenv("KAFKA_LEGACY_CLUSTER", "harbor")
	if err := run(); err == nil || !strings.Contains(err.Error(), "stored data exists for unconfigured cluster default") {
		t.Fatalf("startup should refuse before Kafka and HTTP: %v", err)
	}
}
