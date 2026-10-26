package config

import (
	"strings"
	"testing"
)

// baseEnv is the minimum a MariaDB-backed install needs.
func baseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_DSN", "kafka_usage:disposable-not-secret@tcp(127.0.0.1:1)/kafka_usage")
	t.Setenv("KAFKA_POLL_INTERVAL", "1m")
	t.Setenv("KAFKA_TIMEOUT", "45s")
	t.Setenv("KAFKA_BROKER_SKEW_PCT", "60")
	t.Setenv("BASE_PATH", "/kafka")
}

func TestLoadAcceptsDisposableChartSecrets(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_BROKERS", "kafka-0.kafka.svc:9092")
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-512")
	t.Setenv("KAFKA_SASL_USERNAME", "kafka-usage")
	t.Setenv("KAFKA_SASL_PASSWORD", "disposable-not-secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("documented secret wiring failed validation: %v", err)
	}
	if cfg.DatabaseDSN == "" {
		t.Fatal("config dropped the disposable DSN")
	}
	if len(cfg.Clusters) != 1 {
		t.Fatalf("clusters = %d, want 1", len(cfg.Clusters))
	}
	c := cfg.Clusters[0]
	if c.ID != "default" || c.SASLCred == "" || c.SASLMechanism != "scram-sha-512" {
		t.Fatalf("legacy cluster not preserved: %+v", c)
	}
	if cfg.LegacyCluster != "default" {
		t.Fatalf("legacy cluster = %q, want default", cfg.LegacyCluster)
	}
	if len(c.Brokers) != 1 || c.Brokers[0] != "kafka-0.kafka.svc:9092" {
		t.Fatalf("brokers = %v", c.Brokers)
	}
}

func TestLoadRejectsSASLWithoutPassword(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_BROKERS", "kafka-0.kafka.svc:9092")
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-512")
	t.Setenv("KAFKA_SASL_USERNAME", "kafka-usage")
	t.Setenv("KAFKA_SASL_PASSWORD", "")

	if _, err := Load(); err == nil {
		t.Fatal("SASL without a password must fail startup validation")
	}
}

func TestLoadRejectsMissingBrokers(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_BROKERS", "")

	if _, err := Load(); err == nil {
		t.Fatal("a single-cluster layout without KAFKA_BROKERS must fail")
	}
}

// Two clusters sharing one service principal only set their own broker lists.
func TestLoadMultipleClustersSharePrincipal(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_CLUSTERS", "harbor,summit")
	t.Setenv("KAFKA_HARBOR_BROKERS", "harbor-kafka-0.harbor.svc:9092,harbor-kafka-1.harbor.svc:9092")
	t.Setenv("KAFKA_SUMMIT_BROKERS", "summit-kafka-0.summit.svc:9092")
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-512")
	t.Setenv("KAFKA_SASL_USERNAME", "kafka-usage")
	t.Setenv("KAFKA_SASL_PASSWORD", "disposable-not-secret")
	t.Setenv("KAFKA_TLS_ENABLED", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("multi-cluster load: %v", err)
	}
	if len(cfg.Clusters) != 2 {
		t.Fatalf("clusters = %d, want 2", len(cfg.Clusters))
	}
	harbor, ok := cfg.Lookup("harbor")
	if !ok {
		t.Fatal("harbor cluster missing")
	}
	if len(harbor.Brokers) != 2 {
		t.Fatalf("harbor brokers = %v, want 2 seeds", harbor.Brokers)
	}
	if harbor.SASLUsername != "kafka-usage" || harbor.SASLCred != "disposable-not-secret" {
		t.Fatal("harbor did not inherit the shared SASL principal")
	}
	if !harbor.TLS.Enabled {
		t.Fatal("harbor did not inherit KAFKA_TLS_ENABLED")
	}
	if harbor.TopicFilter != ".*" || harbor.IncludeInternal {
		t.Fatalf("harbor defaults drifted: filter=%q internal=%v", harbor.TopicFilter, harbor.IncludeInternal)
	}
	// No cluster is named "default", so the first declared cluster owns the
	// unprefixed routes and existing monitors keep working.
	if cfg.LegacyCluster != "harbor" {
		t.Fatalf("legacy cluster = %q, want harbor", cfg.LegacyCluster)
	}
}

// Per-cluster overrides must beat the shared value, including explicit false.
func TestLoadPerClusterOverrides(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_CLUSTERS", "harbor,prod-eu")
	t.Setenv("KAFKA_HARBOR_BROKERS", "harbor:9092")
	t.Setenv("KAFKA_PROD_EU_BROKERS", "prod:9092")
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-512")
	t.Setenv("KAFKA_SASL_USERNAME", "shared")
	t.Setenv("KAFKA_SASL_PASSWORD", "shared-secret")
	t.Setenv("KAFKA_TLS_ENABLED", "true")

	// prod-eu authenticates with its own principal and dials without TLS.
	t.Setenv("KAFKA_PROD_EU_SASL_USERNAME", "prod-only")
	t.Setenv("KAFKA_PROD_EU_SASL_PASSWORD", "prod-secret")
	t.Setenv("KAFKA_PROD_EU_TLS_ENABLED", "false")
	t.Setenv("KAFKA_PROD_EU_TOPIC_FILTER", "^orders$")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	harbor, _ := cfg.Lookup("harbor")
	if harbor.SASLUsername != "shared" || !harbor.TLS.Enabled {
		t.Fatalf("harbor should keep shared values: user=%q tls=%v", harbor.SASLUsername, harbor.TLS.Enabled)
	}
	prod, ok := cfg.Lookup("prod-eu")
	if !ok {
		t.Fatal("hyphenated cluster id not addressable")
	}
	if prod.SASLUsername != "prod-only" || prod.SASLCred != "prod-secret" {
		t.Fatal("per-cluster SASL override lost")
	}
	if prod.TLS.Enabled {
		t.Fatal("explicit KAFKA_PROD_EU_TLS_ENABLED=false must beat the shared true")
	}
	if !prod.TopicAllowed("orders") || prod.TopicAllowed("payments") {
		t.Fatalf("per-cluster topic filter not applied: %q", prod.TopicFilter)
	}
	if !harbor.TopicAllowed("payments") {
		t.Fatal("harbor should keep the shared catch-all filter")
	}
}

func TestLoadRejectsBrokersAlongsideClusterList(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_CLUSTERS", "harbor")
	t.Setenv("KAFKA_HARBOR_BROKERS", "harbor:9092")
	t.Setenv("KAFKA_BROKERS", "legacy:9092")

	if _, err := Load(); err == nil {
		t.Fatal("mixing KAFKA_BROKERS with KAFKA_CLUSTERS is ambiguous and must fail")
	}
}

func TestLoadRejectsClusterWithoutBrokers(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_CLUSTERS", "harbor,summit")
	t.Setenv("KAFKA_HARBOR_BROKERS", "harbor:9092")

	_, err := Load()
	if err == nil {
		t.Fatal("a cluster without brokers must fail")
	}
	if !strings.Contains(err.Error(), "KAFKA_SUMMIT_BROKERS") {
		t.Fatalf("error should name the missing variable, got: %v", err)
	}
}

func TestLoadRejectsBadClusterIDs(t *testing.T) {
	for _, list := range []string{"harbor,harbor", "9harbor", "harbor$", "am p", strings.Repeat("a", 33)} {
		t.Run(list, func(t *testing.T) {
			baseEnv(t)
			t.Setenv("KAFKA_CLUSTERS", list)
			t.Setenv("KAFKA_HARBOR_BROKERS", "harbor:9092")
			if _, err := Load(); err == nil {
				t.Fatalf("KAFKA_CLUSTERS=%q should be rejected", list)
			}
		})
	}
}

// An upper-case id is normalised rather than rejected: KAFKA_CLUSTERS=HARBOR is
// a plausible typo and KAFKA_HARBOR_BROKERS is what it addresses anyway.
func TestLoadNormalisesClusterIDCase(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_CLUSTERS", " HARBOR , summit ")
	t.Setenv("KAFKA_HARBOR_BROKERS", "harbor:9092")
	t.Setenv("KAFKA_SUMMIT_BROKERS", "summit:9092")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Clusters) != 2 || cfg.Clusters[0].ID != "harbor" || cfg.Clusters[1].ID != "summit" {
		t.Fatalf("clusters = %+v", cfg.Clusters)
	}
}

func TestLoadLegacyClusterSelection(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_CLUSTERS", "harbor,summit")
	t.Setenv("KAFKA_HARBOR_BROKERS", "harbor:9092")
	t.Setenv("KAFKA_SUMMIT_BROKERS", "summit:9092")
	t.Setenv("KAFKA_LEGACY_CLUSTER", "summit")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LegacyCluster != "summit" {
		t.Fatalf("legacy cluster = %q, want summit", cfg.LegacyCluster)
	}

	t.Setenv("KAFKA_LEGACY_CLUSTER", "nope")
	if _, err := Load(); err == nil {
		t.Fatal("unknown KAFKA_LEGACY_CLUSTER must fail")
	}
}

func TestLoadRejectsPerClusterSASLWithoutPassword(t *testing.T) {
	baseEnv(t)
	t.Setenv("KAFKA_CLUSTERS", "harbor")
	t.Setenv("KAFKA_HARBOR_BROKERS", "harbor:9092")
	t.Setenv("KAFKA_HARBOR_SASL_MECHANISM", "scram-sha-512")
	t.Setenv("KAFKA_HARBOR_SASL_USERNAME", "harbor-user")

	if _, err := Load(); err == nil {
		t.Fatal("per-cluster SASL without a password must fail")
	}
}

// envKey must address hyphenated ids as underscores.
func TestEnvKey(t *testing.T) {
	if got := envKey("prod-eu"); got != "PROD_EU" {
		t.Fatalf("envKey(prod-eu) = %q", got)
	}
}
