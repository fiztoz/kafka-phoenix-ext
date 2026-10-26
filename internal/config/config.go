// Package config loads kafka-phoenix-ext configuration from the environment.
//
// One process can poll several Kafka clusters. Cluster ids are declared in
// KAFKA_CLUSTERS and each cluster's settings live under KAFKA_<ID>_*. Any
// per-cluster variable that is left unset falls back to the shared KAFKA_*
// value, so clusters that share a service principal only need their own
// broker list. Leaving KAFKA_CLUSTERS unset keeps the original single-cluster
// layout: one cluster named "default" built from KAFKA_BROKERS.
package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// defaultClusterID is the implicit cluster when KAFKA_CLUSTERS is unset. It
// is also the cluster the persistence layer labels pre-upgrade rows with.
const defaultClusterID = "default"

// maxClusters bounds KAFKA_CLUSTERS so a typo cannot open hundreds of pollers.
const maxClusters = 20

// TLS is the per-cluster TLS dial configuration.
type TLS struct {
	Enabled  bool
	CAFile   string // path on disk; takes precedence over CA
	CA       string // inline PEM
	Insecure bool
}

// Cluster is one polled Kafka cluster. Hysteresis, growth windows and
// thresholds are kept per cluster, keyed by (cluster, topic) in the store.
type Cluster struct {
	ID              string
	Brokers         []string
	SASLMechanism   string
	SASLUsername    string
	SASLCred        string
	TopicFilter     string
	IncludeInternal bool
	TLS             TLS

	topicRe *regexp.Regexp
}

// TopicAllowed reports whether a topic name passes this cluster's filter.
func (c *Cluster) TopicAllowed(name string) bool {
	if c.topicRe == nil {
		return true
	}
	return c.topicRe.MatchString(name)
}

// Config is the full environment surface of kafka-phoenix-ext.
type Config struct {
	// Clusters is never empty; Load guarantees at least one entry.
	Clusters []Cluster

	// LegacyCluster is the cluster that serves the unprefixed routes
	// (/health/ready, /topic/{name}, ...). It keeps monitors written for the
	// single-cluster layout working after a multi-cluster migration.
	LegacyCluster string

	PollInterval time.Duration
	KafkaTimeout time.Duration

	BrokerSkewPct  int   // one broker holding more than this % of cluster bytes is skew
	BrokerMaxBytes int64 // absolute per-broker byte limit; 0 disables

	ListenAddr     string
	DatabaseDSN    string // MariaDB DSN or file:/data/kafka-phoenix-ext.db
	DatabaseEngine string // mariadb | sqlite; inferred when empty
	BasePath       string
	UIToken        string
	LogLevel       string
}

// Lookup returns the cluster with the given id.
func (c *Config) Lookup(id string) (Cluster, bool) {
	for i := range c.Clusters {
		if c.Clusters[i].ID == id {
			return c.Clusters[i], true
		}
	}
	return Cluster{}, false
}

// envSurface is the flat KAFKA_* block. The cluster-scoped values here are
// the shared defaults that a per-cluster KAFKA_<ID>_* variable overrides.
type envSurface struct {
	Brokers       string `env:"KAFKA_BROKERS"`        // legacy single-cluster seed list
	ClusterList   string `env:"KAFKA_CLUSTERS"`       // comma-separated cluster ids
	LegacyCluster string `env:"KAFKA_LEGACY_CLUSTER"` // cluster serving unprefixed routes

	SASLMechanism   string `env:"KAFKA_SASL_MECHANISM" envDefault:"none"`
	SASLUsername    string `env:"KAFKA_SASL_USERNAME"`
	SASLCred        string `env:"KAFKA_SASL_PASSWORD"` // from Secret; never logged
	TopicFilter     string `env:"KAFKA_TOPIC_FILTER" envDefault:".*"`
	IncludeInternal bool   `env:"KAFKA_INCLUDE_INTERNAL" envDefault:"false"`

	TLSEnabled  bool   `env:"KAFKA_TLS_ENABLED" envDefault:"false"`
	TLSCAFile   string `env:"KAFKA_TLS_CA_FILE"`
	TLSCA       string `env:"KAFKA_TLS_CA"`
	TLSInsecure bool   `env:"KAFKA_TLS_INSECURE" envDefault:"false"`

	PollInterval time.Duration `env:"KAFKA_POLL_INTERVAL" envDefault:"15m"`
	KafkaTimeout time.Duration `env:"KAFKA_TIMEOUT" envDefault:"45s"`

	BrokerSkewPct  int   `env:"KAFKA_BROKER_SKEW_PCT" envDefault:"60"`
	BrokerMaxBytes int64 `env:"KAFKA_BROKER_MAX_BYTES" envDefault:"0"`

	ListenAddr     string `env:"LISTEN_ADDR" envDefault:":8080"`
	DatabaseDSN    string `env:"DATABASE_DSN,required"` // MariaDB DSN or file:/data/kafka-phoenix-ext.db
	DatabaseEngine string `env:"DATABASE_ENGINE"`       // mariadb | sqlite; inferred when empty
	BasePath       string `env:"BASE_PATH" envDefault:"/kafka"`
	UIToken        string `env:"UI_TOKEN"`
	LogLevel       string `env:"LOG_LEVEL" envDefault:"info"`
}

// clusterEnv holds the KAFKA_<ID>_* overrides. Pointers and empty strings
// mean "unset" so the shared KAFKA_* value can win; that is what lets two
// clusters share one SASL principal with only two extra lines of config.
type clusterEnv struct {
	Brokers         string `env:"BROKERS"`
	SASLMechanism   string `env:"SASL_MECHANISM"`
	SASLUsername    string `env:"SASL_USERNAME"`
	SASLCred        string `env:"SASL_PASSWORD"`
	TopicFilter     string `env:"TOPIC_FILTER"`
	IncludeInternal *bool  `env:"INCLUDE_INTERNAL"`
	TLSEnabled      *bool  `env:"TLS_ENABLED"`
	TLSCAFile       string `env:"TLS_CA_FILE"`
	TLSCA           string `env:"TLS_CA"`
	TLSInsecure     *bool  `env:"TLS_INSECURE"`

	// brokersVar is only used for error messages.
	brokersVar string
}

// Load parses the environment and validates locked constraints.
func Load() (*Config, error) {
	var raw envSurface
	if err := env.Parse(&raw); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	cfg := &Config{
		PollInterval:   raw.PollInterval,
		KafkaTimeout:   raw.KafkaTimeout,
		BrokerSkewPct:  raw.BrokerSkewPct,
		BrokerMaxBytes: raw.BrokerMaxBytes,
		ListenAddr:     strings.TrimSpace(raw.ListenAddr),
		DatabaseDSN:    strings.TrimSpace(raw.DatabaseDSN),
		DatabaseEngine: strings.ToLower(strings.TrimSpace(raw.DatabaseEngine)),
		UIToken:        raw.UIToken,
		LogLevel:       strings.TrimSpace(raw.LogLevel),
	}

	ids, err := parseClusterIDs(raw.ClusterList)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		// Original single-cluster layout: KAFKA_BROKERS is the one cluster.
		c, err := buildCluster(defaultClusterID, raw, clusterEnv{
			Brokers:    raw.Brokers,
			brokersVar: "KAFKA_BROKERS",
		})
		if err != nil {
			return nil, err
		}
		cfg.Clusters = []Cluster{c}
	} else {
		if strings.TrimSpace(raw.Brokers) != "" {
			return nil, fmt.Errorf("config: KAFKA_BROKERS and KAFKA_CLUSTERS are mutually exclusive; " +
				"declare every cluster with KAFKA_<ID>_BROKERS instead")
		}
		for _, id := range ids {
			ce, err := parseClusterEnv(id)
			if err != nil {
				return nil, err
			}
			c, err := buildCluster(id, raw, ce)
			if err != nil {
				return nil, err
			}
			cfg.Clusters = append(cfg.Clusters, c)
		}
	}

	if cfg.LegacyCluster, err = resolveLegacyCluster(strings.TrimSpace(raw.LegacyCluster), cfg.Clusters); err != nil {
		return nil, err
	}

	if cfg.PollInterval < time.Minute {
		return nil, fmt.Errorf("config: KAFKA_POLL_INTERVAL must be >= 1m, got %s", cfg.PollInterval)
	}
	if cfg.KafkaTimeout <= 0 || cfg.KafkaTimeout > 10*time.Minute {
		return nil, fmt.Errorf("config: KAFKA_TIMEOUT must be in (0, 10m], got %s", cfg.KafkaTimeout)
	}
	if cfg.BrokerSkewPct < 1 || cfg.BrokerSkewPct > 100 {
		return nil, fmt.Errorf("config: KAFKA_BROKER_SKEW_PCT must be in [1, 100], got %d", cfg.BrokerSkewPct)
	}
	if cfg.BrokerMaxBytes < 0 {
		return nil, fmt.Errorf("config: KAFKA_BROKER_MAX_BYTES must be >= 0 (0 disables), got %d", cfg.BrokerMaxBytes)
	}

	cfg.BasePath = normalizeBasePath(raw.BasePath)

	switch cfg.DatabaseEngine {
	case "":
		if strings.HasPrefix(cfg.DatabaseDSN, "file:") {
			cfg.DatabaseEngine = "sqlite"
		} else {
			cfg.DatabaseEngine = "mariadb"
		}
	case "mariadb", "sqlite":
	default:
		return nil, fmt.Errorf("config: DATABASE_ENGINE must be mariadb or sqlite, got %q", cfg.DatabaseEngine)
	}

	return cfg, nil
}

// parseClusterIDs splits and validates KAFKA_CLUSTERS.
func parseClusterIDs(list string) ([]string, error) {
	var ids []string
	seen := map[string]bool{}
	prefixes := map[string]string{}
	for _, part := range strings.Split(list, ",") {
		id := strings.ToLower(strings.TrimSpace(part))
		if id == "" {
			continue
		}
		if !validClusterID(id) {
			return nil, fmt.Errorf("config: cluster id %q in KAFKA_CLUSTERS must match "+
				"[a-z][a-z0-9_-]* (max 32 chars)", part)
		}
		if seen[id] {
			return nil, fmt.Errorf("config: duplicate cluster id %q in KAFKA_CLUSTERS", id)
		}
		key := envKey(id)
		if previous, ok := prefixes[key]; ok {
			return nil, fmt.Errorf("config: cluster ids %q and %q share environment prefix KAFKA_%s_", previous, id, key)
		}
		seen[id] = true
		prefixes[key] = id
		ids = append(ids, id)
	}
	if len(ids) > maxClusters {
		return nil, fmt.Errorf("config: KAFKA_CLUSTERS lists %d clusters, at most %d are supported",
			len(ids), maxClusters)
	}
	return ids, nil
}

var clusterIDRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

func validClusterID(id string) bool { return clusterIDRe.MatchString(id) }

// envKey is the uppercase env-fragment for a cluster id: "prod-eu" ->
// "PROD_EU", addressing KAFKA_PROD_EU_BROKERS.
func envKey(id string) string {
	return strings.ToUpper(strings.ReplaceAll(id, "-", "_"))
}

// parseClusterEnv reads the KAFKA_<ID>_* overrides for one cluster.
func parseClusterEnv(id string) (clusterEnv, error) {
	var ce clusterEnv
	prefix := "KAFKA_" + envKey(id) + "_"
	err := env.ParseWithOptions(&ce, env.Options{Prefix: prefix})
	if err != nil {
		return clusterEnv{}, fmt.Errorf("config: cluster %q: %w", id, err)
	}
	ce.brokersVar = prefix + "BROKERS"
	return ce, nil
}

// buildCluster merges one cluster's overrides over the shared defaults and
// validates the result.
func buildCluster(id string, shared envSurface, ce clusterEnv) (Cluster, error) {
	c := Cluster{
		ID:              id,
		Brokers:         SplitBrokers(ce.Brokers),
		SASLMechanism:   strings.ToLower(strings.TrimSpace(pick(ce.SASLMechanism, shared.SASLMechanism))),
		SASLUsername:    strings.TrimSpace(pick(ce.SASLUsername, shared.SASLUsername)),
		SASLCred:        pick(ce.SASLCred, shared.SASLCred),
		TopicFilter:     strings.TrimSpace(pick(ce.TopicFilter, shared.TopicFilter)),
		IncludeInternal: pickBool(ce.IncludeInternal, shared.IncludeInternal),
		TLS: TLS{
			Enabled:  pickBool(ce.TLSEnabled, shared.TLSEnabled),
			CAFile:   strings.TrimSpace(pick(ce.TLSCAFile, shared.TLSCAFile)),
			CA:       pick(ce.TLSCA, shared.TLSCA),
			Insecure: pickBool(ce.TLSInsecure, shared.TLSInsecure),
		},
	}
	if c.SASLMechanism == "" {
		c.SASLMechanism = "none"
	}
	if c.TopicFilter == "" {
		c.TopicFilter = ".*"
	}

	name := ce.brokersVar
	if name == "" {
		name = "KAFKA_" + envKey(id) + "_BROKERS"
	}
	if len(c.Brokers) == 0 {
		return Cluster{}, fmt.Errorf("config: %s must not be empty", name)
	}
	if strings.ContainsAny(ce.Brokers, "\n\t") {
		return Cluster{}, fmt.Errorf("config: %s must be comma-separated hosts", name)
	}

	switch c.SASLMechanism {
	case "none":
	case "plain", "scram-sha-256", "scram-sha-512":
		if c.SASLUsername == "" || c.SASLCred == "" {
			return Cluster{}, fmt.Errorf("config: cluster %q needs KAFKA_%s_SASL_USERNAME and "+
				"KAFKA_%s_SASL_PASSWORD when its SASL mechanism is %q",
				id, envKey(id), envKey(id), c.SASLMechanism)
		}
	default:
		return Cluster{}, fmt.Errorf("config: cluster %q: KAFKA_%s_SASL_MECHANISM must be none, "+
			"plain, scram-sha-256 or scram-sha-512, got %q",
			id, envKey(id), c.SASLMechanism)
	}

	re, err := regexp.Compile(c.TopicFilter)
	if err != nil {
		return Cluster{}, fmt.Errorf("config: cluster %q: KAFKA_%s_TOPIC_FILTER is not a valid regexp: %w",
			id, envKey(id), err)
	}
	c.topicRe = re
	return c, nil
}

// resolveLegacyCluster picks the cluster that owns the unprefixed routes.
func resolveLegacyCluster(requested string, clusters []Cluster) (string, error) {
	if len(clusters) == 0 {
		return "", nil
	}
	if requested != "" {
		id := strings.ToLower(requested)
		for _, c := range clusters {
			if c.ID == id {
				return c.ID, nil
			}
		}
		return "", fmt.Errorf("config: KAFKA_LEGACY_CLUSTER %q is not one of the configured clusters", requested)
	}
	for _, c := range clusters {
		if c.ID == defaultClusterID {
			return c.ID, nil
		}
	}
	// A fresh multi-cluster install has no "default": the first declared
	// cluster keeps the old monitor paths working until they are updated.
	return clusters[0].ID, nil
}

func pick(override, shared string) string {
	if override != "" {
		return override
	}
	return shared
}

func pickBool(override *bool, shared bool) bool {
	if override != nil {
		return *override
	}
	return shared
}

func normalizeBasePath(v string) string {
	base := strings.TrimSpace(v)
	if base == "" {
		return "/"
	}
	if !strings.HasPrefix(base, "/") {
		base = "/" + base
	}
	base = strings.TrimRight(base, "/")
	if base == "" {
		return "/"
	}
	return base
}

// SplitBrokers trims and filters empty entries from a comma-separated seed list.
func SplitBrokers(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}
