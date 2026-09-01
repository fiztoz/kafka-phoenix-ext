// Package config loads kafka-phoenix-ext configuration from the environment.
package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config is the full environment surface of kafka-phoenix-ext.
type Config struct {
	Brokers         string `env:"KAFKA_BROKERS,required"` // comma-separated seeds, e.g. "kafka-0:9092,kafka-1:9092"
	SASLMechanism   string `env:"KAFKA_SASL_MECHANISM" envDefault:"none"`
	SASLUsername    string `env:"KAFKA_SASL_USERNAME"` // required when mechanism != none
	SASLCred        string `env:"KAFKA_SASL_PASSWORD"` // from Secret; never logged
	TopicFilter     string `env:"KAFKA_TOPIC_FILTER" envDefault:".*"`
	IncludeInternal bool   `env:"KAFKA_INCLUDE_INTERNAL" envDefault:"false"`

	TLSEnabled  bool   `env:"KAFKA_TLS_ENABLED" envDefault:"false"`
	TLSCAFile   string `env:"KAFKA_TLS_CA_FILE"` // path on disk; takes precedence over KAFKA_TLS_CA
	TLSCA       string `env:"KAFKA_TLS_CA"`      // inline PEM; no volume support
	TLSInsecure bool   `env:"KAFKA_TLS_INSECURE" envDefault:"false"`

	PollInterval time.Duration `env:"KAFKA_POLL_INTERVAL" envDefault:"15m"`
	KafkaTimeout time.Duration `env:"KAFKA_TIMEOUT" envDefault:"45s"`

	BrokerSkewPct  int   `env:"KAFKA_BROKER_SKEW_PCT" envDefault:"60"` // one broker holding more than this % of cluster bytes is skew
	BrokerMaxBytes int64 `env:"KAFKA_BROKER_MAX_BYTES" envDefault:"0"` // absolute per-broker byte limit; 0 disables

	ListenAddr     string `env:"LISTEN_ADDR" envDefault:":8080"`
	DatabaseDSN    string `env:"DATABASE_DSN,required"` // MariaDB DSN or file:/data/kafka-phoenix-ext.db
	DatabaseEngine string `env:"DATABASE_ENGINE"`       // mariadb | sqlite; inferred when empty
	BasePath       string `env:"BASE_PATH" envDefault:"/kafka"`
	UIToken        string `env:"UI_TOKEN"`
	LogLevel       string `env:"LOG_LEVEL" envDefault:"info"`

	topicRe *regexp.Regexp // compiled KAFKA_TOPIC_FILTER
}

// TopicAllowed reports whether a topic name passes the configured filter.
func (c *Config) TopicAllowed(name string) bool {
	return c.topicRe.MatchString(name)
}

// Load parses the environment and validates locked constraints.
func Load() (*Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	cfg.Brokers = strings.TrimSpace(cfg.Brokers)
	if cfg.Brokers == "" {
		return nil, fmt.Errorf("config: KAFKA_BROKERS must not be empty")
	}
	if strings.ContainsAny(cfg.Brokers, "\n\t") {
		return nil, fmt.Errorf("config: KAFKA_BROKERS must be comma-separated hosts")
	}

	cfg.SASLMechanism = strings.ToLower(strings.TrimSpace(cfg.SASLMechanism))
	switch cfg.SASLMechanism {
	case "none":
	case "plain", "scram-sha-256", "scram-sha-512":
		if strings.TrimSpace(cfg.SASLUsername) == "" || cfg.SASLCred == "" {
			return nil, fmt.Errorf("config: KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required when KAFKA_SASL_MECHANISM is %q", cfg.SASLMechanism)
		}
	default:
		return nil, fmt.Errorf("config: KAFKA_SASL_MECHANISM must be none, plain, scram-sha-256 or scram-sha-512, got %q", cfg.SASLMechanism)
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

	cfg.TopicFilter = strings.TrimSpace(cfg.TopicFilter)
	re, err := regexp.Compile(cfg.TopicFilter)
	if err != nil {
		return nil, fmt.Errorf("config: KAFKA_TOPIC_FILTER is not a valid regexp: %w", err)
	}
	cfg.topicRe = re

	cfg.BasePath = strings.TrimSpace(cfg.BasePath)
	if cfg.BasePath == "" {
		cfg.BasePath = "/"
	}
	if !strings.HasPrefix(cfg.BasePath, "/") {
		cfg.BasePath = "/" + cfg.BasePath
	}
	cfg.BasePath = strings.TrimRight(cfg.BasePath, "/")
	if cfg.BasePath == "" {
		cfg.BasePath = "/"
	}

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

	return &cfg, nil
}
