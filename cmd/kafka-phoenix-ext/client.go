package main

import (
	"log/slog"
	"strings"

	"github.com/fiztoz/kafka-phoenix-ext/internal/config"
	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
)

// newKafkaClient wires the admin client from environment config.
// Credentials flow env → kgo SASL exactly once; they are never logged.
func newKafkaClient(cfg *config.Config, log *slog.Logger) (*kafka.Client, error) {
	brokers := splitBrokers(cfg.Brokers)
	return kafka.NewClient(kafka.ClientOptions{
		Brokers:   brokers,
		Timeout:   cfg.KafkaTimeout,
		TLS:       cfg.TLSEnabled,
		CAFile:    cfg.TLSCAFile,
		CAInline:  cfg.TLSCA,
		Mechanism: cfg.SASLMechanism,
		Username:  cfg.SASLUsername,
		Cred:      cfg.SASLCred,
		Logger:    func(msg string, args ...any) { log.Debug(msg, args...) },
	})
}

// splitBrokers trims and filters empty entries from KAFKA_BROKERS.
func splitBrokers(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}
