package main

import (
	"log/slog"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/config"
	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
)

// newKafkaClient wires one cluster's admin client from its config.
// Credentials flow env → kgo SASL exactly once per cluster; they are never
// logged. timeout comes from the global KAFKA_TIMEOUT.
func newKafkaClient(c config.Cluster, timeout time.Duration, log *slog.Logger) (*kafka.Client, error) {
	clientLog := func(msg string, args ...any) {
		log.Debug(msg, append([]any{"cluster", c.ID}, args...)...)
	}
	return kafka.NewClient(kafka.ClientOptions{
		Brokers:   c.Brokers,
		Timeout:   timeout,
		TLS:       c.TLS.Enabled,
		CAFile:    c.TLS.CAFile,
		CAInline:  c.TLS.CA,
		Mechanism: c.SASLMechanism,
		Username:  c.SASLUsername,
		Cred:      c.SASLCred,
		Logger:    clientLog,
	})
}
