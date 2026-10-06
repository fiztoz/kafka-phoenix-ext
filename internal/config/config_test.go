package config

import "testing"

func TestLoadAcceptsDisposableChartSecrets(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka-0.kafka.svc:9092")
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-512")
	t.Setenv("KAFKA_SASL_USERNAME", "kafka-usage")
	t.Setenv("KAFKA_SASL_PASSWORD", "disposable-not-secret")
	t.Setenv("DATABASE_DSN", "kafka_usage:disposable-not-secret@tcp(127.0.0.1:1)/kafka_usage")
	t.Setenv("KAFKA_POLL_INTERVAL", "1m")
	t.Setenv("KAFKA_TIMEOUT", "45s")
	t.Setenv("KAFKA_BROKER_SKEW_PCT", "60")
	t.Setenv("BASE_PATH", "/kafka")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("documented secret wiring failed validation: %v", err)
	}
	if cfg.DatabaseDSN == "" || cfg.SASLCred == "" || cfg.SASLMechanism != "scram-sha-512" {
		t.Fatalf("config did not keep disposable DSN/SASL values: mechanism=%q", cfg.SASLMechanism)
	}
}

func TestLoadRejectsSASLWithoutPassword(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka-0.kafka.svc:9092")
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-512")
	t.Setenv("KAFKA_SASL_USERNAME", "kafka-usage")
	t.Setenv("KAFKA_SASL_PASSWORD", "")
	t.Setenv("DATABASE_DSN", "kafka_usage:disposable-not-secret@tcp(127.0.0.1:1)/kafka_usage")
	t.Setenv("KAFKA_POLL_INTERVAL", "1m")
	t.Setenv("KAFKA_TIMEOUT", "45s")
	t.Setenv("KAFKA_BROKER_SKEW_PCT", "60")

	if _, err := Load(); err == nil {
		t.Fatal("SASL without a password must fail startup validation")
	}
}
