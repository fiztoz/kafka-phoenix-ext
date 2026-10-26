package main

import (
	"context"
	"fmt"

	"github.com/fiztoz/kafka-phoenix-ext/internal/config"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

// Never silently abandon pre-upgrade state or thresholds when cluster IDs
// change. A fresh database may use arbitrary IDs; an existing installation
// must keep its original cluster as default (or explicitly migrate its data
// offline). KAFKA_LEGACY_CLUSTER affects routes, not durable ownership.
func validateLegacyScope(ctx context.Context, cfg *config.Config, st store.Store) error {
	if _, ok := cfg.Lookup("default"); ok {
		return nil
	}
	scope := st.Scoped("default")
	states, err := scope.States(ctx)
	if err != nil {
		return fmt.Errorf("check legacy state: %w", err)
	}
	thresholds, err := scope.Thresholds(ctx)
	if err != nil {
		return fmt.Errorf("check legacy thresholds: %w", err)
	}
	if len(states) != 0 || len(thresholds) != 0 {
		return fmt.Errorf("stored data exists for unconfigured cluster default; retain default in KAFKA_CLUSTERS and set KAFKA_DEFAULT_BROKERS to the original brokers; KAFKA_LEGACY_CLUSTER only selects routes")
	}
	return nil
}
