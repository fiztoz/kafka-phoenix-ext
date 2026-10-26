package main

import (
	"log/slog"

	"github.com/fiztoz/kafka-phoenix-ext/internal/config"
	srvhttp "github.com/fiztoz/kafka-phoenix-ext/internal/http"
	"github.com/fiztoz/kafka-phoenix-ext/internal/poller"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

// newHTTPServer wires the dashboard/API server deps from environment config.
func newHTTPServer(cfg *config.Config, m *poller.Manager, st store.Store, log *slog.Logger) (*srvhttp.Server, error) {
	return srvhttp.New(srvhttp.Deps{
		BasePath:      cfg.BasePath,
		UIToken:       cfg.UIToken,
		Clusters:      m,
		Store:         st,
		Log:           log,
		LegacyCluster: cfg.LegacyCluster,
	})
}
