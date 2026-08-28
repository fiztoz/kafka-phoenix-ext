package main

import (
	"log/slog"

	"github.com/fiztoz/kafka-phoenix-ext/internal/config"
	srvhttp "github.com/fiztoz/kafka-phoenix-ext/internal/http"
	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/poller"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

// pollerOptions derives the describe filter from environment config.
func pollerOptions(cfg *config.Config) kafka.DescribeOptions {
	return kafka.DescribeOptions{
		IncludeInternal: cfg.IncludeInternal,
		TopicAllowed:    cfg.TopicAllowed,
	}
}

// newHTTPServer wires the dashboard/API server deps from environment config.
func newHTTPServer(cfg *config.Config, p *poller.Poller, st store.Store, log *slog.Logger) (*srvhttp.Server, error) {
	deps := srvhttp.Deps{
		BasePath:  cfg.BasePath,
		Snapshots: p,
		Store:     st,
		Log:       log,
	}
	deps.UIToken = cfg.UIToken
	return srvhttp.New(deps)
}
