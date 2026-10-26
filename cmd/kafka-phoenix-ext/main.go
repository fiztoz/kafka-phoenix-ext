// Command kafka-phoenix-ext is the Uptime Phoenix Kafka extension: it polls
// one or more Kafka clusters' log-dir sizes via the Kafka admin protocol,
// renders a per-topic storage dashboard/wallboard, and exposes per-cluster
// /health/* endpoints for Phoenix HTTP monitors.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafkaext "github.com/fiztoz/kafka-phoenix-ext"
	"github.com/fiztoz/kafka-phoenix-ext/internal/config"
	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/poller"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kafka-phoenix-ext:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err // bad env: crash fast, this is unrecoverable
	}

	level := slog.LevelInfo
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	st, err := openStore(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := st.Migrate(ctx); err != nil {
		_ = st.Close()
		return fmt.Errorf("migrate: %w", err)
	}

	if err := validateLegacyScope(ctx, cfg, st); err != nil {
		_ = st.Close()
		return err
	}

	// One admin client and one poller per cluster. A cluster that is
	// unreachable at startup must not stop the others: the poller keeps the
	// last good sample and /health/c/{cluster}/ready reports 503 for it alone.
	clients := make([]*kafka.Client, 0, len(cfg.Clusters))
	pollers := make([]*poller.Poller, 0, len(cfg.Clusters))
	closeAll := func() {
		for _, c := range clients {
			c.Close()
		}
		_ = st.Close()
	}
	for _, cluster := range cfg.Clusters {
		client, err := newKafkaClient(cluster, cfg.KafkaTimeout, log)
		if err != nil {
			closeAll()
			return fmt.Errorf("cluster %s: %w", cluster.ID, err)
		}
		clients = append(clients, client)
		pollers = append(pollers, poller.New(ctx, cluster.ID, client, st.Scoped(cluster.ID),
			pollerOptions(cluster), poller.SkewPolicy{
				SharePct: cfg.BrokerSkewPct,
				MaxBytes: cfg.BrokerMaxBytes,
			}, cfg.PollInterval, log))
	}
	manager := poller.NewManager(cfg.PollInterval, pollers...)
	go manager.Run(ctx)

	srv, err := newHTTPServer(cfg, manager, st, log)
	if err != nil {
		closeAll()
		return err
	}

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	httpErr := make(chan error, 1)
	go func() {
		log.Info("kafka-phoenix-ext listening", "addr", cfg.ListenAddr,
			"base_path", cfg.BasePath, "clusters", manager.Clusters(),
			"legacy_cluster", cfg.LegacyCluster)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	select {
	case err := <-httpErr:
		stop()
		shutdown(httpServer, clients, st, log)
		return fmt.Errorf("http: %w", err) // cannot bind: crash
	case <-ctx.Done():
	}

	shutdown(httpServer, clients, st, log)
	log.Info("kafka-phoenix-ext stopped")
	return nil
}

func shutdown(httpServer *http.Server, clients []*kafka.Client, st store.Store, log *slog.Logger) {
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := httpServer.Shutdown(shCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	cancel()
	for _, c := range clients {
		c.Close()
	}
	if err := st.Close(); err != nil {
		log.Warn("store close", "err", err)
	}
}

func openStore(cfg *config.Config) (store.Store, error) {
	switch cfg.DatabaseEngine {
	case "sqlite":
		return store.OpenSQLite(cfg.DatabaseDSN, kafkaext.Migrations)
	default:
		return store.OpenMariaDB(cfg.DatabaseDSN, kafkaext.Migrations)
	}
}
