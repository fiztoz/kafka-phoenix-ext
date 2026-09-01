// Command kafka-phoenix-ext is the Uptime Phoenix Kafka extension: it polls
// cluster log-dir sizes via the Kafka admin protocol, renders a per-topic
// storage dashboard/wallboard, and exposes /health/ready + /health/thresholds
// for Phoenix HTTP monitors.
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

	client, err := newKafkaClient(cfg, log)
	if err != nil {
		_ = st.Close()
		return err
	}

	// Kafka may be down at startup: never crash-loop for that. HTTP comes up,
	// /health/live is 200 and /health/ready is 503 until a poll succeeds.
	p := poller.New(ctx, client, st, pollerOptions(cfg), poller.SkewPolicy{
		SharePct: cfg.BrokerSkewPct,
		MaxBytes: cfg.BrokerMaxBytes,
	}, cfg.PollInterval, log)
	go p.Run(ctx)

	srv, err := newHTTPServer(cfg, p, st, log)
	if err != nil {
		_ = st.Close()
		client.Close()
		return err
	}

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	httpErr := make(chan error, 1)
	go func() {
		log.Info("kafka-phoenix-ext listening", "addr", cfg.ListenAddr, "base_path", cfg.BasePath)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	select {
	case err := <-httpErr:
		stop()
		shutdown(httpServer, client, st, log)
		return fmt.Errorf("http: %w", err) // cannot bind: crash
	case <-ctx.Done():
	}

	shutdown(httpServer, client, st, log)
	log.Info("kafka-phoenix-ext stopped")
	return nil
}

func shutdown(httpServer *http.Server, client interface{ Close() }, st store.Store, log *slog.Logger) {
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := httpServer.Shutdown(shCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	cancel()
	client.Close()
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
