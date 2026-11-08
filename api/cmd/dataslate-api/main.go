package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dataslate.local/api/internal/kubeexec"
	"dataslate.local/api/internal/platform"
)

func main() {
	if err := run(); err != nil {
		slog.Error("DataSlate API stopped", "reason", err.Error())
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if len(os.Args) > 1 && (os.Args[1] == "migrate" || os.Args[1] == "--migrate") {
		dsn := os.Getenv("DATASLATE_DATABASE_URL")
		if dsn == "" {
			return errors.New("DATASLATE_DATABASE_URL is required for migration")
		}
		db, err := platform.OpenPostgres(ctx, dsn, true)
		if err != nil {
			return err
		}
		defer db.Close()
		deadline, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err = db.Migrate(deadline); err != nil {
			return errors.New("database migration failed; inspect PostgreSQL health and schema ownership")
		}
		slog.Info("DataSlate schema migrations applied")
		return nil
	}
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		return errors.New("usage: dataslate-api [serve|migrate]")
	}
	cfg, err := platform.LoadConfig()
	if err != nil {
		return err
	}
	var store platform.Store
	var auth platform.Authenticator
	if cfg.AuthMode == "development" {
		store = platform.NewMemoryStore()
		auth = platform.DevelopmentAuth{}
		slog.Warn("Explicit development mode: local administrator with ephemeral in-memory state")
	} else {
		db, err := platform.OpenPostgres(ctx, cfg.DatabaseURL, true)
		if err != nil {
			return err
		}
		defer db.Close()
		store = db
		auth, err = platform.NewJWTAuth(ctx, cfg)
		if err != nil {
			return errors.New("OIDC discovery failed; verify issuer availability and configuration")
		}
	}
	api := &platform.Server{Config: cfg, Store: store, Auth: auth}
	if cfg.ExecutionBindingsFile != "" {
		backend, err := kubeexec.New(kubeexec.ConfigFromEnv())
		if err != nil {
			return err
		}
		if err = backend.ValidateBindings(cfg.ExecutionBindings); err != nil {
			return err
		}
		check, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = backend.Discover(check)
		cancel()
		if err != nil {
			return errors.New("Apache Spark v1 discovery failed for configured execution bindings")
		}
		api.Backend = backend
	}
	server := &http.Server{Addr: cfg.Addr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 25 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 * 1024}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	slog.Info("DataSlate API started", "address", cfg.Addr, "authMode", cfg.AuthMode, "persistence", store.Mode(), "executionEnabled", len(cfg.ExecutionBindings) > 0)
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
