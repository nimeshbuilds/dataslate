package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"dataslate.local/api/internal/platform"
)

func main() {
	if err := run(); err != nil {
		slog.Error("DataSlate workflow controller stopped", "reason", err.Error())
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if len(os.Args) > 1 {
		return errors.New("dataslate-workflow-controller accepts configuration through environment only")
	}
	dsn := os.Getenv("DATASLATE_DATABASE_URL")
	if dsn == "" {
		return errors.New("DATASLATE_DATABASE_URL is required")
	}
	bindingsFile := os.Getenv("DATASLATE_WORKFLOW_BINDINGS_FILE")
	if bindingsFile == "" {
		return errors.New("DATASLATE_WORKFLOW_BINDINGS_FILE is required")
	}
	bindings, err := platform.LoadWorkflowBindings(bindingsFile)
	if err != nil {
		return err
	}
	_ = bindings
	backend := &platform.FileWorkflowBackend{BindingsFile: bindingsFile}
	startup, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	db, err := platform.OpenPostgres(startup, dsn, true)
	if err != nil {
		return errors.New("workflow database connection failed")
	}
	defer db.Close()
	if err = db.Ready(startup); err != nil {
		return errors.New("workflow database schema is not ready; run migrations first")
	}
	identity := make([]byte, 16)
	if _, err = rand.Read(identity); err != nil {
		return errors.New("cannot generate controller lease identity")
	}
	owner := "controller-" + hex.EncodeToString(identity)
	addr := os.Getenv("DATASLATE_WORKFLOW_CONTROLLER_ADDR")
	if addr == "" {
		addr = ":8086"
	}
	if _, _, err = net.SplitHostPort(addr); err != nil {
		return errors.New("DATASLATE_WORKFLOW_CONTROLLER_ADDR must be host:port")
	}
	var lastProgress atomic.Int64
	lastProgress.Store(time.Now().Unix())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		check, done := context.WithTimeout(r.Context(), 2*time.Second)
		defer done()
		if time.Since(time.Unix(lastProgress.Load(), 0)) > 130*time.Second || db.Ready(check) != nil {
			http.Error(w, "controller is not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	slog.Info("DataSlate governed workflow controller started")
	for {
		select {
		case <-ctx.Done():
			shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
			defer finish()
			return server.Shutdown(shutdown)
		case err = <-done:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return errors.New("controller health listener failed")
		case <-ticker.C:
			work, finish := context.WithTimeout(ctx, 100*time.Second)
			_, err = platform.ReconcileWorkflowOnce(work, db, backend, owner)
			finish()
			if err != nil && !errors.Is(err, platform.ErrLeaseLost) {
				slog.Warn("workflow reconciliation could not be persisted; lease will be recovered")
			}
			lastProgress.Store(time.Now().Unix())
		}
	}
}
