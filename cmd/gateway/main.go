package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adrianliechti/sns-gateway/backend"
	"github.com/adrianliechti/sns-gateway/backend/memory"
	"github.com/adrianliechti/sns-gateway/backend/nats"
	"github.com/adrianliechti/sns-gateway/gateway"
	"github.com/adrianliechti/sns-gateway/internal/config"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, flag.ErrHelp) {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	c, err := config.Parse(os.Args[1:], os.Getenv, os.Stderr)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if c.Debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var be backend.Backend
	if c.Backend == "memory" {
		be = memory.New()
	} else {
		startup, cancel := context.WithTimeout(ctx, 15*time.Second)
		be, err = nats.New(startup, nats.Options{URL: c.NATSURL, Namespace: c.NATSNamespace, CredentialsFile: c.NATSCredentials, Token: c.NATSToken, Replicas: c.NATSReplicas, MaxBytes: c.NATSMaxBytes, Logger: logger})
		cancel()
		if err != nil {
			return err
		}
	}
	defer be.Close()
	g, err := gateway.New(be, gateway.Options{Region: c.Region, AccountID: c.AccountID, AccessKey: c.AccessKey, SecretKey: c.SecretKey, PublicURL: c.PublicURL, Logger: logger})
	if err != nil {
		return err
	}
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()
	workersDone := make(chan error, 1)
	go func() { workersDone <- g.Run(workerCtx) }()
	srv := &http.Server{Addr: c.Listen, Handler: g, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 * 1024}
	serverDone := make(chan error, 1)
	go func() {
		if c.CertFile != "" {
			serverDone <- srv.ListenAndServeTLS(c.CertFile, c.KeyFile)
		} else {
			serverDone <- srv.ListenAndServe()
		}
	}()
	slog.Info("starting SNS gateway", "backend", c.Backend, "listen", c.Listen, "region", c.Region)
	select {
	case <-ctx.Done():
	case err = <-serverDone:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := srv.Shutdown(shutdown); shutdownErr != nil {
		_ = srv.Close()
		if err == nil {
			err = shutdownErr
		}
	}
	cancelWorkers()
	<-workersDone
	logger.Debug("SNS event", "event", "gateway.stopped")
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
