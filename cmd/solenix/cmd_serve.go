package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	solenix "github.com/bbvtaev/solenix-core"
	"github.com/bbvtaev/solenix-core/collector"
	"github.com/bbvtaev/solenix-core/server"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the solenix-core server",
	RunE:  runServe,
}

var configPath string

func init() {
	serveCmd.Flags().StringVar(&configPath, "config", "", "path to solenix.yaml config file")
}

func runServe(_ *cobra.Command, _ []string) error {
	var cfg solenix.Config
	var err error

	if configPath != "" {
		cfg, err = solenix.LoadConfig(configPath)
		if err != nil {
			return err
		}
		slog.Info("loaded config", "path", configPath)
	} else {
		cfg = solenix.DefaultConfig()
		slog.Info("using default config")
	}

	db, err := solenix.Open(cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	slog.Info("solenix-core started",
		"mode", cfg.Mode,
		"data_dir", cfg.DataDir,
		"grpc_addr", cfg.GRPCAddr,
		"retention", cfg.RetentionDuration,
		"auth", cfg.Auth.Enabled,
	)

	srv := server.New(db)
	go func() {
		slog.Info("gRPC server listening", "addr", cfg.GRPCAddr)
		if err := srv.Listen(cfg.GRPCAddr); err != nil {
			slog.Error("gRPC server error", "err", err)
			os.Exit(1)
		}
	}()

	if cfg.Mode == solenix.ModeSelfHosted && cfg.HTTPAddr != "" {
		httpSrv := server.NewHTTP(db, cfg)
		go func() {
			slog.Info("UI available", "url", "http://localhost"+cfg.HTTPAddr)
			if err := httpSrv.ListenHTTP(cfg.HTTPAddr); err != nil {
				slog.Error("HTTP server error", "err", err)
			}
		}()
	}

	if cfg.Collector.Enabled {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := collector.New(db, cfg.Collector)
		go c.Run(ctx)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	slog.Info("shutting down", "signal", sig)
	return nil
}
