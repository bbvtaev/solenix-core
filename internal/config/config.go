package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bbvtaev/solenix/internal/model"
	"gopkg.in/yaml.v3"
)

// Config holds runtime parameters for the database and servers.
// DataDir is fixed at ~/.solenix/data and is not user-configurable via YAML;
// it can be overridden programmatically (e.g. in tests).
type Config struct {
	// Storage
	DataDir             string        // fixed: ~/.solenix/data; overridable in tests
	Database            string        `yaml:"database"`           // database name; default "default"
	WALMaxSize          int64         `yaml:"wal_max_size"`       // bytes; default 32 MiB
	RetentionDuration   time.Duration `yaml:"retention"`
	FlushInterval       time.Duration `yaml:"flush_interval"`     // chunk flush interval; default 2m
	CompactionThreshold int           `yaml:"compaction_threshold"` // chunk files per metric before compaction; default 10

	// Server
	GRPCAddr string `yaml:"grpc_addr"`
	HTTPAddr string `yaml:"http_addr"`

	// Collector
	Collector model.CollectorConfig `yaml:"collector"`
}

// rawConfig is an intermediate struct for YAML parsing.
// Durations are stored as strings and converted to time.Duration.
type rawConfig struct {
	Database      string `yaml:"database"`
	WALMaxSize    int64  `yaml:"wal_max_size"`
	Retention     string `yaml:"retention"`
	FlushInterval string `yaml:"flush_interval"`
	GRPCAddr      string `yaml:"grpc_addr"`
	HTTPAddr      string `yaml:"http_addr"`
	Collector     struct {
		Enabled  bool   `yaml:"enabled"`
		Interval string `yaml:"interval"`
	} `yaml:"collector"`
}

// LoadConfig reads a YAML file and returns a Config.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}

	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg := DefaultConfig()

	if raw.Database != "" {
		cfg.Database = raw.Database
	}
	if raw.WALMaxSize > 0 {
		cfg.WALMaxSize = raw.WALMaxSize << 20 // MiB → bytes
	}
	if raw.GRPCAddr != "" {
		cfg.GRPCAddr = raw.GRPCAddr
	}
	if raw.HTTPAddr != "" {
		cfg.HTTPAddr = raw.HTTPAddr
	}
	if raw.Retention != "" {
		d, err := time.ParseDuration(raw.Retention)
		if err != nil {
			return Config{}, fmt.Errorf("retention %q: %w", raw.Retention, err)
		}
		cfg.RetentionDuration = d
	}
	if raw.FlushInterval != "" {
		d, err := time.ParseDuration(raw.FlushInterval)
		if err != nil {
			return Config{}, fmt.Errorf("flush_interval %q: %w", raw.FlushInterval, err)
		}
		cfg.FlushInterval = d
	}
	cfg.Collector.Enabled = raw.Collector.Enabled
	if raw.Collector.Interval != "" {
		d, err := time.ParseDuration(raw.Collector.Interval)
		if err != nil {
			return Config{}, fmt.Errorf("collector.interval %q: %w", raw.Collector.Interval, err)
		}
		cfg.Collector.Interval = d
	}

	return cfg, nil
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		DataDir:             defaultDataDir(),
		Database:            "default",
		WALMaxSize:          32 << 20, // 32 MiB
		GRPCAddr:            ":8731",
		HTTPAddr:            ":8080",
		FlushInterval:       2 * time.Minute,
		CompactionThreshold: 10,
		Collector: model.CollectorConfig{
			Enabled:  true,
			Interval: 15 * time.Second,
		},
	}
}

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".solenix", "data")
	}
	return filepath.Join(home, ".solenix", "data")
}
