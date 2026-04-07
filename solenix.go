// Package solenix — public API for solenix-core TSDB.
// Implementation is split across internal sub-packages;
// this file re-exports everything so external consumers need no import changes.
package solenix

import (
	"github.com/bbvtaev/solenix/internal/config"
	"github.com/bbvtaev/solenix/internal/model"
	"github.com/bbvtaev/solenix/internal/storage"
)

type (
	DB              = storage.DB
	Point           = model.Point
	WriteSeries     = model.WriteSeries
	SeriesResult    = model.SeriesResult
	AggType         = model.AggType
	AggPoint        = model.AggPoint
	AggResult       = model.AggResult
	Config          = config.Config
	CollectorConfig = model.CollectorConfig
)

const (
	Version = model.Version

	AggAvg   AggType = model.AggAvg
	AggMin   AggType = model.AggMin
	AggMax   AggType = model.AggMax
	AggSum   AggType = model.AggSum
	AggCount AggType = model.AggCount
)

// ParseAggType парсит строку в AggType ("avg", "min", "max", "sum", "count").
func ParseAggType(s string) (AggType, error) { return model.ParseAggType(s) }

// Open открывает (или создаёт) БД согласно конфигу.
func Open(cfg Config) (*DB, error) { return storage.Open(cfg) }

// LoadConfig читает YAML файл и возвращает Config.
func LoadConfig(path string) (Config, error) { return config.LoadConfig(path) }

// DefaultConfig возвращает конфиг со значениями по умолчанию.
func DefaultConfig() Config { return config.DefaultConfig() }
