<h1  align="center">Solenix v6.2.1-alpha</h1>

## Configuration

Solenix is configured via a YAML file passed with `--config`:

```bash
solenix serve --config solenix.yaml
```

If no config is provided, all defaults are used.

### Full reference

```yaml
# Database name. Data is stored at ~/.solenix/data/<database>/.
# Switch between databases by changing this value.
# Default: "default"
database: "default"

# gRPC server address.
# Default: ":8731"
grpc_addr: ":8731"

# HTTP server address (UI + REST API). Set to "" to disable.
# Default: ":8080"
http_addr: ":8080"

# WAL segment size in MiB. When a segment reaches this size it is
# rotated and flushed to chunk files immediately.
# Default: 32
wal_max_size: 32

# How often WAL data is flushed to immutable chunk files.
# Accepts Go duration strings: "30s", "2m", "1h".
# Default: "2m"
flush_interval: "2m"

# Number of chunk files per metric that triggers a compaction.
# Default: 10
compaction_threshold: 10

# How long data is retained. Points older than this are deleted automatically.
# Accepts Go duration strings: "24h", "720h", "8760h".
# Omit or set to "0s" to retain data forever.
# Default: not set (retain forever)
retention: "720h"

# Built-in system metrics collector (CPU, memory, disk, network).
collector:
  # Default: true
  enabled: true
  # Collection interval. Default: "15s"
  interval: "15s"
```

### Data directory

Data is always stored at `~/.solenix/data/<database>/` and cannot be changed via config.
Each database directory contains:

```
~/.solenix/data/
├── VERSION              # on-disk format version
└── default/
    ├── .lock            # prevents opening the same database twice
    ├── wal/             # write-ahead log segments (000001.wal, ...)
    └── chunks/          # immutable time-series chunk files
```
