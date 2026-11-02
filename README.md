# LRCLIB

## Introduction

LRCLIB server written in Rust with Axum and SQLite3 database.

This is the source code of [lrclib.net](https://lrclib.net) rewritten in Rust from scratch. LRCLIB is a completely free service for finding and contributing synchronized lyrics, with an easy-to-use and machine-friendly APIs.

## Setup

Build the project:
```
cargo build --release
```

Run the server:

```
LRCLIB_LOG=info cargo run --release -- serve --database db.sqlite3
```

Server will be available at http://0.0.0.0:3300

## Database configuration

You have three environment variables available to tweak the database connection:

* `LRCLIB_MMAP_SIZE` (default `30000000000` - 30GB): set [SQLite's `mmap_size`](https://www.sqlite.org/mmap.html) parameter. This should be less than 75% of the available RAM. Using an excessive value might cause memory swapping, decreasing performance.
* `LRCLIB_CACHE_SIZE` (default `-1000000` - 1GB): set [SQLite's `cache_size`](https://sqlite.org/pragma.html#pragma_cache_size) parameter. Negative values are kilobytes.
* `LRCLIB_POOL_MAX_SIZE` (default `16`): set the maximum number of SQLite connections managed by the r2d2 pool. The value must be a positive integer.

Use these variables to optimize memory usage in low traffic and/or low memory situations. The default values are generously sized to be used on a production system.

## Mirroring

LRCLIB can publish its database for mirrors and can run as a mirror of another server. The model is the same as OpenStreetMap's: full dumps every week, plus hourly diffs that are aggregated into daily diffs.

### Publishing

* `LRCLIB_MIRROR_PUBLISH_DIR`: enables publishing. Dumps and diffs are written here.
* `LRCLIB_MIRROR_DIFF_PERIODS` (default `1h,1d`): comma-separated diff periods, finest first. Each period must be a larger multiple of the previous one (e.g. `1m,1h,1d`). Units: `s`, `m`, `h`, `d`, `w`.
* `LRCLIB_MIRROR_DUMP_PERIOD` (default `7d`): how often a full dump is written.
* `LRCLIB_MIRROR_KEEP_DUMPS` (default `1`): number of dumps kept on disk; `0` keeps all.
* `LRCLIB_MIRROR_SERVE` (default `true`): serve the published `dumps/` and `replication/` subtrees at `/mirror/`. Set to `false` to serve them with a separate web server or CDN instead; if you do, keep `state.json`/`latest.json` uncached (this server sends `Cache-Control: no-cache` for them) and do not expose other files in the directory.

Periods are aligned to UTC (hourly on the hour, daily at midnight; `7d` falls on Thursdays). Writing a dump temporarily needs free disk space for an uncompressed copy of the database. Diff files are never deleted (as with OpenStreetMap), so the publish directory grows over time; only dumps are rotated. Only one server may publish from a given database or into a given directory; a second one fails at startup.

### Following another server

* `LRCLIB_MIRROR_UPSTREAM`: base URL of the upstream mirror tree, e.g. `https://lrclib.example/mirror`, or a local directory containing it.
* `LRCLIB_MIRROR_UPSTREAM_STREAM` (default `1h`): which upstream diff period to follow.
* `LRCLIB_MIRROR_UPSTREAM_POLL` (default `5m`): how often to check for new diffs.

If the database file does not exist, it is downloaded from the upstream's latest dump on startup. Afterwards new diffs are applied automatically. A following server is read-only: publishing and flagging return `403`.

```shell
LRCLIB_MIRROR_UPSTREAM=https://lrclib.example/mirror cargo run --release -- serve --database mirror.sqlite3
```

Bootstrapping needs about twice the dump size of free disk space next to the database file (the download plus the restored database). The search index is rebuilt after bootstrapping.

`LRCLIB_MIRROR_UPSTREAM_STREAM` must match one of the upstream's `LRCLIB_MIRROR_DIFF_PERIODS`; if the stream does not exist, the mirror logs a warning and keeps waiting. Coarser streams appear only after their first period boundary, and switching an existing mirror to a stream the upstream added later requires re-bootstrapping.

If the upstream starts a new replication epoch (for example after it disabled and re-enabled publishing), or its database is restored to an earlier state, the mirror logs an error and stops following while it keeps serving its current data. To re-bootstrap from a fresh dump, delete the database file and restart.

The `backfill-*` maintenance commands refuse to modify a mirror's database; run them on the upstream instead.

## Setup with Podman/Docker

### Basic

Build the image:

```
podman build -t lrclib-oss:latest -f Dockerfile .
```

Run the container:

```
podman run --rm -it -d -v lrclib-data:/data -p 3300:3300 -e LRCLIB_LOG=info --name lrclib-oss lrclib-oss:latest
```

Server will be available at http://0.0.0.0:3300

### Access the SQLite database

Run the following command to directly interact with the database in command line:

```
podman run --rm -it -v lrclib-data:/data lrclib-oss:latest sqlite3 /data/db.sqlite3
```

### Quadlet

You can use Quadlet to run the Podman container in the background. It also handles auto-start the container after machine restart for you.

Create a file named `lrclib.container` at `~/.config/containers/systemd/`:

```
mkdir -p $HOME/.config/containers/systemd/
vi $HOME/.config/containers/systemd/lrclib.container
```

The example content of `lrclib.container`:

```
[Container]
Image=lrclib-oss:latest
PublishPort=3300:3300
Volume=lrclib-data:/data
ContainerName=lrclib-oss
Environment=LRCLIB_LOG=info

[Service]
Restart=always

[Install]
WantedBy=multi-user.target default.target
```

Reload the daemon:

```
systemctl --user daemon-reload
```

Start the service:

```
systemctl --user start lrclib.service
```

Check the status to see if `lrclib.service` is actually running:

```
systemctl --user status lrclib.service
```

Restart when the container image is updated:

```
systemctl --user restart lrclib.service
```

## Donation

Toss a coin to your developer?

**GitHub Sponsors (Recommended - 100% of your support goes to the developer):**

https://github.com/sponsors/tranxuanthang

**Buy Me a Coffee:**

https://www.buymeacoffee.com/thangtran

**Paypal:**

https://paypal.me/tranxuanthang98

**Monero (XMR):**

```
43ZN5qDdGQhPGthFnngD8rjCHYLsEFBcyJjDC1GPZzVxWSfT8R48QCLNGyy6Z9LvatF5j8kSgv23DgJpixJg8bnmMnKm3b7
```

**Litecoin (LTC):**

```
ltc1q7texq5qsp59gclqlwf6asrqmhm98gruvz94a48
```

## Contact

If you prefer to contact by email:

[hoangtudevops@protonmail.com](mailto:hoangtudevops@protonmail.com)
