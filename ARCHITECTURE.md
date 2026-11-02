# Architecture

## Overview

Rust workspace for the LRCLIB API server.

- Root crate: thin CLI wrapper in `src/main.rs`.
- Main app: `server/` crate.
- Storage: SQLite via `rusqlite` + `r2d2`.
- HTTP: Axum.

Most real behavior lives in `server/`.

This is the public self-hosted product.

## Workspace Layout

- `src/main.rs`: CLI dispatch for `serve` and one-shot maintenance commands.
- `src/backfill_lock.rs`: shared exclusive process lock keyed by database identity, used by the maintenance backfills.
- `src/backfill_lyricsfiles.rs`: resumable Lyricsfile backfill orchestration, throttling, and dry-run reporting.
- `src/backfill_word_sync.rs`: idempotent word-sync backfill sweep with `instr` prefiltering, dry-run sampling, progress reporting, and row limits.
- `server/src/lib.rs`: thin crate facade that declares modules and re-exports the public server API.
- `server/src/bootstrap.rs`: server startup, listener lifecycle, graceful shutdown, and runtime composition.
- `server/src/metrics.rs`: bounded in-memory aggregate and route-specific latency histograms and interval counters.
- `server/src/state.rs`: `AppState` and construction of shared caches and resources.
- `server/src/router.rs`: Axum route registration, request tracing, metrics middleware, and CORS.
- `server/src/background.rs`: request metrics, recent lyrics counter, and daily PRAGMA optimize tasks.
- `server/src/routes/*.rs`: HTTP handlers.
- `server/src/repositories/*.rs`: SQL access layer.
- `server/src/entities/*.rs`: small DB/domain structs.
- `server/src/lyricsfile.rs`: shared Lyricsfile parsing, legacy LRC conversion, YAML generation, and metadata retargeting used by the publish path.
- `server/src/search_index.rs` + `server/src/search_index/*.rs`: embedded Tantivy search index over track metadata (schema, query building, polling indexer).
- `server/src/mirror.rs` + `server/src/mirror/*.rs`: OSM-style replication (configuration and on-disk format, changelog triggers, diff writing/merging/applying, dumps, publisher schedule, follower).
- `server/src/utils.rs`: input normalization, publish-token verification, cache helpers.
- `server/migrations/*`: embedded SQLite schema/migrations, applied on startup.
- `TANTIVY_SEARCH_PLAN.md`: implementation and rollout record for replacing the `/api/search` FTS5 query path with an embedded Tantivy index.

## Runtime Flow

Server startup in `server::serve`, implemented in `server/src/bootstrap.rs`:

- init tracing from `LRCLIB_LOG`
- parse mirror configuration (`LRCLIB_MIRROR_*`); when following an upstream and the database file does not exist, restore it from the upstream's latest dump
- open SQLite pool
- apply PRAGMAs + migrations
- when following, require the database to carry an upstream position; install or remove mirror change capture to match publishing configuration (see Mirroring)
- bootstrap the Tantivy search index + indexer thread; failures leave `/api/search` unavailable while other routes continue serving
- build shared `AppState`
- spawn background tasks
- build Axum router
- serve on `0.0.0.0:<port>` with graceful shutdown
- after shutdown, stop the search indexer with a final commit

Background tasks:

- request metrics reporter: once per minute logs aggregate HTTP metrics plus `/api/search` and `/api/get` request, cache, failure, and latency metrics
- recent lyrics counter: refreshes `last 10 minutes` publish count once/minute in `spawn_blocking`, so the indexed count query does not run on an async worker
- daily `PRAGMA optimize`: waits 24h after startup (the startup `PRAGMA optimize=0x10002` already runs during `init_db`), then runs `PRAGMA optimize` once/day with `analysis_limit = 400` inside `spawn_blocking`, so maintenance does not block the async runtime or scan entire indexes on large databases
- search indexer: dedicated OS thread that tails the `tracks` table into the Tantivy index (see Search Index)
- mirror publisher (when enabled): one task writing due diffs at each base-period boundary and one independent task writing dumps, both in `spawn_blocking`
- mirror follower (when enabled): polls the upstream diff stream and applies new diffs in `spawn_blocking`

## AppState

`AppState`, defined in `server/src/state.rs`, is the shared runtime container.

- `pool`: SQLite connection pool.
- `search_index`: optional embedded Tantivy index (`None` when bootstrap fails); `/api/search` requires it to be ready.
- `challenge_cache`: proof-of-work publish challenges, TTL 5 min.
- `get_metadata_cache`: `Cache<String, Option<TrackResponse>>` with 24h TTL / 4h idle; stores both hits and misses. Uses `try_get_with` for coalescing concurrent requests.
- `search_cache`: cached `/api/search` responses, 24h TTL, 4h idle timeout.
- `load_permits` / `load_queue_timeout`: bounded admission for blocking-load tasks; configurable via `LRCLIB_MAX_CONCURRENT_LOADS` (default 32) and `LRCLIB_LOAD_QUEUE_MS` (default 250). `GET /api/search`, `GET /api/get`, and `GET /api/get/:track_id` return `503 + Retry-After: 1` when the load budget is exhausted.

- Moka caches use AHash for lower-overhead in-memory key hashing.
- `http_metrics`: aggregate HTTP metrics plus `/api/search` and metadata `/api/get` request, cache hit/miss, failure, route-latency, and backend-stage histograms; the reporter snapshots and resets these once per minute.
- `recent_lyrics_count`: recent publish-volume metric used to calculate challenge difficulty.
- `read_only`: set when following an upstream mirror; `POST /api/request-challenge`, `/api/publish`, and `/api/flag` then return `403 ReadOnlyMirror`.

## Layering

There is no separate service layer.

- route handlers validate + normalize input
- handlers read/write caches when needed
- handlers call repository functions directly
- repositories contain SQL and row mapping

If you need API behavior changes, start in `routes/`.
If you need query/schema changes, start in `repositories/` and `migrations/`.

## API Surface

Routes are mounted under `/api` in `server/src/router.rs`.

- `GET /api/get`: lookup by metadata.
- `GET /api/get/:track_id`: lookup by track id.
- `GET /api/search`: Tantivy-backed search.
- `POST /api/request-challenge`: create proof-of-work challenge.
- `POST /api/publish`: publish lyrics using challenge solution.
- `POST /api/flag`: flag current lyrics for a track.

When mirror publishing is enabled with `LRCLIB_MIRROR_SERVE` (default on), only the publish directory's `dumps/` and `replication/` subtrees are served under `/mirror/` (tower-http `ServeDir`, range requests supported). Hidden and in-progress files (`.tmp`, `.part`, `.partial`, `.lock`) return 404. `state.json` and `latest.json` are sent with `Cache-Control: no-cache`, everything else with `public, max-age=86400`, so a CDN in front never delays followers.

## Main Request Paths

### `GET /api/get`

- Normalizes input with `utils::process_param`.
- Single-key cache lookup: `get:v5:{track_name_lower}:{artist_name_lower}:{album_name_lower}:{duration_bucket}`. All requests use the cache (including duration-less requests).
- Coalescing via Moka's `try_get_with` on the same cache — concurrent misses for the same key share one database load.
- On cache miss, queries `track_repository::get_track_by_metadata`.
- Both hits (`Some`) and misses (`None`) are cached with 24h TTL / 4h idle.
- Returns the track joined with `tracks.last_lyrics_id`.
- No manual invalidation on publish; eventual consistency via TTL expiry.

### `GET /api/search`

- Normalizes query params.
- Uses `search_cache`.
- Cache keys are versioned as `search:v4`.
- On a cache miss, the Tantivy query and subsequent `track_repository::get_tracks_by_ids` SQLite join run in the same `spawn_blocking` task. The connection is acquired only after Tantivy returns nonempty ranked ids, and response order preserves the ranking.
- If the index is unavailable, still catching up, or a Tantivy query fails, the search request fails; there is no FTS5 query fallback.
- Concurrent misses for the same cache key are coalesced into one database load by Moka.
- Cache-miss and background-refresh searches run through a bounded admission queue on `spawn_blocking`; cache hits remain fully async. The admission semaphore is sized by `LRCLIB_MAX_CONCURRENT_LOADS` (default 32). When saturated, new loads wait up to `LRCLIB_LOAD_QUEUE_MS` ms (default 250, 0 = strict shed) before returning `503 Service Unavailable` + `Retry-After: 1`.

- Once-per-minute metrics report route latency plus `spawn_blocking` wait, Tantivy query, database-pool wait, and SQLite result-fetch latency p50/p95/p99, along with requests, cache hits/misses, and 5xx failures. Backend-stage histograms contain actual coalesced loader executions, not every request that observed an initial cache miss.

### `POST /api/request-challenge`

- Generates a random prefix.
- Computes difficulty from recent publish volume.
- Stores `challenge:<prefix> -> target` in `challenge_cache`.

### `POST /api/publish`

- Requires `X-Publish-Token`.
- Token format is `prefix:nonce` and is verified against the cached challenge by SHA-256 threshold comparison.
- In one DB transaction, find or insert the track, then insert a new lyrics row.
- DB trigger updates `tracks.last_lyrics_id`.
- If `lyricsfile` is supplied, it is stored as raw YAML, legacy `plainLyrics` / `syncedLyrics` inputs are ignored, and stored legacy columns are derived best-effort from the Lyricsfile payload.
- If only legacy lyrics are supplied, the shared converter generates Lyricsfile YAML before the transaction. It preserves plain text, converts LRC timestamps to millisecond `lines`, sorts lines, infers each `end_ms` from the next line, and retains the request duration at millisecond precision.
- If only synced lyrics are supplied, plain lyrics are derived while preserving stanza separators and untimed text.
- Instrumental markers (`[au: instrumental]`) produce a lyrics row with no text and `instrumental = true`.
- For `lyricsfile` publishes, `instrumental` is populated by a best-effort YAML read of `metadata.instrumental`; instrumental rows have no stored plain or synced text.
- Repository insert functions require a non-empty Lyricsfile, so all current API write paths set `has_lyricsfile = true`.
- `has_word_sync` comes from `PreparedLyrics`, so supplied Lyricsfiles are detected during the same parse used to derive legacy fields; legacy plain/synced publishes always store `false`.
- Metadata cache entries for the published track expire naturally via TTL (24h); publish does not actively invalidate them. Response-shape changes must bump the `get:`/`search:` cache key versions instead.

### `POST /api/flag`

- Reuses the same publish-token validation.
- Inserts into `flags` for the track's current `last_lyrics_id`.

## Maintenance CLI

`lrclib backfill-lyricsfiles --database <FILE>` populates Lyricsfile for historical rows where `has_lyricsfile = false`.

- Existing nonblank Lyricsfile is preserved and only its false presence flag is repaired; generated rows use legacy plain/synced text, stored instrumental state, and current track metadata.
- The update changes only `lyricsfile`, `has_lyricsfile`, and `has_word_sync`; `created_at`, `updated_at`, legacy representations, source, instrumental state, and `tracks.last_lyrics_id` remain unchanged. `has_word_sync` is derived from a preserved Lyricsfile and is always false for generated legacy rows.
- The batch update uses an exact compare-and-set against the selected Lyricsfile value. Concurrent changes are never overwritten or marked complete; they remain eligible for a later run.
- `--dry-run` opens SQLite read-only, never applies migrations or takes the writer lock, and does not write.

`lrclib backfill-word-sync --database <FILE>` detects word-level sync in stored Lyricsfiles and populates `has_word_sync`.

- Detection is canonical: a row is word-synced when any line in its stored Lyricsfile has a non-empty `words` array (`lyricsfile::parse_lyricsfile`); legacy plain/synced inputs can never produce word timing.
- The sweep scans `has_word_sync = false` rows in id order with a cheap `instr(lyricsfile, 'words')` prefilter, so only hinted Lyricsfiles are parsed in Rust. Instrumental files, malformed YAML, and false-positive hints are counted and left `false`.
- `apply_word_sync_backfill_batch` flips rows to `true` only behind a `has_word_sync = false` guard; `created_at`, `updated_at`, and `tracks.last_lyrics_id` remain unchanged.
- Rows with no stored Lyricsfile are correctly `false` and are skipped by the candidate query. The command is an idempotent full sweep rather than a drain: reruns re-check `false` rows, and `--limit` / `--start-after-id` support canaries and chunking. Each run takes `<database>.word-sync-backfill.lock`.
- `backfill-lyricsfiles` also writes `has_word_sync` for its candidates (derived for preserved Lyricsfiles, `false` for generated ones), so the two maintenance commands can run in either order.
- Defaults are 500 rows per pass and 50ms between write batches; `--sleep-ms 0` is intended for an offline run. `--dry-run` opens SQLite read-only, never applies migrations or takes the writer lock, and reports the sample scan/confirm counts.

## Data Model

Core schema is in `server/migrations/`.

- `tracks`: canonical track metadata plus normalized `*_lower` columns and `last_lyrics_id`.
- `lyrics`: lyrics versions linked to a track; latest version is selected through `tracks.last_lyrics_id`. It also stores raw `lyricsfile` YAML plus `has_lyricsfile` for efficient presence/count queries. The column remains nullable for historical rows, but current repository writes require it. `has_word_sync` marks versions whose stored Lyricsfile contains at least one non-empty `lines[].words` array; it is `NOT NULL DEFAULT FALSE` and maintained by the write paths and the word-sync backfill.
- `flags`: reports against a lyrics row.
- `missing_tracks`: table created by migration 04; no code writes to it anymore.
- `tracks_fts`: retained FTS5 virtual table over normalized track fields; it is not queried by `/api/search`.
- `mirror_changes`: replication changelog `(seq AUTOINCREMENT, table_name, row_id)`; only written by runtime-installed triggers while mirror publishing is enabled.
- `mirror_state`: key/value replication state (`publisher_epoch`, `publisher_pruned_seq` on publishers; `upstream_epoch`, `upstream_change_seq` on followers).

Important DB behavior:

- `tracks.last_lyrics_id` is maintained by an `AFTER INSERT ON lyrics` trigger.
- FTS rows are maintained by insert/update/delete triggers on `tracks`.
- track uniqueness is enforced on normalized `(name_lower, artist_name_lower, album_name_lower, duration)`.
- the uniqueness constraint's autoindex covers full metadata lookups; `idx_tracks_metadata_no_album` separately supports metadata lookups that omit the album but include duration.
- DB opens in WAL mode with tuned PRAGMAs in `server/src/db.rs`; connection-local PRAGMAs and a busy timeout are applied to every pooled connection. Startup logs the linked SQLite version plus the effective and compiled maximum mmap sizes, with all SQLite compile options available at debug level. `LRCLIB_MMAP_SIZE` and `LRCLIB_CACHE_SIZE` override the mmap and suggested page-cache limits, while `LRCLIB_POOL_MAX_SIZE` sets the r2d2 connection limit (default 16).
- `LRCLIB_MAX_CONCURRENT_LOADS` (default 32) and `LRCLIB_LOAD_QUEUE_MS` (default 250) control the blocking-load admission. `LRCLIB_MAX_BLOCKING_THREADS` optionally caps the tokio blocking thread pool (only when explicitly set; default follows tokio). When permitted loads are exhausted, `/api/get`, `/api/get/:track_id`, and `/api/search` return `503 + Retry-After: 1`.
- `idx_lyrics_has_lyricsfile` supports efficient counts and filters over Lyricsfile adoption.
- `idx_lyrics_has_word_sync` is a partial index over `has_word_sync = TRUE`; it stays small because word-synced rows are the exception.

Current lyrics API responses remain backward-compatible:

- `plainLyrics` and `syncedLyrics` are still returned for legacy rows
- `lyricsfile` is returned as an additional raw YAML field when present
- `hasWordSync` reports the current lyrics version's word-level sync flag

## Search Index (Tantivy)

`server/src/search_index.rs` embeds a Tantivy index over `(track_id, name_lower, artist_name_lower, album_name_lower)`. It is the sole search backend for `/api/search` (see `TANTIVY_SEARCH_PLAN.md`).

- SQLite remains the source of truth; the index is a disposable derived artifact under `<db_path>.tantivy/` (override: `LRCLIB_SEARCH_INDEX_DIR`).
- The indexer is a dedicated OS thread that polls `tracks` for `id > cursor` (default every 2s, batches of 10k). This is race-free because `tracks.id` is AUTOINCREMENT-monotonic and the indexed name columns are immutable after insert; there is no hook in the publish path.
- The cursor (last indexed track id) is stored as the Tantivy commit payload, so it is atomic with each commit. Restarts replay `id > cursor`; crashes lose only uncommitted work that the replay re-indexes.
- `SearchIndex::is_ready()` turns true once the indexer has caught up. Search requests fail until then; missing/corrupt/version-mismatched indexes rebuild in the background while non-search routes continue serving.
- The index format version lives in `lrclib_index_version` inside the index dir; bump `INDEX_FORMAT_VERSION` when the schema/tokenizer changes to force a rebuild. Recreation refuses to wipe directories that do not look like a search index.
- Query semantics: `q` = AND of terms across the three columns; structured params = per-column phrase queries. Inputs are expected to be normalized by `utils::prepare_input`.
- Tantivy emits its own `tracing` logs; use e.g. `LRCLIB_LOG=info,tantivy=warn` to quiet them.

## Mirroring

OpenStreetMap-style replication in `server/src/mirror*`: periodic full dumps plus chains of sequence-numbered diffs. A server can publish, follow, or both (a follower that also publishes re-publishes what it applies).

Configuration (env, parsed by `MirrorConfig::from_env`; invalid values abort startup):

- `LRCLIB_MIRROR_PUBLISH_DIR`: enables publishing into this directory.
- `LRCLIB_MIRROR_DIFF_PERIODS` (default `1h,1d`): comma-separated diff streams, finest first; each must be a larger multiple of the previous. Units `s`, `m`, `h`, `d`, `w`. The first stream is built from the changelog, each later one by merging the previous stream.
- `LRCLIB_MIRROR_DUMP_PERIOD` (default `7d`), `LRCLIB_MIRROR_KEEP_DUMPS` (default `1`, `0` keeps all), `LRCLIB_MIRROR_SERVE` (default `true`). A dump period shorter than the coarsest diff period is allowed but logs a warning, since every dump copies the whole database.
- `LRCLIB_MIRROR_UPSTREAM`: enables following; base URL of an upstream `/mirror` tree (or a local directory). `LRCLIB_MIRROR_UPSTREAM_STREAM` (default `1h`) picks the upstream stream, `LRCLIB_MIRROR_UPSTREAM_POLL` (default `5m`) the poll interval.

Period boundaries are aligned to multiples of the period since the Unix epoch in UTC: hourly on the hour, daily at midnight, `7d` on Thursdays 00:00 UTC.

Change capture:

- When publishing is enabled, `prepare_publisher` installs `AFTER INSERT/UPDATE/DELETE` triggers (named `lrclib_mirror_changes_<table>_{ai,au,ad}`; only these exact names are ever dropped) on `lyrics`, `flags`, `missing_tracks`, and `tracks` that append `(table, id)` to `mirror_changes`. Because they are database triggers, writes from the maintenance CLIs are captured too. `seq` is monotonic in commit order and never reused (read via `sqlite_sequence`, so it survives pruning).
- When publishing is disabled, startup drops the triggers and clears the changelog so non-publishing servers never accumulate it. If nothing is installed this is a read-only check, so ordinary servers take no writer lock at startup.
- An epoch id identifies one continuous changelog. Startup keeps the epoch only when the triggers exist and the published base stream covers everything already pruned; otherwise (first start, publishing re-enabled, publish dir lost) a new epoch starts, the changelog is cleared, and a dump is written immediately. Followers refuse diffs from a different epoch and must re-bootstrap. Startup also starts a new epoch when the database is behind what was already published or dumped (`latest_change_seq` below the published cursor, e.g. restored from a backup), because its change sequence would reuse numbers followers already applied.
- One publisher per database and per publish directory: `prepare_publisher` takes exclusive process-lifetime locks on `<db>.mirror-publisher.lock` (keyed by canonical path) and `<dir>/.publisher.lock`, so a second instance fails at startup instead of pruning the other's changelog or overwriting its sequence numbers.

Publish directory layout:

```
dumps/lrclib-<YYYYMMDDTHHMMSSZ>.sqlite3.gz   + .state.json, latest.json -> newest
replication/<period>/state.json             newest diff state (followers poll this)
replication/<period>/AAA/BBB/CCC.jsonl.gz   + CCC.state.json
```

- Diff data is gzip JSON lines (`{"op":"upsert","table":..,"row":{..}}` / `{"op":"delete","table":..,"id":..}`), grouped in `MIRRORED_TABLES` order with `tracks` last; within a table all deletes precede all upserts, so a row deleted and re-inserted under a new id with the same unique key (`tracks` and `missing_tracks` metadata) never conflicts, including in merged diffs. Each `(table, id)` appears once with its final state at `to_change_seq`, so applying is idempotent and overlapping a dump or earlier diff is harmless.
- Base diffs (`write_base_diff`) read `to_change_seq` and all changed rows inside one read transaction (one WAL snapshot). After the diff's state files are published, `Publisher::run_due_diffs` prunes the changelog through `to_change_seq` in separate small write transactions. A base diff is written at every boundary, empty or not; after downtime one catch-up diff is labelled with the current boundary.
- Coarser diffs (`merge_diffs`) merge the finer diffs since the previous aggregate whose nominal timestamp is at or before the boundary, newest first, keeping only the first op per key; memory is bounded by the key set. Ranges chain contiguously (`from_change_seq` = previous `to_change_seq`). A newly added coarser stream starts with the current window only, so followers of it need a dump newer than that window: an existing follower switched to a newly added stream fails with a gap error and must re-bootstrap, even though the finer history is on disk.
- Dumps (`create_dump`) use `VACUUM INTO` (one snapshot, non-blocking for writers, needs free disk for an uncompressed copy). The copy's `sqlite_sequence` gives the dump's `change_seq`; publisher triggers, changelog, and state are stripped and `upstream_epoch` / `upstream_change_seq` are stamped in. Dumps run on their own task so they never delay diffs; long dumps hold a read snapshot, so the WAL can grow meanwhile.
- Diff files and their per-sequence states are never deleted (OSM-style): followers walk back through per-sequence states, so pruning would strand slow followers. Disk use grows with change volume; operators can archive old sequences manually. Only dumps have retention (`LRCLIB_MIRROR_KEEP_DUMPS`).
- A new epoch does not clear `replication/`: stream sequence numbers continue from the previous epoch's last sequence and old-epoch files stay in place (followers ignore them via the epoch check). Delete them manually if unwanted.
- State and data files are written via synced temp file + rename + parent-directory fsync (`rename_durable`), so renames survive power loss; data before per-sequence state before the stream `state.json`, so readers never see a state pointing at a missing file. A crash mid-publish rewrites the same sequence on retry.

Following:

- Bootstrap (only when the database file does not exist): needs about twice the dump size free next to the database (compressed download plus restored file); the parent directory is created if missing. Download `dumps/latest.json`'s file, verify SHA-256, gunzip into place, then normal migrations run. Because publishers prune superseded dumps (default retention is 1), the chosen dump can vanish mid-bootstrap; on failure `latest.json` is re-read and a newer dump is retried immediately, while other failures retry with exponential backoff (5 attempts, 5s doubling up to 60s) before startup fails. After a restore, startup marks the search index stale (`search_index::invalidate`) so it is rebuilt from the restored database rather than keeping documents from a previous one.
- Every upstream request and every body chunk has a 60s idle timeout (no total timeout, since dumps are large), so a stalled upstream surfaces as an error instead of hanging sync or startup.
- Sync: read the stream `state.json`, walk back per-sequence states to the first diff with `from_change_seq <= upstream_change_seq`, then download (SHA-256 verified) and apply each in order. `apply_diff` runs one transaction per diff: update-by-id, else insert, else delete; column names are validated against local `PRAGMA table_info` (an unknown upstream column fails the sync until this server is upgraded). Foreign keys are disabled while applying because `tracks` and `lyrics` reference each other. The new upstream position is written in the same transaction. Transient failures (network, I/O) retry after `max(60s, poll interval)`. A new upstream epoch or a gap in the diff chain is permanent: it is logged once and following stops (the server keeps serving its current data) until an operator re-bootstraps. An upstream at the same position is a no-op; an upstream behind the local position (rewound within the same epoch) is permanent like an epoch change. A missing stream `state.json` is not an error (coarse streams appear only after their first boundary) but is logged once as a warning pointing at `LRCLIB_MIRROR_UPSTREAM_STREAM`, then once at info when it appears.
- Local triggers keep working on followers (`tracks_fts`, `last_lyrics_id`); `tracks` is applied last so its exact upstream row wins. The Tantivy indexer picks up new tracks by id as usual, since diffs commit atomically and upstream ids are monotonic.
- Mirroring assumes `tracks` rows are effectively append-only with immutable indexed name columns (true for every current write path). The format and triggers carry `tracks` updates and deletes, but the Tantivy indexer only appends `id > cursor`, so a replicated delete would leave a stale document whose result `get_tracks_by_ids` silently drops. Adding a write path that deletes or renames tracks requires teaching the indexer to remove documents.
- Follower caches are not invalidated by applied diffs: like upstream publishes, `/api/get` and `/api/search` results converge within the cache TTL (up to 24h), but on a mirror that lag applies to every update.
- Followers are read-only (see `AppState.read_only`) because local writes would collide with upstream ids. The maintenance backfills refuse to write to a follower database (`ensure_not_follower_database`); dry runs still work.
- Mirror startup errors (invalid config, failed bootstrap, failed publisher preparation) print the full error chain and exit with status 1.
- The crate exports only what startup and the CLI need (`MirrorConfig`, `Period`, state structs, `prepare_publisher`/`spawn_publisher`, follower bootstrap/guards/spawn); diff writing, merging, applying, and dumps are crate-private.

## Matching Rules

Track matching is intentionally fuzzy but narrow:

- names are normalized by `utils::prepare_input`
- punctuation is mostly removed/collapsed
- comparisons are lowercase
- metadata duration matching uses about +/-2 seconds
- `/api/get` prefers tracks whose current lyrics include synced lyrics

If you change matching behavior, review both:

- `utils::prepare_input`
- repository metadata queries
- `search_index/query.rs`

## Agent Notes

- Most edits belong in `server/`, not the root crate.
- Preserve write transactions around track + lyrics creation.
- All new lyrics repository writes must supply a non-empty Lyricsfile; old rows may still contain `NULL`.
- All new lyrics repository writes must pass `PreparedLyrics.has_word_sync`; word timing only exists in Lyricsfile `lines[].words`, never in legacy plain/synced text.
- Historical Lyricsfile and word-sync population belongs in the root maintenance CLI; keep schema-aware batch SQL and its timestamp-invariance tests in `lyrics_repository`.
- New mirrored tables must be added to `mirror::MIRRORED_TABLES` (apply order matters: `tracks` last). Column additions need no mirror changes, but followers must be upgraded before publishers start emitting the new column.
- Schema changes require a new migration and usually repository updates. Migrations are append-only: never edit or renumber an applied migration; add a new numbered migration for each schema change.
