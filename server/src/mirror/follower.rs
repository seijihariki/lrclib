use super::changelog::{get_state, UPSTREAM_CHANGE_SEQ_KEY, UPSTREAM_EPOCH_KEY};
use super::diff::open_diff;
use super::{
    apply_diff, diff_data_path, diff_state_path, rename_durable, sha256_file, stream_state_path,
    DiffState, DumpState, FollowerConfig, DUMPS_DIR, LATEST_DUMP_STATE,
};
use anyhow::{bail, ensure, Context, Result};
use flate2::read::MultiGzDecoder;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Connection;
use serde::de::DeserializeOwned;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

const FOLLOW_RETRY: Duration = Duration::from_secs(60);
/// Per request and per body chunk; dumps are too large for a total timeout.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const BOOTSTRAP_ATTEMPTS: u32 = 5;
#[cfg(not(test))]
const BOOTSTRAP_BACKOFF: Duration = Duration::from_secs(5);
#[cfg(test)]
const BOOTSTRAP_BACKOFF: Duration = Duration::from_millis(50);
const BOOTSTRAP_MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Only an operator re-bootstrap fixes this, so the follower stops instead of
/// retrying.
#[derive(Debug)]
struct ReplicationBroken(String);

impl std::fmt::Display for ReplicationBroken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ReplicationBroken {}

async fn with_idle_timeout<T>(
    what: &str,
    future: impl std::future::Future<Output = reqwest::Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(IDLE_TIMEOUT, future).await {
        Ok(result) => Ok(result.with_context(|| format!("failed to fetch {what}"))?),
        Err(_) => bail!(
            "timed out after {}s fetching {what}",
            IDLE_TIMEOUT.as_secs()
        ),
    }
}

enum Upstream {
    Http {
        client: reqwest::Client,
        base: String,
    },
    Local(PathBuf),
}

impl Upstream {
    fn new(location: &str) -> Result<Self> {
        if location.starts_with("http://") || location.starts_with("https://") {
            let client = reqwest::Client::builder()
                .user_agent(concat!("LRCLIB mirror/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(Duration::from_secs(30))
                .build()?;
            Ok(Self::Http {
                client,
                base: location.trim_end_matches('/').to_owned(),
            })
        } else {
            Ok(Self::Local(PathBuf::from(location)))
        }
    }

    async fn response(&self, path: &str) -> Result<Option<reqwest::Response>> {
        let Self::Http { client, base } = self else {
            unreachable!()
        };
        let url = format!("{base}/{path}");
        let response = with_idle_timeout(&url, client.get(&url).send()).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(response.error_for_status()?))
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        let bytes = match self {
            Self::Http { .. } => match self.response(path).await? {
                Some(response) => with_idle_timeout(path, response.bytes()).await?.to_vec(),
                None => return Ok(None),
            },
            Self::Local(root) => match tokio::fs::read(root.join(path)).await {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            },
        };
        Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
            format!("invalid upstream state file {path}")
        })?))
    }

    async fn download(&self, path: &str, dest: &Path, sha256: &str) -> Result<()> {
        match self {
            Self::Http { .. } => {
                let mut response = self
                    .response(path)
                    .await?
                    .with_context(|| format!("upstream file {path} not found"))?;
                let mut file = BufWriter::new(File::create(dest)?);
                while let Some(chunk) = with_idle_timeout(path, response.chunk()).await? {
                    file.write_all(&chunk)?;
                }
                file.into_inner()
                    .map_err(|error| error.into_error())?
                    .sync_all()?;
            }
            Self::Local(root) => {
                tokio::fs::copy(root.join(path), dest)
                    .await
                    .with_context(|| format!("failed to copy upstream file {path}"))?;
            }
        }
        let dest_owned = dest.to_owned();
        let (actual, _) = tokio::task::spawn_blocking(move || sha256_file(&dest_owned)).await??;
        ensure!(
            actual == sha256,
            "checksum mismatch for upstream file {path}: expected {sha256}, got {actual}"
        );
        Ok(())
    }
}

fn sibling(database: &Path, suffix: &str) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

/// Restores the latest upstream dump into `database` when the file does not
/// exist yet. Returns whether a dump was restored.
///
/// Publishers delete superseded dumps, so the chosen dump can disappear; a
/// failure retries immediately when a newer dump replaced it, else with backoff.
pub async fn bootstrap_database_if_missing(
    config: &FollowerConfig,
    database: &Path,
) -> Result<bool> {
    if database.exists() {
        return Ok(false);
    }
    let upstream = Upstream::new(&config.upstream)?;
    let mut backoff = BOOTSTRAP_BACKOFF;

    for attempt in 1..=BOOTSTRAP_ATTEMPTS {
        let (error, file) = match latest_dump(&upstream, config).await {
            Err(error) => (error, None),
            Ok(dump) => match restore_dump(&upstream, &dump, database).await {
                Ok(()) => {
                    tracing::info!(
                        database = %database.display(),
                        file = %dump.file,
                        "restored upstream mirror dump"
                    );
                    return Ok(true);
                }
                Err(error) => (error, Some(dump.file)),
            },
        };
        if attempt == BOOTSTRAP_ATTEMPTS {
            return Err(error.context(format!(
                "failed to bootstrap from upstream after {attempt} attempts"
            )));
        }

        let superseded = match (
            &file,
            upstream.get_json::<DumpState>(LATEST_DUMP_STATE).await,
        ) {
            (Some(file), Ok(Some(latest))) => latest.file != *file,
            _ => false,
        };
        if superseded {
            tracing::warn!(
                error = format!("{error:#}"),
                "upstream published a newer dump during bootstrap; retrying with it"
            );
        } else {
            tracing::warn!(
                error = format!("{error:#}"),
                retry_in_ms = backoff.as_millis() as u64,
                "mirror bootstrap failed; retrying"
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BOOTSTRAP_MAX_BACKOFF);
        }
    }
    unreachable!("the last attempt always returns")
}

async fn latest_dump(upstream: &Upstream, config: &FollowerConfig) -> Result<DumpState> {
    upstream
        .get_json(LATEST_DUMP_STATE)
        .await?
        .with_context(|| format!("upstream {} has no dump yet", config.upstream))
}

async fn restore_dump(upstream: &Upstream, dump: &DumpState, database: &Path) -> Result<()> {
    tracing::info!(
        file = %dump.file,
        size = dump.size,
        change_seq = dump.change_seq,
        "bootstrapping database from upstream mirror dump"
    );
    if let Some(parent) = database
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let download = sibling(database, ".mirror-download");
    let restore = sibling(database, ".mirror-restore");
    let result: Result<()> = async {
        upstream
            .download(
                &format!("{DUMPS_DIR}/{}", dump.file),
                &download,
                &dump.sha256,
            )
            .await?;
        let (gz_path, raw_path) = (download.clone(), restore.clone());
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut reader = MultiGzDecoder::new(BufReader::new(File::open(&gz_path)?));
            let mut writer = BufWriter::new(File::create(&raw_path)?);
            std::io::copy(&mut reader, &mut writer)?;
            writer
                .into_inner()
                .map_err(|error| error.into_error())?
                .sync_all()?;
            Ok(())
        })
        .await??;
        rename_durable(&restore, database)?;
        Ok(())
    }
    .await;
    let _ = std::fs::remove_file(&download);
    let _ = std::fs::remove_file(&restore);
    result
}

fn upstream_position(conn: &Connection) -> Result<Option<(String, i64)>> {
    let epoch = get_state(conn, UPSTREAM_EPOCH_KEY)?;
    let change_seq = get_state(conn, UPSTREAM_CHANGE_SEQ_KEY)?
        .map(|value| value.parse::<i64>())
        .transpose()?;
    Ok(epoch.zip(change_seq))
}

pub fn ensure_follower_database(conn: &Connection) -> Result<()> {
    if upstream_position(conn)?.is_none() {
        bail!(
            "the database was not restored from an upstream mirror dump; \
             point LRCLIB_DATABASE_FILE at a path that does not exist yet to bootstrap it"
        );
    }
    Ok(())
}

pub fn ensure_not_follower_database(conn: &Connection) -> Result<()> {
    if upstream_position(conn)?.is_some() {
        bail!(
            "this database is a mirror of an upstream LRCLIB server; \
             run maintenance on the upstream instead (changes arrive through diffs)"
        );
    }
    Ok(())
}

/// Returns the number of diffs applied, or `None` when the upstream has no such
/// stream (yet).
async fn sync_once(
    upstream: &Upstream,
    config: &FollowerConfig,
    pool: &Pool<SqliteConnectionManager>,
    scratch: &Path,
) -> Result<Option<usize>> {
    let read_pool = pool.clone();
    let (epoch, mut change_seq) = tokio::task::spawn_blocking(move || -> Result<(String, i64)> {
        let conn = read_pool.get()?;
        upstream_position(&conn)?.context("database has no upstream position")
    })
    .await??;

    let stream = &config.stream;
    let Some(latest) = upstream
        .get_json::<DiffState>(&stream_state_path(stream))
        .await?
    else {
        return Ok(None);
    };
    if latest.epoch != epoch {
        return Err(ReplicationBroken(format!(
            "upstream started a new replication epoch ({}); remove the database and restart to re-bootstrap from a fresh dump",
            latest.epoch
        ))
        .into());
    }
    if latest.to_change_seq == change_seq {
        return Ok(Some(0));
    }
    if latest.to_change_seq < change_seq {
        // Its change sequence will be reused for different changes, so
        // continuing could silently diverge.
        return Err(ReplicationBroken(format!(
            "upstream stream {stream} is at change {} but this database is already at change {change_seq}; \
             the upstream was rewound, so remove the database and restart to re-bootstrap from a fresh dump",
            latest.to_change_seq
        ))
        .into());
    }

    // Walk back to the first diff that starts at or before our position.
    let gap = || -> anyhow::Error {
        ReplicationBroken(format!(
            "upstream stream {stream} has no diffs back to change {change_seq}; \
             remove the database and restart to re-bootstrap from a fresh dump"
        ))
        .into()
    };
    let mut chain = vec![latest];
    while chain.last().unwrap().from_change_seq > change_seq {
        let sequence = chain.last().unwrap().sequence;
        if sequence <= 1 {
            return Err(gap());
        }
        match upstream
            .get_json::<DiffState>(&diff_state_path(stream, sequence - 1))
            .await?
        {
            Some(previous) if previous.epoch == epoch => chain.push(previous),
            _ => return Err(gap()),
        }
    }
    chain.reverse();

    let mut applied = 0;
    for state in chain {
        if state.to_change_seq <= change_seq {
            continue;
        }
        upstream
            .download(
                &diff_data_path(stream, state.sequence),
                scratch,
                &state.sha256,
            )
            .await?;
        let apply_pool = pool.clone();
        let apply_path = scratch.to_owned();
        let apply_epoch = epoch.clone();
        let to = state.to_change_seq;
        let operations = tokio::task::spawn_blocking(move || -> Result<u64> {
            let mut conn = apply_pool.get()?;
            apply_diff(&mut conn, open_diff(&apply_path)?, &apply_epoch, to)
        })
        .await??;
        let _ = std::fs::remove_file(scratch);
        change_seq = to;
        applied += 1;
        tracing::info!(
            event = "mirror_apply",
            stream = %stream,
            sequence = state.sequence,
            to_change_seq = to,
            operations,
            "applied upstream mirror diff"
        );
    }
    Ok(Some(applied))
}

/// Spawns the loop that polls the upstream stream and applies new diffs.
pub fn spawn_follower(
    pool: Pool<SqliteConnectionManager>,
    config: FollowerConfig,
    database: &Path,
) -> Result<()> {
    let upstream = Upstream::new(&config.upstream)?;
    let scratch = sibling(database, ".mirror-diff");
    tracing::info!(
        upstream = %config.upstream,
        stream = %config.stream,
        poll_interval_s = config.poll_interval.as_secs(),
        "mirror following enabled; write endpoints are disabled"
    );
    tokio::spawn(async move {
        let mut stream_missing = false;
        loop {
            let wait = match sync_once(&upstream, &config, &pool, &scratch).await {
                Ok(None) => {
                    // Coarse streams appear only after their first boundary.
                    if !stream_missing {
                        tracing::warn!(
                            upstream = %config.upstream,
                            stream = %config.stream,
                            "upstream has no diffs for this stream yet; if this persists, check that \
                             LRCLIB_MIRROR_UPSTREAM_STREAM matches one of the upstream's LRCLIB_MIRROR_DIFF_PERIODS"
                        );
                        stream_missing = true;
                    }
                    config.poll_interval
                }
                Ok(Some(_)) => {
                    if stream_missing {
                        tracing::info!(stream = %config.stream, "upstream diff stream is now available");
                        stream_missing = false;
                    }
                    config.poll_interval
                }
                Err(error) if error.downcast_ref::<ReplicationBroken>().is_some() => {
                    tracing::error!(
                        error = format!("{error:#}"),
                        "mirror following stopped; the server keeps serving its current data"
                    );
                    break;
                }
                Err(error) => {
                    tracing::error!(error = format!("{error:#}"), "mirror sync failed");
                    FOLLOW_RETRY.max(config.poll_interval)
                }
            };
            tokio::time::sleep(wait).await;
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::changelog::set_state;
    use super::super::diff::tests::{publish, snapshot, temp_dir};
    use super::super::publisher::tests::{at, config, source_db, test_pool};
    use super::super::{prepare_publisher, Period};
    use super::*;
    use crate::db::migrate;

    fn follower_pool(path: &Path) -> Pool<SqliteConnectionManager> {
        let mut conn = Connection::open(path).unwrap();
        migrate(&mut conn).unwrap();
        Pool::builder()
            .max_size(2)
            .build(SqliteConnectionManager::file(path))
            .unwrap()
    }

    #[tokio::test]
    async fn follower_bootstraps_from_dump_and_applies_diffs() {
        let dir = temp_dir("follower");
        let source_pool = test_pool(&dir);
        let publisher_config = config(&dir);
        let publisher = prepare_publisher(&source_pool, Some(&publisher_config), &source_db(&dir))
            .unwrap()
            .unwrap();
        let mut source = source_pool.get().unwrap();

        publish(&source, "a", "one");
        publisher.run_due_diffs(&mut source, at(1, 10)).unwrap();
        publish(&source, "b", "two");
        // Dump lands in the middle of the next hourly diff's range.
        publisher.run_due_dump(&source, at(1, 10)).unwrap();
        publish(&source, "a", "three");
        publisher.run_due_diffs(&mut source, at(1, 11)).unwrap();

        let follower_config = FollowerConfig {
            upstream: publisher_config.dir.to_string_lossy().into_owned(),
            stream: Period::parse("1h").unwrap().label(),
            poll_interval: Duration::from_secs(60),
        };
        // The database's parent directory does not exist yet.
        let database = dir.join("nested").join("follower.sqlite3");
        assert!(bootstrap_database_if_missing(&follower_config, &database)
            .await
            .unwrap());
        assert!(!bootstrap_database_if_missing(&follower_config, &database)
            .await
            .unwrap());
        let pool = follower_pool(&database);
        ensure_follower_database(&pool.get().unwrap()).unwrap();

        let upstream = Upstream::new(&follower_config.upstream).unwrap();
        let scratch = dir.join("scratch.gz");
        assert_eq!(
            sync_once(&upstream, &follower_config, &pool, &scratch)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(snapshot(&pool.get().unwrap()), snapshot(&source));

        source
            .execute("UPDATE lyrics SET has_word_sync = 1 WHERE id = 1", [])
            .unwrap();
        publisher.run_due_diffs(&mut source, at(1, 12)).unwrap();
        assert_eq!(
            sync_once(&upstream, &follower_config, &pool, &scratch)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            sync_once(&upstream, &follower_config, &pool, &scratch)
                .await
                .unwrap(),
            Some(0)
        );
        assert_eq!(snapshot(&pool.get().unwrap()), snapshot(&source));

        let missing_stream = FollowerConfig {
            stream: "30m".to_owned(),
            ..follower_config.clone()
        };
        assert_eq!(
            sync_once(&upstream, &missing_stream, &pool, &scratch)
                .await
                .unwrap(),
            None
        );

        let state_path = publisher_config.dir.join(stream_state_path("1h"));
        let current = std::fs::read(&state_path).unwrap();
        let mut rewound: DiffState = serde_json::from_slice(&current).unwrap();
        rewound.to_change_seq -= 1;
        std::fs::write(&state_path, serde_json::to_vec(&rewound).unwrap()).unwrap();
        let error = sync_once(&upstream, &follower_config, &pool, &scratch)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<ReplicationBroken>().is_some());
        assert!(error.to_string().contains("rewound"));
        std::fs::write(&state_path, current).unwrap();

        drop(source);
        drop(publisher);
        std::fs::remove_dir_all(&publisher_config.dir).unwrap();
        let reset = prepare_publisher(&source_pool, Some(&publisher_config), &source_db(&dir))
            .unwrap()
            .unwrap();
        reset
            .run_due_diffs(&mut source_pool.get().unwrap(), at(1, 13))
            .unwrap();
        let error = sync_once(&upstream, &follower_config, &pool, &scratch)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("new replication epoch"));
        assert!(error.downcast_ref::<ReplicationBroken>().is_some());

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn bootstrap_survives_a_pruned_dump_and_gives_up_eventually() {
        let dir = temp_dir("bootstrap-race");
        let source_pool = test_pool(&dir);
        let publisher_config = config(&dir);
        let publisher = prepare_publisher(&source_pool, Some(&publisher_config), &source_db(&dir))
            .unwrap()
            .unwrap();
        let source = source_pool.get().unwrap();
        publish(&source, "a", "one");
        publisher.run_due_dump(&source, at(1, 10)).unwrap();

        // `latest.json` names a dump that was pruned before we fetched it.
        let latest_path = publisher_config.dir.join(LATEST_DUMP_STATE);
        let current = std::fs::read(&latest_path).unwrap();
        let mut stale: DumpState = serde_json::from_slice(&current).unwrap();
        stale.file = "lrclib-20260101T000000Z.sqlite3.gz".to_owned();
        std::fs::write(&latest_path, serde_json::to_vec(&stale).unwrap()).unwrap();
        let restore_latest = {
            let latest_path = latest_path.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                std::fs::write(latest_path, current).unwrap();
            })
        };

        let follower_config = FollowerConfig {
            upstream: publisher_config.dir.to_string_lossy().into_owned(),
            stream: "1h".to_owned(),
            poll_interval: Duration::from_secs(60),
        };
        let database = dir.join("follower.sqlite3");
        assert!(bootstrap_database_if_missing(&follower_config, &database)
            .await
            .unwrap());
        restore_latest.await.unwrap();
        let conn = Connection::open(&database).unwrap();
        ensure_follower_database(&conn).unwrap();
        assert_eq!(snapshot(&conn), snapshot(&source));

        let missing = FollowerConfig {
            upstream: dir.join("nowhere").to_string_lossy().into_owned(),
            ..follower_config
        };
        let error = bootstrap_database_if_missing(&missing, &dir.join("other.sqlite3"))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("after 5 attempts"));
        assert!(!dir.join("other.sqlite3").exists());

        std::fs::remove_dir_all(dir).unwrap();
    }

    async fn serve(router: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn follower_tracks_a_coarse_stream_over_http() {
        let dir = temp_dir("follower-http");
        let source_pool = test_pool(&dir);
        let publisher_config = config(&dir);
        let publisher = prepare_publisher(&source_pool, Some(&publisher_config), &source_db(&dir))
            .unwrap()
            .unwrap();
        let upstream_state = crate::state::build_app_state(source_pool.clone(), None, false);
        let upstream_url = serve(crate::router::build_router(
            upstream_state,
            Some(publisher_config.dir.clone()),
        ))
        .await;

        let mut source = source_pool.get().unwrap();
        publish(&source, "a", "one");
        publisher.run_due_dump(&source, at(1, 10)).unwrap();
        publisher.run_due_diffs(&mut source, at(1, 11)).unwrap();
        let insert =
            "INSERT INTO tracks (name, name_lower, artist_name_lower, album_name_lower, duration)
                      VALUES ('x', 'x', 'a', 'b', 100)";
        source.execute(insert, []).unwrap();
        publisher.run_due_diffs(&mut source, at(2, 0)).unwrap();
        // Within one daily window: delete, then re-insert the same unique
        // metadata under a new id, in separate hourly diffs.
        source
            .execute("DELETE FROM tracks WHERE name = 'x'", [])
            .unwrap();
        publisher.run_due_diffs(&mut source, at(2, 1)).unwrap();
        source.execute(insert, []).unwrap();
        publish(&source, "b", "two");
        publisher.run_due_diffs(&mut source, at(2, 2)).unwrap();
        publisher.run_due_diffs(&mut source, at(3, 0)).unwrap();

        let follower_config = FollowerConfig {
            upstream: format!("{upstream_url}/mirror"),
            stream: "1d".to_owned(),
            poll_interval: Duration::from_secs(60),
        };
        let database = dir.join("follower.sqlite3");
        assert!(bootstrap_database_if_missing(&follower_config, &database)
            .await
            .unwrap());
        let pool = follower_pool(&database);
        let upstream = Upstream::new(&follower_config.upstream).unwrap();
        assert!(matches!(upstream, Upstream::Http { .. }));
        let applied = sync_once(&upstream, &follower_config, &pool, &dir.join("scratch.gz"))
            .await
            .unwrap();
        assert_eq!(applied, Some(2));
        assert_eq!(snapshot(&pool.get().unwrap()), snapshot(&source));

        let follower_url = serve(crate::router::build_router(
            crate::state::build_app_state(pool.clone(), None, true),
            None,
        ))
        .await;
        let client = reqwest::Client::new();
        for route in ["request-challenge", "publish", "flag"] {
            let response = client
                .post(format!("{follower_url}/api/{route}"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN, "{route}");
            let body: serde_json::Value = response.json().await.unwrap();
            assert_eq!(body["name"], "ReadOnlyMirror");
        }
        std::fs::write(publisher_config.dir.join("db.sqlite3"), b"secret").unwrap();
        std::fs::write(
            publisher_config.dir.join("replication/1h/state.json.tmp"),
            b"partial",
        )
        .unwrap();
        for hidden in [
            "db.sqlite3",
            ".publisher.lock",
            "replication/1h/state.json.tmp",
        ] {
            let status = client
                .get(format!("{upstream_url}/mirror/{hidden}"))
                .send()
                .await
                .unwrap()
                .status();
            assert_eq!(status, reqwest::StatusCode::NOT_FOUND, "{hidden}");
        }
        let state = client
            .get(format!("{upstream_url}/mirror/replication/1d/state.json"))
            .send()
            .await
            .unwrap();
        assert_eq!(state.headers()[reqwest::header::CACHE_CONTROL], "no-cache");
        let data = client
            .get(format!(
                "{upstream_url}/mirror/replication/1d/000/000/001.jsonl.gz"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(data.status(), reqwest::StatusCode::OK);
        assert_eq!(
            data.headers()[reqwest::header::CACHE_CONTROL],
            "public, max-age=86400"
        );

        let mirror_listing = client
            .get(format!("{follower_url}/mirror/dumps/latest.json"))
            .send()
            .await
            .unwrap();
        assert_eq!(mirror_listing.status(), reqwest::StatusCode::NOT_FOUND);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn plain_databases_cannot_follow() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        assert!(ensure_follower_database(&conn).is_err());
        ensure_not_follower_database(&conn).unwrap();

        set_state(&conn, UPSTREAM_EPOCH_KEY, "epoch").unwrap();
        set_state(&conn, UPSTREAM_CHANGE_SEQ_KEY, "1").unwrap();
        ensure_follower_database(&conn).unwrap();
        assert!(ensure_not_follower_database(&conn).is_err());
    }
}
