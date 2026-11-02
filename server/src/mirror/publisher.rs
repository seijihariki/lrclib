use super::changelog::{
    get_state, latest_change_seq, prune, start_epoch, stop_publishing, triggers_installed,
    PUBLISHER_EPOCH_KEY, PUBLISHER_PRUNED_KEY,
};
use super::dump::latest_dump;
use super::{
    create_dump, diff_data_path, diff_state_path, merge_diffs, read_json, sequence_path,
    sha256_file, stream_state_path, write_base_diff, write_json_atomic, DiffState, Period,
    PublisherConfig,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Connection;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const DIFF_RETRY: Duration = Duration::from_secs(60);
const DUMP_RETRY: Duration = Duration::from_secs(15 * 60);

#[derive(Debug)]
pub struct Publisher {
    config: PublisherConfig,
    epoch: String,
    _locks: [File; 2],
}

/// Two publishers on the same database would prune each other's changelog; two
/// on the same directory would write the same sequence numbers.
fn acquire_lock(path: &Path, what: &str) -> Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open publisher lock {}", path.display()))?;
    file.try_lock().map_err(|error| {
        anyhow!(
            "another mirror publisher already uses this {what} (lock {}): {error}",
            path.display()
        )
    })?;
    file.set_len(0)?;
    writeln!(file, "pid={}", std::process::id())?;
    Ok(file)
}

fn database_lock_path(database: &Path) -> Result<PathBuf> {
    let database = std::fs::canonicalize(database)
        .with_context(|| format!("failed to resolve database path {}", database.display()))?;
    let mut path = database.into_os_string();
    path.push(".mirror-publisher.lock");
    Ok(PathBuf::from(path))
}

/// Installs or removes change capture to match the configuration. The current
/// epoch is kept only when followers can still continue it; otherwise a new one
/// starts and they must re-bootstrap from its first dump.
pub fn prepare_publisher(
    pool: &Pool<SqliteConnectionManager>,
    config: Option<&PublisherConfig>,
    database: &Path,
) -> Result<Option<Arc<Publisher>>> {
    let mut conn = pool.get()?;
    let Some(config) = config else {
        if stop_publishing(&mut conn)? {
            tracing::warn!(
                "mirror publishing is disabled; removed change capture triggers and changelog"
            );
        }
        return Ok(None);
    };

    config.validate()?;
    std::fs::create_dir_all(&config.dir)?;
    let locks = [
        acquire_lock(&database_lock_path(database)?, "database")?,
        acquire_lock(&config.dir.join(".publisher.lock"), "publish directory")?,
    ];
    let coarsest = config.diff_periods[config.diff_periods.len() - 1];
    if config.dump_period.secs() < coarsest.secs() {
        tracing::warn!(
            dump_period = %config.dump_period.label(),
            coarsest_diff_period = %coarsest.label(),
            "mirror dumps are more frequent than the coarsest diffs; every dump copies the whole database"
        );
    }
    let base = config.diff_periods[0].label();
    let published: Option<DiffState> = read_json(&config.dir.join(stream_state_path(&base)))?;
    let epoch = get_state(&conn, PUBLISHER_EPOCH_KEY)?;
    let pruned: i64 = get_state(&conn, PUBLISHER_PRUNED_KEY)?
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);

    let latest = latest_change_seq(&conn)?;
    let dumped = latest_dump(&config.dir)?;

    let epoch = match epoch {
        Some(epoch) if triggers_installed(&conn)? => {
            let published_cursor = published
                .filter(|state| state.epoch == epoch)
                .map_or(0, |state| state.to_change_seq);
            let dumped_cursor = dumped
                .filter(|state| state.epoch == epoch)
                .map_or(0, |state| state.change_seq);
            if published_cursor < pruned {
                tracing::warn!(
                    published_cursor,
                    pruned,
                    "published diffs are behind the pruned changelog; starting a new epoch"
                );
                None
            } else if published_cursor.max(dumped_cursor) > latest {
                // A rewound database would reuse change seqs followers have
                // already applied with different contents.
                tracing::warn!(
                    published_cursor,
                    dumped_cursor,
                    latest,
                    "database is behind what was already published; starting a new epoch"
                );
                None
            } else {
                Some(epoch)
            }
        }
        _ => None,
    };
    let epoch = match epoch {
        Some(epoch) => epoch,
        None => {
            let epoch = start_epoch(&mut conn)?;
            tracing::info!(epoch = %epoch, "started a new mirror replication epoch");
            epoch
        }
    };

    tracing::info!(
        epoch = %epoch,
        directory = %config.dir.display(),
        diff_periods = ?config.diff_periods.iter().map(Period::label).collect::<Vec<_>>(),
        dump_period = %config.dump_period.label(),
        "mirror publishing enabled"
    );
    Ok(Some(Arc::new(Publisher {
        config: config.clone(),
        epoch,
        _locks: locks,
    })))
}

impl Publisher {
    pub fn config(&self) -> &PublisherConfig {
        &self.config
    }

    fn stream_state(&self, stream: &str) -> Result<Option<DiffState>> {
        read_json(&self.config.dir.join(stream_state_path(stream)))
    }

    fn diff_state(&self, stream: &str, sequence: u64) -> Result<Option<DiffState>> {
        read_json(&self.config.dir.join(diff_state_path(stream, sequence)))
    }

    /// Records a written diff: the per-sequence state first, then the stream
    /// `state.json` that followers poll.
    fn publish_diff(
        &self,
        period: &Period,
        sequence: u64,
        timestamp: DateTime<Utc>,
        from_change_seq: i64,
        to_change_seq: i64,
        operations: u64,
    ) -> Result<DiffState> {
        let stream = period.label();
        let (sha256, size) = sha256_file(&self.config.dir.join(diff_data_path(&stream, sequence)))?;
        let state = DiffState {
            epoch: self.epoch.clone(),
            sequence,
            period: stream.clone(),
            timestamp,
            generated_at: Utc::now(),
            from_change_seq,
            to_change_seq,
            operations,
            file: format!("{}.jsonl.gz", sequence_path(sequence)),
            size,
            sha256,
        };
        write_json_atomic(
            &self.config.dir.join(diff_state_path(&stream, sequence)),
            &state,
        )?;
        write_json_atomic(&self.config.dir.join(stream_state_path(&stream)), &state)?;
        tracing::info!(
            event = "mirror_diff",
            stream = %stream,
            sequence,
            from_change_seq,
            to_change_seq,
            operations,
            size,
            "published mirror diff"
        );
        Ok(state)
    }

    /// Writes every diff that is due at `now`: the base stream from the
    /// changelog, then each coarser stream from the one below it.
    pub fn run_due_diffs(&self, conn: &mut Connection, now: DateTime<Utc>) -> Result<()> {
        let base = self.config.diff_periods[0];
        let stream = base.label();
        let boundary = base.boundary(now);
        let latest = self.stream_state(&stream)?;
        let current = latest.as_ref().filter(|state| state.epoch == self.epoch);

        if current.is_none_or(|state| state.timestamp < boundary) {
            let from = current.map_or(0, |state| state.to_change_seq);
            let sequence = latest.as_ref().map_or(1, |state| state.sequence + 1);
            let path = self.config.dir.join(diff_data_path(&stream, sequence));
            let (to, operations) = write_base_diff(conn, from, &path)?;
            self.publish_diff(&base, sequence, boundary, from, to, operations)?;
            prune(conn, to)?;
        }

        for pair in self.config.diff_periods.windows(2) {
            self.aggregate(&pair[0], &pair[1], now)?;
        }
        Ok(())
    }

    fn aggregate(&self, finer: &Period, coarser: &Period, now: DateTime<Utc>) -> Result<()> {
        let boundary = coarser.boundary(now);
        let window_start = boundary - chrono::Duration::seconds(coarser.secs() as i64);
        let finer_stream = finer.label();
        let stream = coarser.label();

        let latest = self.stream_state(&stream)?;
        let current = latest.as_ref().filter(|state| state.epoch == self.epoch);
        if current.is_some_and(|state| state.timestamp >= boundary) {
            return Ok(());
        }
        let Some(mut cursor) = self
            .stream_state(&finer_stream)?
            .filter(|state| state.epoch == self.epoch && state.timestamp >= boundary)
        else {
            return Ok(());
        };

        // Walk back to the previous aggregate or, on a fresh stream, to the
        // start of this window.
        let mut included = Vec::new();
        loop {
            if cursor.epoch != self.epoch {
                break;
            }
            match current {
                Some(state) if cursor.to_change_seq <= state.to_change_seq => break,
                None if cursor.timestamp <= window_start => break,
                _ => {}
            }
            let sequence = cursor.sequence;
            if cursor.timestamp <= boundary {
                included.push(cursor);
            }
            match (sequence > 1)
                .then(|| self.diff_state(&finer_stream, sequence - 1))
                .transpose()?
                .flatten()
            {
                Some(previous) => cursor = previous,
                None => break,
            }
        }
        if included.is_empty() {
            return Ok(());
        }
        included.reverse();

        let inputs: Vec<_> = included
            .iter()
            .map(|state| {
                self.config
                    .dir
                    .join(diff_data_path(&finer_stream, state.sequence))
            })
            .collect();
        let sequence = latest.as_ref().map_or(1, |state| state.sequence + 1);
        let out = self.config.dir.join(diff_data_path(&stream, sequence));
        let operations = merge_diffs(&inputs, &out)?;
        self.publish_diff(
            coarser,
            sequence,
            boundary,
            included[0].from_change_seq,
            included[included.len() - 1].to_change_seq,
            operations,
        )?;
        Ok(())
    }

    pub fn run_due_dump(&self, conn: &Connection, now: DateTime<Utc>) -> Result<bool> {
        let boundary = self.config.dump_period.boundary(now);
        let timestamp = match latest_dump(&self.config.dir)? {
            Some(state) if state.epoch == self.epoch => {
                if state.timestamp >= boundary {
                    return Ok(false);
                }
                boundary
            }
            // Bootstrap dump for a new epoch, labelled with the actual time.
            _ => now,
        };
        let started = std::time::Instant::now();
        let state = create_dump(conn, &self.config.dir, timestamp, self.config.keep_dumps)?;
        tracing::info!(
            event = "mirror_dump",
            file = %state.file,
            change_seq = state.change_seq,
            size = state.size,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "published mirror dump"
        );
        Ok(true)
    }
}

fn until(target: DateTime<Utc>) -> Duration {
    (target - Utc::now()).to_std().unwrap_or(Duration::ZERO) + Duration::from_secs(1)
}

/// Spawns the diff and dump schedules, separately so a long dump never delays
/// diffs.
pub fn spawn_publisher(pool: Pool<SqliteConnectionManager>, publisher: Arc<Publisher>) {
    {
        let pool = pool.clone();
        let publisher = Arc::clone(&publisher);
        tokio::spawn(async move {
            loop {
                let task_pool = pool.clone();
                let task_publisher = Arc::clone(&publisher);
                let result = tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut conn = task_pool.get()?;
                    task_publisher.run_due_diffs(&mut conn, Utc::now())
                })
                .await;
                let wait = match result {
                    Ok(Ok(())) => until(publisher.config.diff_periods[0].next_boundary(Utc::now())),
                    Ok(Err(error)) => {
                        tracing::error!(
                            error = format!("{error:#}"),
                            "mirror diff generation failed"
                        );
                        DIFF_RETRY
                    }
                    Err(error) => {
                        tracing::error!(error = %error, "mirror diff task failed");
                        DIFF_RETRY
                    }
                };
                tokio::time::sleep(wait).await;
            }
        });
    }

    tokio::spawn(async move {
        loop {
            let task_pool = pool.clone();
            let task_publisher = Arc::clone(&publisher);
            let result = tokio::task::spawn_blocking(move || -> Result<bool> {
                let conn = task_pool.get()?;
                task_publisher.run_due_dump(&conn, Utc::now())
            })
            .await;
            let wait = match result {
                Ok(Ok(_)) => until(publisher.config.dump_period.next_boundary(Utc::now())),
                Ok(Err(error)) => {
                    tracing::error!(error = format!("{error:#}"), "mirror dump failed");
                    DUMP_RETRY
                }
                Err(error) => {
                    tracing::error!(error = %error, "mirror dump task failed");
                    DUMP_RETRY
                }
            };
            tokio::time::sleep(wait).await;
        }
    });
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::diff::tests::{publish, temp_dir};
    use super::*;
    use crate::db::migrate;
    use chrono::TimeZone;

    pub(crate) fn test_pool(dir: &Path) -> Pool<SqliteConnectionManager> {
        let path = dir.join("source.sqlite3");
        let mut conn = Connection::open(&path).unwrap();
        migrate(&mut conn).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        Pool::builder()
            .max_size(4)
            .build(SqliteConnectionManager::file(path))
            .unwrap()
    }

    pub(crate) fn source_db(dir: &Path) -> PathBuf {
        dir.join("source.sqlite3")
    }

    pub(crate) fn config(dir: &Path) -> PublisherConfig {
        PublisherConfig {
            dir: dir.join("mirror"),
            diff_periods: vec![Period::parse("1h").unwrap(), Period::parse("1d").unwrap()],
            dump_period: Period::parse("7d").unwrap(),
            keep_dumps: 2,
            serve: true,
        }
    }

    pub(crate) fn at(day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, day, hour, 0, 30).unwrap()
    }

    fn stream(publisher: &Publisher, label: &str) -> Option<DiffState> {
        publisher.stream_state(label).unwrap()
    }

    #[test]
    fn hourly_diffs_aggregate_into_daily_diffs() {
        let dir = temp_dir("publisher");
        let pool = test_pool(&dir);
        let config = config(&dir);
        let publisher = prepare_publisher(&pool, Some(&config), &source_db(&dir))
            .unwrap()
            .unwrap();
        let mut conn = pool.get().unwrap();

        publish(&conn, "a", "one");
        publisher.run_due_diffs(&mut conn, at(1, 22)).unwrap();
        publish(&conn, "b", "two");
        publisher.run_due_diffs(&mut conn, at(1, 23)).unwrap();
        // Same hour again: nothing due.
        publisher.run_due_diffs(&mut conn, at(1, 23)).unwrap();
        assert_eq!(stream(&publisher, "1h").unwrap().sequence, 2);
        assert!(stream(&publisher, "1d").is_none());

        publish(&conn, "a", "three");
        publisher.run_due_diffs(&mut conn, at(2, 0)).unwrap();
        let hourly = stream(&publisher, "1h").unwrap();
        let daily = stream(&publisher, "1d").unwrap();
        assert_eq!(hourly.sequence, 3);
        assert_eq!(daily.sequence, 1);
        assert_eq!(daily.from_change_seq, 0);
        assert_eq!(daily.to_change_seq, hourly.to_change_seq);
        assert_eq!(daily.operations, 5); // 3 lyrics + 2 tracks

        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM mirror_changes", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, 0);

        // A catch-up hourly diff after downtime belongs to the next daily
        // window, so the daily diff in between is skipped, not published empty.
        publish(&conn, "c", "four");
        publisher.run_due_diffs(&mut conn, at(3, 5)).unwrap();
        assert_eq!(stream(&publisher, "1h").unwrap().sequence, 4);
        assert_eq!(stream(&publisher, "1d").unwrap().sequence, 1);

        publisher.run_due_diffs(&mut conn, at(4, 0)).unwrap();
        let hourly = stream(&publisher, "1h").unwrap();
        let next_daily = stream(&publisher, "1d").unwrap();
        assert_eq!(hourly.sequence, 5);
        assert_eq!(next_daily.sequence, 2);
        assert_eq!(next_daily.from_change_seq, daily.to_change_seq);
        assert_eq!(next_daily.to_change_seq, hourly.to_change_seq);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn epochs_survive_restarts_and_reset_when_state_is_lost() {
        let dir = temp_dir("epoch");
        let pool = test_pool(&dir);
        let config = config(&dir);
        let database = source_db(&dir);
        let first = prepare_publisher(&pool, Some(&config), &database)
            .unwrap()
            .unwrap();
        let first_epoch = first.epoch.clone();
        let mut conn = pool.get().unwrap();
        publish(&conn, "a", "one");
        first.run_due_diffs(&mut conn, at(1, 1)).unwrap();
        drop(conn);

        let error = prepare_publisher(&pool, Some(&config), &database).unwrap_err();
        assert!(error.to_string().contains("another mirror publisher"));
        let other_dir = PublisherConfig {
            dir: dir.join("other-mirror"),
            ..config.clone()
        };
        assert!(prepare_publisher(&pool, Some(&other_dir), &database).is_err());
        drop(first);

        let restarted = prepare_publisher(&pool, Some(&config), &database)
            .unwrap()
            .unwrap();
        assert_eq!(restarted.epoch, first_epoch);
        drop(restarted);

        // Losing the published diffs after pruning forces a new epoch.
        std::fs::remove_dir_all(&config.dir).unwrap();
        let reset = prepare_publisher(&pool, Some(&config), &database)
            .unwrap()
            .unwrap();
        let reset_epoch = reset.epoch.clone();
        assert_ne!(reset_epoch, first_epoch);
        drop(reset);

        assert!(prepare_publisher(&pool, None, &database).unwrap().is_none());
        let conn = pool.get().unwrap();
        assert!(!triggers_installed(&conn).unwrap());
        drop(conn);
        let reenabled = prepare_publisher(&pool, Some(&config), &database)
            .unwrap()
            .unwrap();
        assert_ne!(reenabled.epoch, reset_epoch);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_database_rewound_behind_published_diffs_starts_a_new_epoch() {
        let dir = temp_dir("rewind");
        let pool = test_pool(&dir);
        let config = config(&dir);
        let database = source_db(&dir);
        let publisher = prepare_publisher(&pool, Some(&config), &database)
            .unwrap()
            .unwrap();
        let epoch = publisher.epoch.clone();
        let mut conn = pool.get().unwrap();
        publish(&conn, "a", "one");
        publisher.run_due_diffs(&mut conn, at(1, 1)).unwrap();
        drop(publisher);

        // Simulate restoring a backup taken before that diff was published.
        conn.execute(
            "UPDATE sqlite_sequence SET seq = 1 WHERE name = 'mirror_changes'",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE mirror_state SET value = '0' WHERE key = 'publisher_pruned_seq'",
            [],
        )
        .unwrap();
        drop(conn);

        let restored = prepare_publisher(&pool, Some(&config), &database)
            .unwrap()
            .unwrap();
        assert_ne!(restored.epoch, epoch);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn dumps_run_for_new_epochs_and_on_schedule() {
        let dir = temp_dir("dump-schedule");
        let pool = test_pool(&dir);
        let config = config(&dir);
        let publisher = prepare_publisher(&pool, Some(&config), &source_db(&dir))
            .unwrap()
            .unwrap();
        let conn = pool.get().unwrap();

        // 2026-10-01 is a Thursday, i.e. a 7d boundary since the Unix epoch.
        assert!(publisher.run_due_dump(&conn, at(3, 12)).unwrap());
        assert!(!publisher.run_due_dump(&conn, at(5, 12)).unwrap());
        assert!(publisher.run_due_dump(&conn, at(8, 0)).unwrap());
        let latest = latest_dump(&config.dir).unwrap().unwrap();
        assert_eq!(
            latest.timestamp,
            Utc.with_ymd_and_hms(2026, 10, 8, 0, 0, 0).unwrap()
        );
        assert_eq!(latest.epoch, publisher.epoch);

        std::fs::remove_dir_all(dir).unwrap();
    }
}
