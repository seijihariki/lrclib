mod changelog;
mod diff;
mod dump;
mod follower;
mod publisher;

pub use follower::{
    bootstrap_database_if_missing, ensure_follower_database, ensure_not_follower_database,
    spawn_follower,
};
pub use publisher::{prepare_publisher, spawn_publisher, Publisher};

pub(crate) use changelog::MIRRORED_TABLES;
pub(crate) use diff::{apply_diff, merge_diffs, write_base_diff};
pub(crate) use dump::create_dump;

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

const DEFAULT_DIFF_PERIODS: &str = "1h,1d";
const DEFAULT_DUMP_PERIOD: &str = "7d";
const DEFAULT_KEEP_DUMPS: usize = 1;
const DEFAULT_FOLLOW_POLL: &str = "5m";
/// Keeps scheduling arithmetic well inside `i64`.
const MAX_PERIOD_SECS: u64 = 366 * 24 * 60 * 60;

/// A replication period. Boundaries are aligned to multiples of the period
/// since the Unix epoch (UTC), so `1h` fires on the hour and `1d` at midnight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Period {
    secs: u64,
}

impl Period {
    pub fn from_secs(secs: u64) -> Result<Self> {
        ensure!(secs > 0, "period must be positive");
        ensure!(secs <= MAX_PERIOD_SECS, "period must be at most 366 days");
        Ok(Self { secs })
    }

    pub fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        let split = value
            .find(|c: char| !c.is_ascii_digit())
            .with_context(|| format!("period `{value}` is missing a unit (s, m, h, d, w)"))?;
        let (amount, unit) = value.split_at(split);
        let amount: u64 = amount
            .parse()
            .with_context(|| format!("invalid period `{value}`"))?;
        let multiplier = match unit {
            "s" => 1,
            "m" => 60,
            "h" => 60 * 60,
            "d" => 60 * 60 * 24,
            "w" => 60 * 60 * 24 * 7,
            _ => bail!("invalid unit in period `{value}` (expected s, m, h, d, or w)"),
        };
        amount
            .checked_mul(multiplier)
            .context("period is too large")
            .and_then(Self::from_secs)
            .with_context(|| format!("invalid period `{value}`"))
    }

    pub fn secs(&self) -> u64 {
        self.secs
    }

    /// Canonical label, also used as the stream directory name.
    pub fn label(&self) -> String {
        for (unit, size) in [("d", 86_400), ("h", 3_600), ("m", 60)] {
            if self.secs.is_multiple_of(size) {
                return format!("{}{unit}", self.secs / size);
            }
        }
        format!("{}s", self.secs)
    }

    pub fn boundary(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let now = now.timestamp().max(0) as u64;
        Utc.timestamp_opt((now - now % self.secs) as i64, 0)
            .unwrap()
    }

    pub fn next_boundary(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        self.boundary(now) + chrono::Duration::seconds(self.secs as i64)
    }
}

fn parse_diff_periods(value: &str) -> Result<Vec<Period>> {
    let periods = value
        .split(',')
        .filter(|part| !part.trim().is_empty())
        .map(Period::parse)
        .collect::<Result<Vec<_>>>()?;
    validate_diff_periods(&periods)?;
    Ok(periods)
}

fn validate_diff_periods(periods: &[Period]) -> Result<()> {
    ensure!(!periods.is_empty(), "at least one diff period is required");
    for pair in periods.windows(2) {
        ensure!(
            pair[1].secs > pair[0].secs && pair[1].secs % pair[0].secs == 0,
            "diff period {} must be a larger multiple of {}",
            pair[1].label(),
            pair[0].label()
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    pub dir: PathBuf,
    /// Finest first; the first period is built from the changelog, each later
    /// one by merging the previous stream.
    pub diff_periods: Vec<Period>,
    pub dump_period: Period,
    /// Number of dumps retained on disk; `0` keeps all.
    pub keep_dumps: usize,
    pub serve: bool,
}

impl PublisherConfig {
    pub fn validate(&self) -> Result<()> {
        validate_diff_periods(&self.diff_periods).context("invalid diff periods")
    }
}

#[derive(Debug, Clone)]
pub struct FollowerConfig {
    /// Base URL (`http(s)://...`) or local directory of the upstream mirror tree.
    pub upstream: String,
    pub stream: String,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct MirrorConfig {
    pub publisher: Option<PublisherConfig>,
    pub follower: Option<FollowerConfig>,
}

impl MirrorConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok().filter(|value| !value.is_empty()))
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let publisher = match lookup("LRCLIB_MIRROR_PUBLISH_DIR") {
            Some(dir) => {
                let diff_periods = parse_diff_periods(
                    &lookup("LRCLIB_MIRROR_DIFF_PERIODS")
                        .unwrap_or_else(|| DEFAULT_DIFF_PERIODS.to_owned()),
                )
                .context("invalid LRCLIB_MIRROR_DIFF_PERIODS")?;
                let dump_period = Period::parse(
                    &lookup("LRCLIB_MIRROR_DUMP_PERIOD")
                        .unwrap_or_else(|| DEFAULT_DUMP_PERIOD.to_owned()),
                )
                .context("invalid LRCLIB_MIRROR_DUMP_PERIOD")?;
                let keep_dumps = match lookup("LRCLIB_MIRROR_KEEP_DUMPS") {
                    Some(value) => value.parse().context("invalid LRCLIB_MIRROR_KEEP_DUMPS")?,
                    None => DEFAULT_KEEP_DUMPS,
                };
                let serve = match lookup("LRCLIB_MIRROR_SERVE") {
                    Some(value) => parse_bool(&value).context("invalid LRCLIB_MIRROR_SERVE")?,
                    None => true,
                };
                Some(PublisherConfig {
                    dir: PathBuf::from(dir),
                    diff_periods,
                    dump_period,
                    keep_dumps,
                    serve,
                })
            }
            None => None,
        };

        let follower = match lookup("LRCLIB_MIRROR_UPSTREAM") {
            Some(upstream) => {
                let stream = match lookup("LRCLIB_MIRROR_UPSTREAM_STREAM") {
                    Some(value) => Period::parse(&value)
                        .context("invalid LRCLIB_MIRROR_UPSTREAM_STREAM")?
                        .label(),
                    None => "1h".to_owned(),
                };
                let poll = Period::parse(
                    &lookup("LRCLIB_MIRROR_UPSTREAM_POLL")
                        .unwrap_or_else(|| DEFAULT_FOLLOW_POLL.to_owned()),
                )
                .context("invalid LRCLIB_MIRROR_UPSTREAM_POLL")?;
                Some(FollowerConfig {
                    upstream: normalize_upstream(&upstream),
                    stream,
                    poll_interval: Duration::from_secs(poll.secs()),
                })
            }
            None => None,
        };

        Ok(Self {
            publisher,
            follower,
        })
    }
}

/// Trailing slashes are dropped from URLs only; a local path such as `/` must
/// keep its meaning.
fn normalize_upstream(upstream: &str) -> String {
    if upstream.starts_with("http://") || upstream.starts_with("https://") {
        upstream.trim_end_matches('/').to_owned()
    } else {
        upstream.to_owned()
    }
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("expected a boolean, got `{value}`"),
    }
}

/// State of one published diff. The diff contains the final state, as of
/// change sequence `to_change_seq`, of every row changed in
/// `(from_change_seq, to_change_seq]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiffState {
    pub epoch: String,
    pub sequence: u64,
    pub period: String,
    /// Nominal end of the window this diff covers.
    pub timestamp: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub from_change_seq: i64,
    pub to_change_seq: i64,
    pub operations: u64,
    pub file: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DumpState {
    pub epoch: String,
    pub change_seq: i64,
    pub timestamp: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub file: String,
    pub size: u64,
    pub sha256: String,
}

pub const DUMPS_DIR: &str = "dumps";
pub const LATEST_DUMP_STATE: &str = "dumps/latest.json";
pub const REPLICATION_DIR: &str = "replication";

pub fn stream_state_path(stream: &str) -> String {
    format!("{REPLICATION_DIR}/{stream}/state.json")
}

pub fn sequence_path(sequence: u64) -> String {
    format!(
        "{:03}/{:03}/{:03}",
        sequence / 1_000_000,
        (sequence / 1_000) % 1_000,
        sequence % 1_000
    )
}

pub fn diff_state_path(stream: &str, sequence: u64) -> String {
    format!(
        "{REPLICATION_DIR}/{stream}/{}.state.json",
        sequence_path(sequence)
    )
}

pub fn diff_data_path(stream: &str, sequence: u64) -> String {
    format!(
        "{REPLICATION_DIR}/{stream}/{}.jsonl.gz",
        sequence_path(sequence)
    )
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to parse {}", path.display())
            })?))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    write_atomic(path, &bytes)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = tmp_path(path);
    let mut file = File::create(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    rename_durable(&tmp, path)
}

/// Renames `from` to `to` and fsyncs the parent directory, so the rename
/// itself survives a power loss (file contents must already be synced).
fn rename_durable(from: &Path, to: &Path) -> Result<()> {
    std::fs::rename(from, to).with_context(|| format!("failed to write {}", to.display()))?;
    #[cfg(unix)]
    if let Some(parent) = to.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .with_context(|| format!("failed to sync directory {}", parent.display()))?;
    }
    Ok(())
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn sha256_file(path: &Path) -> Result<(String, u64)> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1 << 16];
    let mut size = 0;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }
    Ok((hex::encode(hasher.finalize()), size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn periods_parse_and_label_canonically() {
        assert_eq!(Period::parse("1h").unwrap().secs(), 3600);
        assert_eq!(Period::parse("90m").unwrap().label(), "90m");
        assert_eq!(Period::parse("60m").unwrap().label(), "1h");
        assert_eq!(Period::parse("1w").unwrap().label(), "7d");
        assert_eq!(Period::parse("45s").unwrap().label(), "45s");
        assert!(Period::parse("0h").is_err());
        assert!(Period::parse("10").is_err());
        assert!(Period::parse("1y").is_err());
        assert!(Period::parse("366d").is_ok());
        assert!(Period::parse("367d").is_err());
        // Would overflow u64 seconds.
        assert!(Period::parse("614891469123651721m").is_err());
    }

    #[test]
    fn period_boundaries_align_to_utc() {
        let hour = Period::parse("1h").unwrap();
        let now = Utc.with_ymd_and_hms(2026, 10, 30, 13, 42, 7).unwrap();
        assert_eq!(
            hour.boundary(now),
            Utc.with_ymd_and_hms(2026, 10, 30, 13, 0, 0).unwrap()
        );
        assert_eq!(
            hour.next_boundary(now),
            Utc.with_ymd_and_hms(2026, 10, 30, 14, 0, 0).unwrap()
        );
        let day = Period::parse("1d").unwrap();
        assert_eq!(
            day.boundary(now),
            Utc.with_ymd_and_hms(2026, 10, 30, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn diff_periods_must_nest() {
        assert_eq!(parse_diff_periods("1h,1d").unwrap().len(), 2);
        assert_eq!(parse_diff_periods("1m,1h,1d").unwrap().len(), 3);
        assert!(parse_diff_periods("1d,1h").is_err());
        assert!(parse_diff_periods("1h,90m").is_err());
        assert!(parse_diff_periods("").is_err());
        let empty = PublisherConfig {
            dir: PathBuf::new(),
            diff_periods: Vec::new(),
            dump_period: Period::parse("7d").unwrap(),
            keep_dumps: 1,
            serve: true,
        };
        assert!(empty.validate().is_err());
    }

    #[test]
    fn sequence_paths_are_osm_style() {
        assert_eq!(sequence_path(1), "000/000/001");
        assert_eq!(sequence_path(1_234_567), "001/234/567");
        assert_eq!(
            diff_data_path("1h", 42),
            "replication/1h/000/000/042.jsonl.gz"
        );
    }

    #[test]
    fn config_reads_env_with_defaults() {
        let env: HashMap<&str, &str> = HashMap::from([
            ("LRCLIB_MIRROR_PUBLISH_DIR", "/srv/mirror"),
            ("LRCLIB_MIRROR_UPSTREAM", "https://lrclib.net/mirror/"),
        ]);
        let config =
            MirrorConfig::from_lookup(|key| env.get(key).map(|value| value.to_string())).unwrap();
        let publisher = config.publisher.unwrap();
        assert_eq!(publisher.dir, PathBuf::from("/srv/mirror"));
        assert_eq!(
            publisher
                .diff_periods
                .iter()
                .map(Period::label)
                .collect::<Vec<_>>(),
            vec!["1h", "1d"]
        );
        assert_eq!(publisher.dump_period.label(), "7d");
        assert_eq!(publisher.keep_dumps, DEFAULT_KEEP_DUMPS);
        assert!(publisher.serve);
        let follower = config.follower.unwrap();
        assert_eq!(follower.upstream, "https://lrclib.net/mirror");
        assert_eq!(follower.stream, "1h");
        assert_eq!(follower.poll_interval, Duration::from_secs(300));

        let local = HashMap::from([("LRCLIB_MIRROR_UPSTREAM", "/")]);
        let config =
            MirrorConfig::from_lookup(|key| local.get(key).map(|value| value.to_string())).unwrap();
        assert_eq!(config.follower.unwrap().upstream, "/");

        let empty = MirrorConfig::from_lookup(|_| None).unwrap();
        assert!(empty.publisher.is_none() && empty.follower.is_none());
    }
}
