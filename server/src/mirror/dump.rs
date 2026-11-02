use super::changelog::{
    get_state, latest_change_seq, set_state, strip_publisher_state, PUBLISHER_EPOCH_KEY,
    UPSTREAM_CHANGE_SEQ_KEY, UPSTREAM_EPOCH_KEY,
};
use super::{
    read_json, rename_durable, sha256_file, tmp_path, write_json_atomic, DumpState, DUMPS_DIR,
    LATEST_DUMP_STATE,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use flate2::{write::GzEncoder, Compression};
use rusqlite::Connection;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
};

/// Writes a gzip-compressed SQLite snapshot into `<dir>/dumps/` and points
/// `latest.json` at it. The copy is stripped of publisher state and stamped with
/// the upstream position a follower resumes from.
pub fn create_dump(
    conn: &Connection,
    dir: &Path,
    timestamp: DateTime<Utc>,
    keep: usize,
) -> Result<DumpState> {
    let dumps_dir = dir.join(DUMPS_DIR);
    std::fs::create_dir_all(&dumps_dir)?;
    let name = format!("lrclib-{}", timestamp.format("%Y%m%dT%H%M%SZ"));
    let snapshot = dumps_dir.join(format!(".{name}.sqlite3.partial"));
    let _ = std::fs::remove_file(&snapshot);

    let result = (|| {
        conn.execute(
            "VACUUM INTO ?1",
            [snapshot.to_str().context("dump path is not valid UTF-8")?],
        )
        .context("VACUUM INTO failed")?;

        let (epoch, change_seq) = {
            let copy = Connection::open(&snapshot)?;
            let epoch = get_state(&copy, PUBLISHER_EPOCH_KEY)?
                .context("snapshot has no publisher epoch; is publishing enabled?")?;
            let change_seq = latest_change_seq(&copy)?;
            strip_publisher_state(&copy)?;
            set_state(&copy, UPSTREAM_EPOCH_KEY, &epoch)?;
            set_state(&copy, UPSTREAM_CHANGE_SEQ_KEY, &change_seq.to_string())?;
            (epoch, change_seq)
        };

        let file = format!("{name}.sqlite3.gz");
        let out = dumps_dir.join(&file);
        let tmp = tmp_path(&out);
        {
            let mut reader = BufReader::new(File::open(&snapshot)?);
            let mut writer =
                GzEncoder::new(BufWriter::new(File::create(&tmp)?), Compression::default());
            std::io::copy(&mut reader, &mut writer)?;
            let file = writer
                .finish()?
                .into_inner()
                .map_err(|error| error.into_error())?;
            file.sync_all()?;
        }
        rename_durable(&tmp, &out)?;
        let (sha256, size) = sha256_file(&out)?;

        let state = DumpState {
            epoch,
            change_seq,
            timestamp,
            generated_at: Utc::now(),
            file,
            size,
            sha256,
        };
        write_json_atomic(&dumps_dir.join(format!("{name}.state.json")), &state)?;
        write_json_atomic(&dir.join(LATEST_DUMP_STATE), &state)?;
        Ok(state)
    })();

    let _ = std::fs::remove_file(&snapshot);
    if result.is_ok() && keep > 0 {
        if let Err(error) = prune_dumps(&dumps_dir, keep) {
            tracing::warn!(error = %error, "failed to prune old mirror dumps");
        }
    }
    result
}

pub(crate) fn latest_dump(dir: &Path) -> Result<Option<DumpState>> {
    read_json(&dir.join(LATEST_DUMP_STATE))
}

/// Names embed a sortable UTC timestamp, so the oldest sort first.
fn prune_dumps(dumps_dir: &Path, keep: usize) -> Result<()> {
    let mut states: Vec<PathBuf> = std::fs::read_dir(dumps_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("lrclib-") && name.ends_with(".state.json"))
        })
        .collect();
    states.sort();
    let excess = states.len().saturating_sub(keep);
    for state_path in states.into_iter().take(excess) {
        if let Some(state) = read_json::<DumpState>(&state_path)? {
            let _ = std::fs::remove_file(dumps_dir.join(&state.file));
        }
        std::fs::remove_file(&state_path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::changelog::start_epoch;
    use super::super::diff::tests::{publish, snapshot, temp_dir};
    use super::*;
    use crate::db::migrate;
    use chrono::TimeZone;
    use flate2::read::GzDecoder;

    #[test]
    fn dumps_capture_change_seq_and_strip_publisher_state() {
        let dir = temp_dir("dump");
        let mut source = Connection::open(dir.join("source.sqlite3")).unwrap();
        migrate(&mut source).unwrap();
        let epoch = start_epoch(&mut source).unwrap();
        publish(&source, "one", "first");

        let mut timestamps = Vec::new();
        for day in 1..=3 {
            let timestamp = Utc.with_ymd_and_hms(2026, 10, day, 0, 0, 0).unwrap();
            timestamps.push(timestamp);
            let state = create_dump(&source, &dir, timestamp, 2).unwrap();
            assert_eq!(state.epoch, epoch);
            assert_eq!(state.change_seq, 3);
        }
        let latest = latest_dump(&dir).unwrap().unwrap();
        assert_eq!(latest.timestamp, timestamps[2]);
        assert!(!dir
            .join("dumps/lrclib-20261001T000000Z.sqlite3.gz")
            .exists());
        assert!(dir
            .join("dumps/lrclib-20261002T000000Z.sqlite3.gz")
            .exists());

        let restored = dir.join("restored.sqlite3");
        std::io::copy(
            &mut GzDecoder::new(File::open(dir.join("dumps").join(&latest.file)).unwrap()),
            &mut File::create(&restored).unwrap(),
        )
        .unwrap();
        let restored = Connection::open(restored).unwrap();
        assert_eq!(snapshot(&restored), snapshot(&source));
        let triggers: i64 = restored
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'trigger' AND name LIKE 'lrclib_mirror_%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(triggers, 0);
        assert_eq!(
            get_state(&restored, UPSTREAM_CHANGE_SEQ_KEY)
                .unwrap()
                .as_deref(),
            Some("3")
        );
        assert_eq!(
            get_state(&restored, UPSTREAM_EPOCH_KEY).unwrap(),
            Some(epoch)
        );
        assert!(get_state(&restored, PUBLISHER_EPOCH_KEY).unwrap().is_none());

        std::fs::remove_dir_all(dir).unwrap();
    }
}
