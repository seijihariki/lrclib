use super::changelog::{latest_change_seq, set_state, UPSTREAM_CHANGE_SEQ_KEY, UPSTREAM_EPOCH_KEY};
use super::{rename_durable, tmp_path, MIRRORED_TABLES};
use anyhow::{bail, ensure, Context, Result};
use flate2::{read::MultiGzDecoder, write::GzEncoder, Compression};
use rusqlite::{
    params, params_from_iter,
    types::{Value as SqlValue, ValueRef},
    Connection,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

const FETCH_CHUNK: usize = 500;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "lowercase")]
enum DiffOp {
    Upsert {
        table: String,
        row: Map<String, Value>,
    },
    Delete {
        table: String,
        id: i64,
    },
}

impl DiffOp {
    fn table(&self) -> &str {
        match self {
            DiffOp::Upsert { table, .. } | DiffOp::Delete { table, .. } => table,
        }
    }

    fn id(&self) -> Result<i64> {
        match self {
            DiffOp::Upsert { row, .. } => row
                .get("id")
                .and_then(Value::as_i64)
                .context("upsert row is missing an integer id"),
            DiffOp::Delete { id, .. } => Ok(*id),
        }
    }
}

fn table_index(table: &str) -> Result<usize> {
    MIRRORED_TABLES
        .iter()
        .position(|candidate| *candidate == table)
        .with_context(|| format!("unknown mirrored table `{table}`"))
}

fn sql_to_json(table: &str, column: &str, value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::from(value),
        ValueRef::Real(value) => Value::from(value),
        ValueRef::Text(value) => match std::str::from_utf8(value) {
            Ok(text) => Value::from(text),
            Err(_) => {
                tracing::warn!(
                    table,
                    column,
                    "mirror diff replaced invalid UTF-8 in TEXT value"
                );
                Value::from(String::from_utf8_lossy(value).into_owned())
            }
        },
        ValueRef::Blob(value) => Value::Object(Map::from_iter([(
            "$blob".to_owned(),
            hex::encode(value).into(),
        )])),
    }
}

fn json_to_sql(value: &Value) -> Result<SqlValue> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(*value as i64),
        Value::Number(number) => match number.as_i64() {
            Some(value) => SqlValue::Integer(value),
            None => SqlValue::Real(number.as_f64().context("invalid number")?),
        },
        Value::String(value) => SqlValue::Text(value.clone()),
        Value::Object(object) => match object.get("$blob").and_then(Value::as_str) {
            Some(encoded) => SqlValue::Blob(hex::decode(encoded)?),
            None => bail!("unsupported object value"),
        },
        Value::Array(_) => bail!("unsupported array value"),
    })
}

fn gz_writer(path: &Path) -> Result<GzEncoder<BufWriter<File>>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    Ok(GzEncoder::new(BufWriter::new(file), Compression::default()))
}

fn finish_gz(writer: GzEncoder<BufWriter<File>>, tmp: &Path, out: &Path) -> Result<()> {
    let file = writer
        .finish()?
        .into_inner()
        .map_err(|error| error.into_error())?;
    file.sync_all()?;
    rename_durable(tmp, out)?;
    Ok(())
}

pub(crate) fn open_diff(path: &Path) -> Result<impl BufRead> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    Ok(BufReader::new(MultiGzDecoder::new(BufReader::new(file))))
}

/// Returns `(to_change_seq, operations)`.
pub fn write_base_diff(conn: &mut Connection, from: i64, out: &Path) -> Result<(i64, u64)> {
    // A deferred read transaction pins one WAL snapshot from its first read,
    // so `to` and every fetched row agree exactly.
    let tx = conn.transaction()?;
    let to = latest_change_seq(&tx)?;
    ensure!(
        to >= from,
        "changelog sequence {to} is behind the published cursor {from}"
    );

    let tmp = tmp_path(out);
    let mut writer = gz_writer(&tmp)?;
    let mut operations = 0;

    for table in MIRRORED_TABLES {
        let ids = {
            let mut statement = tx.prepare_cached(
                "SELECT DISTINCT row_id FROM mirror_changes
                 WHERE seq > ?1 AND seq <= ?2 AND table_name = ?3
                 ORDER BY row_id",
            )?;
            let ids = statement
                .query_map(params![from, to, table], |row| row.get(0))?
                .collect::<rusqlite::Result<Vec<i64>>>()?;
            ids
        };

        // Rows that no longer exist become deletes, written before any upsert.
        let mut existing = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(FETCH_CHUNK) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let present = tx
                .prepare(&format!(
                    "SELECT id FROM {table} WHERE id IN ({placeholders})"
                ))?
                .query_map(params_from_iter(chunk), |row| row.get(0))?
                .collect::<rusqlite::Result<HashSet<i64>>>()?;
            for id in chunk {
                if present.contains(id) {
                    existing.push(*id);
                } else {
                    let op = DiffOp::Delete {
                        table: table.to_owned(),
                        id: *id,
                    };
                    serde_json::to_writer(&mut writer, &op)?;
                    writer.write_all(b"\n")?;
                    operations += 1;
                }
            }
        }

        for chunk in existing.chunks(FETCH_CHUNK) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let mut statement = tx.prepare(&format!(
                "SELECT * FROM {table} WHERE id IN ({placeholders}) ORDER BY id"
            ))?;
            let names: Vec<String> = statement
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            let mut rows = statement.query(params_from_iter(chunk))?;
            while let Some(row) = rows.next()? {
                let mut object = Map::new();
                for (index, name) in names.iter().enumerate() {
                    object.insert(name.clone(), sql_to_json(table, name, row.get_ref(index)?));
                }
                let op = DiffOp::Upsert {
                    table: table.to_owned(),
                    row: object,
                };
                serde_json::to_writer(&mut writer, &op)?;
                writer.write_all(b"\n")?;
                operations += 1;
            }
        }
    }

    finish_gz(writer, &tmp, out)?;
    tx.finish()?;
    Ok((to, operations))
}

/// Merges consecutive diffs (oldest first), keeping only the newest operation
/// for each `(table, id)`.
pub fn merge_diffs(inputs: &[PathBuf], out: &Path) -> Result<u64> {
    // Two parts per table, written in output order: deletes, then upserts.
    let part_paths: Vec<PathBuf> = MIRRORED_TABLES
        .iter()
        .flat_map(|table| ["delete", "upsert"].map(move |kind| (table, kind)))
        .map(|(table, kind)| {
            let mut part = out.as_os_str().to_os_string();
            part.push(format!(".{table}.{kind}.part"));
            PathBuf::from(part)
        })
        .collect();
    let result = merge_into_parts(inputs, &part_paths).and_then(|operations| {
        let tmp = tmp_path(out);
        let mut writer = gz_writer(&tmp)?;
        for part in &part_paths {
            std::io::copy(&mut File::open(part)?, &mut writer)?;
        }
        finish_gz(writer, &tmp, out)?;
        Ok(operations)
    });
    for part in &part_paths {
        let _ = std::fs::remove_file(part);
    }
    result
}

fn merge_into_parts(inputs: &[PathBuf], part_paths: &[PathBuf]) -> Result<u64> {
    if let Some(parent) = part_paths[0].parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut parts = part_paths
        .iter()
        .map(|path| Ok(BufWriter::new(File::create(path)?)))
        .collect::<Result<Vec<_>>>()?;
    let mut seen: HashSet<(usize, i64)> = HashSet::new();
    let mut operations = 0;

    // Newest first: the first time a key is seen it carries its final state.
    for input in inputs.iter().rev() {
        for line in open_diff(input)?.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let op: DiffOp = serde_json::from_str(&line)
                .with_context(|| format!("invalid diff line in {}", input.display()))?;
            let table = table_index(op.table())?;
            if seen.insert((table, op.id()?)) {
                let part = match op {
                    DiffOp::Delete { .. } => &mut parts[table * 2],
                    DiffOp::Upsert { .. } => &mut parts[table * 2 + 1],
                };
                part.write_all(line.as_bytes())?;
                part.write_all(b"\n")?;
                operations += 1;
            }
        }
    }

    for part in &mut parts {
        part.flush()?;
    }
    Ok(operations)
}

/// Applies a diff in one transaction, together with the upstream position it
/// brings the database to. Foreign keys are not enforced meanwhile: `tracks` and
/// `lyrics` reference each other, so no row order satisfies them mid-diff.
pub fn apply_diff(
    conn: &mut Connection,
    reader: impl BufRead,
    upstream_epoch: &str,
    to_change_seq: i64,
) -> Result<u64> {
    let foreign_keys: bool = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    conn.pragma_update(None, "foreign_keys", false)?;
    let result = apply_diff_unchecked(conn, reader, upstream_epoch, to_change_seq);
    conn.pragma_update(None, "foreign_keys", foreign_keys)?;
    result
}

fn apply_diff_unchecked(
    conn: &mut Connection,
    reader: impl BufRead,
    upstream_epoch: &str,
    to_change_seq: i64,
) -> Result<u64> {
    let tx = conn.transaction()?;

    let mut columns: HashMap<&str, HashSet<String>> = HashMap::new();
    for table in MIRRORED_TABLES {
        let mut statement = tx.prepare(&format!("PRAGMA table_info({table})"))?;
        let names = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<HashSet<_>>>()?;
        columns.insert(table, names);
    }

    let mut operations = 0;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let op: DiffOp = serde_json::from_str(&line).context("invalid diff line")?;
        let table = MIRRORED_TABLES[table_index(op.table())?];
        let id = op.id()?;

        match op {
            DiffOp::Upsert { row, .. } => {
                let known = &columns[table];
                // Column names come from upstream; only accept ones that
                // exist locally so they are safe to interpolate.
                for name in row.keys() {
                    ensure!(
                        known.contains(name),
                        "upstream column {table}.{name} does not exist locally; upgrade this server"
                    );
                }
                let names: Vec<&String> = row.keys().filter(|name| *name != "id").collect();
                let mut values = names
                    .iter()
                    .map(|name| json_to_sql(&row[name.as_str()]))
                    .collect::<Result<Vec<_>>>()?;
                values.push(SqlValue::Integer(id));

                let updated = if names.is_empty() {
                    tx.query_row(
                        &format!("SELECT COUNT(*) FROM {table} WHERE id = ?1"),
                        [id],
                        |row| row.get::<_, i64>(0),
                    )? as usize
                } else {
                    let assignments = names
                        .iter()
                        .enumerate()
                        .map(|(index, name)| format!("\"{name}\" = ?{}", index + 1))
                        .collect::<Vec<_>>()
                        .join(", ");
                    tx.prepare_cached(&format!(
                        "UPDATE {table} SET {assignments} WHERE id = ?{}",
                        names.len() + 1
                    ))?
                    .execute(params_from_iter(&values))?
                };

                if updated == 0 {
                    let column_list = names
                        .iter()
                        .map(|name| format!(", \"{name}\""))
                        .collect::<String>();
                    let placeholders = (2..=names.len() + 1)
                        .map(|index| format!(", ?{index}"))
                        .collect::<String>();
                    values.rotate_right(1);
                    tx.prepare_cached(&format!(
                        "INSERT INTO {table} (id{column_list}) VALUES (?1{placeholders})"
                    ))?
                    .execute(params_from_iter(&values))?;
                }
            }
            DiffOp::Delete { .. } => {
                tx.prepare_cached(&format!("DELETE FROM {table} WHERE id = ?1"))?
                    .execute([id])?;
            }
        }
        operations += 1;
    }

    set_state(&tx, UPSTREAM_EPOCH_KEY, upstream_epoch)?;
    set_state(&tx, UPSTREAM_CHANGE_SEQ_KEY, &to_change_seq.to_string())?;
    tx.commit()?;
    Ok(operations)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::changelog::{prune, start_epoch, tests::test_conn};
    use super::*;

    pub(crate) fn temp_dir(label: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("lrclib-mirror-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    pub(crate) fn snapshot(conn: &Connection) -> Vec<String> {
        let mut out = Vec::new();
        for table in MIRRORED_TABLES {
            let mut statement = conn
                .prepare(&format!("SELECT * FROM {table} ORDER BY id"))
                .unwrap();
            let count = statement.column_count();
            let names: Vec<String> = statement
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            let mut rows = statement.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                let values: Vec<String> = (0..count)
                    .map(|index| format!("{}={:?}", names[index], row.get_ref(index).unwrap()))
                    .collect();
                out.push(format!("{table}: {}", values.join(" ")));
            }
        }
        out
    }

    pub(crate) fn publish(conn: &Connection, name: &str, lyrics: &str) -> i64 {
        conn.execute(
            "INSERT INTO tracks (name, name_lower, artist_name, artist_name_lower, album_name, album_name_lower, duration, created_at, updated_at)
             VALUES (?1, lower(?1), 'artist', 'artist', 'album', 'album', 200.5, '2026-10-30 00:00:00', '2026-10-30 00:00:00')
             ON CONFLICT DO NOTHING",
            [name],
        )
        .unwrap();
        let track_id: i64 = conn
            .query_row("SELECT id FROM tracks WHERE name = ?1", [name], |row| {
                row.get(0)
            })
            .unwrap();
        conn.execute(
            "INSERT INTO lyrics (track_id, plain_lyrics, has_plain_lyrics, has_synced_lyrics, instrumental, lyricsfile, has_lyricsfile, created_at, updated_at)
             VALUES (?1, ?2, 1, 0, 0, ?2, 1, '2026-10-30 00:00:00', '2026-10-30 00:00:00')",
            params![track_id, lyrics],
        )
        .unwrap();
        track_id
    }

    fn apply_file(conn: &mut Connection, path: &Path, to: i64) -> u64 {
        apply_diff(conn, open_diff(path).unwrap(), "epoch", to).unwrap()
    }

    #[test]
    fn base_diffs_replicate_inserts_updates_and_deletes() {
        let dir = temp_dir("base");
        let mut source = test_conn();
        let mut replica = test_conn();
        start_epoch(&mut source).unwrap();

        publish(&source, "one", "first");
        publish(&source, "two", "second");
        publish(&source, "one", "first, revised");
        source
            .execute("INSERT INTO tracks (name) VALUES ('orphan')", [])
            .unwrap();
        let (to1, ops1) = write_base_diff(&mut source, 0, &dir.join("1.gz")).unwrap();
        assert_eq!(ops1, 6); // 3 lyrics + 3 tracks
        apply_file(&mut replica, &dir.join("1.gz"), to1);
        assert_eq!(snapshot(&replica), snapshot(&source));

        source
            .execute("UPDATE lyrics SET has_word_sync = 1 WHERE id = 2", [])
            .unwrap();
        source
            .execute(
                "INSERT INTO flags (lyrics_id, content) VALUES (3, 'bad')",
                [],
            )
            .unwrap();
        source
            .execute("DELETE FROM tracks WHERE name = 'orphan'", [])
            .unwrap();
        prune(&source, to1).unwrap();
        let (to2, ops2) = write_base_diff(&mut source, to1, &dir.join("2.gz")).unwrap();
        assert_eq!(ops2, 3);
        apply_file(&mut replica, &dir.join("2.gz"), to2);
        assert_eq!(snapshot(&replica), snapshot(&source));

        apply_file(&mut replica, &dir.join("1.gz"), to1);
        apply_file(&mut replica, &dir.join("2.gz"), to2);
        assert_eq!(snapshot(&replica), snapshot(&source));

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn merged_diffs_equal_applying_each_diff() {
        let dir = temp_dir("merge");
        let mut source = test_conn();
        start_epoch(&mut source).unwrap();
        let mut inputs = Vec::new();
        let mut cursor = 0;
        for round in 0..4 {
            publish(
                &source,
                &format!("track-{}", round % 2),
                &format!("lyrics {round}"),
            );
            if round == 2 {
                source
                    .execute("DELETE FROM lyrics WHERE id = 1", [])
                    .unwrap();
            }
            let path = dir.join(format!("{round}.gz"));
            cursor = write_base_diff(&mut source, cursor, &path).unwrap().0;
            inputs.push(path);
        }

        let merged = dir.join("merged.gz");
        let operations = merge_diffs(&inputs, &merged).unwrap();
        assert_eq!(operations, 6); // 4 lyrics ids + 2 tracks

        let mut replica = test_conn();
        apply_file(&mut replica, &merged, cursor);
        assert_eq!(snapshot(&replica), snapshot(&source));
        assert!(std::fs::read_dir(&dir).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".part")));

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn merged_diffs_delete_before_reinserting_a_unique_key() {
        let dir = temp_dir("merge-unique");
        let mut source = test_conn();
        let mut replica = test_conn();
        start_epoch(&mut source).unwrap();
        let insert =
            "INSERT INTO tracks (name, name_lower, artist_name_lower, album_name_lower, duration)
                      VALUES ('x', 'x', 'a', 'b', 100)";

        source.execute(insert, []).unwrap();
        let (to0, _) = write_base_diff(&mut source, 0, &dir.join("0.gz")).unwrap();
        apply_file(&mut replica, &dir.join("0.gz"), to0);

        source.execute("DELETE FROM tracks", []).unwrap();
        let (to1, _) = write_base_diff(&mut source, to0, &dir.join("1.gz")).unwrap();
        source.execute(insert, []).unwrap();
        let (to2, _) = write_base_diff(&mut source, to1, &dir.join("2.gz")).unwrap();

        let merged = dir.join("merged.gz");
        merge_diffs(&[dir.join("1.gz"), dir.join("2.gz")], &merged).unwrap();
        apply_file(&mut replica, &merged, to2);
        assert_eq!(snapshot(&replica), snapshot(&source));

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn apply_rejects_unknown_tables_and_columns() {
        let mut replica = test_conn();
        let unknown_table = r#"{"op":"delete","table":"users","id":1}"#;
        assert!(apply_diff(&mut replica, unknown_table.as_bytes(), "e", 1).is_err());
        let unknown_column =
            r#"{"op":"upsert","table":"tracks","row":{"id":1,"name\" = 1; --":"x"}}"#;
        assert!(apply_diff(&mut replica, unknown_column.as_bytes(), "e", 1).is_err());
        let count: i64 = replica
            .query_row("SELECT COUNT(*) FROM tracks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}
