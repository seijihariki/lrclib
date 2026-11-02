use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

/// Mirrored tables in diff apply order. `tracks` comes last so its exact row
/// state overrides `tracks.last_lyrics_id` side effects of the lyrics insert
/// trigger on the follower.
pub const MIRRORED_TABLES: [&str; 4] = ["lyrics", "flags", "missing_tracks", "tracks"];

const TRIGGER_PREFIX: &str = "lrclib_mirror_changes_";
const TRIGGER_SUFFIXES: [&str; 3] = ["ai", "au", "ad"];
pub(crate) const PUBLISHER_EPOCH_KEY: &str = "publisher_epoch";
pub(crate) const PUBLISHER_PRUNED_KEY: &str = "publisher_pruned_seq";
pub(crate) const UPSTREAM_EPOCH_KEY: &str = "upstream_epoch";
pub(crate) const UPSTREAM_CHANGE_SEQ_KEY: &str = "upstream_change_seq";

const PRUNE_BATCH: i64 = 10_000;

fn trigger_sql(table: &str) -> String {
    format!(
        "CREATE TRIGGER {TRIGGER_PREFIX}{table}_ai AFTER INSERT ON {table}
         BEGIN
           INSERT INTO mirror_changes (table_name, row_id) VALUES ('{table}', NEW.id);
         END;
         CREATE TRIGGER {TRIGGER_PREFIX}{table}_au AFTER UPDATE ON {table}
         BEGIN
           INSERT INTO mirror_changes (table_name, row_id) VALUES ('{table}', NEW.id);
           INSERT INTO mirror_changes (table_name, row_id)
             SELECT '{table}', OLD.id WHERE OLD.id IS NOT NEW.id;
         END;
         CREATE TRIGGER {TRIGGER_PREFIX}{table}_ad AFTER DELETE ON {table}
         BEGIN
           INSERT INTO mirror_changes (table_name, row_id) VALUES ('{table}', OLD.id);
         END;"
    )
}

fn trigger_names() -> impl Iterator<Item = String> {
    MIRRORED_TABLES.into_iter().flat_map(|table| {
        TRIGGER_SUFFIXES
            .into_iter()
            .map(move |suffix| format!("{TRIGGER_PREFIX}{table}_{suffix}"))
    })
}

fn installed_trigger_names(conn: &Connection) -> Result<Vec<String>> {
    let mut statement = conn.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'trigger' AND name = ?1)",
    )?;
    let mut installed = Vec::new();
    for name in trigger_names() {
        if statement.query_row([&name], |row| row.get::<_, bool>(0))? {
            installed.push(name);
        }
    }
    Ok(installed)
}

pub(crate) fn triggers_installed(conn: &Connection) -> Result<bool> {
    Ok(installed_trigger_names(conn)?.len() == MIRRORED_TABLES.len() * TRIGGER_SUFFIXES.len())
}

fn drop_triggers(conn: &Connection) -> Result<()> {
    for name in installed_trigger_names(conn)? {
        conn.execute_batch(&format!("DROP TRIGGER IF EXISTS \"{name}\";"))?;
    }
    Ok(())
}

pub(crate) fn start_epoch(conn: &mut Connection) -> Result<String> {
    let epoch = uuid::Uuid::new_v4().to_string();
    let tx = conn.transaction()?;
    drop_triggers(&tx)?;
    tx.execute("DELETE FROM mirror_changes", [])?;
    for table in MIRRORED_TABLES {
        tx.execute_batch(&trigger_sql(table))?;
    }
    set_state(&tx, PUBLISHER_EPOCH_KEY, &epoch)?;
    set_state(&tx, PUBLISHER_PRUNED_KEY, "0")?;
    tx.commit()?;
    Ok(epoch)
}

/// Removes change capture and returns whether triggers were installed. Servers
/// that never published hit only the read-only check, so startup takes no
/// writer lock.
pub(crate) fn stop_publishing(conn: &mut Connection) -> Result<bool> {
    let has_changes: bool =
        conn.query_row("SELECT EXISTS(SELECT 1 FROM mirror_changes)", [], |row| {
            row.get(0)
        })?;
    if installed_trigger_names(conn)?.is_empty()
        && !has_changes
        && get_state(conn, PUBLISHER_EPOCH_KEY)?.is_none()
        && get_state(conn, PUBLISHER_PRUNED_KEY)?.is_none()
    {
        return Ok(false);
    }
    let tx = conn.transaction()?;
    let had_triggers = !installed_trigger_names(&tx)?.is_empty();
    drop_triggers(&tx)?;
    tx.execute("DELETE FROM mirror_changes", [])?;
    tx.execute(
        "DELETE FROM mirror_state WHERE key IN (?1, ?2)",
        params![PUBLISHER_EPOCH_KEY, PUBLISHER_PRUNED_KEY],
    )?;
    tx.commit()?;
    Ok(had_triggers)
}

pub(crate) fn strip_publisher_state(conn: &Connection) -> Result<()> {
    drop_triggers(conn)?;
    conn.execute("DELETE FROM mirror_changes", [])?;
    conn.execute("DELETE FROM mirror_state", [])?;
    Ok(())
}

/// Highest change seq ever assigned (survives pruning).
pub(crate) fn latest_change_seq(conn: &Connection) -> Result<i64> {
    Ok(conn
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = 'mirror_changes'",
            [],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

/// Deletes in small batches so the writer lock is never held for long.
pub(crate) fn prune(conn: &Connection, upto: i64) -> Result<()> {
    loop {
        let deleted = conn.execute(
            "DELETE FROM mirror_changes WHERE seq IN (
               SELECT seq FROM mirror_changes WHERE seq <= ?1 ORDER BY seq LIMIT ?2
             )",
            params![upto, PRUNE_BATCH],
        )?;
        if (deleted as i64) < PRUNE_BATCH {
            break;
        }
    }
    let pruned = get_state(conn, PUBLISHER_PRUNED_KEY)?
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if upto > pruned {
        set_state(conn, PUBLISHER_PRUNED_KEY, &upto.to_string())?;
    }
    Ok(())
}

pub(crate) fn get_state(conn: &Connection, key: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT value FROM mirror_state WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()?)
}

pub(crate) fn set_state(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO mirror_state (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::migrate;

    pub(crate) fn test_conn() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        conn
    }

    fn changes(conn: &Connection) -> Vec<(String, i64)> {
        let mut statement = conn
            .prepare("SELECT table_name, row_id FROM mirror_changes ORDER BY seq")
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    #[test]
    fn disabled_publishing_does_not_write_when_nothing_is_installed() {
        let mut conn = test_conn();
        conn.execute_batch(
            "CREATE TRIGGER mirror_changes_user_owned AFTER INSERT ON flags BEGIN SELECT 1; END;",
        )
        .unwrap();
        conn.pragma_update(None, "query_only", true).unwrap();
        assert!(!stop_publishing(&mut conn).unwrap());
        conn.pragma_update(None, "query_only", false).unwrap();

        start_epoch(&mut conn).unwrap();
        assert!(stop_publishing(&mut conn).unwrap());
        let user_trigger: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'mirror_changes_user_owned')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(user_trigger);
    }

    #[test]
    fn triggers_capture_inserts_updates_and_deletes() {
        let mut conn = test_conn();
        conn.execute("INSERT INTO tracks (name) VALUES ('before')", [])
            .unwrap();
        assert!(!triggers_installed(&conn).unwrap());

        start_epoch(&mut conn).unwrap();
        assert!(triggers_installed(&conn).unwrap());
        conn.execute("INSERT INTO tracks (name) VALUES ('a')", [])
            .unwrap();
        conn.execute("INSERT INTO lyrics (track_id) VALUES (2)", [])
            .unwrap();
        conn.execute("DELETE FROM tracks WHERE id = 1", []).unwrap();

        assert_eq!(
            changes(&conn),
            vec![
                ("tracks".to_owned(), 2),
                ("lyrics".to_owned(), 1),
                // last_lyrics_id trigger
                ("tracks".to_owned(), 2),
                ("tracks".to_owned(), 1),
            ]
        );
        assert_eq!(latest_change_seq(&conn).unwrap(), 4);

        prune(&conn, 3).unwrap();
        assert_eq!(changes(&conn), vec![("tracks".to_owned(), 1)]);
        assert_eq!(latest_change_seq(&conn).unwrap(), 4);
        assert_eq!(
            get_state(&conn, PUBLISHER_PRUNED_KEY).unwrap().as_deref(),
            Some("3")
        );

        assert!(stop_publishing(&mut conn).unwrap());
        assert!(!triggers_installed(&conn).unwrap());
        conn.execute("INSERT INTO tracks (name) VALUES ('after')", [])
            .unwrap();
        assert!(changes(&conn).is_empty());
        assert!(get_state(&conn, PUBLISHER_EPOCH_KEY).unwrap().is_none());
    }
}
