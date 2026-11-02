CREATE TABLE mirror_changes (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  row_id INTEGER NOT NULL
);

CREATE TABLE mirror_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
