CREATE TABLE event_monitor_cursors (
  account_id    INTEGER PRIMARY KEY REFERENCES cloud_accounts(id) ON DELETE CASCADE,
  last_event_id TEXT NOT NULL DEFAULT '',
  updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
