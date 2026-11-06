export const TRANSFER_METRICS_SCHEMA_SQL = `
CREATE TABLE transfer_metric_records (
  torrent_id INTEGER PRIMARY KEY, binding_digest TEXT NOT NULL,
  observed_at TEXT NOT NULL, snapshot_json TEXT NOT NULL
);
CREATE TABLE transfer_metric_samples (
  resolution TEXT NOT NULL CHECK(resolution IN ('raw','hourly')),
  bucket TEXT NOT NULL, observed_at TEXT NOT NULL, snapshot_json TEXT NOT NULL,
  PRIMARY KEY(resolution,bucket)
);
CREATE INDEX idx_transfer_metric_samples_time ON transfer_metric_samples(resolution,observed_at);
CREATE TABLE transfer_tracker_mappings (
  hostname TEXT PRIMARY KEY NOT NULL, inferred_name TEXT, manual_name TEXT,
  updated_at TEXT NOT NULL
);
`;
