export const RETENTION_SCHEMA_SQL = `
CREATE TABLE retention_resources (
  namespace TEXT NOT NULL CHECK (length(namespace) BETWEEN 1 AND 64),
  resource_id TEXT NOT NULL CHECK (length(resource_id) BETWEEN 1 AND 256),
  desired_presence TEXT NOT NULL CHECK (desired_presence IN ('present','evicted')),
  actual_presence TEXT NOT NULL CHECK (actual_presence IN ('present','missing','unknown')),
  revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
  updated_at TEXT NOT NULL,
  PRIMARY KEY(namespace,resource_id)
);
CREATE TABLE retention_events (
  namespace TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
  action TEXT NOT NULL CHECK (action IN ('requested','evicted','observed','withdrawn')),
  reason TEXT NOT NULL CHECK (length(trim(reason)) BETWEEN 1 AND 1000),
  desired_presence TEXT NOT NULL CHECK (desired_presence IN ('present','evicted')),
  actual_presence TEXT NOT NULL CHECK (actual_presence IN ('present','missing','unknown')),
  created_at TEXT NOT NULL,
  PRIMARY KEY(namespace,resource_id,revision),
  FOREIGN KEY(namespace,resource_id) REFERENCES retention_resources(namespace,resource_id)
    ON DELETE RESTRICT
);
`;
