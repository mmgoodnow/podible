export const RETENTION_EXECUTION_SCHEMA_SQL = `
CREATE TABLE retention_evictions (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  plan_json TEXT NOT NULL,
  job_id INTEGER NOT NULL REFERENCES jobs(id),
  created_at TEXT NOT NULL,
  finished_at TEXT,
  result_json TEXT,
  FOREIGN KEY(namespace,resource_id) REFERENCES retention_resources(namespace,resource_id) ON DELETE RESTRICT
);
CREATE INDEX retention_evictions_job ON retention_evictions(job_id,id);
CREATE INDEX retention_evictions_resource ON retention_evictions(namespace,resource_id,finished_at);
CREATE TRIGGER retention_eviction_plan_immutable BEFORE UPDATE OF namespace,resource_id,plan_json,job_id,created_at ON retention_evictions
BEGIN SELECT RAISE(ABORT,'Retention eviction plan is immutable'); END;
`;
