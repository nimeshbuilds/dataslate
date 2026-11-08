-- Evidence survives driver/source cleanup independently of mutable lifecycle summaries.
ALTER TABLE dataslate_runs ADD CONSTRAINT dataslate_runs_workspace_identity UNIQUE(workspace_id,id);
CREATE TABLE dataslate_ingestion_outcomes (
 run_id text PRIMARY KEY REFERENCES dataslate_runs(id) ON DELETE CASCADE,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 document jsonb NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(workspace_id,run_id) REFERENCES dataslate_runs(workspace_id,id) ON DELETE CASCADE
);
