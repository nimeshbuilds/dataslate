CREATE TABLE dataslate_ingestion_pipelines (
 id text PRIMARY KEY,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 document jsonb NOT NULL CHECK(jsonb_typeof(document)='object'),
 revision integer NOT NULL CHECK(revision>0)
);
CREATE INDEX dataslate_ingestion_workspace ON dataslate_ingestion_pipelines(workspace_id,id);
-- Reservations outlive the editable pipeline. They are never garbage collected on deletion.
CREATE TABLE dataslate_ingestion_reservations (
 pipeline_id text PRIMARY KEY,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 destination_uri text NOT NULL,
 checkpoint_uri text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dataslate_runs_ingestion_writer ON dataslate_runs(next_reconcile_at,id)
 WHERE document ? 'ingestion' AND COALESCE((document->>'writerReleased')::boolean,false)=false;
