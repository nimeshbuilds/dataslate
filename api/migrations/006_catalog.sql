-- Observed discovery is private and bound to the exact administrator policy hash.
-- Table metadata and storage credentials are never cached here.
CREATE TABLE dataslate_catalog_observations (
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 binding_id text NOT NULL,
 checked_at timestamptz NOT NULL,
 document jsonb NOT NULL,
 PRIMARY KEY(workspace_id,binding_id)
);
