CREATE TABLE dataslate_publications (
 id text PRIMARY KEY,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 issuer text NOT NULL, subject text NOT NULL,
 idempotency_key text NOT NULL, fingerprint text NOT NULL,
 document jsonb NOT NULL, final boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL,
 next_reconcile_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_owner text, lease_fence bigint NOT NULL DEFAULT 0, lease_until timestamptz,
 UNIQUE(workspace_id,issuer,subject,idempotency_key),
 UNIQUE(workspace_id,id)
);
CREATE INDEX dataslate_publication_queue ON dataslate_publications(next_reconcile_at,id) WHERE NOT final;
CREATE TABLE dataslate_publication_targets (
 target_key text PRIMARY KEY,
 mapping_key text NOT NULL,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 binding_id text NOT NULL,
 active_id text,
 published boolean NOT NULL DEFAULT false,
 FOREIGN KEY(workspace_id,active_id) REFERENCES dataslate_publications(workspace_id,id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE dataslate_published_tables (
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 binding_id text NOT NULL,
 document jsonb NOT NULL,
 PRIMARY KEY(workspace_id,binding_id)
);
