-- Runs retain immutable submission identity and runtime snapshots. Idempotency is owner/workspace-scoped.
CREATE TABLE dataslate_runs (
 id text PRIMARY KEY,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 issuer text NOT NULL,
 subject text NOT NULL,
 idempotency_key text NOT NULL,
 request_fingerprint text NOT NULL,
 document jsonb NOT NULL CHECK (jsonb_typeof(document) = 'object'),
 state text NOT NULL CHECK (state IN ('queued','submitting','running','succeeded','failed','cancel_requested','cancelled','unknown')),
 created_at timestamptz NOT NULL,
 lease_owner text,
 lease_fence bigint NOT NULL DEFAULT 0,
 lease_until timestamptz,
 next_reconcile_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(workspace_id,issuer,subject,idempotency_key)
);
CREATE INDEX dataslate_runs_owner ON dataslate_runs(workspace_id,issuer,subject,created_at DESC);
CREATE INDEX dataslate_runs_reconcile ON dataslate_runs(next_reconcile_at) WHERE state NOT IN ('succeeded','failed','cancelled');
