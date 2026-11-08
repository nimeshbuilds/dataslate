CREATE TABLE dataslate_workflows (
 id text PRIMARY KEY,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 issuer text NOT NULL, subject text NOT NULL,
 idempotency_key text NOT NULL, fingerprint text NOT NULL,
 document jsonb NOT NULL, final boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL,
 next_reconcile_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_owner text, lease_fence bigint NOT NULL DEFAULT 0, lease_until timestamptz,
 UNIQUE(workspace_id,issuer,subject,idempotency_key)
);
CREATE INDEX dataslate_workflow_queue ON dataslate_workflows(next_reconcile_at,id) WHERE NOT final;
