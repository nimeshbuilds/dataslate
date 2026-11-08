CREATE TABLE dataslate_queries (
 id text PRIMARY KEY,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 issuer text NOT NULL, subject text NOT NULL,
 idempotency_key text NOT NULL, request_fingerprint text NOT NULL,
 document jsonb NOT NULL, policy jsonb NOT NULL, cursor jsonb NOT NULL,
 state text NOT NULL,
 processed jsonb NOT NULL DEFAULT '[]',
 created_at timestamptz NOT NULL,
 lease_owner text, lease_fence bigint NOT NULL DEFAULT 0, lease_until timestamptz,
 next_reconcile_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(workspace_id,issuer,subject,idempotency_key)
);
CREATE INDEX dataslate_queries_owner ON dataslate_queries(workspace_id,issuer,subject,created_at DESC);
CREATE INDEX dataslate_queries_reconcile ON dataslate_queries(next_reconcile_at,id) WHERE state NOT IN ('succeeded','failed','cancelled');
CREATE TABLE dataslate_query_outputs (
 query_id text PRIMARY KEY REFERENCES dataslate_queries(id) ON DELETE CASCADE,
 source jsonb NOT NULL, result jsonb NOT NULL
);
