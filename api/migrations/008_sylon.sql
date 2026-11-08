CREATE TABLE dataslate_sylon_conversations (
 id text PRIMARY KEY,workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 binding_id text NOT NULL,issuer text NOT NULL,subject text NOT NULL,document jsonb NOT NULL,
 created_at timestamptz NOT NULL,
 UNIQUE(workspace_id,id)
);
CREATE INDEX dataslate_sylon_conversations_owner ON dataslate_sylon_conversations(workspace_id,issuer,subject,created_at DESC);
CREATE TABLE dataslate_sylon_jobs (
 id text PRIMARY KEY,workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 conversation_id text NOT NULL,
 issuer text NOT NULL,subject text NOT NULL,idempotency_key text NOT NULL,fingerprint text NOT NULL,
 policy_hash text NOT NULL,document jsonb NOT NULL,context jsonb NOT NULL,
 state text NOT NULL,created_at timestamptz NOT NULL,
 lease_owner text,lease_fence bigint NOT NULL DEFAULT 0,lease_until timestamptz,
 UNIQUE(conversation_id,idempotency_key),
 FOREIGN KEY(workspace_id,conversation_id) REFERENCES dataslate_sylon_conversations(workspace_id,id) ON DELETE CASCADE
);
CREATE INDEX dataslate_sylon_jobs_conversation ON dataslate_sylon_jobs(conversation_id,created_at,id);
CREATE INDEX dataslate_sylon_jobs_pending ON dataslate_sylon_jobs(created_at,id) WHERE state IN ('queued','preparing','generating','cancel_requested');
