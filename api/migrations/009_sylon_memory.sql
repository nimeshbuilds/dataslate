CREATE TABLE dataslate_sylon_memories (
 id text PRIMARY KEY,workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 issuer text NOT NULL,subject text NOT NULL,binding_id text NOT NULL,policy_hash text NOT NULL,
 origin_job_id text NOT NULL,origin_conversation_id text NOT NULL,
 revision integer NOT NULL CHECK(revision>0),state text NOT NULL CHECK(state IN ('active','expired','deleted')),
 expires_at timestamptz NOT NULL,updated_at timestamptz NOT NULL,document jsonb NOT NULL,
 UNIQUE(workspace_id,id)
);
CREATE INDEX dataslate_sylon_memories_owner ON dataslate_sylon_memories(workspace_id,issuer,subject,updated_at DESC) WHERE state<>'deleted' AND (state<>'expired' OR ((document->'Memory'->>'purgePending')::boolean));
CREATE INDEX dataslate_sylon_memories_recall ON dataslate_sylon_memories(workspace_id,issuer,subject,binding_id,expires_at) WHERE state='active';
CREATE INDEX dataslate_sylon_memories_origin ON dataslate_sylon_memories(workspace_id,origin_conversation_id) WHERE state='active';
CREATE TABLE dataslate_sylon_memory_mutations (
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),issuer text NOT NULL,subject text NOT NULL,
 idempotency_key text NOT NULL,fingerprint text NOT NULL,receipt jsonb NOT NULL,
 PRIMARY KEY(workspace_id,issuer,subject,idempotency_key)
);
ALTER TABLE dataslate_sylon_jobs ADD CONSTRAINT dataslate_sylon_jobs_workspace_id_unique UNIQUE(workspace_id,id);
CREATE TABLE dataslate_sylon_memory_uses (
 workspace_id text NOT NULL,job_id text NOT NULL,memory_id text NOT NULL,memory_revision integer NOT NULL,
 PRIMARY KEY(job_id,memory_id,memory_revision),
 FOREIGN KEY(workspace_id,memory_id) REFERENCES dataslate_sylon_memories(workspace_id,id),
 FOREIGN KEY(workspace_id,job_id) REFERENCES dataslate_sylon_jobs(workspace_id,id) ON DELETE CASCADE
);
CREATE INDEX dataslate_sylon_memory_uses_source ON dataslate_sylon_memory_uses(workspace_id,memory_id,memory_revision);
CREATE TABLE dataslate_sylon_memory_purges (
 id bigserial PRIMARY KEY,workspace_id text NOT NULL,memory_id text NOT NULL,through_revision integer NOT NULL,
 FOREIGN KEY(workspace_id,memory_id) REFERENCES dataslate_sylon_memories(workspace_id,id),
 UNIQUE(memory_id,through_revision)
);

CREATE INDEX dataslate_sylon_memories_expiry ON dataslate_sylon_memories(expires_at,id) WHERE state='active';
