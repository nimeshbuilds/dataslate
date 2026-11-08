-- Built-in lexical search works without extensions. Operators explicitly install
-- pgvector in public before enabling hybrid bindings; no extension is installed here.
CREATE TABLE dataslate_search_changes (
 notebook_id text PRIMARY KEY REFERENCES dataslate_notebooks(id) ON DELETE CASCADE,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 revision integer NOT NULL
);
INSERT INTO dataslate_search_changes SELECT id,workspace_id,revision FROM dataslate_notebooks;
CREATE TABLE dataslate_search_jobs (
 id text PRIMARY KEY,workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),binding_id text NOT NULL,
 issuer text NOT NULL,subject text NOT NULL,idempotency_key text NOT NULL,fingerprint text NOT NULL,
 policy_hash text NOT NULL,document_key text NOT NULL,document jsonb NOT NULL,state text NOT NULL,
 notebook_id text REFERENCES dataslate_notebooks(id) ON DELETE CASCADE,
 lease_owner text,lease_fence bigint NOT NULL DEFAULT 0,lease_until timestamptz,
 created_at timestamptz NOT NULL,
 UNIQUE(workspace_id,issuer,subject,idempotency_key)
);
CREATE INDEX dataslate_search_jobs_sources ON dataslate_search_jobs(workspace_id,binding_id,policy_hash,notebook_id);
CREATE INDEX dataslate_search_jobs_pending ON dataslate_search_jobs(created_at,id) WHERE state IN ('queued','indexing');
CREATE TABLE dataslate_search_documents (
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),binding_id text NOT NULL,id text NOT NULL,
 policy_hash text NOT NULL,authority_hash text NOT NULL DEFAULT '',catalog_key text NOT NULL DEFAULT '',
 notebook_id text REFERENCES dataslate_notebooks(id) ON DELETE CASCADE,notebook_revision integer,
 indexed_at timestamptz NOT NULL,expires_at timestamptz,document jsonb NOT NULL,
 PRIMARY KEY(workspace_id,binding_id,id)
);
CREATE TABLE dataslate_search_chunks (
 workspace_id text NOT NULL,binding_id text NOT NULL,document_id text NOT NULL,ordinal integer NOT NULL,
 content text NOT NULL,lexical tsvector GENERATED ALWAYS AS (to_tsvector('simple',content)) STORED,
 embedding real[],
 PRIMARY KEY(workspace_id,binding_id,document_id,ordinal),
 FOREIGN KEY(workspace_id,binding_id,document_id) REFERENCES dataslate_search_documents(workspace_id,binding_id,id) ON DELETE CASCADE,
 CHECK(ordinal>=0 AND ordinal<32),CHECK(octet_length(content)<=2000),
 CHECK(embedding IS NULL OR cardinality(embedding) BETWEEN 1 AND 2000)
);
CREATE INDEX dataslate_search_chunks_lexical ON dataslate_search_chunks USING gin(lexical);
