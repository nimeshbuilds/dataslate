-- Independent immutable run source survives notebook history deletion.
CREATE TABLE dataslate_run_sources (
 run_id text PRIMARY KEY REFERENCES dataslate_runs(id) ON DELETE CASCADE,
 document jsonb NOT NULL CHECK (jsonb_typeof(document) = 'object'),
 CHECK (octet_length(document->>'source') BETWEEN 1 AND 262144)
);
CREATE INDEX dataslate_runs_source_cleanup ON dataslate_runs(next_reconcile_at,id)
 WHERE document->'sourceArtifact'->>'state' IN ('pending','ready','cleanup_pending');
