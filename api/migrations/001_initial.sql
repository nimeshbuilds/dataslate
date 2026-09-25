CREATE TABLE dataslate_workspaces (
 id text PRIMARY KEY,
 name text NOT NULL,
 slug text NOT NULL UNIQUE,
 created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL,
 revision integer NOT NULL CHECK (revision > 0)
);
CREATE TABLE dataslate_notebooks (
 id text PRIMARY KEY,
 workspace_id text NOT NULL REFERENCES dataslate_workspaces(id),
 name text NOT NULL,
 kind text NOT NULL CHECK (kind IN ('notebook', 'sql')),
 cells jsonb NOT NULL CHECK (jsonb_typeof(cells) = 'array'),
 created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL,
 revision integer NOT NULL CHECK (revision > 0),
 UNIQUE (workspace_id, id)
);
CREATE INDEX dataslate_notebooks_workspace ON dataslate_notebooks(workspace_id);
CREATE TABLE dataslate_notebook_revisions (
 workspace_id text NOT NULL,
 notebook_id text NOT NULL,
 revision integer NOT NULL,
 document jsonb NOT NULL,
 PRIMARY KEY (workspace_id, notebook_id, revision),
 FOREIGN KEY (workspace_id, notebook_id) REFERENCES dataslate_notebooks(workspace_id,id) ON DELETE CASCADE
);
