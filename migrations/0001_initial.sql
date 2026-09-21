CREATE TABLE IF NOT EXISTS models (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    model_type TEXT NOT NULL,
    creator TEXT,
    description TEXT,
    base_model TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    extensions TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_models_type ON models(model_type);
CREATE INDEX IF NOT EXISTS idx_models_name ON models(name COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_models_creator ON models(creator COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_models_base_model ON models(base_model COLLATE NOCASE);

CREATE TABLE IF NOT EXISTS model_versions (
    id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    version_name TEXT,
    base_model TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    source TEXT,
    source_model_id TEXT,
    source_version_id TEXT,
    source_url TEXT,
    activation_prompts TEXT NOT NULL DEFAULT '[]',
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_versions_model ON model_versions(model_id);

CREATE TABLE IF NOT EXISTS model_files (
    id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    version_id TEXT REFERENCES model_versions(id) ON DELETE SET NULL,
    path TEXT NOT NULL,
    relative_path TEXT,
    filename TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    sha256 TEXT,
    status TEXT NOT NULL DEFAULT 'available',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_files_model ON model_files(model_id);
CREATE INDEX IF NOT EXISTS idx_model_files_hash ON model_files(sha256);
CREATE INDEX IF NOT EXISTS idx_model_files_path ON model_files(path);

CREATE TABLE IF NOT EXISTS tags (
    name TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS model_tags (
    model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    tag_name TEXT NOT NULL REFERENCES tags(name) ON DELETE CASCADE,
    PRIMARY KEY(model_id, tag_name)
);

CREATE INDEX IF NOT EXISTS idx_model_tags_tag ON model_tags(tag_name);

CREATE TABLE IF NOT EXISTS model_sources (
    id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    external_model_id TEXT,
    external_version_id TEXT,
    url TEXT,
    imported_at INTEGER NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    UNIQUE(model_id, provider, external_model_id, external_version_id)
);

CREATE INDEX IF NOT EXISTS idx_model_sources_provider ON model_sources(provider, external_model_id);

CREATE TABLE IF NOT EXISTS model_assets (
    id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    source TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_assets_model ON model_assets(model_id, kind);

CREATE TABLE IF NOT EXISTS model_relationships (
    id TEXT PRIMARY KEY,
    source_model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    target_model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    relationship_type TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    UNIQUE(source_model_id, target_model_id, relationship_type)
);

CREATE INDEX IF NOT EXISTS idx_model_relationships_source ON model_relationships(source_model_id);
CREATE INDEX IF NOT EXISTS idx_model_relationships_target ON model_relationships(target_model_id);

CREATE TABLE IF NOT EXISTS registry_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    model_id TEXT,
    payload TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_registry_events_id ON registry_events(id);
CREATE INDEX IF NOT EXISTS idx_registry_events_model ON registry_events(model_id, id);
CREATE INDEX IF NOT EXISTS idx_registry_events_type ON registry_events(event_type, id);

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO schema_meta(key, value)
VALUES ('api_version', 'v1')
ON CONFLICT(key) DO NOTHING;
