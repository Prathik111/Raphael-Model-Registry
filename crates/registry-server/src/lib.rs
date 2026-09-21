use directories::ProjectDirs;
use rand::RngCore;
use registry_core::{
    AssetKind, EventRepository, FileStatus, IntegrityIssue, IntegrityReport, Model, ModelAsset,
    ModelFile, ModelRelationship, ModelRepository, ModelSearch, ModelSource, ModelType,
    ModelVersion, ModelVersionRepository, NewModel, NewModelAsset, NewModelFile,
    NewModelRelationship, NewModelSource, NewModelVersion, RegistryError, RegistryEvent,
    RelationshipRepository, Result, SourceRepository, Tag, TagRepository, UpdateModel,
    UpdateModelVersion, new_id, now_unix,
};
use serde_json::{Value, json};
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::net::TcpListener;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("server error: {0}")]
    Server(String),
}

fn db_error(error: sqlx::Error) -> RegistryError {
    RegistryError::Storage(error.to_string())
}

fn json_string(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

pub(crate) fn optional_string(row: &sqlx::sqlite::SqliteRow, name: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(name).ok().flatten()
}

fn model_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Model> {
    let model_type = row.get::<String, _>("model_type").parse::<ModelType>()?;
    let extensions = serde_json::from_str::<Value>(&row.get::<String, _>("extensions"))
        .map_err(|e| RegistryError::Storage(format!("invalid model extensions: {e}")))?;
    Ok(Model {
        id: row.get("id"),
        name: row.get("name"),
        model_type,
        creator: optional_string(row, "creator"),
        description: optional_string(row, "description"),
        base_model: optional_string(row, "base_model"),
        revision: row.get("revision"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        extensions,
    })
}

fn version_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ModelVersion> {
    let activation_prompts =
        serde_json::from_str::<Vec<String>>(&row.get::<String, _>("activation_prompts"))
            .map_err(|e| RegistryError::Storage(format!("invalid activation prompts: {e}")))?;
    let metadata = serde_json::from_str::<Value>(&row.get::<String, _>("metadata"))
        .map_err(|e| RegistryError::Storage(format!("invalid version metadata: {e}")))?;
    if !metadata.is_object() {
        return Err(RegistryError::Storage(
            "version metadata must be a JSON object".into(),
        ));
    }
    Ok(ModelVersion {
        id: row.get("id"),
        model_id: row.get("model_id"),
        version_name: optional_string(row, "version_name"),
        base_model: optional_string(row, "base_model"),
        revision: row.get("revision"),
        source: optional_string(row, "source"),
        source_model_id: optional_string(row, "source_model_id"),
        source_version_id: optional_string(row, "source_version_id"),
        source_url: optional_string(row, "source_url"),
        activation_prompts,
        metadata,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn file_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ModelFile> {
    Ok(ModelFile {
        id: row.get("id"),
        model_id: row.get("model_id"),
        version_id: optional_string(row, "version_id"),
        path: row.get("path"),
        relative_path: optional_string(row, "relative_path"),
        filename: row.get("filename"),
        size_bytes: row.get("size_bytes"),
        modified_at: row.get("modified_at"),
        sha256: optional_string(row, "sha256"),
        status: row.get::<String, _>("status").parse::<FileStatus>()?,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn asset_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ModelAsset> {
    Ok(ModelAsset {
        id: row.get("id"),
        model_id: row.get("model_id"),
        kind: row.get::<String, _>("kind").parse::<AssetKind>()?,
        path: row.get("path"),
        source: optional_string(row, "source"),
        metadata: serde_json::from_str::<Value>(&row.get::<String, _>("metadata"))
            .map_err(|e| RegistryError::Storage(format!("invalid asset metadata: {e}")))?,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn source_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ModelSource> {
    Ok(ModelSource {
        id: row.get("id"),
        model_id: row.get("model_id"),
        provider: row.get("provider"),
        external_model_id: optional_string(row, "external_model_id"),
        external_version_id: optional_string(row, "external_version_id"),
        url: optional_string(row, "url"),
        imported_at: row.get("imported_at"),
        metadata: serde_json::from_str::<Value>(&row.get::<String, _>("metadata"))
            .map_err(|e| RegistryError::Storage(format!("invalid source metadata: {e}")))?,
    })
}

fn relationship_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ModelRelationship> {
    Ok(ModelRelationship {
        id: row.get("id"),
        source_model_id: row.get("source_model_id"),
        target_model_id: row.get("target_model_id"),
        relationship_type: row.get::<String, _>("relationship_type").parse()?,
        metadata: serde_json::from_str::<Value>(&row.get::<String, _>("metadata"))
            .map_err(|e| RegistryError::Storage(format!("invalid relationship metadata: {e}")))?,
        created_at: row.get("created_at"),
    })
}

async fn insert_event(
    tx: &mut Transaction<'_, Sqlite>,
    event_type: &str,
    actor: &str,
    model_id: Option<&str>,
    payload: Value,
) -> Result<i64> {
    let result = sqlx::query(
        "INSERT INTO registry_events(event_type,actor,model_id,payload,created_at) VALUES(?,?,?,?,?)",
    )
    .bind(event_type)
    .bind(actor)
    .bind(model_id)
    .bind(json_string(&payload))
    .bind(now_unix())
    .execute(&mut **tx)
    .await
    .map_err(db_error)?;
    Ok(result.last_insert_rowid())
}

#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| RegistryError::Storage(e.to_string()))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(options)
            .await
            .map_err(db_error)?;
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .map_err(|e| RegistryError::Storage(format!("migration failed: {e}")))?;
        Ok(Self { pool })
    }

    pub async fn in_memory() -> Result<Self> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|e| RegistryError::Storage(e.to_string()))?
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(db_error)?;
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .map_err(|e| RegistryError::Storage(format!("migration failed: {e}")))?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn model_exists(&self, id: &str) -> Result<bool> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM models WHERE id=?")
                .bind(id)
                .fetch_one(&self.pool)
                .await
                .map_err(db_error)?
                > 0,
        )
    }

    async fn version_belongs(&self, model_id: &str, version_id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM model_versions WHERE id=? AND model_id=?",
        )
        .bind(version_id)
        .bind(model_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?
            > 0)
    }
}

#[async_trait::async_trait]
impl ModelRepository for SqliteStore {
    async fn create_model(&self, actor: &str, input: NewModel) -> Result<Model> {
        let id = input.id.unwrap_or_else(|| new_id("model"));
        let now = now_unix();
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query("INSERT INTO models(id,name,model_type,creator,description,base_model,revision,created_at,updated_at,extensions) VALUES(?,?,?,?,?,?,?,?,?,?)")
            .bind(&id).bind(input.name.trim()).bind(input.model_type.to_string()).bind(input.creator)
            .bind(input.description).bind(input.base_model).bind(1_i64).bind(now).bind(now).bind(json_string(&input.extensions))
            .execute(&mut *tx).await.map_err(|e| {
                if let sqlx::Error::Database(db) = &e {
                    if db.is_unique_violation() { RegistryError::Conflict(format!("model id '{id}' already exists")) } else { db_error(e) }
                } else { db_error(e) }
            })?;
        insert_event(&mut tx, "model.created", actor, Some(&id), json!({"id":id})).await?;
        tx.commit().await.map_err(db_error)?;
        self.get_model(&id).await
    }

    async fn get_model(&self, id: &str) -> Result<Model> {
        sqlx::query("SELECT id,name,model_type,creator,description,base_model,revision,created_at,updated_at,extensions FROM models WHERE id=?")
            .bind(id).fetch_optional(&self.pool).await.map_err(db_error)?
            .map(|row| model_from_row(&row)).transpose()?
            .ok_or_else(|| RegistryError::NotFound(format!("model '{id}'")))
    }

    async fn update_model(&self, actor: &str, id: &str, input: UpdateModel) -> Result<Model> {
        let expected_revision = input.expected_revision;
        let now = now_unix();
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let mut qb = sqlx::QueryBuilder::<Sqlite>::new("UPDATE models SET ");
        let mut assignment_count = 0_u8;
        macro_rules! assignment {
            ($column:literal, $value:expr) => {{
                if assignment_count > 0 {
                    qb.push(", ");
                }
                qb.push($column).push("=").push_bind($value);
                assignment_count += 1;
            }};
        }
        if let Some(value) = input.name {
            assignment!("name", value);
        }
        if let Some(value) = input.model_type {
            assignment!("model_type", value.to_string());
        }
        if let Some(value) = input.creator {
            assignment!("creator", value);
        }
        if let Some(value) = input.description {
            assignment!("description", value);
        }
        if let Some(value) = input.base_model {
            assignment!("base_model", value);
        }
        if let Some(value) = input.extensions {
            assignment!("extensions", json_string(&value));
        }
        if assignment_count > 0 {
            qb.push(", ");
        }
        qb.push("revision=revision+1, updated_at=").push_bind(now);
        qb.push(" WHERE id=")
            .push_bind(id)
            .push(" AND revision=")
            .push_bind(expected_revision);
        if qb
            .build()
            .execute(&mut *tx)
            .await
            .map_err(db_error)?
            .rows_affected()
            == 0
        {
            let current = sqlx::query_scalar::<_, i64>("SELECT revision FROM models WHERE id=?")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db_error)?;
            return match current {
                None => Err(RegistryError::NotFound(format!("model '{id}'"))),
                Some(current) => Err(RegistryError::RevisionConflict {
                    expected: expected_revision,
                    current,
                }),
            };
        }
        insert_event(
            &mut tx,
            "model.updated",
            actor,
            Some(id),
            json!({"id":id,"revision":expected_revision+1}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        self.get_model(id).await
    }

    async fn delete_model(&self, actor: &str, id: &str) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM models WHERE id=?")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .map_err(db_error)?;
        if exists == 0 {
            return Err(RegistryError::NotFound(format!("model '{id}'")));
        }
        sqlx::query("DELETE FROM models WHERE id=?")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        insert_event(&mut tx, "model.deleted", actor, Some(id), json!({"id":id})).await?;
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }

    async fn search_models(&self, query: ModelSearch) -> Result<registry_core::SearchResult> {
        let query = query.normalized();
        let mut qb = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT m.id,m.name,m.model_type,m.creator,m.description,m.base_model,m.revision,m.created_at,m.updated_at,m.extensions,COUNT(*) OVER() AS total FROM models m WHERE 1=1",
        );
        if let Some(q) = &query.q {
            let pattern = format!("%{q}%");
            qb.push(" AND (m.name LIKE ")
                .push_bind(pattern.clone())
                .push(" OR m.creator LIKE ")
                .push_bind(pattern.clone())
                .push(" OR m.description LIKE ")
                .push_bind(pattern.clone())
                .push(" OR m.id LIKE ")
                .push_bind(pattern)
                .push(')');
        }
        if let Some(model_type) = &query.model_type {
            qb.push(" AND m.model_type=")
                .push_bind(model_type.to_string());
        }
        if let Some(tag) = &query.tag {
            qb.push(" AND EXISTS (SELECT 1 FROM model_tags mt JOIN tags t ON t.name=mt.tag_name WHERE mt.model_id=m.id AND lower(t.name)=lower(")
                .push_bind(tag).push("))");
        }
        if let Some(creator) = &query.creator {
            qb.push(" AND lower(m.creator)=lower(")
                .push_bind(creator)
                .push(')');
        }
        if let Some(base) = &query.base_model {
            qb.push(" AND lower(m.base_model)=lower(")
                .push_bind(base)
                .push(')');
        }
        if let Some(source) = &query.source {
            qb.push(" AND EXISTS (SELECT 1 FROM model_sources ms WHERE ms.model_id=m.id AND lower(ms.provider)=lower(")
                .push_bind(source).push("))");
        }
        if let Some(hash) = &query.hash {
            qb.push(" AND EXISTS (SELECT 1 FROM model_files mf WHERE mf.model_id=m.id AND lower(mf.sha256)=lower(")
                .push_bind(hash).push("))");
        }
        qb.push(" ORDER BY m.name COLLATE NOCASE,m.id LIMIT ")
            .push_bind(query.limit)
            .push(" OFFSET ")
            .push_bind(query.offset);
        let rows = qb.build().fetch_all(&self.pool).await.map_err(db_error)?;
        let total = rows
            .first()
            .map(|row| row.get::<i64, _>("total"))
            .unwrap_or(0);
        let items = rows
            .iter()
            .map(model_from_row)
            .collect::<Result<Vec<_>>>()?;
        Ok(registry_core::SearchResult {
            items,
            total,
            limit: query.limit,
            offset: query.offset,
        })
    }
}

#[async_trait::async_trait]
impl ModelVersionRepository for SqliteStore {
    async fn create_version(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelVersion,
    ) -> Result<ModelVersion> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let id = input.id.unwrap_or_else(|| new_id("version"));
        let now = now_unix();
        let prompts = Value::Array(
            input
                .activation_prompts
                .into_iter()
                .map(Value::String)
                .collect(),
        );
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query("INSERT INTO model_versions(id,model_id,version_name,base_model,revision,source,source_model_id,source_version_id,source_url,activation_prompts,metadata,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&id).bind(model_id).bind(input.version_name).bind(input.base_model).bind(1_i64)
            .bind(input.source).bind(input.source_model_id).bind(input.source_version_id).bind(input.source_url)
            .bind(json_string(&prompts)).bind(json_string(&input.metadata)).bind(now).bind(now)
            .execute(&mut *tx).await.map_err(|e| if let sqlx::Error::Database(db)=&e {
                if db.is_unique_violation() { RegistryError::Conflict(format!("model version '{id}' already exists")) } else { db_error(e) }
            } else { db_error(e) })?;
        insert_event(
            &mut tx,
            "model.version.created",
            actor,
            Some(model_id),
            json!({"id":id}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        self.get_version(model_id, &id).await
    }

    async fn get_version(&self, model_id: &str, version_id: &str) -> Result<ModelVersion> {
        sqlx::query("SELECT id,model_id,version_name,base_model,revision,source,source_model_id,source_version_id,source_url,activation_prompts,metadata,created_at,updated_at FROM model_versions WHERE id=? AND model_id=?")
            .bind(version_id).bind(model_id).fetch_optional(&self.pool).await.map_err(db_error)?
            .map(|row| version_from_row(&row)).transpose()?
            .ok_or_else(|| RegistryError::NotFound(format!("model version '{version_id}'")))
    }

    async fn list_versions(&self, model_id: &str) -> Result<Vec<ModelVersion>> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let rows = sqlx::query("SELECT id,model_id,version_name,base_model,revision,source,source_model_id,source_version_id,source_url,activation_prompts,metadata,created_at,updated_at FROM model_versions WHERE model_id=? ORDER BY created_at DESC,id")
            .bind(model_id).fetch_all(&self.pool).await.map_err(db_error)?;
        rows.iter().map(version_from_row).collect()
    }

    async fn update_version(
        &self,
        actor: &str,
        model_id: &str,
        version_id: &str,
        input: UpdateModelVersion,
    ) -> Result<ModelVersion> {
        if !self.version_belongs(model_id, version_id).await? {
            return Err(RegistryError::NotFound(format!(
                "model version '{version_id}'"
            )));
        }
        let expected_revision = input.expected_revision;
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let mut qb = sqlx::QueryBuilder::<Sqlite>::new("UPDATE model_versions SET ");
        let mut assignment_count = 0_u8;
        macro_rules! assignment {
            ($column:literal, $value:expr) => {{
                if assignment_count > 0 {
                    qb.push(", ");
                }
                qb.push($column).push("=").push_bind($value);
                assignment_count += 1;
            }};
        }
        if let Some(value) = input.version_name {
            assignment!("version_name", value);
        }
        if let Some(value) = input.base_model {
            assignment!("base_model", value);
        }
        if let Some(value) = input.source {
            assignment!("source", value);
        }
        if let Some(value) = input.source_model_id {
            assignment!("source_model_id", value);
        }
        if let Some(value) = input.source_version_id {
            assignment!("source_version_id", value);
        }
        if let Some(value) = input.source_url {
            assignment!("source_url", value);
        }
        if let Some(value) = input.activation_prompts {
            let prompts = Value::Array(value.into_iter().map(Value::String).collect());
            assignment!("activation_prompts", json_string(&prompts));
        }
        if let Some(value) = input.metadata {
            assignment!("metadata", json_string(&value));
        }
        if assignment_count > 0 {
            qb.push(", ");
        }
        qb.push("revision=revision+1, updated_at=")
            .push_bind(now_unix());
        qb.push(" WHERE id=")
            .push_bind(version_id)
            .push(" AND model_id=")
            .push_bind(model_id)
            .push(" AND revision=")
            .push_bind(expected_revision);
        if qb
            .build()
            .execute(&mut *tx)
            .await
            .map_err(db_error)?
            .rows_affected()
            == 0
        {
            let current = sqlx::query_scalar::<_, i64>(
                "SELECT revision FROM model_versions WHERE id=? AND model_id=?",
            )
            .bind(version_id)
            .bind(model_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?;
            return match current {
                None => Err(RegistryError::NotFound(format!(
                    "model version '{version_id}'"
                ))),
                Some(current) => Err(RegistryError::RevisionConflict {
                    expected: expected_revision,
                    current,
                }),
            };
        }
        insert_event(
            &mut tx,
            "model.version.updated",
            actor,
            Some(model_id),
            json!({"id":version_id,"revision":expected_revision+1}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        self.get_version(model_id, version_id).await
    }
}

#[async_trait::async_trait]
impl registry_core::ModelFileRepository for SqliteStore {
    async fn list_files(&self, model_id: &str) -> Result<Vec<ModelFile>> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let rows = sqlx::query("SELECT id,model_id,version_id,path,relative_path,filename,size_bytes,modified_at,sha256,status,created_at,updated_at FROM model_files WHERE model_id=? ORDER BY filename COLLATE NOCASE,id")
            .bind(model_id).fetch_all(&self.pool).await.map_err(db_error)?;
        rows.iter().map(file_from_row).collect()
    }

    async fn add_file(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelFile,
    ) -> Result<ModelFile> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        if let Some(version_id) = &input.version_id {
            if !self.version_belongs(model_id, version_id).await? {
                return Err(RegistryError::NotFound(format!("version '{version_id}'")));
            }
        }
        if let Some(hash) = &input.sha256 {
            if let Some(existing) = sqlx::query_scalar::<_, String>(
                "SELECT id FROM model_files WHERE model_id=? AND lower(sha256)=lower(?) LIMIT 1",
            )
            .bind(model_id)
            .bind(hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_error)?
            {
                return sqlx::query("SELECT id,model_id,version_id,path,relative_path,filename,size_bytes,modified_at,sha256,status,created_at,updated_at FROM model_files WHERE id=?")
                    .bind(existing).fetch_one(&self.pool).await.map_err(db_error).and_then(|r| file_from_row(&r));
            }
        }
        let id = input.id.unwrap_or_else(|| new_id("file"));
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query("INSERT INTO model_files(id,model_id,version_id,path,relative_path,filename,size_bytes,modified_at,sha256,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&id).bind(model_id).bind(input.version_id).bind(input.path).bind(input.relative_path).bind(input.filename)
            .bind(input.size_bytes).bind(input.modified_at).bind(input.sha256).bind(input.status.to_string()).bind(now_unix()).bind(now_unix())
            .execute(&mut *tx).await.map_err(db_error)?;
        insert_event(
            &mut tx,
            "model.file.attached",
            actor,
            Some(model_id),
            json!({"id":id}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        sqlx::query("SELECT id,model_id,version_id,path,relative_path,filename,size_bytes,modified_at,sha256,status,created_at,updated_at FROM model_files WHERE id=?")
            .bind(id).fetch_one(&self.pool).await.map_err(db_error).and_then(|r| file_from_row(&r))
    }

    async fn remove_file(&self, actor: &str, model_id: &str, file_id: &str) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let deleted = sqlx::query("DELETE FROM model_files WHERE id=? AND model_id=?")
            .bind(file_id)
            .bind(model_id)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?
            .rows_affected();
        if deleted == 0 {
            return Err(RegistryError::NotFound(format!("file '{file_id}'")));
        }
        insert_event(
            &mut tx,
            "model.file.removed",
            actor,
            Some(model_id),
            json!({"id":file_id}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl TagRepository for SqliteStore {
    async fn list_tags(&self) -> Result<Vec<Tag>> {
        let rows = sqlx::query("SELECT t.name,COUNT(mt.model_id) AS count FROM tags t LEFT JOIN model_tags mt ON mt.tag_name=t.name GROUP BY t.name ORDER BY t.name COLLATE NOCASE")
            .fetch_all(&self.pool).await.map_err(db_error)?;
        Ok(rows
            .into_iter()
            .map(|row| Tag {
                name: row.get("name"),
                count: row.get("count"),
            })
            .collect())
    }

    async fn list_model_tags(&self, model_id: &str) -> Result<Vec<String>> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        sqlx::query_scalar::<_, String>(
            "SELECT tag_name FROM model_tags WHERE model_id=? ORDER BY tag_name COLLATE NOCASE",
        )
        .bind(model_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)
    }

    async fn add_tag(&self, actor: &str, model_id: &str, tag: &str) -> Result<Vec<String>> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query("INSERT INTO tags(name) VALUES(?) ON CONFLICT(name) DO NOTHING")
            .bind(tag)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        sqlx::query("INSERT INTO model_tags(model_id,tag_name) VALUES(?,?) ON CONFLICT DO NOTHING")
            .bind(model_id)
            .bind(tag)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        insert_event(
            &mut tx,
            "model.tags.updated",
            actor,
            Some(model_id),
            json!({"action":"add","tag":tag}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        self.list_model_tags(model_id).await
    }

    async fn remove_tag(&self, actor: &str, model_id: &str, tag: &str) -> Result<Vec<String>> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query("DELETE FROM model_tags WHERE model_id=? AND tag_name=?")
            .bind(model_id)
            .bind(tag)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        sqlx::query("DELETE FROM tags WHERE name=? AND NOT EXISTS(SELECT 1 FROM model_tags WHERE tag_name=?)").bind(tag).bind(tag).execute(&mut *tx).await.map_err(db_error)?;
        insert_event(
            &mut tx,
            "model.tags.updated",
            actor,
            Some(model_id),
            json!({"action":"remove","tag":tag}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        self.list_model_tags(model_id).await
    }
}

#[async_trait::async_trait]
impl SourceRepository for SqliteStore {
    async fn list_sources(&self, model_id: &str) -> Result<Vec<ModelSource>> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let rows=sqlx::query("SELECT id,model_id,provider,external_model_id,external_version_id,url,imported_at,metadata FROM model_sources WHERE model_id=? ORDER BY imported_at DESC,id")
            .bind(model_id).fetch_all(&self.pool).await.map_err(db_error)?;
        rows.iter().map(source_from_row).collect()
    }

    async fn add_source(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelSource,
    ) -> Result<ModelSource> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let existing=sqlx::query("SELECT id FROM model_sources WHERE model_id=? AND provider=? AND (external_model_id IS ? OR external_model_id=?) AND (external_version_id IS ? OR external_version_id=?) LIMIT 1")
            .bind(model_id).bind(input.provider.trim())
            .bind(input.external_model_id.clone()).bind(input.external_model_id.clone())
            .bind(input.external_version_id.clone()).bind(input.external_version_id.clone())
            .fetch_optional(&mut *tx).await.map_err(db_error)?;
        let id = if let Some(row) = existing {
            row.get::<String, _>("id")
        } else {
            let id = new_id("source");
            sqlx::query("INSERT INTO model_sources(id,model_id,provider,external_model_id,external_version_id,url,imported_at,metadata) VALUES(?,?,?,?,?,?,?,?)")
                .bind(&id).bind(model_id).bind(input.provider.trim()).bind(input.external_model_id).bind(input.external_version_id).bind(input.url).bind(now_unix()).bind(json_string(&input.metadata))
                .execute(&mut *tx).await.map_err(db_error)?;
            id
        };
        insert_event(
            &mut tx,
            "model.source.updated",
            actor,
            Some(model_id),
            json!({"id":id}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        let row=sqlx::query("SELECT id,model_id,provider,external_model_id,external_version_id,url,imported_at,metadata FROM model_sources WHERE id=?").bind(id).fetch_one(&self.pool).await.map_err(db_error)?;
        source_from_row(&row)
    }
}

#[async_trait::async_trait]
impl registry_core::AssetRepository for SqliteStore {
    async fn list_assets(&self, model_id: &str) -> Result<Vec<ModelAsset>> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let rows=sqlx::query("SELECT id,model_id,kind,path,source,metadata,created_at,updated_at FROM model_assets WHERE model_id=? ORDER BY kind,path COLLATE NOCASE")
            .bind(model_id).fetch_all(&self.pool).await.map_err(db_error)?;
        rows.iter().map(asset_from_row).collect()
    }

    async fn add_asset(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelAsset,
    ) -> Result<ModelAsset> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let id = input.id.unwrap_or_else(|| new_id("asset"));
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query("INSERT INTO model_assets(id,model_id,kind,path,source,metadata,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)")
            .bind(&id)
            .bind(model_id)
            .bind(input.kind.to_string())
            .bind(input.path)
            .bind(input.source)
            .bind(json_string(&input.metadata))
            .bind(now_unix())
            .bind(now_unix())
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        insert_event(
            &mut tx,
            "model.asset.attached",
            actor,
            Some(model_id),
            json!({"id": id}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;

        let row = sqlx::query(
            "SELECT id,model_id,kind,path,source,metadata,created_at,updated_at
             FROM model_assets WHERE id=?",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        asset_from_row(&row)
    }
}

#[async_trait::async_trait]
impl RelationshipRepository for SqliteStore {
    async fn list_relationships(&self, model_id: &str) -> Result<Vec<ModelRelationship>> {
        if !self.model_exists(model_id).await? {
            return Err(RegistryError::NotFound(format!("model '{model_id}'")));
        }
        let rows=sqlx::query("SELECT id,source_model_id,target_model_id,relationship_type,metadata,created_at FROM model_relationships WHERE source_model_id=? OR target_model_id=? ORDER BY created_at DESC,id")
            .bind(model_id).bind(model_id).fetch_all(&self.pool).await.map_err(db_error)?;
        rows.iter().map(relationship_from_row).collect()
    }

    async fn add_relationship(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelRelationship,
    ) -> Result<ModelRelationship> {
        if !self.model_exists(model_id).await? || !self.model_exists(&input.target_model_id).await?
        {
            return Err(RegistryError::NotFound(
                "relationship endpoint model not found".into(),
            ));
        }
        let id = input.id.unwrap_or_else(|| new_id("relationship"));
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query("INSERT INTO model_relationships(id,source_model_id,target_model_id,relationship_type,metadata,created_at) VALUES(?,?,?,?,?,?)")
            .bind(&id).bind(model_id).bind(input.target_model_id).bind(input.relationship_type.to_string()).bind(json_string(&input.metadata)).bind(now_unix())
            .execute(&mut *tx).await.map_err(|e| {
                if let sqlx::Error::Database(db)=&e {
                    if db.is_unique_violation() { RegistryError::Conflict("relationship already exists".into()) } else { db_error(e) }
                } else { db_error(e) }
            })?;
        insert_event(
            &mut tx,
            "model.relationship.created",
            actor,
            Some(model_id),
            json!({"id":id}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        let row=sqlx::query("SELECT id,source_model_id,target_model_id,relationship_type,metadata,created_at FROM model_relationships WHERE id=?").bind(id).fetch_one(&self.pool).await.map_err(db_error)?;
        relationship_from_row(&row)
    }

    async fn delete_relationship(
        &self,
        actor: &str,
        model_id: &str,
        relationship_id: &str,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let deleted=sqlx::query("DELETE FROM model_relationships WHERE id=? AND (source_model_id=? OR target_model_id=?)").bind(relationship_id).bind(model_id).bind(model_id).execute(&mut *tx).await.map_err(db_error)?.rows_affected();
        if deleted == 0 {
            return Err(RegistryError::NotFound(format!(
                "relationship '{relationship_id}'"
            )));
        }
        insert_event(
            &mut tx,
            "model.relationship.deleted",
            actor,
            Some(model_id),
            json!({"id":relationship_id}),
        )
        .await?;
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl EventRepository for SqliteStore {
    async fn list_events(&self, after_id: i64, limit: i64) -> Result<Vec<RegistryEvent>> {
        let rows=sqlx::query("SELECT id,event_type,actor,model_id,payload,created_at FROM registry_events WHERE id>? ORDER BY id LIMIT ?")
            .bind(after_id).bind(limit.clamp(1,500)).fetch_all(&self.pool).await.map_err(db_error)?;
        rows.into_iter()
            .map(|row| {
                let payload = serde_json::from_str::<Value>(&row.get::<String, _>("payload"))
                    .map_err(|e| RegistryError::Storage(format!("invalid event payload: {e}")))?;
                if !payload.is_object() {
                    return Err(RegistryError::Storage(
                        "event payload must be a JSON object".into(),
                    ));
                }
                Ok(RegistryEvent {
                    id: row.get("id"),
                    event_type: row.get("event_type"),
                    actor: row.get("actor"),
                    model_id: optional_string(&row, "model_id"),
                    payload,
                    created_at: row.get("created_at"),
                })
            })
            .collect()
    }

    async fn integrity_report(&self) -> Result<IntegrityReport> {
        let counts = [
            (
                "models",
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM models")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db_error)?,
            ),
            (
                "versions",
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_versions")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db_error)?,
            ),
            (
                "files",
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_files")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db_error)?,
            ),
            (
                "tags",
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tags")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db_error)?,
            ),
            (
                "sources",
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_sources")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db_error)?,
            ),
            (
                "assets",
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_assets")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db_error)?,
            ),
            (
                "relationships",
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_relationships")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db_error)?,
            ),
        ];
        let mut issues = Vec::new();
        for row in sqlx::query("SELECT id,model_type FROM models WHERE model_type NOT IN ('checkpoint','lora','vae','embedding','controlnet','upscaler','text_encoder','clip_vision','ip_adapter','other')").fetch_all(&self.pool).await.map_err(db_error)? {
            issues.push(IntegrityIssue{code:"invalid_model_type".into(),message:format!("unsupported model type '{}'",row.get::<String,_>("model_type")),entity:Some("model".into()),id:Some(row.get("id"))});
        }
        for row in sqlx::query("SELECT sha256,COUNT(*) AS count FROM model_files WHERE sha256 IS NOT NULL AND trim(sha256)<>'' GROUP BY lower(sha256) HAVING COUNT(*)>1").fetch_all(&self.pool).await.map_err(db_error)? {
            issues.push(IntegrityIssue{code:"duplicate_hash".into(),message:format!("sha256 '{}' is attached to {} files",row.get::<String,_>("sha256"),row.get::<i64,_>("count")),entity:Some("model_file".into()),id:None});
        }
        for row in sqlx::query(
            "SELECT id FROM models WHERE json_valid(extensions)=0 OR json_type(extensions) <> 'object'
             UNION ALL
             SELECT id FROM model_versions WHERE json_valid(metadata)=0 OR json_type(metadata) <> 'object'
             UNION ALL
             SELECT id FROM model_versions WHERE json_valid(activation_prompts)=0 OR json_type(activation_prompts) <> 'array'
             UNION ALL
             SELECT id FROM model_assets WHERE json_valid(metadata)=0 OR json_type(metadata) <> 'object'
             UNION ALL
             SELECT id FROM model_sources WHERE json_valid(metadata)=0 OR json_type(metadata) <> 'object'
             UNION ALL
             SELECT id FROM model_relationships WHERE json_valid(metadata)=0 OR json_type(metadata) <> 'object'
             UNION ALL
             SELECT id FROM registry_events WHERE json_valid(payload)=0 OR json_type(payload) <> 'object'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?
        {
            issues.push(IntegrityIssue {
                code: "bad_json".into(),
                message: "stored JSON has invalid syntax or an unexpected type".into(),
                entity: None,
                id: Some(row.get("id")),
            });
        }
        for row in
            sqlx::query("SELECT id FROM model_files WHERE trim(path)='' OR trim(filename)=''")
                .fetch_all(&self.pool)
                .await
                .map_err(db_error)?
        {
            issues.push(IntegrityIssue {
                code: "invalid_path".into(),
                message: "model file has an empty path or filename".into(),
                entity: Some("model_file".into()),
                id: Some(row.get("id")),
            });
        }
        for row in sqlx::query("SELECT name FROM tags t WHERE NOT EXISTS (SELECT 1 FROM model_tags mt WHERE mt.tag_name=t.name)").fetch_all(&self.pool).await.map_err(db_error)? {
            issues.push(IntegrityIssue{code:"orphaned_tag".into(),message:format!("tag '{}' has no model references",row.get::<String,_>("name")),entity:Some("tag".into()),id:Some(row.get("name"))});
        }
        for row in sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)?
        {
            issues.push(IntegrityIssue {
                code: "foreign_key_violation".into(),
                message: format!(
                    "foreign key violation in table '{}'",
                    row.try_get::<String, _>("table").unwrap_or_default()
                ),
                entity: None,
                id: None,
            });
        }
        Ok(IntegrityReport {
            ok: issues.is_empty(),
            checked_models: counts[0].1,
            checked_versions: counts[1].1,
            checked_files: counts[2].1,
            checked_tags: counts[3].1,
            checked_sources: counts[4].1,
            checked_assets: counts[5].1,
            checked_relationships: counts[6].1,
            issues,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RegistryConfig {
    pub data_dir: PathBuf,
    pub database_path: PathBuf,
    pub bind: String,
    pub port: u16,
    pub auth_token: Option<String>,
    pub cors_origin: Option<String>,
}

impl RegistryConfig {
    pub fn from_env(data_dir: Option<PathBuf>) -> Self {
        let default_data_dir = ProjectDirs::from("com", "Raphael", "ModelRegistry")
            .map(|p| p.data_dir().to_path_buf())
            .unwrap_or_else(|| PathBuf::from(".raphael-model-registry"));
        let data_dir = data_dir
            .or_else(|| std::env::var_os("RAPHAEL_REGISTRY_DATA_DIR").map(PathBuf::from))
            .unwrap_or(default_data_dir);
        let database_path = std::env::var_os("RAPHAEL_REGISTRY_DATABASE")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join("registry.sqlite"));
        Self {
            data_dir,
            database_path,
            bind: std::env::var("RAPHAEL_REGISTRY_BIND").unwrap_or_else(|_| "127.0.0.1".into()),
            port: std::env::var("RAPHAEL_REGISTRY_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(43217),
            auth_token: std::env::var("RAPHAEL_REGISTRY_AUTH_TOKEN")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            cors_origin: std::env::var("RAPHAEL_REGISTRY_CORS_ORIGIN")
                .ok()
                .filter(|v| !v.trim().is_empty()),
        }
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }
}

pub fn load_or_create_token(config: &RegistryConfig) -> io::Result<String> {
    fs::create_dir_all(&config.data_dir)?;
    if let Some(token) = &config.auth_token {
        return Ok(token.clone());
    }
    let token_path = config.data_dir.join("registry.token");
    if let Ok(value) = fs::read_to_string(&token_path) {
        let token = value.trim().to_string();
        if !token.is_empty() {
            return Ok(token);
        }
    }
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let token = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    fs::write(&token_path, format!("{token}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600));
    }
    Ok(token)
}

pub struct InstanceLock {
    path: PathBuf,
    _file: File,
}

impl InstanceLock {
    pub async fn acquire(config: &RegistryConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.data_dir)?;
        let path = config.data_dir.join("registry.lock");
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => Ok(Self { path, _file: file }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let health_url = format!("http://{}/health", config.address());
                let running = reqwest::get(health_url)
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false);
                if running {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "another Registry instance is already running",
                    ));
                }
                let _ = fs::remove_file(&path);
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                Ok(Self { path, _file: file })
            }
            Err(error) => Err(error),
        }
    }

    pub fn write_info(&mut self, bind: &str, port: u16, token_path: &Path) -> io::Result<()> {
        let info = json!({"pid":std::process::id(),"bind":bind,"port":port,"address":format!("{bind}:{port}"),"token_file":token_path});
        let mut file = self._file.try_clone()?;
        file.set_len(0)?;
        file.write_all(info.to_string().as_bytes())?;
        file.flush()?;
        fs::write(
            self.path.with_file_name("registry.pid"),
            std::process::id().to_string(),
        )?;
        fs::write(self.path.with_file_name("registry.port"), port.to_string())?;
        Ok(())
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(self.path.with_file_name("registry.pid"));
        let _ = fs::remove_file(self.path.with_file_name("registry.port"));
    }
}

pub async fn run_server(config: RegistryConfig) -> std::result::Result<(), ServerError> {
    fs::create_dir_all(&config.data_dir)?;
    let token = load_or_create_token(&config)?;
    let mut lock = InstanceLock::acquire(&config).await?;
    let listener = TcpListener::bind(config.address())
        .await
        .map_err(|e| ServerError::Server(format!("cannot bind {}: {e}", config.address())))?;
    lock.write_info(
        &config.bind,
        config.port,
        &config.data_dir.join("registry.token"),
    )?;
    let store = Arc::new(SqliteStore::connect(&config.database_path).await?);
    let service = registry_core::RegistryService::new(store);
    let state = registry_api::AppState::new(service, token, now_unix());
    let app = registry_api::router(state);
    let app = if let Some(origin) = &config.cors_origin {
        let origin = origin
            .parse::<axum::http::HeaderValue>()
            .map_err(|e| ServerError::Server(format!("invalid CORS origin: {e}")))?;
        app.layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(origin)
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any),
        )
    } else {
        app
    }
    .layer(tower_http::trace::TraceLayer::new_for_http());
    tracing::info!(address=%config.address(),database=%config.database_path.display(),"Raphael Model Registry listening");
    axum::serve(listener, app)
        .await
        .map_err(|e| ServerError::Server(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use registry_core::{ModelType, NewModel, RegistryService, UpdateModel};

    #[tokio::test]
    async fn sqlite_crud_and_revision_conflict_work() {
        let store = Arc::new(SqliteStore::in_memory().await.unwrap());
        let service = RegistryService::new(store);
        let model = service
            .create_model(
                "test",
                NewModel {
                    id: Some("model_test".into()),
                    name: "Test checkpoint".into(),
                    model_type: ModelType::Checkpoint,
                    creator: None,
                    description: None,
                    base_model: Some("sdxl".into()),
                    extensions: json!({}),
                },
            )
            .await
            .unwrap();
        assert_eq!(model.revision, 1);
        let updated = service
            .update_model(
                "test",
                &model.id,
                UpdateModel {
                    name: Some("Updated".into()),
                    expected_revision: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.revision, 2);
        let conflict = service
            .update_model(
                "other",
                &model.id,
                UpdateModel {
                    name: Some("stale".into()),
                    expected_revision: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            conflict,
            RegistryError::RevisionConflict { current: 2, .. }
        ));
    }

    #[tokio::test]
    async fn http_api_smoke_covers_auth_crud_and_relationships() {
        use registry_core::{FileStatus, ModelType};
        let store = Arc::new(SqliteStore::in_memory().await.unwrap());
        let service = registry_core::RegistryService::new(store);
        let token = "integration-test-token".to_string();
        let app = registry_api::router(registry_api::AppState::new(
            service,
            token.clone(),
            registry_core::now_unix(),
        ));

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let base = format!("http://{}", address);

        assert!(
            client
                .get(format!("{base}/health"))
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );

        let unauthorized = client
            .get(format!("{base}/api/v1/models"))
            .send()
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

        let checkpoint_response = client
            .post(format!("{base}/api/v1/models"))
            .bearer_auth(&token)
            .header("x-raphael-actor", "integration-test")
            .json(&serde_json::json!({
                "id": "model_http_checkpoint",
                "name": "HTTP Checkpoint",
                "model_type": "checkpoint",
                "base_model": "sdxl",
                "extensions": {}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(checkpoint_response.status(), reqwest::StatusCode::CREATED);
        let checkpoint: registry_core::Model = checkpoint_response.json().await.unwrap();
        assert_eq!(checkpoint.model_type, ModelType::Checkpoint);

        let lora_response = client
            .post(format!("{base}/api/v1/models"))
            .bearer_auth(&token)
            .json(&serde_json::json!({
                "id": "model_http_lora",
                "name": "HTTP LoRA",
                "model_type": "lora",
                "base_model": "sdxl",
                "extensions": {}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(lora_response.status(), reqwest::StatusCode::CREATED);

        let update_response = client
            .patch(format!("{base}/api/v1/models/{}", checkpoint.id))
            .bearer_auth(&token)
            .json(&serde_json::json!({
                "description": "updated through HTTP",
                "expected_revision": checkpoint.revision
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(update_response.status(), reqwest::StatusCode::OK);
        let updated: registry_core::Model = update_response.json().await.unwrap();
        assert_eq!(updated.revision, 2);

        let stale = client
            .patch(format!("{base}/api/v1/models/{}", checkpoint.id))
            .bearer_auth(&token)
            .json(&serde_json::json!({
                "name": "stale update",
                "expected_revision": 1
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);

        let version_response = client
            .post(format!("{base}/api/v1/models/{}/versions", checkpoint.id))
            .bearer_auth(&token)
            .json(&serde_json::json!({
                "version_name": "v1",
                "source": "civitai",
                "activation_prompts": ["trigger"],
                "metadata": {}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(version_response.status(), reqwest::StatusCode::CREATED);
        let version: registry_core::ModelVersion = version_response.json().await.unwrap();

        let file_response = client
            .post(format!("{base}/api/v1/models/{}/files", checkpoint.id))
            .bearer_auth(&token)
            .json(&serde_json::json!({
                "version_id": version.id,
                "path": "D:/models/http-checkpoint.safetensors",
                "filename": "http-checkpoint.safetensors",
                "size_bytes": 1234,
                "modified_at": 1,
                "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "status": "available"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(file_response.status(), reqwest::StatusCode::CREATED);
        let file: registry_core::ModelFile = file_response.json().await.unwrap();
        assert_eq!(file.status, FileStatus::Available);

        for request in [
            client
                .post(format!("{base}/api/v1/models/{}/tags", checkpoint.id))
                .bearer_auth(&token)
                .json(&serde_json::json!({"tag": "Portrait"})),
            client
                .post(format!("{base}/api/v1/models/{}/sources", checkpoint.id))
                .bearer_auth(&token)
                .json(&serde_json::json!({
                    "provider": "civitai",
                    "external_model_id": "123",
                    "external_version_id": "456",
                    "url": "https://civitai.com/models/123",
                    "metadata": {}
                })),
            client
                .post(format!("{base}/api/v1/models/{}/assets", checkpoint.id))
                .bearer_auth(&token)
                .json(&serde_json::json!({
                    "kind": "thumbnail",
                    "path": "D:/models/thumb.png",
                    "metadata": {}
                })),
            client
                .post(format!(
                    "{base}/api/v1/models/{}/relationships",
                    checkpoint.id
                ))
                .bearer_auth(&token)
                .json(&serde_json::json!({
                    "target_model_id": "model_http_lora",
                    "relationship_type": "compatible_with",
                    "metadata": {}
                })),
        ] {
            assert!(request.send().await.unwrap().status().is_success());
        }

        let compatibility = client
            .get(format!(
                "{base}/api/v1/compatibility?checkpoint={}&lora=model_http_lora",
                checkpoint.id
            ))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(compatibility.status(), reqwest::StatusCode::OK);
        let compatibility_body: serde_json::Value = compatibility.json().await.unwrap();
        assert_eq!(compatibility_body["compatible"], true);

        let events = client
            .get(format!("{base}/api/v1/events/snapshot"))
            .bearer_auth(&token)
            .query(&[("after_id", 0_i64)])
            .send()
            .await
            .unwrap();
        assert_eq!(events.status(), reqwest::StatusCode::OK);
        let events_body: Vec<registry_core::RegistryEvent> = events.json().await.unwrap();
        assert!(
            events_body
                .iter()
                .any(|event| event.event_type == "model.created")
        );
        assert!(
            events_body
                .iter()
                .any(|event| event.event_type == "model.updated")
        );
        assert!(
            events_body
                .iter()
                .any(|event| event.event_type == "model.file.attached")
        );
        assert!(
            events_body
                .iter()
                .any(|event| event.event_type == "model.relationship.created")
        );

        let delete_response = client
            .delete(format!("{base}/api/v1/models/{}", checkpoint.id))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(delete_response.status(), reqwest::StatusCode::NO_CONTENT);

        let missing = client
            .get(format!("{base}/api/v1/models/{}", checkpoint.id))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);

        server.abort();
    }

    #[tokio::test]
    async fn validation_normalizes_hashes_and_detects_corrupt_version_json() {
        use registry_core::{
            FileStatus, ModelType, NewModel, NewModelFile, NewModelVersion, UpdateModelVersion,
        };

        let store = Arc::new(SqliteStore::in_memory().await.unwrap());
        let service = registry_core::RegistryService::new(store.clone());
        let model = service
            .create_model(
                "test",
                NewModel {
                    id: Some("model_validation".into()),
                    name: "Validation".into(),
                    model_type: ModelType::Checkpoint,
                    creator: None,
                    description: None,
                    base_model: Some("sdxl".into()),
                    extensions: json!({}),
                },
            )
            .await
            .unwrap();
        let version = service
            .create_version(
                "test",
                &model.id,
                NewModelVersion {
                    version_name: Some("v1".into()),
                    metadata: json!({}),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let file = service
            .add_file(
                "test",
                &model.id,
                NewModelFile {
                    version_id: Some(version.id.clone()),
                    path: "model.safetensors".into(),
                    relative_path: None,
                    filename: "model.safetensors".into(),
                    size_bytes: 1,
                    modified_at: 1,
                    sha256: Some(format!("  {}  ", "A".repeat(64))),
                    status: FileStatus::Available,
                    id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(file.sha256.as_deref(), Some("a".repeat(64).as_str()));

        let invalid_source = service
            .update_version(
                "test",
                &model.id,
                &version.id,
                UpdateModelVersion {
                    source: Some(Some("   ".into())),
                    expected_revision: version.revision,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(invalid_source, RegistryError::Validation(_)));

        let invalid_prompt = service
            .update_version(
                "test",
                &model.id,
                &version.id,
                UpdateModelVersion {
                    activation_prompts: Some(vec!["x".repeat(8_193)]),
                    expected_revision: version.revision,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(invalid_prompt, RegistryError::Validation(_)));

        sqlx::query("UPDATE model_versions SET activation_prompts='{}' WHERE id=?")
            .bind(&version.id)
            .execute(store.pool())
            .await
            .unwrap();
        let corrupt_read = service
            .get_version(&model.id, &version.id)
            .await
            .unwrap_err();
        assert!(matches!(corrupt_read, RegistryError::Storage(_)));

        sqlx::query("UPDATE model_versions SET metadata='[]' WHERE id=?")
            .bind(&version.id)
            .execute(store.pool())
            .await
            .unwrap();
        let report = service.integrity_report().await.unwrap();
        assert!(!report.ok);
        assert!(report.issues.iter().any(|issue| issue.code == "bad_json"));
    }

    #[tokio::test]
    async fn tags_relationships_and_events_are_durable() {
        let store = Arc::new(SqliteStore::in_memory().await.unwrap());
        let service = RegistryService::new(store);
        let a = service
            .create_model(
                "test",
                NewModel {
                    id: Some("model_a".into()),
                    name: "A".into(),
                    model_type: ModelType::Checkpoint,
                    creator: None,
                    description: None,
                    base_model: Some("sdxl".into()),
                    extensions: json!({}),
                },
            )
            .await
            .unwrap();
        let b = service
            .create_model(
                "test",
                NewModel {
                    id: Some("model_b".into()),
                    name: "B".into(),
                    model_type: ModelType::Lora,
                    creator: None,
                    description: None,
                    base_model: Some("sdxl".into()),
                    extensions: json!({}),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            service.add_tag("test", &a.id, "Portrait").await.unwrap(),
            vec!["portrait".to_string()]
        );
        let rel = service
            .add_relationship(
                "test",
                &a.id,
                NewModelRelationship {
                    id: None,
                    target_model_id: b.id.clone(),
                    relationship_type: registry_core::RelationshipType::CompatibleWith,
                    metadata: json!({}),
                },
            )
            .await
            .unwrap();
        assert_eq!(rel.source_model_id, a.id);
        let events = service.list_events(0, 100).await.unwrap();
        assert!(events.iter().any(|e| e.event_type == "model.created"));
        assert!(events.iter().any(|e| e.event_type == "model.tags.updated"));
        assert!(
            events
                .iter()
                .any(|e| e.event_type == "model.relationship.created")
        );
    }
}
