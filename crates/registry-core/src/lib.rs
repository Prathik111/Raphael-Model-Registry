use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("revision conflict: expected {expected}, current {current}")]
    RevisionConflict { expected: i64, current: i64 },
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("authorization required")]
    Unauthorized,
    #[error("bad request: {0}")]
    BadRequest(String),
}

pub type Result<T> = std::result::Result<T, RegistryError>;

pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelType {
    Checkpoint,
    Lora,
    Vae,
    Embedding,
    ControlNet,
    Upscaler,
    TextEncoder,
    ClipVision,
    IpAdapter,
    Other,
}

impl ModelType {
    pub const ALL: &'static [Self] = &[
        Self::Checkpoint,
        Self::Lora,
        Self::Vae,
        Self::Embedding,
        Self::ControlNet,
        Self::Upscaler,
        Self::TextEncoder,
        Self::ClipVision,
        Self::IpAdapter,
        Self::Other,
    ];
}

impl fmt::Display for ModelType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Checkpoint => "checkpoint",
            Self::Lora => "lora",
            Self::Vae => "vae",
            Self::Embedding => "embedding",
            Self::ControlNet => "controlnet",
            Self::Upscaler => "upscaler",
            Self::TextEncoder => "text_encoder",
            Self::ClipVision => "clip_vision",
            Self::IpAdapter => "ip_adapter",
            Self::Other => "other",
        };
        f.write_str(s)
    }
}

impl std::str::FromStr for ModelType {
    type Err = RegistryError;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "checkpoint" | "checkpoints" => Ok(Self::Checkpoint),
            "lora" | "loras" | "lycoris" => Ok(Self::Lora),
            "vae" | "vaes" => Ok(Self::Vae),
            "embedding" | "embeddings" | "textual_inversion" => Ok(Self::Embedding),
            "controlnet" | "controlnets" => Ok(Self::ControlNet),
            "upscaler" | "upscalers" => Ok(Self::Upscaler),
            "text_encoder" | "text-encoder" => Ok(Self::TextEncoder),
            "clip_vision" | "clip-vision" => Ok(Self::ClipVision),
            "ip_adapter" | "ip-adapter" | "ipadapter" => Ok(Self::IpAdapter),
            "other" => Ok(Self::Other),
            other => Err(RegistryError::Validation(format!(
                "unsupported model_type '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub id: String,
    pub name: String,
    pub model_type: ModelType,
    pub creator: Option<String>,
    pub description: Option<String>,
    pub base_model: Option<String>,
    pub revision: i64,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default)]
    pub extensions: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewModel {
    pub id: Option<String>,
    pub name: String,
    pub model_type: ModelType,
    pub creator: Option<String>,
    pub description: Option<String>,
    pub base_model: Option<String>,
    #[serde(default = "empty_object")]
    pub extensions: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpdateModel {
    pub name: Option<String>,
    pub model_type: Option<ModelType>,
    pub creator: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub base_model: Option<Option<String>>,
    pub extensions: Option<Value>,
    pub expected_revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelVersion {
    pub id: String,
    pub model_id: String,
    pub version_name: Option<String>,
    pub base_model: Option<String>,
    pub revision: i64,
    pub source: Option<String>,
    pub source_model_id: Option<String>,
    pub source_version_id: Option<String>,
    pub source_url: Option<String>,
    #[serde(default)]
    pub activation_prompts: Vec<String>,
    #[serde(default)]
    pub metadata: Value,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewModelVersion {
    pub id: Option<String>,
    pub version_name: Option<String>,
    pub base_model: Option<String>,
    pub source: Option<String>,
    pub source_model_id: Option<String>,
    pub source_version_id: Option<String>,
    pub source_url: Option<String>,
    #[serde(default)]
    pub activation_prompts: Vec<String>,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpdateModelVersion {
    pub version_name: Option<Option<String>>,
    pub base_model: Option<Option<String>>,
    pub source: Option<Option<String>>,
    pub source_model_id: Option<Option<String>>,
    pub source_version_id: Option<Option<String>>,
    pub source_url: Option<Option<String>>,
    pub activation_prompts: Option<Vec<String>>,
    pub metadata: Option<Value>,
    pub expected_revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileStatus {
    Available,
    Missing,
    Invalid,
    Offline,
}

impl fmt::Display for FileStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Available => "available",
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Offline => "offline",
        })
    }
}

impl std::str::FromStr for FileStatus {
    type Err = RegistryError;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "available" => Ok(Self::Available),
            "missing" => Ok(Self::Missing),
            "invalid" => Ok(Self::Invalid),
            "offline" => Ok(Self::Offline),
            other => Err(RegistryError::Validation(format!(
                "unsupported file status '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelFile {
    pub id: String,
    pub model_id: String,
    pub version_id: Option<String>,
    pub path: String,
    pub relative_path: Option<String>,
    pub filename: String,
    pub size_bytes: i64,
    pub modified_at: i64,
    pub sha256: Option<String>,
    pub status: FileStatus,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewModelFile {
    pub id: Option<String>,
    pub version_id: Option<String>,
    pub path: String,
    pub relative_path: Option<String>,
    pub filename: String,
    pub size_bytes: i64,
    pub modified_at: i64,
    pub sha256: Option<String>,
    #[serde(default = "available_status")]
    pub status: FileStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AssetKind {
    Thumbnail,
    Cover,
    Gallery,
    Preview,
}

impl fmt::Display for AssetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Thumbnail => "thumbnail",
            Self::Cover => "cover",
            Self::Gallery => "gallery",
            Self::Preview => "preview",
        })
    }
}

impl std::str::FromStr for AssetKind {
    type Err = RegistryError;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "thumbnail" => Ok(Self::Thumbnail),
            "cover" => Ok(Self::Cover),
            "gallery" => Ok(Self::Gallery),
            "preview" => Ok(Self::Preview),
            other => Err(RegistryError::Validation(format!(
                "unsupported asset kind '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelAsset {
    pub id: String,
    pub model_id: String,
    pub kind: AssetKind,
    pub path: String,
    pub source: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewModelAsset {
    pub id: Option<String>,
    pub kind: AssetKind,
    pub path: String,
    pub source: Option<String>,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub name: String,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSource {
    pub id: String,
    pub model_id: String,
    pub provider: String,
    pub external_model_id: Option<String>,
    pub external_version_id: Option<String>,
    pub url: Option<String>,
    pub imported_at: i64,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewModelSource {
    pub provider: String,
    pub external_model_id: Option<String>,
    pub external_version_id: Option<String>,
    pub url: Option<String>,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipType {
    CompatibleWith,
    DerivedFrom,
    RecommendedWith,
    Requires,
    RelatedTo,
}

impl fmt::Display for RelationshipType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::CompatibleWith => "compatible_with",
            Self::DerivedFrom => "derived_from",
            Self::RecommendedWith => "recommended_with",
            Self::Requires => "requires",
            Self::RelatedTo => "related_to",
        })
    }
}

impl std::str::FromStr for RelationshipType {
    type Err = RegistryError;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "compatible_with" => Ok(Self::CompatibleWith),
            "derived_from" => Ok(Self::DerivedFrom),
            "recommended_with" => Ok(Self::RecommendedWith),
            "requires" => Ok(Self::Requires),
            "related_to" => Ok(Self::RelatedTo),
            other => Err(RegistryError::Validation(format!(
                "unsupported relationship type '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRelationship {
    pub id: String,
    pub source_model_id: String,
    pub target_model_id: String,
    pub relationship_type: RelationshipType,
    #[serde(default)]
    pub metadata: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewModelRelationship {
    pub id: Option<String>,
    pub target_model_id: String,
    pub relationship_type: RelationshipType,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ModelSearch {
    pub q: Option<String>,
    pub model_type: Option<ModelType>,
    pub tag: Option<String>,
    pub creator: Option<String>,
    pub base_model: Option<String>,
    pub source: Option<String>,
    pub hash: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

impl ModelSearch {
    pub fn normalized(mut self) -> Self {
        self.limit = self.limit.clamp(1, 200);
        self.offset = self.offset.max(0);
        self.q = self
            .q
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        self.tag = self
            .tag
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        self.creator = self
            .creator
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        self.base_model = self
            .base_model
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        self.source = self
            .source
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        self.hash = self
            .hash
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| !v.is_empty());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub items: Vec<Model>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityResult {
    pub model_id: String,
    pub requested_type: ModelType,
    pub candidates: Vec<Model>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEvent {
    pub id: i64,
    pub event_type: String,
    pub actor: String,
    pub model_id: Option<String>,
    #[serde(default)]
    pub payload: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityIssue {
    pub code: String,
    pub message: String,
    pub entity: Option<String>,
    pub id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub ok: bool,
    pub checked_models: i64,
    pub checked_versions: i64,
    pub checked_files: i64,
    pub checked_tags: i64,
    pub checked_sources: i64,
    pub checked_assets: i64,
    pub checked_relationships: i64,
    pub issues: Vec<IntegrityIssue>,
}

pub fn validate_name(value: &str, field: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(RegistryError::Validation(format!(
            "{field} must not be empty"
        )));
    }
    if trimmed.chars().count() > 512 {
        return Err(RegistryError::Validation(format!(
            "{field} exceeds 512 characters"
        )));
    }
    Ok(trimmed.to_string())
}

pub fn validate_sha256(value: &Option<String>) -> Result<()> {
    if let Some(hash) = value {
        let normalized = hash.trim();
        if normalized.len() != 64 || !normalized.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(RegistryError::Validation(
                "sha256 must be exactly 64 hexadecimal characters".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn validate_extensions(value: &Value) -> Result<()> {
    if !value.is_object() {
        return Err(RegistryError::Validation(
            "extensions must be a JSON object".to_string(),
        ));
    }
    Ok(())
}

fn validate_version_input(input: &NewModelVersion) -> Result<()> {
    if let Some(source) = &input.source {
        if source.trim().is_empty() {
            return Err(RegistryError::Validation(
                "source must not be empty".to_string(),
            ));
        }
    }
    for prompt in &input.activation_prompts {
        if prompt.chars().count() > 8_192 {
            return Err(RegistryError::Validation(
                "activation prompt exceeds 8192 characters".to_string(),
            ));
        }
    }
    validate_extensions(&input.metadata)
        .map_err(|e| RegistryError::Validation(format!("metadata: {e}")))?;
    Ok(())
}

#[async_trait]
pub trait ModelRepository: Send + Sync {
    async fn create_model(&self, actor: &str, input: NewModel) -> Result<Model>;
    async fn get_model(&self, id: &str) -> Result<Model>;
    async fn update_model(&self, actor: &str, id: &str, input: UpdateModel) -> Result<Model>;
    async fn delete_model(&self, actor: &str, id: &str) -> Result<()>;
    async fn search_models(&self, query: ModelSearch) -> Result<SearchResult>;
}

#[async_trait]
pub trait ModelVersionRepository: Send + Sync {
    async fn create_version(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelVersion,
    ) -> Result<ModelVersion>;
    async fn get_version(&self, model_id: &str, version_id: &str) -> Result<ModelVersion>;
    async fn list_versions(&self, model_id: &str) -> Result<Vec<ModelVersion>>;
    async fn update_version(
        &self,
        actor: &str,
        model_id: &str,
        version_id: &str,
        input: UpdateModelVersion,
    ) -> Result<ModelVersion>;
}

#[async_trait]
pub trait ModelFileRepository: Send + Sync {
    async fn list_files(&self, model_id: &str) -> Result<Vec<ModelFile>>;
    async fn add_file(&self, actor: &str, model_id: &str, input: NewModelFile)
    -> Result<ModelFile>;
    async fn remove_file(&self, actor: &str, model_id: &str, file_id: &str) -> Result<()>;
}

#[async_trait]
pub trait TagRepository: Send + Sync {
    async fn list_tags(&self) -> Result<Vec<Tag>>;
    async fn list_model_tags(&self, model_id: &str) -> Result<Vec<String>>;
    async fn add_tag(&self, actor: &str, model_id: &str, tag: &str) -> Result<Vec<String>>;
    async fn remove_tag(&self, actor: &str, model_id: &str, tag: &str) -> Result<Vec<String>>;
}

#[async_trait]
pub trait SourceRepository: Send + Sync {
    async fn list_sources(&self, model_id: &str) -> Result<Vec<ModelSource>>;
    async fn add_source(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelSource,
    ) -> Result<ModelSource>;
}

#[async_trait]
pub trait AssetRepository: Send + Sync {
    async fn list_assets(&self, model_id: &str) -> Result<Vec<ModelAsset>>;
    async fn add_asset(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelAsset,
    ) -> Result<ModelAsset>;
}

#[async_trait]
pub trait RelationshipRepository: Send + Sync {
    async fn list_relationships(&self, model_id: &str) -> Result<Vec<ModelRelationship>>;
    async fn add_relationship(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelRelationship,
    ) -> Result<ModelRelationship>;
    async fn delete_relationship(
        &self,
        actor: &str,
        model_id: &str,
        relationship_id: &str,
    ) -> Result<()>;
}

#[async_trait]
pub trait EventRepository: Send + Sync {
    async fn list_events(&self, after_id: i64, limit: i64) -> Result<Vec<RegistryEvent>>;
    async fn integrity_report(&self) -> Result<IntegrityReport>;
}

pub trait RegistryRepository:
    ModelRepository
    + ModelVersionRepository
    + ModelFileRepository
    + TagRepository
    + SourceRepository
    + AssetRepository
    + RelationshipRepository
    + EventRepository
{
}

impl<T> RegistryRepository for T where
    T: ModelRepository
        + ModelVersionRepository
        + ModelFileRepository
        + TagRepository
        + SourceRepository
        + AssetRepository
        + RelationshipRepository
        + EventRepository
{
}

#[derive(Clone)]
pub struct RegistryService {
    repo: Arc<dyn RegistryRepository>,
}

impl RegistryService {
    pub fn new(repo: Arc<dyn RegistryRepository>) -> Self {
        Self { repo }
    }

    pub async fn create_model(&self, actor: &str, mut input: NewModel) -> Result<Model> {
        input.name = validate_name(&input.name, "name")?;
        validate_extensions(&input.extensions)?;
        self.repo.create_model(actor, input).await
    }

    pub async fn get_model(&self, id: &str) -> Result<Model> {
        self.repo.get_model(id).await
    }

    pub async fn update_model(
        &self,
        actor: &str,
        id: &str,
        mut input: UpdateModel,
    ) -> Result<Model> {
        if let Some(name) = &input.name {
            input.name = Some(validate_name(name, "name")?);
        }
        if let Some(extensions) = &input.extensions {
            validate_extensions(extensions)?;
        }
        self.repo.update_model(actor, id, input).await
    }

    pub async fn delete_model(&self, actor: &str, id: &str) -> Result<()> {
        self.repo.delete_model(actor, id).await
    }

    pub async fn search_models(&self, query: ModelSearch) -> Result<SearchResult> {
        self.repo.search_models(query.normalized()).await
    }

    pub async fn create_version(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelVersion,
    ) -> Result<ModelVersion> {
        validate_version_input(&input)?;
        self.repo.create_version(actor, model_id, input).await
    }

    pub async fn get_version(&self, model_id: &str, version_id: &str) -> Result<ModelVersion> {
        self.repo.get_version(model_id, version_id).await
    }

    pub async fn list_versions(&self, model_id: &str) -> Result<Vec<ModelVersion>> {
        self.repo.list_versions(model_id).await
    }

    pub async fn update_version(
        &self,
        actor: &str,
        model_id: &str,
        version_id: &str,
        input: UpdateModelVersion,
    ) -> Result<ModelVersion> {
        if let Some(source) = &input.source {
            if let Some(value) = source.as_ref() {
                if value.trim().is_empty() {
                    return Err(RegistryError::Validation("source must not be empty".into()));
                }
            }
        }
        if let Some(prompts) = &input.activation_prompts {
            for prompt in prompts {
                if prompt.chars().count() > 8_192 {
                    return Err(RegistryError::Validation(
                        "activation prompt exceeds 8192 characters".into(),
                    ));
                }
            }
        }
        if let Some(metadata) = &input.metadata {
            validate_extensions(metadata)?;
        }
        self.repo
            .update_version(actor, model_id, version_id, input)
            .await
    }

    pub async fn list_files(&self, model_id: &str) -> Result<Vec<ModelFile>> {
        self.repo.list_files(model_id).await
    }

    pub async fn add_file(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelFile,
    ) -> Result<ModelFile> {
        if input.path.trim().is_empty() || input.filename.trim().is_empty() {
            return Err(RegistryError::Validation(
                "file path and filename are required".into(),
            ));
        }
        if input.size_bytes < 0 {
            return Err(RegistryError::Validation(
                "size_bytes must be non-negative".into(),
            ));
        }
        input.sha256 = input
            .sha256
            .map(|hash| hash.trim().to_ascii_lowercase())
            .filter(|hash| !hash.is_empty());
        validate_sha256(&input.sha256)?;
        self.repo.add_file(actor, model_id, input).await
    }

    pub async fn remove_file(&self, actor: &str, model_id: &str, file_id: &str) -> Result<()> {
        self.repo.remove_file(actor, model_id, file_id).await
    }

    pub async fn list_tags(&self) -> Result<Vec<Tag>> {
        self.repo.list_tags().await
    }

    pub async fn list_model_tags(&self, model_id: &str) -> Result<Vec<String>> {
        self.repo.list_model_tags(model_id).await
    }

    pub async fn add_tag(&self, actor: &str, model_id: &str, tag: &str) -> Result<Vec<String>> {
        let normalized = tag.trim().to_ascii_lowercase();
        if normalized.is_empty() || normalized.len() > 128 {
            return Err(RegistryError::Validation(
                "tag must be 1..128 characters".into(),
            ));
        }
        self.repo.add_tag(actor, model_id, &normalized).await
    }

    pub async fn remove_tag(&self, actor: &str, model_id: &str, tag: &str) -> Result<Vec<String>> {
        let normalized = tag.trim().to_ascii_lowercase();
        self.repo.remove_tag(actor, model_id, &normalized).await
    }

    pub async fn list_sources(&self, model_id: &str) -> Result<Vec<ModelSource>> {
        self.repo.list_sources(model_id).await
    }

    pub async fn add_source(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelSource,
    ) -> Result<ModelSource> {
        if input.provider.trim().is_empty() {
            return Err(RegistryError::Validation("provider is required".into()));
        }
        self.repo.add_source(actor, model_id, input).await
    }

    pub async fn list_assets(&self, model_id: &str) -> Result<Vec<ModelAsset>> {
        self.repo.list_assets(model_id).await
    }

    pub async fn add_asset(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelAsset,
    ) -> Result<ModelAsset> {
        if input.path.trim().is_empty() {
            return Err(RegistryError::Validation("asset path is required".into()));
        }
        validate_extensions(&input.metadata)?;
        self.repo.add_asset(actor, model_id, input).await
    }

    pub async fn list_relationships(&self, model_id: &str) -> Result<Vec<ModelRelationship>> {
        self.repo.list_relationships(model_id).await
    }

    pub async fn add_relationship(
        &self,
        actor: &str,
        model_id: &str,
        input: NewModelRelationship,
    ) -> Result<ModelRelationship> {
        if model_id == input.target_model_id {
            return Err(RegistryError::Validation(
                "a model cannot relate to itself".into(),
            ));
        }
        validate_extensions(&input.metadata)?;
        self.repo.add_relationship(actor, model_id, input).await
    }

    pub async fn delete_relationship(
        &self,
        actor: &str,
        model_id: &str,
        relationship_id: &str,
    ) -> Result<()> {
        self.repo
            .delete_relationship(actor, model_id, relationship_id)
            .await
    }

    pub async fn integrity_report(&self) -> Result<IntegrityReport> {
        self.repo.integrity_report().await
    }

    pub async fn list_events(&self, after_id: i64, limit: i64) -> Result<Vec<RegistryEvent>> {
        self.repo
            .list_events(after_id.max(0), limit.clamp(1, 500))
            .await
    }
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn default_limit() -> i64 {
    50
}

fn available_status() -> FileStatus {
    FileStatus::Available
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_type_aliases_are_stable() {
        assert_eq!(
            "checkpoint".parse::<ModelType>().unwrap(),
            ModelType::Checkpoint
        );
        assert_eq!("LoRA".parse::<ModelType>().unwrap(), ModelType::Lora);
        assert!("bogus".parse::<ModelType>().is_err());
    }

    #[test]
    fn normalize_search_caps_pagination() {
        let q = ModelSearch {
            limit: 9_999,
            offset: -4,
            ..Default::default()
        }
        .normalized();
        assert_eq!(q.limit, 200);
        assert_eq!(q.offset, 0);
    }

    #[test]
    fn sha256_validation_is_strict() {
        assert!(validate_sha256(&Some("a".repeat(64))).is_ok());
        assert!(validate_sha256(&Some("z".repeat(64))).is_err());
    }

    #[test]
    fn new_ids_are_prefixed() {
        assert!(new_id("model_").starts_with("model__"));
        assert!(new_id("version_").starts_with("version__"));
    }
}
