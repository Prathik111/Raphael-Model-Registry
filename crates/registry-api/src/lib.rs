use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::delete,
};
use futures_util::Stream;
use registry_core::{
    CompatibilityResult, ModelSearch, ModelType, NewModel, NewModelAsset, NewModelFile,
    NewModelRelationship, NewModelSource, NewModelVersion, RegistryError, RegistryEvent,
    RegistryService, Result as CoreResult, UpdateModel, UpdateModelVersion,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::time::sleep;

#[derive(Clone)]
pub struct AppState {
    pub service: RegistryService,
    pub token: Arc<str>,
    pub started_at: i64,
}

impl AppState {
    pub fn new(service: RegistryService, token: impl Into<Arc<str>>, started_at: i64) -> Self {
        Self {
            service,
            token: token.into(),
            started_at,
        }
    }
}

#[derive(Debug, Serialize)]
struct ApiErrorBody {
    error: &'static str,
    message: String,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: "valid bearer authentication is required".into(),
        }
    }

    fn from_core(error: RegistryError) -> Self {
        let status = match &error {
            RegistryError::NotFound(_) => StatusCode::NOT_FOUND,
            RegistryError::Validation(_) | RegistryError::BadRequest(_) => StatusCode::BAD_REQUEST,
            RegistryError::RevisionConflict { .. } | RegistryError::Conflict(_) => {
                StatusCode::CONFLICT
            }
            RegistryError::Unauthorized => StatusCode::UNAUTHORIZED,
            RegistryError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = match &error {
            RegistryError::Storage(_) => "internal storage error".to_string(),
            _ => error.to_string(),
        };
        Self { status, message }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiErrorBody {
                error: if self.status == StatusCode::UNAUTHORIZED {
                    "unauthorized"
                } else {
                    "request_failed"
                },
                message: self.message,
            }),
        )
            .into_response()
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Debug, Deserialize)]
pub struct TagRequest {
    pub tag: String,
}

#[derive(Debug, Deserialize)]
pub struct CompatibilityQuery {
    #[serde(rename = "type")]
    pub model_type: Option<ModelType>,
}

#[derive(Debug, Deserialize)]
pub struct DirectCompatibilityQuery {
    pub checkpoint: String,
    pub lora: String,
}

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    pub after_id: i64,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    api_version: &'static str,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    status: &'static str,
    api_version: &'static str,
    started_at: i64,
    checked_models: i64,
    checked_versions: i64,
}

#[derive(Debug, Serialize)]
struct DirectCompatibilityResponse {
    checkpoint: String,
    lora: String,
    compatible: bool,
    reason: &'static str,
}

fn actor(headers: &HeaderMap) -> String {
    headers
        .get("x-raphael-actor")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("api-client")
        .chars()
        .take(128)
        .collect()
}

fn require_auth(headers: &HeaderMap, token: &str) -> ApiResult<()> {
    let expected = format!("Bearer {token}");
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if supplied != expected {
        Err(ApiError::unauthorized())
    } else {
        Ok(())
    }
}

fn core<T>(result: CoreResult<T>) -> ApiResult<T> {
    result.map_err(ApiError::from_core)
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", axum::routing::get(health))
        .route("/api/v1/status", axum::routing::get(status))
        .route("/api/v1/model-types", axum::routing::get(model_types))
        .route(
            "/api/v1/models",
            axum::routing::get(list_models).post(create_model),
        )
        .route("/api/v1/models/search", axum::routing::get(search_models))
        .route(
            "/api/v1/models/{id}",
            axum::routing::get(get_model)
                .patch(update_model)
                .delete(delete_model),
        )
        .route(
            "/api/v1/models/{id}/versions",
            axum::routing::get(list_versions).post(create_version),
        )
        .route(
            "/api/v1/models/{id}/versions/{version_id}",
            axum::routing::get(get_version).patch(update_version),
        )
        .route(
            "/api/v1/models/{id}/files",
            axum::routing::get(list_files).post(add_file),
        )
        .route("/api/v1/models/{id}/files/{file_id}", delete(remove_file))
        .route(
            "/api/v1/models/{id}/tags",
            axum::routing::get(list_model_tags).post(add_tag),
        )
        .route("/api/v1/models/{id}/tags/{tag}", delete(remove_tag))
        .route("/api/v1/tags", axum::routing::get(list_tags))
        .route(
            "/api/v1/models/{id}/sources",
            axum::routing::get(list_sources).post(add_source),
        )
        .route(
            "/api/v1/models/{id}/assets",
            axum::routing::get(list_assets).post(add_asset),
        )
        .route(
            "/api/v1/models/{id}/relationships",
            axum::routing::get(list_relationships).post(add_relationship),
        )
        .route(
            "/api/v1/models/{id}/relationships/{relationship_id}",
            delete(delete_relationship),
        )
        .route(
            "/api/v1/models/{id}/compatibility",
            axum::routing::get(model_compatibility),
        )
        .route(
            "/api/v1/compatibility",
            axum::routing::get(direct_compatibility),
        )
        .route("/api/v1/checkpoints", axum::routing::get(list_checkpoints))
        .route("/api/v1/loras", axum::routing::get(list_loras))
        .route("/api/v1/events", axum::routing::get(events))
        .route(
            "/api/v1/events/snapshot",
            axum::routing::get(events_snapshot),
        )
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "raphael-model-registry",
        api_version: "v1",
    })
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<StatusResponse>> {
    require_auth(&headers, &state.token)?;
    let report = core(state.service.integrity_report().await)?;
    Ok(Json(StatusResponse {
        status: if report.ok { "ok" } else { "degraded" },
        api_version: "v1",
        started_at: state.started_at,
        checked_models: report.checked_models,
        checked_versions: report.checked_versions,
    }))
}

async fn model_types(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<String>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(
        ModelType::ALL.iter().map(ToString::to_string).collect(),
    ))
}

async fn list_models(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ModelSearch>,
) -> ApiResult<Json<registry_core::SearchResult>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(state.service.search_models(query).await)?))
}

async fn search_models(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ModelSearch>,
) -> ApiResult<Json<registry_core::SearchResult>> {
    list_models(State(state), headers, Query(query)).await
}

async fn create_model(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<NewModel>,
) -> ApiResult<(StatusCode, Json<registry_core::Model>)> {
    require_auth(&headers, &state.token)?;
    Ok((
        StatusCode::CREATED,
        Json(core(
            state.service.create_model(&actor(&headers), input).await,
        )?),
    ))
}

async fn get_model(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<registry_core::Model>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(state.service.get_model(&id).await)?))
}

async fn update_model(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<UpdateModel>,
) -> ApiResult<Json<registry_core::Model>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(
        state
            .service
            .update_model(&actor(&headers), &id, input)
            .await,
    )?))
}

async fn delete_model(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    require_auth(&headers, &state.token)?;
    core(state.service.delete_model(&actor(&headers), &id).await)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<NewModelVersion>,
) -> ApiResult<(StatusCode, Json<registry_core::ModelVersion>)> {
    require_auth(&headers, &state.token)?;
    Ok((
        StatusCode::CREATED,
        Json(core(
            state
                .service
                .create_version(&actor(&headers), &id, input)
                .await,
        )?),
    ))
}

async fn list_versions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<registry_core::ModelVersion>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(state.service.list_versions(&id).await)?))
}

async fn get_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, version_id)): Path<(String, String)>,
) -> ApiResult<Json<registry_core::ModelVersion>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(
        state.service.get_version(&id, &version_id).await,
    )?))
}

async fn update_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, version_id)): Path<(String, String)>,
    Json(input): Json<UpdateModelVersion>,
) -> ApiResult<Json<registry_core::ModelVersion>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(
        state
            .service
            .update_version(&actor(&headers), &id, &version_id, input)
            .await,
    )?))
}

async fn list_files(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<registry_core::ModelFile>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(state.service.list_files(&id).await)?))
}

async fn add_file(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<NewModelFile>,
) -> ApiResult<(StatusCode, Json<registry_core::ModelFile>)> {
    require_auth(&headers, &state.token)?;
    Ok((
        StatusCode::CREATED,
        Json(core(
            state.service.add_file(&actor(&headers), &id, input).await,
        )?),
    ))
}

async fn remove_file(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, file_id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    require_auth(&headers, &state.token)?;
    core(
        state
            .service
            .remove_file(&actor(&headers), &id, &file_id)
            .await,
    )?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_tags(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<registry_core::Tag>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(state.service.list_tags().await)?))
}

async fn list_model_tags(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<String>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(state.service.list_model_tags(&id).await)?))
}

async fn add_tag(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<TagRequest>,
) -> ApiResult<Json<Vec<String>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(
        state
            .service
            .add_tag(&actor(&headers), &id, &input.tag)
            .await,
    )?))
}

async fn remove_tag(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, tag)): Path<(String, String)>,
) -> ApiResult<Json<Vec<String>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(
        state.service.remove_tag(&actor(&headers), &id, &tag).await,
    )?))
}

async fn list_sources(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<registry_core::ModelSource>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(state.service.list_sources(&id).await)?))
}

async fn add_source(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<NewModelSource>,
) -> ApiResult<(StatusCode, Json<registry_core::ModelSource>)> {
    require_auth(&headers, &state.token)?;
    Ok((
        StatusCode::CREATED,
        Json(core(
            state.service.add_source(&actor(&headers), &id, input).await,
        )?),
    ))
}

async fn list_assets(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<registry_core::ModelAsset>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(state.service.list_assets(&id).await)?))
}

async fn add_asset(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<NewModelAsset>,
) -> ApiResult<(StatusCode, Json<registry_core::ModelAsset>)> {
    require_auth(&headers, &state.token)?;
    Ok((
        StatusCode::CREATED,
        Json(core(
            state.service.add_asset(&actor(&headers), &id, input).await,
        )?),
    ))
}

async fn list_relationships(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<registry_core::ModelRelationship>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(state.service.list_relationships(&id).await)?))
}

async fn add_relationship(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<NewModelRelationship>,
) -> ApiResult<(StatusCode, Json<registry_core::ModelRelationship>)> {
    require_auth(&headers, &state.token)?;
    Ok((
        StatusCode::CREATED,
        Json(core(
            state
                .service
                .add_relationship(&actor(&headers), &id, input)
                .await,
        )?),
    ))
}

async fn delete_relationship(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, relationship_id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    require_auth(&headers, &state.token)?;
    core(
        state
            .service
            .delete_relationship(&actor(&headers), &id, &relationship_id)
            .await,
    )?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_checkpoints(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(mut query): Query<ModelSearch>,
) -> ApiResult<Json<registry_core::SearchResult>> {
    query.model_type = Some(ModelType::Checkpoint);
    list_models(State(state), headers, Query(query)).await
}

async fn list_loras(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(mut query): Query<ModelSearch>,
) -> ApiResult<Json<registry_core::SearchResult>> {
    query.model_type = Some(ModelType::Lora);
    list_models(State(state), headers, Query(query)).await
}

async fn model_compatibility(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<CompatibilityQuery>,
) -> ApiResult<Json<CompatibilityResult>> {
    require_auth(&headers, &state.token)?;
    let source = core(state.service.get_model(&id).await)?;
    let requested_type = query.model_type.unwrap_or(ModelType::Lora);
    let candidates = core(
        state
            .service
            .search_models(ModelSearch {
                model_type: Some(requested_type.clone()),
                limit: 200,
                ..Default::default()
            })
            .await,
    )?;
    let relationships = core(state.service.list_relationships(&id).await)?;
    let explicit: Vec<String> = relationships
        .iter()
        .filter(|r| r.relationship_type == registry_core::RelationshipType::CompatibleWith)
        .map(|r| {
            if r.source_model_id == id {
                r.target_model_id.clone()
            } else {
                r.source_model_id.clone()
            }
        })
        .collect();
    let candidates = candidates
        .items
        .into_iter()
        .filter(|candidate| {
            explicit.iter().any(|target| target == &candidate.id)
                || (source.base_model.is_some() && source.base_model == candidate.base_model)
        })
        .collect();
    Ok(Json(CompatibilityResult {
        model_id: id,
        requested_type,
        candidates,
    }))
}

async fn direct_compatibility(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DirectCompatibilityQuery>,
) -> ApiResult<Json<DirectCompatibilityResponse>> {
    require_auth(&headers, &state.token)?;
    let checkpoint = core(state.service.get_model(&query.checkpoint).await)?;
    let lora = core(state.service.get_model(&query.lora).await)?;
    if checkpoint.model_type != ModelType::Checkpoint || lora.model_type != ModelType::Lora {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: "checkpoint must be checkpoint and lora must be lora".into(),
        });
    }
    let same_base = checkpoint.base_model.is_some() && checkpoint.base_model == lora.base_model;
    let relationships = core(state.service.list_relationships(&checkpoint.id).await)?;
    let explicit = relationships.iter().any(|r| {
        ((r.source_model_id == checkpoint.id && r.target_model_id == lora.id)
            || (r.source_model_id == lora.id && r.target_model_id == checkpoint.id))
            && r.relationship_type == registry_core::RelationshipType::CompatibleWith
    });
    Ok(Json(DirectCompatibilityResponse {
        checkpoint: query.checkpoint,
        lora: query.lora,
        compatible: same_base || explicit,
        reason: if explicit {
            "explicit_relationship"
        } else if same_base {
            "matching_base_model"
        } else {
            "no_deterministic_match"
        },
    }))
}

async fn events_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> ApiResult<Json<Vec<RegistryEvent>>> {
    require_auth(&headers, &state.token)?;
    Ok(Json(core(
        state.service.list_events(query.after_id.max(0), 500).await,
    )?))
}

async fn events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> ApiResult<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    require_auth(&headers, &state.token)?;
    let service = state.service.clone();
    let mut cursor = query.after_id.max(0);
    let stream = async_stream::stream! {
        loop {
            match service.list_events(cursor, 200).await {
                Ok(items) if !items.is_empty() => {
                    for event in items {
                        cursor = event.id;
                        let data = serde_json::to_string(&event).unwrap_or_else(|_| json!({"error":"event serialization failed"}).to_string());
                        yield Ok(Event::default().id(event.id.to_string()).event(event.event_type.clone()).data(data));
                    }
                }
                Ok(_) => sleep(Duration::from_secs(1)).await,
                Err(error) => {
                    yield Ok(Event::default().event("registry.error").data(json!({"message": error.to_string()}).to_string()));
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
