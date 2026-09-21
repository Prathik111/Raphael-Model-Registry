use registry_core::{
    Model, ModelSearch, NewModel, NewModelFile, NewModelRelationship, NewModelSource,
    NewModelVersion, RegistryEvent, SearchResult, UpdateModel, UpdateModelVersion,
};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    process::Command,
    time::{sleep, timeout},
};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("registry returned {status}: {message}")]
    Api { status: StatusCode, message: String },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("registry did not become available before timeout")]
    StartupTimeout,
}

#[derive(Clone)]
pub struct RegistryClient {
    base_url: String,
    token: String,
    client: Client,
}

impl RegistryClient {
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Result<Self, ClientError> {
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            client: Client::builder().timeout(Duration::from_secs(30)).build()?,
        })
    }

    pub fn from_token_file(
        base_url: impl Into<String>,
        token_path: impl AsRef<Path>,
    ) -> Result<Self, ClientError> {
        let token = fs::read_to_string(token_path)?;
        Self::new(base_url, token.trim())
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.base_url))
            .bearer_auth(&self.token)
            .header("x-raphael-actor", "registry-client")
    }

    async fn send_json<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, ClientError> {
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api {
                status,
                message: body,
            });
        }
        Ok(response.json::<T>().await?)
    }

    pub async fn health(&self) -> bool {
        self.client
            .get(format!("{}/health", self.base_url))
            .send()
            .await
            .map(|response| response.status().is_success())
            .unwrap_or(false)
    }

    pub async fn ensure_running(&self, executable: impl AsRef<Path>) -> Result<(), ClientError> {
        if self.health().await {
            return Ok(());
        }

        Command::new(executable.as_ref()).arg("server").spawn()?;

        match timeout(Duration::from_secs(10), async {
            loop {
                if self.health().await {
                    return Ok(());
                }
                sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(ClientError::StartupTimeout),
        }
    }

    pub async fn get(&self, id: &str) -> Result<Model, ClientError> {
        self.send_json(self.request(reqwest::Method::GET, &format!("/api/v1/models/{id}")))
            .await
    }

    pub async fn search(&self, mut query: ModelSearch) -> Result<SearchResult, ClientError> {
        query.limit = query.limit.clamp(1, 200);
        let mut params = Vec::new();
        if let Some(q) = query.q {
            params.push(("q", q));
        }
        if let Some(model_type) = query.model_type {
            params.push(("model_type", model_type.to_string()));
        }
        if let Some(tag) = query.tag {
            params.push(("tag", tag));
        }
        if let Some(creator) = query.creator {
            params.push(("creator", creator));
        }
        if let Some(base_model) = query.base_model {
            params.push(("base_model", base_model));
        }
        if let Some(source) = query.source {
            params.push(("source", source));
        }
        if let Some(hash) = query.hash {
            params.push(("hash", hash));
        }
        params.push(("limit", query.limit.to_string()));
        params.push(("offset", query.offset.max(0).to_string()));
        self.send_json(
            self.request(reqwest::Method::GET, "/api/v1/models")
                .query(&params),
        )
        .await
    }

    pub async fn create(&self, model: &NewModel) -> Result<Model, ClientError> {
        self.send_json(
            self.request(reqwest::Method::POST, "/api/v1/models")
                .json(model),
        )
        .await
    }

    pub async fn update(&self, id: &str, model: &UpdateModel) -> Result<Model, ClientError> {
        self.send_json(
            self.request(reqwest::Method::PATCH, &format!("/api/v1/models/{id}"))
                .json(model),
        )
        .await
    }

    pub async fn delete(&self, id: &str) -> Result<(), ClientError> {
        let response = self
            .request(reqwest::Method::DELETE, &format!("/api/v1/models/{id}"))
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            Err(ClientError::Api {
                status,
                message: response.text().await.unwrap_or_default(),
            })
        }
    }

    pub async fn versions(
        &self,
        id: &str,
    ) -> Result<Vec<registry_core::ModelVersion>, ClientError> {
        self.send_json(self.request(
            reqwest::Method::GET,
            &format!("/api/v1/models/{id}/versions"),
        ))
        .await
    }

    pub async fn create_version(
        &self,
        id: &str,
        input: &NewModelVersion,
    ) -> Result<registry_core::ModelVersion, ClientError> {
        self.send_json(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/models/{id}/versions"),
            )
            .json(input),
        )
        .await
    }

    pub async fn update_version(
        &self,
        model_id: &str,
        version_id: &str,
        input: &UpdateModelVersion,
    ) -> Result<registry_core::ModelVersion, ClientError> {
        self.send_json(
            self.request(
                reqwest::Method::PATCH,
                &format!("/api/v1/models/{model_id}/versions/{version_id}"),
            )
            .json(input),
        )
        .await
    }

    pub async fn files(&self, id: &str) -> Result<Vec<registry_core::ModelFile>, ClientError> {
        self.send_json(self.request(reqwest::Method::GET, &format!("/api/v1/models/{id}/files")))
            .await
    }

    pub async fn add_file(
        &self,
        id: &str,
        input: &NewModelFile,
    ) -> Result<registry_core::ModelFile, ClientError> {
        self.send_json(
            self.request(reqwest::Method::POST, &format!("/api/v1/models/{id}/files"))
                .json(input),
        )
        .await
    }

    pub async fn tags(&self, id: &str) -> Result<Vec<String>, ClientError> {
        self.send_json(self.request(reqwest::Method::GET, &format!("/api/v1/models/{id}/tags")))
            .await
    }

    pub async fn add_tag(&self, id: &str, tag: &str) -> Result<Vec<String>, ClientError> {
        self.send_json(
            self.request(reqwest::Method::POST, &format!("/api/v1/models/{id}/tags"))
                .json(&serde_json::json!({"tag":tag})),
        )
        .await
    }

    pub async fn sources(&self, id: &str) -> Result<Vec<registry_core::ModelSource>, ClientError> {
        self.send_json(self.request(
            reqwest::Method::GET,
            &format!("/api/v1/models/{id}/sources"),
        ))
        .await
    }

    pub async fn add_source(
        &self,
        id: &str,
        input: &NewModelSource,
    ) -> Result<registry_core::ModelSource, ClientError> {
        self.send_json(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/models/{id}/sources"),
            )
            .json(input),
        )
        .await
    }

    pub async fn add_relationship(
        &self,
        id: &str,
        input: &NewModelRelationship,
    ) -> Result<registry_core::ModelRelationship, ClientError> {
        self.send_json(
            self.request(
                reqwest::Method::POST,
                &format!("/api/v1/models/{id}/relationships"),
            )
            .json(input),
        )
        .await
    }

    pub async fn events(&self, after_id: i64) -> Result<Vec<RegistryEvent>, ClientError> {
        self.send_json(
            self.request(reqwest::Method::GET, "/api/v1/events/snapshot")
                .query(&[("after_id", after_id)]),
        )
        .await
    }

    pub fn sse_url(&self, after_id: i64) -> String {
        format!("{}/api/v1/events?after_id={after_id}", self.base_url)
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

pub fn ensure_core_result<T>(result: Result<T, ClientError>) -> registry_core::Result<T> {
    result.map_err(|e| registry_core::RegistryError::Storage(e.to_string()))
}

#[derive(Debug, Clone)]
pub struct RegistryDiscovery {
    pub base_url: String,
    pub token_file: PathBuf,
}

impl RegistryDiscovery {
    pub fn from_data_dir(data_dir: impl AsRef<Path>, port: u16) -> Self {
        let data_dir = data_dir.as_ref().to_path_buf();
        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            token_file: data_dir.join("registry.token"),
        }
    }

    pub fn client(&self) -> Result<RegistryClient, ClientError> {
        RegistryClient::from_token_file(&self.base_url, &self.token_file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_base_url() {
        let client = RegistryClient::new("http://127.0.0.1:43217/", "token").unwrap();
        assert_eq!(client.base_url(), "http://127.0.0.1:43217");
    }

    #[test]
    fn discovery_points_at_shared_registry_data() {
        let discovery = RegistryDiscovery::from_data_dir("data", 43217);
        assert_eq!(discovery.base_url, "http://127.0.0.1:43217");
        assert!(discovery.token_file.ends_with("registry.token"));
    }
}
