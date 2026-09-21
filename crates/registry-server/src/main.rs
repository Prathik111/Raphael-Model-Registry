use clap::{Args, Parser, Subcommand};
use registry_core::{AssetKind, ModelSearch, ModelType, RegistryService};
use registry_server::{RegistryConfig, SqliteStore, load_or_create_token, run_server};
use serde_json::json;
use sqlx::{
    Row,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::{fs, path::PathBuf};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(name = "raphael-registry", version, about = "Raphael Model Registry")]
struct Cli {
    #[arg(long, env = "RAPHAEL_REGISTRY_DATA_DIR", global = true)]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Server(ServerArgs),
    Health,
    Status,
    IntegrityCheck,
    DbMigrate,
    Backup {
        path: PathBuf,
    },
    VerifyBackup {
        path: PathBuf,
    },
    Restore {
        path: PathBuf,
        #[arg(long)]
        force: bool,
    },
    #[command(subcommand)]
    Models(ModelsCommand),
    ImportModelManager {
        database: PathBuf,
    },
}

#[derive(Debug, Args)]
struct ServerArgs {
    #[arg(long)]
    bind: Option<String>,
    #[arg(long)]
    port: Option<u16>,
    #[arg(long)]
    auth_token: Option<String>,
    #[arg(long)]
    cors_origin: Option<String>,
    /// Explicitly allow plaintext HTTP when binding beyond localhost.
    #[arg(long, env = "RAPHAEL_REGISTRY_ALLOW_INSECURE_LAN")]
    allow_insecure_lan: bool,
}

#[derive(Debug, Subcommand)]
enum ModelsCommand {
    List(ModelsListArgs),
    Get { id: String },
    Search { query: String },
    Export { path: PathBuf },
}

#[derive(Debug, Args)]
struct ModelsListArgs {
    #[arg(long)]
    model_type: Option<String>,
    #[arg(long, default_value_t = 50)]
    limit: i64,
}

fn config(cli: &Cli) -> RegistryConfig {
    let mut config = RegistryConfig::from_env(cli.data_dir.clone());
    if let Command::Server(args) = &cli.command {
        if let Some(bind) = &args.bind {
            config.bind = bind.clone();
        }
        if let Some(port) = args.port {
            config.port = port;
        }
        if let Some(token) = &args.auth_token {
            config.auth_token = Some(token.clone());
        }
        if let Some(origin) = &args.cors_origin {
            config.cors_origin = Some(origin.clone());
        }
        if args.allow_insecure_lan {
            config.allow_insecure_lan = true;
        }
    }
    config
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let cfg = config(&cli);
    match cli.command {
        Command::Server(_) => run_server(cfg).await?,
        Command::Health => {
            let response = reqwest::get(format!("http://{}/health", cfg.address())).await?;
            println!("{}", response.text().await?);
        }
        Command::Status => {
            let token = load_or_create_token(&cfg)?;
            let response = reqwest::Client::new()
                .get(format!("http://{}/api/v1/status", cfg.address()))
                .bearer_auth(token)
                .send()
                .await?;
            println!("{}", response.text().await?);
        }
        Command::IntegrityCheck => {
            let store = SqliteStore::connect(&cfg.database_path).await?;
            let service = RegistryService::new(std::sync::Arc::new(store));
            let report = service.integrity_report().await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.ok {
                std::process::exit(2);
            }
        }
        Command::DbMigrate => {
            let _ = SqliteStore::connect(&cfg.database_path).await?;
            println!(
                "database migrated to latest schema: {}",
                cfg.database_path.display()
            );
        }
        Command::Backup { path } => {
            let path = path.canonicalize().unwrap_or(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let store = SqliteStore::connect(&cfg.database_path).await?;
            sqlx::query("VACUUM INTO ?")
                .bind(path.to_string_lossy().to_string())
                .execute(store.pool())
                .await?;
            store.pool().close().await;
            verify_sqlite_backup(&path).await?;
            println!("backup verified: {}", path.display());
        }
        Command::VerifyBackup { path } => {
            verify_sqlite_backup(&path).await?;
            println!("backup integrity OK: {}", path.display());
        }
        Command::Restore { path, force } => {
            if !force {
                return Err(
                    "restore requires --force because it replaces the Registry database".into(),
                );
            }
            if !path.is_file() {
                return Err("backup file does not exist".into());
            }
            let _instance_lock = registry_server::InstanceLock::acquire(&cfg).await?;
            let store = SqliteStore::connect(&cfg.database_path).await?;
            store.pool().close().await;
            let wal_path = PathBuf::from(format!("{}-wal", cfg.database_path.display()));
            let shm_path = PathBuf::from(format!("{}-shm", cfg.database_path.display()));
            let _ = fs::remove_file(&wal_path);
            let _ = fs::remove_file(&shm_path);
            fs::copy(&path, &cfg.database_path)?;
            verify_sqlite_backup(&cfg.database_path).await?;
            println!("restored and verified: {}", cfg.database_path.display());
        }
        Command::Models(command) => models_command(cfg, command).await?,
        Command::ImportModelManager { database } => {
            let store = std::sync::Arc::new(SqliteStore::connect(&cfg.database_path).await?);
            println!(
                "{}",
                serde_json::to_string_pretty(&import_model_manager(&store, &database).await?)?
            );
        }
    }
    Ok(())
}

async fn verify_sqlite_backup(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&pool)
        .await?;
    pool.close().await;
    if integrity != "ok" {
        return Err(format!("sqlite integrity check failed: {integrity}").into());
    }
    Ok(())
}

async fn models_command(
    cfg: RegistryConfig,
    command: ModelsCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = std::sync::Arc::new(SqliteStore::connect(&cfg.database_path).await?);
    let service = RegistryService::new(store);
    match command {
        ModelsCommand::List(args) => {
            let model_type = args
                .model_type
                .as_deref()
                .map(str::parse::<ModelType>)
                .transpose()?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &service
                        .search_models(ModelSearch {
                            model_type,
                            limit: args.limit,
                            ..Default::default()
                        })
                        .await?
                )?
            );
        }
        ModelsCommand::Get { id } => println!(
            "{}",
            serde_json::to_string_pretty(&service.get_model(&id).await?)?
        ),
        ModelsCommand::Search { query } => println!(
            "{}",
            serde_json::to_string_pretty(
                &service
                    .search_models(ModelSearch {
                        q: Some(query),
                        limit: 200,
                        ..Default::default()
                    })
                    .await?
            )?
        ),
        ModelsCommand::Export { path } => {
            let (models, total) = export_all_models(&service).await?;
            fs::write(
                &path,
                serde_json::to_vec_pretty(
                    &json!({
                        "api_version":"v1",
                        "exported_at":registry_core::now_unix(),
                        "models":models,
                        "total":total
                    }),
                )?,
            )?;
            println!("exported {} models to {}", total, path.display());
        }
    }
    Ok(())
}

async fn export_all_models(
    service: &RegistryService,
) -> Result<(Vec<registry_core::Model>, u64), Box<dyn std::error::Error>> {
    const PAGE_SIZE: i64 = 200;
    let mut offset = 0_i64;
    let mut models = Vec::new();
    let mut total = 0_u64;

    loop {
        let result = service
            .search_models(ModelSearch {
                limit: PAGE_SIZE,
                offset,
                ..Default::default()
            })
            .await?;
        total = result.total;
        let count = result.items.len() as i64;
        models.extend(result.items);
        if count == 0 || offset + count >= result.total as i64 {
            break;
        }
        offset += count;
    }

    if models.len() as u64 != total {
        return Err(format!(
            "export consistency check failed: collected {} models but search reported {}",
            models.len(),
            total
        )
        .into());
    }

    Ok((models, total))
}

async fn import_model_manager(
    store: &std::sync::Arc<SqliteStore>,
    path: &PathBuf,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    if !path.is_file() {
        return Err("legacy database file does not exist".into());
    }

    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let tables = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(&pool)
    .await?;
    let table_names: Vec<String> = tables.iter().map(|r| r.get("name")).collect();

    if !table_names.iter().any(|t| t == "models") {
        return Err("legacy database has no models table".into());
    }

    let columns = sqlx::query("PRAGMA table_info(models)")
        .fetch_all(&pool)
        .await?;
    let names: Vec<String> = columns.iter().map(|r| r.get("name")).collect();
    let known = [
        "id", "path", "relative_path", "filename", "model_type", "size_bytes",
        "modified_at", "civitai_model_id", "civitai_version_id", "civitai_url",
        "civitai_name", "version_name", "base_model", "creator", "description",
        "tags_json", "activation_json", "source_hash", "updated_at",
    ];
    let unmapped: Vec<String> = names
        .iter()
        .filter(|name| !known.contains(&name.as_str()))
        .cloned()
        .collect();

    let rows = sqlx::query(
        "SELECT id,path,relative_path,filename,model_type,size_bytes,modified_at,civitai_model_id,civitai_version_id,civitai_url,civitai_name,version_name,base_model,creator,description,tags_json,activation_json,source_hash,updated_at FROM models",
    )
    .fetch_all(&pool)
    .await?;
    let models_discovered = rows.len() as u64;
    let service = RegistryService::new(store.clone());
    let mut imported = 0_u64;
    let mut failed = Vec::new();

    for row in rows {
        let legacy_id: i64 = row.get("id");
        let model_id = format!("legacy_model_{legacy_id}");

        let model_type = match row.get::<String, _>("model_type").parse::<ModelType>() {
            Ok(value) => value,
            Err(error) => {
                failed.push(json!({
                    "id": legacy_id,
                    "stage": "model",
                    "error": format!("invalid legacy model_type: {error}")
                }));
                continue;
            }
        };

        let tags: Vec<String> = match optional_legacy_string(&row, "tags_json") {
            Some(value) => match serde_json::from_str(&value) {
                Ok(value) => value,
                Err(error) => {
                    failed.push(json!({
                        "id": legacy_id,
                        "stage": "tags",
                        "error": format!("invalid tags_json: {error}")
                    }));
                    continue;
                }
            },
            None => Vec::new(),
        };

        let activation_prompts: Vec<String> = match optional_legacy_string(&row, "activation_json") {
                Some(value) => match serde_json::from_str(&value) {
                    Ok(value) => value,
                    Err(error) => {
                        failed.push(json!({
                            "id": legacy_id,
                            "stage": "version",
                            "error": format!("invalid activation_json: {error}")
                        }));
                        continue;
                    }
                },
                None => Vec::new(),
            };

        let model = match service
            .create_model(
                "model-manager-import",
                registry_core::NewModel {
                    id: Some(model_id.clone()),
                    name: optional_legacy_name(&row),
                    model_type,
                    creator: optional_legacy_string(&row, "creator"),
                    description: optional_legacy_string(&row, "description"),
                    base_model: optional_legacy_string(&row, "base_model"),
                    extensions: json!({
                        "legacy_model_manager_id": legacy_id,
                        "legacy_path": row.get::<String, _>("path")
                    }),
                },
            )
            .await
        {
            Ok(model) => (model, true),
            Err(registry_core::RegistryError::Conflict(_)) => match service
                .get_model(&model_id)
                .await
            {
                Ok(model) => (model, false),
                Err(error) => {
                    failed.push(json!({"id":legacy_id,"stage":"model","error":error.to_string()}));
                    continue;
                }
            },
            Err(error) => {
                failed.push(json!({"id":legacy_id,"stage":"model","error":error.to_string()}));
                continue;
            }
        };

        let (model, created_new) = model;
        let mut item_error: Option<(String, String)> = None;

        for tag in tags {
            if let Err(error) = service
                .add_tag("model-manager-import", &model.id, &tag)
                .await
            {
                item_error = Some(("tag".into(), error.to_string()));
                break;
            }
        }

        let civitai_model_id: Option<i64> = row.try_get("civitai_model_id").ok();
        let civitai_version_id: Option<i64> = row.try_get("civitai_version_id").ok();

        if item_error.is_none()
            && (civitai_model_id.is_some()
                || civitai_version_id.is_some()
                || optional_legacy_string(&row, "civitai_url").is_some())
        {
            if let Err(error) = service
                .add_source(
                    "model-manager-import",
                    &model.id,
                    registry_core::NewModelSource {
                        provider: "civitai".into(),
                        external_model_id: civitai_model_id.map(|v| v.to_string()),
                        external_version_id: civitai_version_id.map(|v| v.to_string()),
                        url: optional_legacy_string(&row, "civitai_url"),
                        metadata: json!({"legacy_model_manager_id":legacy_id}),
                    },
                )
                .await
            {
                item_error = Some(("source".into(), error.to_string()));
            }
        }

        if item_error.is_none() {
            match service
                .create_version(
                    "model-manager-import",
                    &model.id,
                    registry_core::NewModelVersion {
                        id: Some(format!("legacy_version_{legacy_id}")),
                        version_name: optional_legacy_string(&row, "version_name"),
                        base_model: optional_legacy_string(&row, "base_model"),
                        source: civitai_version_id.is_some().then_some("civitai".into()),
                        source_model_id: civitai_model_id.map(|v| v.to_string()),
                        source_version_id: civitai_version_id.map(|v| v.to_string()),
                        source_url: optional_legacy_string(&row, "civitai_url"),
                        activation_prompts,
                        metadata: json!({"legacy_model_manager_id":legacy_id}),
                    },
                )
                .await
            {
                Ok(version) => {
                    if let Err(error) = service
                        .add_file(
                            "model-manager-import",
                            &model.id,
                            registry_core::NewModelFile {
                                id: Some(format!("legacy_file_{legacy_id}")),
                                version_id: Some(version.id),
                                path: row.get("path"),
                                relative_path: optional_legacy_string(&row, "relative_path"),
                                filename: row.get("filename"),
                                size_bytes: row.get("size_bytes"),
                                modified_at: row.get("modified_at"),
                                sha256: optional_legacy_string(&row, "source_hash"),
                                status: registry_core::FileStatus::Available,
                            },
                        )
                        .await
                    {
                        item_error = Some(("file".into(), error.to_string()));
                    }
                }
                Err(error) => {
                    item_error = Some(("version".into(), error.to_string()));
                }
            }
        }

        if let Some((stage, error)) = item_error {
            if created_new {
                if let Err(rollback_error) =
                    service.delete_model("model-manager-import", &model.id).await
                {
                    failed.push(json!({
                        "id": legacy_id,
                        "stage": "rollback",
                        "error": rollback_error.to_string()
                    }));
                }
            }
            failed.push(json!({"id":legacy_id,"stage":stage,"error":error}));
            continue;
        }

        imported += 1;
    }

    let image_import = if table_names.iter().any(|t| t == "images") {
        let image_rows = sqlx::query(
            "SELECT model_id,civitai_image_id,local_path,thumbnail_path,width,height,prompt,negative_prompt,steps,cfg,sampler,seed,meta_json FROM images",
        )
        .fetch_all(&pool)
        .await?;
        let mut count = 0_u64;

        for row in image_rows {
            let legacy_id: i64 = row.get("model_id");
            let model_id = format!("legacy_model_{legacy_id}");
            if service.get_model(&model_id).await.is_ok() {
                for column in ["local_path", "thumbnail_path"] {
                    if let Some(path) = optional_legacy_string(&row, column) {
                        match service
                            .add_asset(
                                "model-manager-import",
                                &model_id,
                                registry_core::NewModelAsset {
                                    id: None,
                                    kind: AssetKind::Gallery,
                                    path,
                                    source: Some("civitai".into()),
                                    metadata: json!({
                                        "legacy_civitai_image_id": row.get::<i64,_>("civitai_image_id"),
                                        "width": row.try_get::<Option<i64>,_>("width").ok().flatten(),
                                        "height": row.try_get::<Option<i64>,_>("height").ok().flatten(),
                                        "prompt": row.try_get::<Option<String>,_>("prompt").ok().flatten(),
                                        "negative_prompt": row.try_get::<Option<String>,_>("negative_prompt").ok().flatten(),
                                        "steps": row.try_get::<Option<i64>,_>("steps").ok().flatten(),
                                        "cfg": row.try_get::<Option<f64>,_>("cfg").ok().flatten(),
                                        "sampler": row.try_get::<Option<String>,_>("sampler").ok().flatten(),
                                        "seed": row.try_get::<Option<i64>,_>("seed").ok().flatten(),
                                        "meta_json": row.try_get::<Option<String>,_>("meta_json").ok().flatten()
                                    }),
                                },
                            )
                            .await
                        {
                            Ok(_) => count += 1,
                            Err(error) => failed.push(json!({
                                "id": legacy_id,
                                "stage": "image",
                                "error": error.to_string()
                            })),
                        }
                    }
                }
            } else {
                failed.push(json!({"id":legacy_id,"stage":"image","error":"model not found"}));
            }
        }
        count
    } else {
        0
    };

    pool.close().await;
    Ok(json!({
        "models_discovered": models_discovered,
        "models_imported": imported,
        "models_failed": failed.iter().filter(|failure| failure.get("stage").and_then(|v| v.as_str()) != Some("image")).count(),
        "gallery_assets_imported": image_import,
        "legacy_tables": table_names,
        "unmapped_model_columns": unmapped,
        "failures": failed
    }))
}

fn optional_legacy_string(row: &sqlx::sqlite::SqliteRow, name: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(name).ok().flatten()
}

fn optional_legacy_name(row: &sqlx::sqlite::SqliteRow) -> String {
    optional_legacy_string(row, "civitai_name")
        .or_else(|| optional_legacy_string(row, "filename"))
        .unwrap_or_else(|| "Imported model".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use registry_core::{ModelType, NewModel, RegistryService};

    #[tokio::test]
    async fn export_collects_more_than_one_page() {
        let store = std::sync::Arc::new(SqliteStore::in_memory().await.unwrap());
        let service = RegistryService::new(store);

        for index in 0..205 {
            service
                .create_model(
                    "test",
                    NewModel {
                        id: Some(format!("export_model_{index}")),
                        name: format!("Export model {index}"),
                        model_type: ModelType::Checkpoint,
                        creator: None,
                        description: None,
                        base_model: None,
                        extensions: serde_json::json!({}),
                    },
                )
                .await
                .unwrap();
        }

        let (models, total) = export_all_models(&service).await.unwrap();
        assert_eq!(total, 205);
        assert_eq!(models.len(), 205);
    }
}
