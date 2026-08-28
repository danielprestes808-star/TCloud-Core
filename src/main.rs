use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::Response,
    routing::{get, post},
};
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit},
};
use chrono::{DateTime, Utc};
use grammers_client::grammers_tl_types as tl;
use grammers_client::{
    Client, InputMessage, SignInError,
    types::{LoginToken, Media, PasswordToken},
};
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use serde::{Deserialize, Serialize};
use sqlx_core::row::Row;
use sqlx_postgres::{PgPool, PgPoolOptions, Postgres};
use std::{
    collections::{HashMap, HashSet},
    env,
    io::Cursor,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use uuid::Uuid;

const LOCAL_USER_ID: &str = "00000000-0000-0000-0000-000000000001";

#[derive(Clone)]
struct AppState {
    db: Option<PgPool>,
    fallback_files: Arc<Vec<TCloudItem>>,
    telegram: Option<Arc<TelegramRuntime>>,
}

struct TelegramRuntime {
    client: Client,
    api_hash: String,
    login_token: Mutex<Option<LoginToken>>,
    password_token: Mutex<Option<PasswordToken>>,
    password_hint: Mutex<Option<String>>,
    session_path: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct TCloudItem {
    id: String,
    parent_id: Option<String>,
    name: String,
    kind: String,
    size: u64,
    mime: String,
    sync_state: String,
    modified_at: String,
    source: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CoreStatus {
    name: &'static str,
    version: &'static str,
    connected: bool,
    telegram_connected: bool,
    telegram_credentials_ready: bool,
    database_connected: bool,
    database_mode: &'static str,
    storage_backend: &'static str,
    files_count: i64,
    devices_count: i64,
    sessions_count: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LiveRevision {
    revision: String,
    changed_at_ms: i64,
    files_count: i64,
    folders_count: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TelegramAuthStatus {
    credentials_configured: bool,
    authorized: bool,
    stage: String,
    session_owner: &'static str,
    can_request_code: bool,
    password_required: bool,
    password_hint: Option<String>,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PhoneRequest {
    phone: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodeRequest {
    code: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PasswordRequest {
    password: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthActionResponse {
    ok: bool,
    authorized: bool,
    stage: String,
    password_required: bool,
    password_hint: Option<String>,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexRequest {
    mode: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexRunResponse {
    accepted: bool,
    id: Option<String>,
    status: String,
    message: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexStatus {
    configured: bool,
    authorized: bool,
    database_required: bool,
    database_connected: bool,
    queue_ready: bool,
    last_run_id: Option<String>,
    last_status: Option<String>,
    dialogs_seen: i32,
    topics_seen: i32,
    messages_seen: i64,
    files_upserted: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TelegramDialogItem {
    id: String,
    telegram_peer_id: i64,
    name: String,
    kind: String,
    last_indexed_at: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DeviceSummary {
    id: String,
    name: String,
    platform: String,
    app_version: Option<String>,
    last_seen_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct Health {
    status: &'static str,
    database: &'static str,
    telegram: &'static str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NameMutationRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateFolderRequest {
    parent_id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileNameMutationRequest {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileParentMutationRequest {
    id: String,
    parent_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileIdMutationRequest {
    id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MutationResponse {
    ok: bool,
    message: String,
    id: Option<String>,
    parent_id: Option<String>,
}

fn mutation_ok(
    message: impl Into<String>,
    id: Option<String>,
    parent_id: Option<String>,
) -> Json<MutationResponse> {
    Json(MutationResponse {
        ok: true,
        message: message.into(),
        id,
        parent_id,
    })
}

fn mutation_error(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<MutationResponse>) {
    (
        status,
        Json(MutationResponse {
            ok: false,
            message: message.into(),
            id: None,
            parent_id: None,
        }),
    )
}

fn clean_mutation_name(value: &str) -> Result<String, String> {
    let clean = value.trim();

    if clean.is_empty() {
        return Err("Informe um nome.".to_string());
    }

    if clean.chars().count() > 128 {
        return Err("Use um nome com no máximo 128 caracteres.".to_string());
    }

    Ok(clean.to_string())
}

fn percent_decode_header(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut output = Vec::<u8>::with_capacity(bytes.len());
    let mut index = 0_usize;

    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let high = (bytes[index + 1] as char).to_digit(16);
            let low = (bytes[index + 2] as char).to_digit(16);

            if let (Some(high), Some(low)) = (high, low) {
                output.push(((high << 4) | low) as u8);
                index += 3;
                continue;
            }
        }

        output.push(bytes[index]);
        index += 1;
    }

    String::from_utf8(output).unwrap_or_else(|_| value.to_string())
}

async fn resolve_peer_by_bot_api_id(
    client: &Client,
    target_peer_id: i64,
) -> Result<grammers_client::types::Peer, String> {
    let mut dialogs = client.iter_dialogs();
    let mut fallback = None::<grammers_client::types::Peer>;

    loop {
        let dialog = match dialogs.next().await {
            Ok(Some(dialog)) => dialog,
            Ok(None) => break,
            Err(error) => return Err(error.to_string()),
        };

        let peer = dialog.peer().clone();
        let bot_api_id = peer.id().bot_api_dialog_id();

        match &peer {
            grammers_client::types::Peer::Channel(channel) => {
                let raw_id = channel.raw.id;
                let abs_raw_id = raw_id.abs();
                let bot_style_id = -1_000_000_000_000_i64 - abs_raw_id;

                if target_peer_id == bot_api_id
                    || target_peer_id == raw_id
                    || target_peer_id == -raw_id
                    || target_peer_id == abs_raw_id
                    || target_peer_id == bot_style_id
                {
                    return Ok(peer);
                }
            }
            _ => {
                if bot_api_id == target_peer_id && fallback.is_none() {
                    fallback = Some(peer);
                }
            }
        }
    }

    if let Some(peer) = fallback {
        return Ok(peer);
    }

    Err(format!(
        "O destino Telegram {target_peer_id} não foi encontrado."
    ))
}
fn channel_input_peer(peer: &grammers_client::types::Peer) -> Result<tl::enums::InputPeer, String> {
    match peer {
        grammers_client::types::Peer::Channel(channel) => Ok(tl::types::InputPeerChannel {
            channel_id: channel.raw.id,
            access_hash: channel.raw.access_hash.unwrap_or(0),
        }
        .into()),
        _ => Err("A operação exige um fórum/supergrupo Telegram.".to_string()),
    }
}

async fn mutation_input_peer(
    client: &Client,
    pool: &PgPool,
    target_peer_id: i64,
) -> Result<tl::enums::InputPeer, String> {
    let credential = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            channel_id,
            access_hash
        FROM telegram_peer_credentials
        WHERE user_id = $1
          AND telegram_peer_id = $2
        LIMIT 1
        "#,
    )
    .bind(local_user_uuid())
    .bind(target_peer_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;

    if let Some(row) = credential {
        let channel_id = row.try_get::<i64, _>("channel_id").unwrap_or_default();

        let access_hash = row.try_get::<i64, _>("access_hash").unwrap_or_default();

        if channel_id != 0 && access_hash != 0 {
            return Ok(tl::enums::InputPeer::Channel(tl::types::InputPeerChannel {
                channel_id,
                access_hash,
            }));
        }
    }

    let peer = resolve_peer_by_bot_api_id(client, target_peer_id).await?;

    channel_input_peer(&peer)
}
async fn mutation_input_channel(
    client: &Client,
    pool: &PgPool,
    target_peer_id: i64,
) -> Result<tl::enums::InputChannel, String> {
    let credential = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            channel_id,
            access_hash
        FROM telegram_peer_credentials
        WHERE user_id = $1
          AND telegram_peer_id = $2
        LIMIT 1
        "#,
    )
    .bind(local_user_uuid())
    .bind(target_peer_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;

    if let Some(row) = credential {
        let channel_id = row.try_get::<i64, _>("channel_id").unwrap_or_default();

        let access_hash = row.try_get::<i64, _>("access_hash").unwrap_or_default();

        if channel_id != 0 && access_hash != 0 {
            return Ok(tl::types::InputChannel {
                channel_id,
                access_hash,
            }
            .into());
        }
    }

    let peer = resolve_peer_by_bot_api_id(client, target_peer_id).await?;

    match peer {
        grammers_client::types::Peer::Channel(channel) => {
            let access_hash = channel.raw.access_hash.unwrap_or(0);

            if access_hash == 0 {
                return Err("O fórum Telegram não possui access_hash utilizável.".to_string());
            }

            Ok(tl::types::InputChannel {
                channel_id: channel.raw.id,
                access_hash,
            }
            .into())
        }
        _ => Err("A operação exige um fórum/supergrupo Telegram.".to_string()),
    }
}

async fn get_media(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response<Body>, (StatusCode, String)> {
    const CHUNK_SIZE: i32 = 65_536;
    const CDN_ALIGNMENT: u64 = 524_288;
    const MAX_RESPONSE_BYTES: u64 = 2 * 1024 * 1024;

    let pool = state.db.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "PostgreSQL indisponivel.".to_string(),
    ))?;
    let telegram = state.telegram.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Telegram indisponivel.".to_string(),
    ))?;

    let file_uuid = Uuid::parse_str(&id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "ID de arquivo invalido.".to_string(),
        )
    })?;

    let row = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT telegram_peer_id, telegram_message_id, size_bytes, mime
        FROM telegram_index_files
        WHERE id=$1 AND deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(file_uuid)
    .fetch_optional(pool)
    .await
    .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    .ok_or((StatusCode::NOT_FOUND, "Arquivo nao encontrado.".to_string()))?;

    let peer_id = row.try_get::<i64, _>("telegram_peer_id").unwrap_or(0);
    let message_id = row.try_get::<i64, _>("telegram_message_id").unwrap_or(0);
    let total = row.try_get::<i64, _>("size_bytes").unwrap_or(0).max(0) as u64;
    let mime = row
        .try_get::<String, _>("mime")
        .unwrap_or_else(|_| "application/octet-stream".to_string());

    if message_id <= 0 {
        return Err((
            StatusCode::NOT_FOUND,
            "Midia remota indisponivel.".to_string(),
        ));
    }

    let peer = resolve_peer_by_bot_api_id(&telegram.client, peer_id)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;

    let messages = telegram
        .client
        .get_messages_by_id(&peer, &[message_id as i32])
        .await
        .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;

    let message = messages.into_iter().flatten().next().ok_or((
        StatusCode::NOT_FOUND,
        "Mensagem Telegram nao encontrada.".to_string(),
    ))?;

    let media = message
        .media()
        .ok_or((StatusCode::NOT_FOUND, "Mensagem sem midia.".to_string()))?;

    // TCLOUD_MEDIA_REPAIR_112_UNKNOWN_SIZE
    // Fotos Telegram podem entrar no indice com size_bytes=0. Nesse caso,
    // baixamos a midia ate o fim (com limite defensivo) para permitir preview.
    if total == 0 {
        const MAX_UNKNOWN_MEDIA_BYTES: usize = 64 * 1024 * 1024;
        let mut download = telegram.client.iter_download(&media).chunk_size(CHUNK_SIZE);
        let mut output = Vec::new();

        while output.len() < MAX_UNKNOWN_MEDIA_BYTES {
            let next = download
                .next()
                .await
                .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;

            let Some(bytes) = next else {
                break;
            };

            let remaining = MAX_UNKNOWN_MEDIA_BYTES.saturating_sub(output.len());
            if remaining == 0 {
                break;
            }
            output.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
        }

        if output.is_empty() {
            return Err((
                StatusCode::BAD_GATEWAY,
                "Telegram nao retornou bytes da midia.".to_string(),
            ));
        }

        let mut response = Response::new(Body::from(output));
        *response.status_mut() = StatusCode::OK;
        let response_headers = response.headers_mut();
        response_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(&mime)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
        );
        response_headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("private, max-age=86400"),
        );
        return Ok(response);
    }

    let range_header = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok());

    let mut start = 0u64;
    let mut requested_end = total.saturating_sub(1);

    if let Some(spec) = range_header.and_then(|value| value.strip_prefix("bytes=")) {
        if let Some((left, right)) = spec.split_once('-') {
            if let Ok(value) = left.trim().parse::<u64>() {
                start = value.min(total.saturating_sub(1));
            }
            if !right.trim().is_empty() {
                if let Ok(value) = right.trim().parse::<u64>() {
                    requested_end = value.min(total.saturating_sub(1));
                }
            }
        }
    }

    let end = requested_end.min(start.saturating_add(MAX_RESPONSE_BYTES).saturating_sub(1));
    let wanted = end.saturating_sub(start).saturating_add(1) as usize;

    let aligned_start = (start / CDN_ALIGNMENT) * CDN_ALIGNMENT;
    let chunk_index = (aligned_start / CHUNK_SIZE as u64) as i32;
    let mut leading_skip = (start - aligned_start) as usize;

    let mut download = telegram.client.iter_download(&media).chunk_size(CHUNK_SIZE);
    if chunk_index > 0 {
        download = download.skip_chunks(chunk_index);
    }

    let mut output = Vec::with_capacity(wanted);

    while output.len() < wanted {
        let next = download
            .next()
            .await
            .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;

        let Some(bytes) = next else {
            break;
        };

        if leading_skip >= bytes.len() {
            leading_skip -= bytes.len();
            continue;
        }

        let slice = &bytes[leading_skip..];
        leading_skip = 0;
        let remaining = wanted - output.len();
        output.extend_from_slice(&slice[..slice.len().min(remaining)]);
    }

    if output.is_empty() {
        return Err((
            StatusCode::BAD_GATEWAY,
            "Telegram nao retornou bytes da midia.".to_string(),
        ));
    }

    let actual_end = start + output.len() as u64 - 1;
    let partial = range_header.is_some() || actual_end + 1 < total;

    let mut response = Response::new(Body::from(output));
    *response.status_mut() = if partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    let response_headers = response.headers_mut();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&mime)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=86400"),
    );
    response_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&(actual_end - start + 1).to_string()).unwrap(),
    );

    if partial {
        response_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{}", start, actual_end, total)).unwrap(),
        );
    }

    Ok(response)
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    // TCLOUD_CLOUD_RUNTIME_100
    let render_port = env::var("PORT").ok();
    let port: u16 = env::var("TCLOUD_CORE_PORT")
        .ok()
        .or_else(|| render_port.clone())
        .and_then(|value| value.parse().ok())
        .unwrap_or(8787);

    let bind_host = env::var("TCLOUD_CORE_HOST").unwrap_or_else(|_| {
        if render_port.is_some() {
            "0.0.0.0".to_string()
        } else {
            "127.0.0.1".to_string()
        }
    });

    let db = connect_database().await;
    let telegram = initialize_telegram(db.as_ref()).await;

    let state = AppState {
        db,
        fallback_files: Arc::new(seed_fallback_files()),
        telegram,
    };

    if let (Some(telegram), Some(pool)) =
        (state.telegram.as_ref().cloned(), state.db.as_ref().cloned())
    {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                if telegram_authorized(&telegram).await {
                    persist_telegram_session_blob(&pool, &telegram.session_path).await;
                }
            }
        });
    }

    let mut allowed_origins = vec![
        "http://localhost:3000".parse::<HeaderValue>().unwrap(),
        "http://127.0.0.1:3000".parse::<HeaderValue>().unwrap(),
        "http://localhost:3001".parse::<HeaderValue>().unwrap(),
        "http://127.0.0.1:3001".parse::<HeaderValue>().unwrap(),
    ];

    if let Ok(origins) = env::var("TCLOUD_CORS_ORIGINS") {
        for origin in origins
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if let Ok(header) = origin.parse::<HeaderValue>() {
                if !allowed_origins.contains(&header) {
                    allowed_origins.push(header);
                }
            }
        }
    }

    let cors = CorsLayer::new()
        .allow_origin(allowed_origins)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(tower_http::cors::Any)
        .expose_headers([
            header::ACCEPT_RANGES,
            header::CONTENT_RANGE,
            header::CONTENT_LENGTH,
            header::CONTENT_TYPE,
        ]);

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/status", get(status))
        .route("/api/v1/platform/capabilities", get(platform_capabilities))
        .route("/api/v1/files", get(list_files))
        .route("/api/v1/files/{id}", get(get_file))
        .route("/api/v1/media/{id}", get(get_media))
        .route("/api/v1/files/upload", post(upload_file))
        .route("/api/v1/files/rename", post(rename_file))
        .route("/api/v1/files/move", post(move_file))
        .route("/api/v1/files/trash", post(trash_file))
        .route("/api/v1/files/restore", post(restore_file))
        .route("/api/v1/files/delete", post(delete_file_permanently))
        .route("/api/v1/trash", get(list_trash))
        .route("/api/v1/forums", get(list_forums).post(create_forum))
        .route("/api/v1/forums/rename", post(rename_forum))
        .route("/api/v1/forums/delete", post(delete_forum_permanently))
        .route("/api/v1/folders", post(create_folder))
        .route("/api/v1/folders/delete", post(delete_folder_permanently))
        .route("/api/v1/devices", get(list_devices))
        .route("/api/v1/auth/status", get(auth_status))
        .route("/api/v1/auth/request-code", post(request_code))
        .route("/api/v1/auth/verify-code", post(verify_code))
        .route("/api/v1/auth/password", post(verify_password))
        .route("/api/v1/auth/logout", post(logout))
        .route("/api/v1/index/telegram", post(queue_telegram_index))
        .route("/api/v1/index/status", get(index_status))
        .route("/api/v1/live/revision", get(live_revision))
        .route("/api/v1/dialogs", get(list_dialogs))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
        .with_state(state.clone());

    // TCLOUD_SYNC_INTEGRITY_611_AUTO_DELTA
    if let (Some(telegram), Some(pool)) =
        (state.telegram.as_ref().cloned(), state.db.as_ref().cloned())
    {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                if !telegram_authorized(&telegram).await {
                    continue;
                }

                // TCLOUD_SYNC_INTEGRITY_612_SQLX_FIX
                let busy = match sqlx_core::query::query(
                    r#"
                    SELECT COUNT(*)::BIGINT AS busy_count
                    FROM index_runs
                    WHERE user_id = $1
                      AND provider = 'telegram'
                      AND status IN ('queued', 'running')
                    "#,
                )
                .bind(local_user_uuid())
                .fetch_one(&pool)
                .await
                {
                    Ok(row) => {
                        use sqlx_core::row::Row;
                        row.try_get::<i64, _>("busy_count").unwrap_or(1)
                    }
                    Err(_) => 1,
                };

                if busy > 0 {
                    continue;
                }

                let run_id = Uuid::new_v4();

                let inserted = sqlx_core::query::query::<Postgres>(
                    r#"
                    INSERT INTO index_runs (
                        id, user_id, provider, mode, status
                    )
                    VALUES ($1, $2, 'telegram', 'delta', 'queued')
                    "#,
                )
                .bind(run_id)
                .bind(local_user_uuid())
                .execute(&pool)
                .await
                .is_ok();

                if inserted {
                    run_telegram_index(
                        Arc::clone(&telegram),
                        pool.clone(),
                        run_id,
                        "delta".to_string(),
                    )
                    .await;
                }
            }
        });
    }

    let address: SocketAddr = format!("{bind_host}:{port}")
        .parse()
        .expect("TCLOUD_CORE_HOST/PORT invalidos");
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("nao foi possivel iniciar o TCloud Core");

    println!("TCloud Core 0.7.0: http://{address}");
    println!(
        "PostgreSQL:         {}",
        if state.db.is_some() {
            "conectado"
        } else {
            "fallback"
        }
    );
    println!(
        "Telegram runtime:   {}",
        if state.telegram.is_some() {
            "grammers 0.8.1"
        } else {
            "indisponivel"
        }
    );

    axum::serve(listener, app)
        .await
        .expect("TCloud Core encerrou com erro");
}

async fn initialize_telegram(db: Option<&PgPool>) -> Option<Arc<TelegramRuntime>> {
    let api_id = env::var("TCLOUD_TELEGRAM_API_ID")
        .ok()?
        .trim()
        .parse::<i32>()
        .ok()?;

    let api_hash = env::var("TCLOUD_TELEGRAM_API_HASH").ok()?;

    if api_hash.trim().len() < 20 {
        return None;
    }

    let session_path = env::var("TCLOUD_TELEGRAM_SESSION")
        .unwrap_or_else(|_| "data/tcloud-telegram.session".to_string());

    if let Some(pool) = db {
        restore_telegram_session_blob(pool, &session_path).await;
    }

    if let Some(parent) = PathBuf::from(&session_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let session = match SqliteSession::open(&session_path) {
        Ok(session) => Arc::new(session),
        Err(error) => {
            eprintln!("Falha ao abrir sessao Telegram: {error}");
            return None;
        }
    };

    let pool = SenderPool::new(Arc::clone(&session), api_id);
    let client = Client::new(&pool);
    let SenderPool { runner, .. } = pool;

    tokio::spawn(async move {
        runner.run().await;
    });

    Some(Arc::new(TelegramRuntime {
        client,
        api_hash,
        login_token: Mutex::new(None),
        password_token: Mutex::new(None),
        password_hint: Mutex::new(None),
        session_path,
    }))
}

const TELEGRAM_SESSION_REMOTE_KEY: &str = "telegram-primary";

fn telegram_session_cipher() -> Option<ChaCha20Poly1305> {
    let raw = env::var("TCLOUD_SESSION_ENCRYPTION_KEY").ok()?;
    let decoded = match hex::decode(raw.trim()) {
        Ok(value) if value.len() == 32 => value,
        _ => {
            eprintln!("TCLOUD_SESSION_ENCRYPTION_KEY deve ter 64 caracteres hexadecimais.");
            return None;
        }
    };

    Some(ChaCha20Poly1305::new(Key::from_slice(&decoded)))
}

fn encrypt_telegram_session(payload: &[u8]) -> Option<Vec<u8>> {
    let cipher = telegram_session_cipher()?;
    let nonce_uuid = Uuid::new_v4();
    let nonce = Nonce::from_slice(&nonce_uuid.as_bytes()[..12]);
    let ciphertext = cipher.encrypt(nonce, payload).ok()?;

    let mut packed = Vec::with_capacity(12 + ciphertext.len());
    packed.extend_from_slice(nonce);
    packed.extend_from_slice(&ciphertext);
    Some(packed)
}

fn decrypt_telegram_session(packed: &[u8]) -> Option<Vec<u8>> {
    if packed.len() <= 12 {
        return None;
    }

    let cipher = telegram_session_cipher()?;
    let nonce = Nonce::from_slice(&packed[..12]);
    cipher.decrypt(nonce, &packed[12..]).ok()
}

async fn restore_telegram_session_blob(pool: &PgPool, session_path: &str) {
    // TCLOUD_CLOUD_FRESH_SESSION_BOOT_100
    // Render usa filesystem efemero, mas uma instancia nova ainda pode iniciar com
    // um arquivo de sessao presente no runtime. Em cloud, o PostgreSQL e a fonte
    // canonica: se houver snapshot remoto, ele sempre sobrescreve o arquivo local;
    // se nao houver, qualquer sessao local residual e removida para que o login
    // crie uma auth key realmente nova e independente.
    let cloud_runtime = env::var("PORT").is_ok();

    let local_exists = std::fs::metadata(session_path)
        .map(|metadata| metadata.len() > 0)
        .unwrap_or(false);

    if telegram_session_cipher().is_none() {
        return;
    }

    if !cloud_runtime && local_exists {
        return;
    }

    use sqlx_core::row::Row;

    let row = match sqlx_core::query::query::<Postgres>(
        r#"
        SELECT encrypted_payload
        FROM tcloud_private.core_runtime_sessions
        WHERE session_key = $1
        "#,
    )
    .bind(TELEGRAM_SESSION_REMOTE_KEY)
    .fetch_optional(pool)
    .await
    {
        Ok(row) => row,
        Err(error) => {
            eprintln!("Falha ao consultar backup da sessao Telegram: {error}");
            return;
        }
    };

    let Some(row) = row else {
        if cloud_runtime {
            let session_files = [
                session_path.to_string(),
                format!("{session_path}-journal"),
                format!("{session_path}-wal"),
                format!("{session_path}-shm"),
            ];

            for path in session_files {
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        println!("Sessao Telegram residual removida do runtime cloud: {path}")
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        eprintln!("Falha ao remover sessao Telegram residual {path}: {error}")
                    }
                }
            }

            println!("Nenhuma sessao Telegram remota encontrada; runtime cloud iniciara limpo.");
        }
        return;
    };

    let encrypted = row
        .try_get::<Vec<u8>, _>("encrypted_payload")
        .unwrap_or_default();

    let Some(payload) = decrypt_telegram_session(&encrypted) else {
        eprintln!("Backup remoto da sessao Telegram nao pode ser descriptografado.");
        return;
    };

    if payload.is_empty() {
        return;
    }

    if let Some(parent) = PathBuf::from(session_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    if cloud_runtime {
        for suffix in ["-journal", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{session_path}{suffix}"));
        }
    }

    match std::fs::write(session_path, payload) {
        Ok(()) => println!("Sessao Telegram restaurada do PostgreSQL."),
        Err(error) => eprintln!("Falha ao restaurar sessao Telegram: {error}"),
    }
}

async fn persist_telegram_session_blob(pool: &PgPool, session_path: &str) {
    if telegram_session_cipher().is_none() {
        return;
    }

    let payload = match std::fs::read(session_path) {
        Ok(payload) if !payload.is_empty() => payload,
        _ => return,
    };

    let Some(encrypted) = encrypt_telegram_session(&payload) else {
        eprintln!("Falha ao criptografar sessao Telegram.");
        return;
    };

    let result = sqlx_core::query::query::<Postgres>(
        r#"
        INSERT INTO tcloud_private.core_runtime_sessions (
            session_key,
            encrypted_payload,
            payload_bytes,
            updated_at
        )
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (session_key)
        DO UPDATE SET
            encrypted_payload = EXCLUDED.encrypted_payload,
            payload_bytes = EXCLUDED.payload_bytes,
            updated_at = NOW()
        "#,
    )
    .bind(TELEGRAM_SESSION_REMOTE_KEY)
    .bind(encrypted)
    .bind(payload.len() as i64)
    .execute(pool)
    .await;

    if let Err(error) = result {
        eprintln!("Falha ao persistir sessao Telegram no PostgreSQL: {error}");
    }
}

async fn run_migrations(pool: &PgPool) -> Result<(), sqlx_core::migrate::MigrateError> {
    let migrator = sqlx_core::migrate::Migrator::new(std::path::Path::new("./migrations")).await?;

    migrator.run(pool).await
}

async fn connect_database() -> Option<PgPool> {
    let database_url = match env::var("TCLOUD_DATABASE_URL").or_else(|_| env::var("DATABASE_URL")) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return None,
    };

    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => {
            println!("PostgreSQL indisponivel: {error}");
            return None;
        }
    };

    if let Err(error) = run_migrations(&pool).await {
        println!("Falha nas migrations PostgreSQL: {error}");
        return None;
    }

    Some(pool)
}

async fn telegram_authorized(telegram: &TelegramRuntime) -> bool {
    telegram.client.is_authorized().await.unwrap_or(false)
}

async fn health(State(state): State<AppState>) -> Json<Health> {
    let telegram_status = if let Some(telegram) = &state.telegram {
        if telegram_authorized(telegram).await {
            "authorized"
        } else {
            "ready"
        }
    } else {
        "not-configured"
    };

    Json(Health {
        status: "ok",
        database: if state.db.is_some() {
            "connected"
        } else {
            "fallback"
        },
        telegram: telegram_status,
    })
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlatformCapabilities {
    foundation: &'static str,
    api_version: &'static str,
    core_version: &'static str,
    forum_management: bool,
    folder_management: bool,
    upload: bool,
    rename: bool,
    move_items: bool,
    trash_restore: bool,
    permanent_delete: bool,
    telegram_delta: bool,
    credential_bridge: bool,
    live_refresh_hint_seconds: u32,
    session_cache: bool,
    max_web_upload_bytes: u64,
}

async fn platform_capabilities() -> Json<PlatformCapabilities> {
    Json(PlatformCapabilities {
        foundation: "5.0",
        api_version: "v1",
        core_version: "0.7.0",
        forum_management: true,
        folder_management: true,
        upload: true,
        rename: true,
        move_items: true,
        trash_restore: true,
        permanent_delete: true,
        telegram_delta: true,
        credential_bridge: true,
        live_refresh_hint_seconds: 8,
        session_cache: true,
        max_web_upload_bytes: 536_870_912,
    })
}
async fn status(State(state): State<AppState>) -> Json<CoreStatus> {
    let (files_count, devices_count, sessions_count) = if let Some(pool) = &state.db {
        (
            count_query(
                pool,
                "SELECT COUNT(*) FROM telegram_index_files WHERE deleted_at IS NULL",
            )
            .await,
            count_query(pool, "SELECT COUNT(*) FROM devices").await,
            count_query(
                pool,
                "SELECT COUNT(*) FROM sessions WHERE revoked_at IS NULL",
            )
            .await,
        )
    } else {
        (state.fallback_files.len() as i64, 0, 0)
    };

    let telegram_connected = if let Some(telegram) = &state.telegram {
        telegram_authorized(telegram).await
    } else {
        false
    };

    Json(CoreStatus {
        name: "TCloud Core",
        version: "0.7.0",
        connected: true,
        telegram_connected,
        telegram_credentials_ready: state.telegram.is_some(),
        database_connected: state.db.is_some(),
        database_mode: if state.db.is_some() {
            "postgresql"
        } else {
            "fallback"
        },
        storage_backend: "telegram",
        files_count,
        devices_count,
        sessions_count,
    })
}

async fn auth_status(State(state): State<AppState>) -> Json<TelegramAuthStatus> {
    let Some(telegram) = &state.telegram else {
        return Json(TelegramAuthStatus {
            credentials_configured: false,
            authorized: false,
            stage: "credentials-required".to_string(),
            session_owner: "tcloud-core",
            can_request_code: false,
            password_required: false,
            password_hint: None,
            message: "Credenciais Telegram ausentes.".to_string(),
        });
    };

    let authorized = telegram_authorized(telegram).await;
    let password_required = telegram.password_token.lock().await.is_some();
    let code_required = telegram.login_token.lock().await.is_some();

    let password_hint = telegram.password_hint.lock().await.clone();

    let stage = if authorized {
        "authorized"
    } else if password_required {
        "password-required"
    } else if code_required {
        "code-required"
    } else {
        "phone-required"
    };

    Json(TelegramAuthStatus {
        credentials_configured: true,
        authorized,
        stage: stage.to_string(),
        session_owner: "tcloud-core",
        can_request_code: !authorized,
        password_required,
        password_hint,
        message: if authorized {
            "Telegram conectado ao TCloud Core.".to_string()
        } else if password_required {
            "Senha de duas etapas necessaria.".to_string()
        } else if code_required {
            "Digite o codigo enviado pelo Telegram.".to_string()
        } else {
            "Informe o telefone para conectar o Telegram.".to_string()
        },
    })
}

async fn request_code(
    State(state): State<AppState>,
    Json(request): Json<PhoneRequest>,
) -> Json<AuthActionResponse> {
    let Some(telegram) = &state.telegram else {
        return auth_error("credentials-required", "Credenciais Telegram ausentes.");
    };

    if telegram_authorized(telegram).await {
        return auth_success("authorized", "Telegram ja esta conectado.");
    }

    let phone = request.phone.trim();

    if phone.len() < 8 || !phone.starts_with('+') {
        return auth_error(
            "phone-required",
            "Use o numero em formato internacional, por exemplo +55...",
        );
    }

    match telegram
        .client
        .request_login_code(phone, &telegram.api_hash)
        .await
    {
        Ok(token) => {
            *telegram.login_token.lock().await = Some(token);
            *telegram.password_token.lock().await = None;
            *telegram.password_hint.lock().await = None;

            auth_success("code-required", "Codigo enviado pelo Telegram.")
        }
        Err(error) => auth_error(
            "phone-required",
            &format!("Telegram recusou o envio do codigo: {error}"),
        ),
    }
}

async fn verify_code(
    State(state): State<AppState>,
    Json(request): Json<CodeRequest>,
) -> Json<AuthActionResponse> {
    let Some(telegram) = &state.telegram else {
        return auth_error("credentials-required", "Telegram nao configurado.");
    };

    if telegram_authorized(telegram).await {
        return auth_success("authorized", "Telegram ja esta conectado.");
    }

    let code = request.code.trim();

    if code.is_empty() {
        return auth_error("code-required", "Informe o codigo recebido.");
    }

    let token = {
        let mut guard = telegram.login_token.lock().await;
        guard.take()
    };

    let Some(token) = token else {
        return auth_error("phone-required", "Solicite um novo codigo primeiro.");
    };

    match telegram.client.sign_in(&token, code).await {
        Ok(user) => {
            *telegram.password_token.lock().await = None;
            *telegram.password_hint.lock().await = None;

            if let Some(pool) = &state.db {
                persist_authorized_account(pool, user.bare_id(), &telegram.session_path).await;
            }

            auth_success("authorized", "Telegram conectado com sucesso.")
        }
        Err(SignInError::PasswordRequired(password_token)) => {
            let hint = password_token.hint().map(|value| value.to_string());

            *telegram.password_hint.lock().await = hint.clone();
            *telegram.password_token.lock().await = Some(password_token);

            Json(AuthActionResponse {
                ok: true,
                authorized: false,
                stage: "password-required".to_string(),
                password_required: true,
                password_hint: hint,
                message: "Conta protegida por senha de duas etapas.".to_string(),
            })
        }
        Err(SignInError::InvalidCode) => {
            *telegram.login_token.lock().await = Some(token);

            auth_error("code-required", "Codigo invalido. Tente novamente.")
        }
        Err(SignInError::SignUpRequired { .. }) => auth_error(
            "phone-required",
            "A conta precisa existir antes em um app oficial do Telegram.",
        ),
        Err(error) => {
            *telegram.login_token.lock().await = Some(token);

            auth_error(
                "code-required",
                &format!("Falha ao entrar no Telegram: {error}"),
            )
        }
    }
}

async fn verify_password(
    State(state): State<AppState>,
    Json(request): Json<PasswordRequest>,
) -> Json<AuthActionResponse> {
    let Some(telegram) = &state.telegram else {
        return auth_error("credentials-required", "Telegram nao configurado.");
    };

    let password = request.password;

    if password.is_empty() {
        return auth_error("password-required", "Informe a senha de duas etapas.");
    }

    let password_token = {
        let mut guard = telegram.password_token.lock().await;
        guard.take()
    };

    let Some(password_token) = password_token else {
        return auth_error("phone-required", "Reinicie o login pelo telefone.");
    };

    match telegram
        .client
        .check_password(password_token, password.trim())
        .await
    {
        Ok(user) => {
            *telegram.password_hint.lock().await = None;

            if let Some(pool) = &state.db {
                persist_authorized_account(pool, user.bare_id(), &telegram.session_path).await;
            }

            auth_success("authorized", "Telegram conectado com 2FA.")
        }
        Err(SignInError::InvalidPassword) => auth_error(
            "phone-required",
            "Senha de duas etapas incorreta. Inicie o login novamente.",
        ),
        Err(error) => auth_error(
            "phone-required",
            &format!("Falha na verificacao 2FA: {error}"),
        ),
    }
}

async fn logout(State(state): State<AppState>) -> Json<AuthActionResponse> {
    let Some(telegram) = &state.telegram else {
        return auth_success("phone-required", "Telegram ja estava desconectado.");
    };

    match telegram.client.sign_out().await {
        Ok(_) => {
            *telegram.login_token.lock().await = None;
            *telegram.password_token.lock().await = None;
            *telegram.password_hint.lock().await = None;

            if let Some(pool) = &state.db {
                let _ = sqlx_core::query::query::<Postgres>(
                    r#"
                    UPDATE telegram_accounts
                    SET
                        authorized = FALSE,
                        updated_at = NOW()
                    WHERE user_id = $1
                    "#,
                )
                .bind(local_user_uuid())
                .execute(pool)
                .await;
            }

            auth_success("phone-required", "Telegram desconectado do Core.")
        }
        Err(error) => auth_error("authorized", &format!("Falha ao sair do Telegram: {error}")),
    }
}

fn auth_success(stage: &str, message: &str) -> Json<AuthActionResponse> {
    Json(AuthActionResponse {
        ok: true,
        authorized: stage == "authorized",
        stage: stage.to_string(),
        password_required: stage == "password-required",
        password_hint: None,
        message: message.to_string(),
    })
}

fn auth_error(stage: &str, message: &str) -> Json<AuthActionResponse> {
    Json(AuthActionResponse {
        ok: false,
        authorized: false,
        stage: stage.to_string(),
        password_required: stage == "password-required",
        password_hint: None,
        message: message.to_string(),
    })
}

fn local_user_uuid() -> Uuid {
    Uuid::parse_str(LOCAL_USER_ID).expect("LOCAL_USER_ID valido")
}

async fn persist_authorized_account(pool: &PgPool, telegram_user_id: i64, session_path: &str) {
    let _ = sqlx_core::query::query::<Postgres>(
        r#"
        INSERT INTO telegram_accounts (
            id,
            user_id,
            telegram_user_id,
            display_name,
            authorized,
            session_storage,
            session_path,
            last_authorized_at,
            last_error
        )
        VALUES (
            $1,
            $2,
            $3,
            'Telegram',
            TRUE,
            'grammers-sqlite',
            $4,
            NOW(),
            NULL
        )
        ON CONFLICT (user_id)
        DO UPDATE SET
            telegram_user_id = EXCLUDED.telegram_user_id,
            authorized = TRUE,
            session_storage = 'grammers-sqlite',
            session_path = EXCLUDED.session_path,
            last_authorized_at = NOW(),
            last_error = NULL,
            updated_at = NOW()
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(local_user_uuid())
    .bind(telegram_user_id)
    .bind(session_path)
    .execute(pool)
    .await;
}

async fn queue_telegram_index(
    State(state): State<AppState>,
    Json(request): Json<IndexRequest>,
) -> Json<IndexRunResponse> {
    let Some(telegram) = &state.telegram else {
        return Json(IndexRunResponse {
            accepted: false,
            id: None,
            status: "blocked".to_string(),
            message: "Telegram nao configurado.".to_string(),
        });
    };

    if !telegram_authorized(telegram).await {
        return Json(IndexRunResponse {
            accepted: false,
            id: None,
            status: "unauthorized".to_string(),
            message: "Conecte o Telegram primeiro.".to_string(),
        });
    }

    let Some(pool) = &state.db else {
        return Json(IndexRunResponse {
            accepted: false,
            id: None,
            status: "database-required".to_string(),
            message: "PostgreSQL precisa estar ativo para indexar.".to_string(),
        });
    };

    let run_id = Uuid::new_v4();
    let mode = match request
        .mode
        .unwrap_or_else(|| "delta".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "full" => "full".to_string(),
        _ => "delta".to_string(),
    };

    let insert = sqlx_core::query::query::<Postgres>(
        r#"
        INSERT INTO index_runs (
            id,
            user_id,
            provider,
            mode,
            status
        )
        VALUES ($1, $2, 'telegram', $3, 'queued')
        "#,
    )
    .bind(run_id)
    .bind(local_user_uuid())
    .bind(&mode)
    .execute(pool)
    .await;

    if let Err(error) = insert {
        return Json(IndexRunResponse {
            accepted: false,
            id: None,
            status: "error".to_string(),
            message: format!("Nao foi possivel criar a fila de indexacao: {error}"),
        });
    }

    let telegram = Arc::clone(telegram);
    let pool = pool.clone();

    tokio::spawn(async move {
        run_telegram_index(telegram, pool, run_id, mode).await;
    });

    Json(IndexRunResponse {
        accepted: true,
        id: Some(run_id.to_string()),
        status: "queued".to_string(),
        message: "Indexacao Telegram iniciada em segundo plano.".to_string(),
    })
}

#[derive(Default)]
struct IndexCounters {
    dialogs_seen: i32,
    dialogs_updated: i32,
    topics_seen: i32,
    messages_seen: i64,
    files_upserted: i64,
}

#[derive(Debug, Clone)]
struct RemoteForumTopic {
    id: i64,
    title: String,
}

async fn fetch_forum_topics(
    client: &Client,
    peer: tl::enums::InputPeer,
) -> Result<Vec<RemoteForumTopic>, String> {
    let mut topics = Vec::<RemoteForumTopic>::new();
    let mut seen = HashSet::<i32>::new();

    let mut offset_date = 0_i32;
    let mut offset_id = 0_i32;
    let mut offset_topic = 0_i32;

    for _page in 0..20 {
        let response = client
            .invoke(&tl::functions::messages::GetForumTopics {
                peer: peer.clone(),
                q: None,
                offset_date,
                offset_id,
                offset_topic,
                limit: 100,
            })
            .await
            .map_err(|error| error.to_string())?;

        let forum_topics = tl::types::messages::ForumTopics::try_from(response)
            .map_err(|_| "Telegram retornou forumTopics em formato inesperado".to_string())?;

        let expected_total = forum_topics.count.max(0) as usize;
        let before = seen.len();
        let mut page_last = None::<(i32, i32, i32)>;

        for raw_topic in forum_topics.topics {
            let Ok(topic) = tl::types::ForumTopic::try_from(raw_topic) else {
                continue;
            };

            page_last = Some((topic.date, topic.top_message, topic.id));

            if topic.id <= 0 || !seen.insert(topic.id) {
                continue;
            }

            let title = if topic.title.trim().is_empty() {
                format!("Pasta {}", topic.id)
            } else {
                topic.title.trim().to_string()
            };

            topics.push(RemoteForumTopic {
                id: i64::from(topic.id),
                title,
            });
        }

        if topics.len() >= expected_total || seen.len() == before {
            break;
        }

        let Some((date, top_message, topic_id)) = page_last else {
            break;
        };

        offset_date = date;
        offset_id = top_message;
        offset_topic = topic_id;
    }

    Ok(topics)
}

fn message_topic_id(message: &tl::enums::Message) -> i64 {
    match message {
        tl::enums::Message::Message(raw) => match &raw.reply_to {
            Some(tl::enums::MessageReplyHeader::Header(header)) => header
                .reply_to_top_id
                .or(header.reply_to_msg_id)
                .map(i64::from)
                .unwrap_or(0),
            _ => 0,
        },
        _ => 0,
    }
}

fn raw_message_id(message: &tl::enums::Message) -> i32 {
    match message {
        tl::enums::Message::Message(value) => value.id,
        tl::enums::Message::Service(value) => value.id,
        tl::enums::Message::Empty(value) => value.id,
    }
}

fn unpack_raw_messages(response: tl::enums::messages::Messages) -> Vec<tl::enums::Message> {
    match response {
        tl::enums::messages::Messages::Messages(value) => value.messages,
        tl::enums::messages::Messages::Slice(value) => value.messages,
        tl::enums::messages::Messages::ChannelMessages(value) => value.messages,
        tl::enums::messages::Messages::NotModified(_) => Vec::new(),
    }
}

async fn reconcile_forum_topics_by_search(
    client: &Client,
    pool: &PgPool,
    peer: &grammers_client::types::Peer,
    peer_id: i64,
    topic_folder_ids: &HashMap<i64, Uuid>,
) -> Result<u64, String> {
    let input_peer: tl::enums::InputPeer = match peer {
        grammers_client::types::Peer::Channel(channel) => tl::types::InputPeerChannel {
            channel_id: channel.raw.id,
            access_hash: channel.raw.access_hash.unwrap_or(0),
        }
        .into(),
        _ => return Ok(0),
    };

    let mut total_assigned = 0_u64;

    for (topic_id, folder_id) in topic_folder_ids {
        let Ok(topic_message_id) = i32::try_from(*topic_id) else {
            continue;
        };

        if topic_message_id <= 0 {
            continue;
        }

        let mut offset_id = 0_i32;
        let mut pages = 0_usize;

        loop {
            pages += 1;

            if pages > 500 {
                break;
            }

            let response = client
                .invoke(&tl::functions::messages::Search {
                    peer: input_peer.clone(),
                    q: String::new(),
                    from_id: None,
                    saved_peer_id: None,
                    saved_reaction: None,
                    top_msg_id: Some(topic_message_id),
                    filter: tl::enums::MessagesFilter::InputMessagesFilterEmpty,
                    min_date: 0,
                    max_date: 0,
                    offset_id,
                    add_offset: 0,
                    limit: 100,
                    max_id: 0,
                    min_id: 0,
                    hash: 0,
                })
                .await
                .map_err(|error| {
                    format!(
                        "messages.search falhou no topico {}: {}",
                        topic_message_id, error
                    )
                })?;

            let raw_messages = unpack_raw_messages(response);

            if raw_messages.is_empty() {
                break;
            }

            let page_len = raw_messages.len();
            let mut next_offset = offset_id;

            for raw_message in &raw_messages {
                let message_id = raw_message_id(raw_message);

                if message_id <= 0 {
                    continue;
                }

                if next_offset == 0 || message_id < next_offset {
                    next_offset = message_id;
                }

                let result = sqlx_core::query::query::<Postgres>(
                    r#"
                        UPDATE telegram_index_files
                        SET
                            parent_id = $3,
                            telegram_topic_id = $4,
                            updated_at = NOW()
                        WHERE user_id = $1
                          AND telegram_peer_id = $2
                          AND telegram_message_id = $5
                          AND deleted_at IS NULL
                        "#,
                )
                .bind(local_user_uuid())
                .bind(peer_id)
                .bind(*folder_id)
                .bind(*topic_id)
                .bind(i64::from(message_id))
                .execute(pool)
                .await
                .map_err(|error| error.to_string())?;

                total_assigned += result.rows_affected();
            }

            if page_len < 100 {
                break;
            }

            if next_offset <= 0 || next_offset == offset_id {
                break;
            }

            offset_id = next_offset;
        }
    }

    Ok(total_assigned)
}
async fn run_telegram_index(
    telegram: Arc<TelegramRuntime>,
    pool: PgPool,
    run_id: Uuid,
    mode: String,
) {
    let _ = sqlx_core::query::query::<Postgres>(
        r#"
        UPDATE index_runs
        SET
            status = 'running',
            started_at = NOW(),
            error = NULL,
            updated_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .execute(&pool)
    .await;

    let result = index_telegram_content(&telegram, &pool, &mode).await;

    match result {
        Ok(counters) => {
            let _ = sqlx_core::query::query::<Postgres>(
                r#"
                UPDATE index_runs
                SET
                    status = 'completed',
                    dialogs_seen = $2,
                    dialogs_updated = $3,
                    topics_seen = $4,
                    messages_seen = $5,
                    files_upserted = $6,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE id = $1
                "#,
            )
            .bind(run_id)
            .bind(counters.dialogs_seen)
            .bind(counters.dialogs_updated)
            .bind(counters.topics_seen)
            .bind(counters.messages_seen)
            .bind(counters.files_upserted)
            .execute(&pool)
            .await;

            let _ = sqlx_core::query::query::<Postgres>(
                r#"
                UPDATE telegram_accounts
                SET
                    last_indexed_at = NOW(),
                    updated_at = NOW()
                WHERE user_id = $1
                "#,
            )
            .bind(local_user_uuid())
            .execute(&pool)
            .await;
        }
        Err(error) => {
            let safe_error = if error.len() > 1000 {
                error[..1000].to_string()
            } else {
                error
            };

            let _ = sqlx_core::query::query::<Postgres>(
                r#"
                UPDATE index_runs
                SET
                    status = 'failed',
                    error = $2,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE id = $1
                "#,
            )
            .bind(run_id)
            .bind(safe_error)
            .execute(&pool)
            .await;
        }
    }
}

async fn index_telegram_content(
    telegram: &TelegramRuntime,
    pool: &PgPool,
    mode: &str,
) -> Result<IndexCounters, String> {
    let me = telegram
        .client
        .get_me()
        .await
        .map_err(|error| error.to_string())?;

    let self_id = me.bare_id();

    let index_all = env::var("TCLOUD_INDEX_ALL_DIALOGS")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "sim"
            )
        })
        .unwrap_or(false);

    let included_names = env::var("TCLOUD_INDEX_INCLUDED_DIALOGS")
        .unwrap_or_else(|_| "Meus Arquivos".to_string())
        .split(|value| value == ';' || value == ',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
        .collect::<HashSet<_>>();

    let max_messages = env::var("TCLOUD_INDEX_MAX_MESSAGES_PER_DIALOG")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20_000)
        .clamp(100, 100_000);

    let managed_peer_ids = sqlx_core::query_scalar::query_scalar::<Postgres, i64>(
        r#"
        SELECT telegram_peer_id
        FROM telegram_index_folders
        WHERE user_id = $1
          AND telegram_topic_id = 0
          AND is_forum = TRUE
          AND deleted_at IS NULL
        "#,
    )
    .bind(local_user_uuid())
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .collect::<HashSet<_>>();

    let mut counters = IndexCounters::default();
    let mut dialogs = telegram.client.iter_dialogs();

    loop {
        let dialog = match dialogs.next().await {
            Ok(Some(dialog)) => dialog,
            Ok(None) => break,
            Err(error) => return Err(error.to_string()),
        };

        counters.dialogs_seen += 1;

        let peer = dialog.peer().clone();
        let peer_id = peer.id().bot_api_dialog_id();
        let raw_name = peer.name().unwrap_or("Sem nome").trim().to_string();

        if let grammers_client::types::Peer::Channel(channel) = &peer {
            let access_hash = channel.raw.access_hash.unwrap_or(0);

            if channel.raw.forum && access_hash != 0 {
                sqlx_core::query::query::<Postgres>(
                    r#"
                    INSERT INTO telegram_peer_credentials (
                        user_id,
                        telegram_peer_id,
                        channel_id,
                        access_hash,
                        name,
                        source,
                        updated_at
                    )
                    VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        'telegram-delta',
                        NOW()
                    )
                    ON CONFLICT (user_id, telegram_peer_id)
                    DO UPDATE SET
                        channel_id = EXCLUDED.channel_id,
                        access_hash = EXCLUDED.access_hash,
                        name = EXCLUDED.name,
                        source = CASE
                            WHEN telegram_peer_credentials.source = 'web-forum'
                                THEN 'web-forum'
                            ELSE 'telegram-delta'
                        END,
                        updated_at = NOW()
                    "#,
                )
                .bind(local_user_uuid())
                .bind(peer_id)
                .bind(channel.raw.id)
                .bind(access_hash)
                .bind(&raw_name)
                .execute(pool)
                .await
                .map_err(|error| error.to_string())?;
            }
        }
        let normalized = raw_name.to_ascii_lowercase();
        let is_self = peer_id == self_id;
        let is_tcloud = normalized == "tcloud"
            || normalized.starts_with("tcloud ")
            || normalized.starts_with("tcloud -");
        let is_explicit = included_names.contains(&normalized);
        let is_managed = managed_peer_ids.contains(&peer_id);

        if !index_all && !is_self && !is_tcloud && !is_explicit && !is_managed {
            continue;
        }

        let display_name = if is_self {
            "Mensagens Salvas".to_string()
        } else if raw_name.is_empty() {
            format!("Telegram {peer_id}")
        } else {
            raw_name
        };

        let forum_topics = if is_self {
            Vec::new()
        } else {
            match dialog.peer() {
                grammers_client::types::Peer::Channel(channel) => {
                    let input_peer: tl::enums::InputPeer = tl::types::InputPeerChannel {
                        channel_id: channel.raw.id,
                        access_hash: channel.raw.access_hash.unwrap_or(0),
                    }
                    .into();

                    fetch_forum_topics(&telegram.client, input_peer)
                        .await
                        .unwrap_or_default()
                }
                _ => Vec::new(),
            }
        };

        let is_forum = !forum_topics.is_empty() || is_explicit || is_managed;

        sqlx_core::query::query::<Postgres>(
            r#"
            INSERT INTO telegram_dialogs (
                id,
                user_id,
                telegram_peer_id,
                kind,
                name,
                is_forum,
                last_indexed_at
            )
            VALUES (
                $1,
                $2,
                $3,
                'dialog',
                $4,
                $5,
                NOW()
            )
            ON CONFLICT (user_id, telegram_peer_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                is_forum = EXCLUDED.is_forum,
                last_indexed_at = NOW(),
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(local_user_uuid())
        .bind(peer_id)
        .bind(&display_name)
        .bind(is_forum)
        .execute(pool)
        .await
        .map_err(|error| error.to_string())?;

        let root_folder_id = sqlx_core::query_scalar::query_scalar::<Postgres, Uuid>(
            r#"
                INSERT INTO telegram_index_folders (
                    id,
                    user_id,
                    parent_id,
                    telegram_peer_id,
                    telegram_topic_id,
                    name,
                    kind,
                    source,
                    is_forum,
                    last_indexed_at
                )
                VALUES (
                    $1,
                    $2,
                    NULL,
                    $3,
                    0,
                    $4,
                    'folder',
                    'telegram',
                    $5,
                    NOW()
                )
                ON CONFLICT (
                    user_id,
                    telegram_peer_id,
                    telegram_topic_id
                )
                DO UPDATE SET
                    parent_id = NULL,
                    name = EXCLUDED.name,
                    source = CASE
                        WHEN telegram_index_folders.source = 'web-forum'
                            THEN 'web-forum'
                        ELSE 'telegram'
                    END,
                    is_forum = EXCLUDED.is_forum,
                    deleted_at = NULL,
                    last_indexed_at = NOW(),
                    updated_at = NOW()
                RETURNING id
                "#,
        )
        .bind(Uuid::new_v4())
        .bind(local_user_uuid())
        .bind(peer_id)
        .bind(&display_name)
        .bind(is_forum)
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())?;

        counters.dialogs_updated += 1;

        let mut topic_folder_ids = HashMap::<i64, Uuid>::new();

        for topic in &forum_topics {
            let topic_folder_id = sqlx_core::query_scalar::query_scalar::<Postgres, Uuid>(
                r#"
                    INSERT INTO telegram_index_folders (
                        id,
                        user_id,
                        parent_id,
                        telegram_peer_id,
                        telegram_topic_id,
                        name,
                        kind,
                        source,
                        is_forum,
                        last_indexed_at
                    )
                    VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        $6,
                        'folder',
                        'telegram-topic',
                        FALSE,
                        NOW()
                    )
                    ON CONFLICT (
                        user_id,
                        telegram_peer_id,
                        telegram_topic_id
                    )
                    DO UPDATE SET
                        parent_id = EXCLUDED.parent_id,
                        name = EXCLUDED.name,
                        source = 'telegram-topic',
                        deleted_at = NULL,
                        last_indexed_at = NOW(),
                        updated_at = NOW()
                    RETURNING id
                    "#,
            )
            .bind(Uuid::new_v4())
            .bind(local_user_uuid())
            .bind(root_folder_id)
            .bind(peer_id)
            .bind(topic.id)
            .bind(&topic.title)
            .fetch_one(pool)
            .await
            .map_err(|error| error.to_string())?;

            topic_folder_ids.insert(topic.id, topic_folder_id);
            counters.topics_seen += 1;
        }

        let persisted_topic_rows = sqlx_core::query::query::<Postgres>(
            r#"
                SELECT
                    id,
                    telegram_topic_id
                FROM telegram_index_folders
                WHERE user_id = $1
                  AND telegram_peer_id = $2
                  AND telegram_topic_id <> 0
                  AND deleted_at IS NULL
                "#,
        )
        .bind(local_user_uuid())
        .bind(peer_id)
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;

        for row in persisted_topic_rows {
            let topic_folder_id = row
                .try_get::<Uuid, _>("id")
                .map_err(|error| error.to_string())?;

            let persisted_topic_id = row
                .try_get::<i64, _>("telegram_topic_id")
                .map_err(|error| error.to_string())?;

            topic_folder_ids.insert(persisted_topic_id, topic_folder_id);
        }
        // TCLOUD_TOPIC_DISCOVERY_773
        if is_forum {
            // TCLOUD_TOPIC_PEER_774
            let input_peer = mutation_input_peer(&telegram.client, pool, peer_id).await?;

            let remote_topics = fetch_forum_topics(&telegram.client, input_peer).await?;

            counters.topics_seen += i32::try_from(remote_topics.len()).unwrap_or(i32::MAX);

            for topic in remote_topics {
                let folder_id = sqlx_core::query_scalar::query_scalar::<Postgres, Uuid>(
                    r#"
                        INSERT INTO telegram_index_folders (
                            id, user_id, parent_id, telegram_peer_id,
                            telegram_topic_id, name, kind, source,
                            is_forum, last_indexed_at
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,'folder','telegram-index',FALSE,NOW())
                        ON CONFLICT (user_id, telegram_peer_id, telegram_topic_id)
                        DO UPDATE SET
                            parent_id = EXCLUDED.parent_id,
                            name = EXCLUDED.name,
                            deleted_at = NULL,
                            last_indexed_at = NOW(),
                            -- TCLOUD_TOPIC_REVISION_STABLE_800
                            updated_at = CASE
                                WHEN telegram_index_folders.parent_id IS DISTINCT FROM EXCLUDED.parent_id
                                  OR telegram_index_folders.name IS DISTINCT FROM EXCLUDED.name
                                  OR telegram_index_folders.deleted_at IS NOT NULL
                                THEN NOW()
                                ELSE telegram_index_folders.updated_at
                            END
                        RETURNING id
                        "#,
                )
                .bind(Uuid::new_v4())
                .bind(local_user_uuid())
                .bind(root_folder_id)
                .bind(peer_id)
                .bind(topic.id)
                .bind(topic.title)
                .fetch_one(pool)
                .await
                .map_err(|error| error.to_string())?;

                topic_folder_ids.insert(topic.id, folder_id);
            }
        }

        let cursor_row = sqlx_core::query::query::<Postgres>(
            r#"
                SELECT
                    last_message_id,
                    structure_version
                FROM telegram_index_cursors
                WHERE user_id = $1
                  AND telegram_peer_id = $2
                "#,
        )
        .bind(local_user_uuid())
        .bind(peer_id)
        .fetch_optional(pool)
        .await
        .map_err(|error| error.to_string())?;

        let stored_cursor = cursor_row
            .as_ref()
            .and_then(|row| row.try_get::<i64, _>("last_message_id").ok())
            .unwrap_or(0);

        let structure_version = cursor_row
            .as_ref()
            .and_then(|row| row.try_get::<i32, _>("structure_version").ok())
            .unwrap_or(1);

        let rebuild_topics = is_forum && structure_version < 2;

        let cursor = if mode == "full" || rebuild_topics {
            0_i64
        } else {
            stored_cursor
        };

        let mut highest_message_id = stored_cursor;
        let mut messages = telegram.client.iter_messages(&peer).limit(max_messages);

        loop {
            let message = match messages.next().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(error) => return Err(error.to_string()),
            };

            let message_id = i64::from(message.id());

            if mode != "full" && !rebuild_topics && cursor > 0 && message_id <= cursor {
                break;
            }

            counters.messages_seen += 1;
            highest_message_id = highest_message_id.max(message_id);

            let Some(media) = message.media() else {
                continue;
            };

            let topic_id = if is_forum {
                message_topic_id(&message.raw)
            } else {
                0
            };

            let parent_id = topic_folder_ids
                .get(&topic_id)
                .copied()
                .unwrap_or(root_folder_id);

            let (name, kind, size_bytes, mime) = match media {
                Media::Document(document) => {
                    let document_name = document.name().trim().to_string();

                    let caption = message.text().trim();

                    let original_ext = std::path::Path::new(&document_name)
                        .extension()
                        .and_then(|value| value.to_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string);

                    let mut display_name = if caption.is_empty() {
                        document_name.clone()
                    } else {
                        caption.to_string()
                    };

                    let mut mime = document
                        .mime_type()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or("application/octet-stream")
                        .to_string();

                    let telegram_video =
                        document.duration().is_some() && document.resolution().is_some();

                    if mime == "application/octet-stream" && telegram_video {
                        mime = "video/mp4".to_string();
                    }

                    if display_name.trim().is_empty() {
                        display_name = fallback_file_name(message_id, &mime, telegram_video);
                    } else if std::path::Path::new(&display_name).extension().is_none() {
                        if let Some(ext) = original_ext.as_deref() {
                            display_name.push('.');
                            display_name.push_str(ext);
                        }
                    }

                    let kind = classify_file_kind(&display_name, &mime);

                    (display_name, kind, document.size() as i64, mime)
                }
                Media::Photo(_) => (
                    format!("imagem_{message_id}.jpg"),
                    "image".to_string(),
                    0_i64,
                    "image/jpeg".to_string(),
                ),
                _ => continue,
            };

            sqlx_core::query::query::<Postgres>(
                r#"
                INSERT INTO telegram_index_files (
                    id,
                    user_id,
                    parent_id,
                    telegram_peer_id,
                    telegram_topic_id,
                    telegram_message_id,
                    name,
                    kind,
                    size_bytes,
                    mime,
                    sync_state,
                    source,
                    message_date
                )
                VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9,
                    $10,
                    'online',
                    'telegram',
                    $11
                )
                ON CONFLICT (
                    user_id,
                    telegram_peer_id,
                    telegram_message_id
                )
                DO UPDATE SET
                    parent_id =
                        CASE
                            WHEN telegram_index_files.manual_parent_override
                                THEN telegram_index_files.parent_id
                            WHEN EXCLUDED.telegram_topic_id <> 0
                                THEN EXCLUDED.parent_id
                            WHEN telegram_index_files.telegram_topic_id <> 0
                                THEN telegram_index_files.parent_id
                            ELSE EXCLUDED.parent_id
                        END,
                    telegram_topic_id =
                        CASE
                            WHEN telegram_index_files.manual_parent_override
                                THEN telegram_index_files.telegram_topic_id
                            WHEN EXCLUDED.telegram_topic_id <> 0
                                THEN EXCLUDED.telegram_topic_id
                            ELSE telegram_index_files.telegram_topic_id
                        END,
                    name = EXCLUDED.name,
                    kind = EXCLUDED.kind,
                    size_bytes = EXCLUDED.size_bytes,
                    mime = EXCLUDED.mime,
                    sync_state =
                        CASE
                            WHEN telegram_index_files.manual_trash
                                THEN 'trash'
                            ELSE 'online'
                        END,
                    source =
                        CASE
                            WHEN telegram_index_files.manual_parent_override
                                THEN telegram_index_files.source
                            ELSE 'telegram'
                        END,
                    message_date = EXCLUDED.message_date,
                    deleted_at =
                        CASE
                            WHEN telegram_index_files.manual_trash
                                THEN telegram_index_files.deleted_at
                            ELSE NULL
                        END,
                    updated_at = NOW()
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(local_user_uuid())
            .bind(parent_id)
            .bind(peer_id)
            .bind(topic_id)
            .bind(message_id)
            .bind(name)
            .bind(kind)
            .bind(size_bytes.max(0))
            .bind(mime)
            .bind(message.date())
            .execute(pool)
            .await
            .map_err(|error| error.to_string())?;

            counters.files_upserted += 1;
        }

        if is_forum && !topic_folder_ids.is_empty() && (mode == "full" || rebuild_topics) {
            let _assigned_by_search = reconcile_forum_topics_by_search(
                &telegram.client,
                pool,
                &peer,
                peer_id,
                &topic_folder_ids,
            )
            .await?;
        }
        if highest_message_id > 0 {
            sqlx_core::query::query::<Postgres>(
                r#"
                INSERT INTO telegram_index_cursors (
                    user_id,
                    telegram_peer_id,
                    last_message_id,
                    structure_version,
                    last_indexed_at
                )
                VALUES ($1, $2, $3, 2, NOW())
                ON CONFLICT (user_id, telegram_peer_id)
                DO UPDATE SET
                    last_message_id =
                        GREATEST(
                            telegram_index_cursors.last_message_id,
                            EXCLUDED.last_message_id
                        ),
                    structure_version =
                        GREATEST(
                            telegram_index_cursors.structure_version,
                            EXCLUDED.structure_version
                        ),
                    last_indexed_at = NOW()
                "#,
            )
            .bind(local_user_uuid())
            .bind(peer_id)
            .bind(highest_message_id)
            .execute(pool)
            .await
            .map_err(|error| error.to_string())?;

            let _ = sqlx_core::query::query::<Postgres>(
                r#"
                UPDATE telegram_index_folders
                SET
                    last_message_id = $3,
                    last_indexed_at = NOW(),
                    updated_at = NOW()
                WHERE user_id = $1
                  AND telegram_peer_id = $2
                  AND telegram_topic_id = 0
                "#,
            )
            .bind(local_user_uuid())
            .bind(peer_id)
            .bind(highest_message_id)
            .execute(pool)
            .await;
        }
    }

    Ok(counters)
}

fn fallback_file_name(message_id: i64, mime: &str, telegram_video: bool) -> String {
    let ext = match mime.to_ascii_lowercase().as_str() {
        "application/pdf" => Some("pdf"),
        "image/jpeg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/webp" => Some("webp"),
        "image/gif" => Some("gif"),
        "video/mp4" => Some("mp4"),
        "video/webm" => Some("webm"),
        "video/x-matroska" => Some("mkv"),
        "video/quicktime" => Some("mov"),
        "audio/mpeg" => Some("mp3"),
        "audio/wav" | "audio/x-wav" => Some("wav"),
        "audio/flac" => Some("flac"),
        _ => None,
    };

    if let Some(ext) = ext {
        if telegram_video {
            format!("video_{message_id}.{ext}")
        } else {
            format!("arquivo_{message_id}.{ext}")
        }
    } else if telegram_video {
        format!("video_{message_id}.mp4")
    } else {
        format!("arquivo_{message_id}")
    }
}

fn classify_file_kind(name: &str, mime: &str) -> String {
    let normalized_mime = mime.to_ascii_lowercase();

    if normalized_mime.starts_with("image/") {
        return "image".to_string();
    }

    if normalized_mime.starts_with("video/") {
        return "video".to_string();
    }

    if normalized_mime.starts_with("audio/") {
        return "audio".to_string();
    }

    if normalized_mime == "application/pdf" || name.to_ascii_lowercase().ends_with(".pdf") {
        return "pdf".to_string();
    }

    "file".to_string()
}

async fn live_revision(State(state): State<AppState>) -> Json<LiveRevision> {
    let Some(pool) = &state.db else {
        return Json(LiveRevision {
            revision: "offline:0:0".to_string(),
            changed_at_ms: 0,
            files_count: 0,
            folders_count: 0,
        });
    };

    let row = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            GREATEST(
                COALESCE(
                    (
                        SELECT
                            (EXTRACT(EPOCH FROM MAX(updated_at)) * 1000)::BIGINT
                        FROM telegram_index_files
                    ),
                    0
                ),
                COALESCE(
                    (
                        SELECT
                            (EXTRACT(EPOCH FROM MAX(updated_at)) * 1000)::BIGINT
                        FROM telegram_index_folders
                    ),
                    0
                )
            ) AS changed_at_ms,
            (
                SELECT COUNT(*)::BIGINT
                FROM telegram_index_files
                WHERE deleted_at IS NULL
            ) AS files_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM telegram_index_folders
                WHERE deleted_at IS NULL
            ) AS folders_count
        "#,
    )
    .fetch_one(pool)
    .await;

    match row {
        Ok(row) => {
            let changed_at_ms: i64 = row.try_get("changed_at_ms").unwrap_or_default();
            let files_count: i64 = row.try_get("files_count").unwrap_or_default();
            let folders_count: i64 = row.try_get("folders_count").unwrap_or_default();

            Json(LiveRevision {
                revision: format!("{}:{}:{}", changed_at_ms, files_count, folders_count),
                changed_at_ms,
                files_count,
                folders_count,
            })
        }
        Err(_) => Json(LiveRevision {
            revision: "error:0:0".to_string(),
            changed_at_ms: 0,
            files_count: 0,
            folders_count: 0,
        }),
    }
}

async fn index_status(State(state): State<AppState>) -> Json<IndexStatus> {
    let authorized = if let Some(telegram) = &state.telegram {
        telegram_authorized(telegram).await
    } else {
        false
    };

    let mut last_run_id = None;
    let mut last_status = None;
    let mut dialogs_seen = 0_i32;
    let mut topics_seen = 0_i32;
    let mut messages_seen = 0_i64;
    let mut files_upserted = 0_i64;

    if let Some(pool) = &state.db {
        if let Ok(Some(row)) = sqlx_core::query::query::<Postgres>(
            r#"
                SELECT
                    id::text AS id,
                    status,
                    dialogs_seen,
                    topics_seen,
                    messages_seen,
                    files_upserted
                FROM index_runs
                WHERE provider = 'telegram'
                ORDER BY created_at DESC
                LIMIT 1
                "#,
        )
        .fetch_optional(pool)
        .await
        {
            last_run_id = row.try_get("id").ok();
            last_status = row.try_get("status").ok();
            dialogs_seen = row.try_get("dialogs_seen").unwrap_or_default();
            topics_seen = row.try_get("topics_seen").unwrap_or_default();
            messages_seen = row.try_get("messages_seen").unwrap_or_default();
            files_upserted = row.try_get("files_upserted").unwrap_or_default();
        }
    }

    Json(IndexStatus {
        configured: state.telegram.is_some(),
        authorized,
        database_required: true,
        database_connected: state.db.is_some(),
        queue_ready: state.db.is_some() && authorized,
        last_run_id,
        last_status,
        dialogs_seen,
        topics_seen,
        messages_seen,
        files_upserted,
    })
}

async fn list_dialogs(State(state): State<AppState>) -> Json<Vec<TelegramDialogItem>> {
    let Some(pool) = &state.db else {
        return Json(Vec::new());
    };

    let rows = match sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            id::text AS id,
            telegram_peer_id,
            name,
            kind,
            last_indexed_at
        FROM telegram_dialogs
        WHERE user_id = $1
        ORDER BY name
        LIMIT 1000
        "#,
    )
    .bind(local_user_uuid())
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Json(Vec::new()),
    };

    let items = rows
        .into_iter()
        .map(|row| {
            let last_indexed = row
                .try_get::<Option<DateTime<Utc>>, _>("last_indexed_at")
                .ok()
                .flatten()
                .map(|value| value.to_rfc3339());

            TelegramDialogItem {
                id: row.try_get("id").unwrap_or_default(),
                telegram_peer_id: row.try_get("telegram_peer_id").unwrap_or_default(),
                name: row.try_get("name").unwrap_or_default(),
                kind: row.try_get("kind").unwrap_or_default(),
                last_indexed_at: last_indexed,
            }
        })
        .collect();

    Json(items)
}

async fn count_query(pool: &PgPool, sql: &'static str) -> i64 {
    sqlx_core::query_scalar::query_scalar::<Postgres, i64>(sql)
        .fetch_one(pool)
        .await
        .unwrap_or_default()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ForumSummary {
    id: String,
    name: String,
    source: String,
    deletable: bool,
}

async fn list_forums(State(state): State<AppState>) -> Json<Vec<ForumSummary>> {
    let Some(pool) = &state.db else {
        return Json(Vec::new());
    };

    let rows = match sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            f.id::text AS id,
            f.name,
            COALESCE(c.source, 'telegram') AS source
        FROM telegram_index_folders f
        LEFT JOIN telegram_peer_credentials c
          ON c.user_id = f.user_id
         AND c.telegram_peer_id = f.telegram_peer_id
        WHERE f.user_id = $1
          AND f.parent_id IS NULL
          AND f.telegram_topic_id = 0
          AND f.is_forum = TRUE
          AND f.deleted_at IS NULL
        ORDER BY lower(f.name), f.name
        "#,
    )
    .bind(local_user_uuid())
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Json(Vec::new()),
    };

    Json(
        rows.into_iter()
            .map(|row| {
                let name = row.try_get::<String, _>("name").unwrap_or_default();
                let source = row
                    .try_get::<String, _>("source")
                    .unwrap_or_else(|_| "telegram".to_string());
                let deletable =
                    source == "web-forum" && !name.trim().eq_ignore_ascii_case("Meus Arquivos");

                ForumSummary {
                    id: row.try_get("id").unwrap_or_default(),
                    name,
                    source,
                    deletable,
                }
            })
            .collect(),
    )
}

async fn create_forum(
    State(state): State<AppState>,
    Json(request): Json<NameMutationRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let clean_name = clean_mutation_name(&request.name)
        .map_err(|message| mutation_error(StatusCode::BAD_REQUEST, message))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let Some(telegram) = &state.telegram else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram não está conectado.",
        ));
    };

    if !telegram_authorized(telegram).await {
        return Err(mutation_error(
            StatusCode::UNAUTHORIZED,
            "A sessão Telegram não está autorizada.",
        ));
    }

    let duplicate = sqlx_core::query_scalar::query_scalar::<Postgres, i64>(
        r#"
        SELECT COUNT(*)
        FROM telegram_index_folders
        WHERE user_id = $1
          AND parent_id IS NULL
          AND telegram_topic_id = 0
          AND lower(name) = lower($2)
          AND deleted_at IS NULL
        "#,
    )
    .bind(local_user_uuid())
    .bind(&clean_name)
    .fetch_one(pool)
    .await
    .unwrap_or_default();

    if duplicate > 0 {
        return Err(mutation_error(
            StatusCode::CONFLICT,
            "Já existe um fórum com esse nome.",
        ));
    }

    let created = telegram
        .client
        .invoke(&tl::functions::channels::CreateChannel {
            broadcast: false,
            megagroup: true,
            for_import: false,
            forum: true,
            title: clean_name.clone(),
            about: "TCloud".to_string(),
            geo_point: None,
            address: None,
            ttl_period: None,
        })
        .await
        .map_err(|error| {
            mutation_error(
                StatusCode::BAD_GATEWAY,
                format!("Telegram não criou o fórum: {error}"),
            )
        })?;

    let read_created_channel = |chats: &[tl::enums::Chat]| -> Option<(i64, i64, i64)> {
        chats.iter().find_map(|chat| {
            let tl::enums::Chat::Channel(channel) = chat else {
                return None;
            };

            if !channel.title.trim().eq_ignore_ascii_case(&clean_name) {
                return None;
            }

            let access_hash = channel.access_hash.unwrap_or(0);
            if access_hash == 0 {
                return None;
            }

            let channel_id = channel.id;
            let peer_id = -1_000_000_000_000_i64 - channel_id.abs();

            Some((peer_id, channel_id, access_hash))
        })
    };

    let mut found = match &created {
        tl::enums::Updates::Updates(value) => read_created_channel(&value.chats),
        tl::enums::Updates::Combined(value) => read_created_channel(&value.chats),
        _ => None,
    };

    if found.is_none() {
        tokio::time::sleep(Duration::from_millis(350)).await;

        for _attempt in 0..30 {
            let mut dialogs = telegram.client.iter_dialogs();

            loop {
                let dialog = match dialogs.next().await {
                    Ok(Some(dialog)) => dialog,
                    Ok(None) => break,
                    Err(error) => {
                        return Err(mutation_error(StatusCode::BAD_GATEWAY, error.to_string()));
                    }
                };

                let peer = dialog.peer();

                if !peer
                    .name()
                    .unwrap_or("")
                    .trim()
                    .eq_ignore_ascii_case(&clean_name)
                {
                    continue;
                }

                if let grammers_client::types::Peer::Channel(channel) = peer {
                    let access_hash = channel.raw.access_hash.unwrap_or(0);

                    if access_hash != 0 {
                        found = Some((peer.id().bot_api_dialog_id(), channel.raw.id, access_hash));
                        break;
                    }
                }
            }

            if found.is_some() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(350)).await;
        }
    }

    let Some((peer_id, channel_id, access_hash)) = found else {
        return Err(mutation_error(
            StatusCode::BAD_GATEWAY,
            "O Telegram criou o fórum, mas o Core ainda não conseguiu obter sua credencial técnica.",
        ));
    };

    let credential_result = sqlx_core::query::query::<Postgres>(
        r#"
        INSERT INTO telegram_peer_credentials (
            user_id,
            telegram_peer_id,
            channel_id,
            access_hash,
            name,
            source,
            updated_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            'web-forum',
            NOW()
        )
        ON CONFLICT (user_id, telegram_peer_id)
        DO UPDATE SET
            channel_id = EXCLUDED.channel_id,
            access_hash = EXCLUDED.access_hash,
            name = EXCLUDED.name,
            source = 'web-forum',
            updated_at = NOW()
        "#,
    )
    .bind(local_user_uuid())
    .bind(peer_id)
    .bind(channel_id)
    .bind(access_hash)
    .bind(&clean_name)
    .execute(pool)
    .await;

    if let Err(error) = credential_result {
        let _ = telegram
            .client
            .invoke(&tl::functions::channels::DeleteChannel {
                channel: tl::types::InputChannel {
                    channel_id,
                    access_hash,
                }
                .into(),
            })
            .await;

        return Err(mutation_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "A credencial técnica não pôde ser salva e o fórum temporário foi revertido: {error}"
            ),
        ));
    }

    let folder_result = sqlx_core::query_scalar::query_scalar::<Postgres, Uuid>(
        r#"
            INSERT INTO telegram_index_folders (
                id,
                user_id,
                parent_id,
                telegram_peer_id,
                telegram_topic_id,
                name,
                kind,
                source,
                is_forum,
                last_indexed_at
            )
            VALUES (
                $1,
                $2,
                NULL,
                $3,
                0,
                $4,
                'folder',
                'web-forum',
                TRUE,
                NOW()
            )
            ON CONFLICT (
                user_id,
                telegram_peer_id,
                telegram_topic_id
            )
            DO UPDATE SET
                parent_id = NULL,
                name = EXCLUDED.name,
                source = 'web-forum',
                is_forum = TRUE,
                deleted_at = NULL,
                updated_at = NOW()
            RETURNING id
            "#,
    )
    .bind(Uuid::new_v4())
    .bind(local_user_uuid())
    .bind(peer_id)
    .bind(&clean_name)
    .fetch_one(pool)
    .await;

    let folder_id = match folder_result {
        Ok(value) => value,
        Err(error) => {
            let _ = sqlx_core::query::query::<Postgres>(
                r#"
                DELETE FROM telegram_peer_credentials
                WHERE user_id = $1
                  AND telegram_peer_id = $2
                "#,
            )
            .bind(local_user_uuid())
            .bind(peer_id)
            .execute(pool)
            .await;

            let _ = telegram
                .client
                .invoke(&tl::functions::channels::DeleteChannel {
                    channel: tl::types::InputChannel {
                        channel_id,
                        access_hash,
                    }
                    .into(),
                })
                .await;

            return Err(mutation_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Fórum criado no Telegram, mas não foi registrado no TCloud: {error}"),
            ));
        }
    };

    Ok(mutation_ok(
        "Fórum criado e conectado ao TCloud.",
        Some(folder_id.to_string()),
        None,
    ))
}

async fn rename_forum(
    State(state): State<AppState>,
    Json(request): Json<FileNameMutationRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let forum_uuid = Uuid::parse_str(&request.id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Fórum inválido."))?;

    let clean_name = clean_mutation_name(&request.name)
        .map_err(|message| mutation_error(StatusCode::BAD_REQUEST, message))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let Some(telegram) = &state.telegram else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram não está conectado.",
        ));
    };

    let row = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            telegram_peer_id,
            name,
            is_forum
        FROM telegram_index_folders
        WHERE id = $1
          AND user_id = $2
          AND parent_id IS NULL
          AND telegram_topic_id = 0
          AND deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(forum_uuid)
    .bind(local_user_uuid())
    .fetch_optional(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let Some(row) = row else {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Fórum não encontrado.",
        ));
    };

    if !row.try_get::<bool, _>("is_forum").unwrap_or(false) {
        return Err(mutation_error(
            StatusCode::BAD_REQUEST,
            "O item selecionado não é um fórum.",
        ));
    }

    let peer_id = row
        .try_get::<i64, _>("telegram_peer_id")
        .unwrap_or_default();

    let duplicate = sqlx_core::query_scalar::query_scalar::<Postgres, i64>(
        r#"
        SELECT COUNT(*)
        FROM telegram_index_folders
        WHERE user_id = $1
          AND parent_id IS NULL
          AND telegram_topic_id = 0
          AND id <> $2
          AND lower(name) = lower($3)
          AND deleted_at IS NULL
        "#,
    )
    .bind(local_user_uuid())
    .bind(forum_uuid)
    .bind(&clean_name)
    .fetch_one(pool)
    .await
    .unwrap_or_default();

    if duplicate > 0 {
        return Err(mutation_error(
            StatusCode::CONFLICT,
            "Já existe um fórum com esse nome.",
        ));
    }

    let input_channel = mutation_input_channel(&telegram.client, pool, peer_id)
        .await
        .map_err(|message| mutation_error(StatusCode::BAD_GATEWAY, message))?;

    telegram
        .client
        .invoke(&tl::functions::channels::EditTitle {
            channel: input_channel,
            title: clean_name.clone(),
        })
        .await
        .map_err(|error| {
            mutation_error(
                StatusCode::BAD_GATEWAY,
                format!("Telegram não renomeou o fórum: {error}"),
            )
        })?;

    sqlx_core::query::query::<Postgres>(
        r#"
        UPDATE telegram_index_folders
        SET
            name = $3,
            updated_at = NOW()
        WHERE id = $1
          AND user_id = $2
        "#,
    )
    .bind(forum_uuid)
    .bind(local_user_uuid())
    .bind(&clean_name)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    sqlx_core::query::query::<Postgres>(
        r#"
        UPDATE telegram_peer_credentials
        SET
            name = $3,
            updated_at = NOW()
        WHERE user_id = $1
          AND telegram_peer_id = $2
        "#,
    )
    .bind(local_user_uuid())
    .bind(peer_id)
    .bind(&clean_name)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(mutation_ok(
        "Fórum renomeado.",
        Some(forum_uuid.to_string()),
        None,
    ))
}

async fn delete_forum_permanently(
    State(state): State<AppState>,
    Json(request): Json<FileIdMutationRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let forum_uuid = Uuid::parse_str(&request.id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Fórum inválido."))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let Some(telegram) = &state.telegram else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram não está conectado.",
        ));
    };

    let row = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            telegram_peer_id,
            name,
            is_forum
        FROM telegram_index_folders
        WHERE id = $1
          AND user_id = $2
          AND parent_id IS NULL
          AND telegram_topic_id = 0
          AND deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(forum_uuid)
    .bind(local_user_uuid())
    .fetch_optional(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let Some(row) = row else {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Fórum não encontrado.",
        ));
    };

    if !row.try_get::<bool, _>("is_forum").unwrap_or(false) {
        return Err(mutation_error(
            StatusCode::BAD_REQUEST,
            "O item selecionado não é um fórum.",
        ));
    }

    let forum_name = row.try_get::<String, _>("name").unwrap_or_default();

    if forum_name.trim().eq_ignore_ascii_case("Meus Arquivos") {
        return Err(mutation_error(
            StatusCode::CONFLICT,
            "Meus Arquivos é o fórum principal e está protegido contra exclusão.",
        ));
    }

    let peer_id = row
        .try_get::<i64, _>("telegram_peer_id")
        .unwrap_or_default();

    let credential_source = sqlx_core::query_scalar::query_scalar::<Postgres, String>(
        r#"
            SELECT source
            FROM telegram_peer_credentials
            WHERE user_id = $1
              AND telegram_peer_id = $2
            LIMIT 1
            "#,
    )
    .bind(local_user_uuid())
    .bind(peer_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    .unwrap_or_default();

    if credential_source != "web-forum" {
        return Err(mutation_error(
            StatusCode::CONFLICT,
            "Por segurança, a exclusão Web é permitida somente para fóruns criados pelo TCloud.",
        ));
    }

    let any_files = sqlx_core::query_scalar::query_scalar::<Postgres, i64>(
        r#"
        SELECT COUNT(*)
        FROM telegram_index_files
        WHERE user_id = $1
          AND telegram_peer_id = $2
        "#,
    )
    .bind(local_user_uuid())
    .bind(peer_id)
    .fetch_one(pool)
    .await
    .unwrap_or_default();

    if any_files > 0 {
        return Err(mutation_error(
            StatusCode::CONFLICT,
            "Esvazie o fórum e a Lixeira dele antes da exclusão permanente.",
        ));
    }

    let input_channel = mutation_input_channel(&telegram.client, pool, peer_id)
        .await
        .map_err(|message| mutation_error(StatusCode::BAD_GATEWAY, message))?;

    telegram
        .client
        .invoke(&tl::functions::channels::DeleteChannel {
            channel: input_channel,
        })
        .await
        .map_err(|error| {
            mutation_error(
                StatusCode::BAD_GATEWAY,
                format!("Telegram não excluiu o fórum: {error}"),
            )
        })?;

    sqlx_core::query::query::<Postgres>(
        r#"
        DELETE FROM telegram_index_folders
        WHERE user_id = $1
          AND telegram_peer_id = $2
          AND parent_id IS NOT NULL
        "#,
    )
    .bind(local_user_uuid())
    .bind(peer_id)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    sqlx_core::query::query::<Postgres>(
        r#"
        DELETE FROM telegram_index_folders
        WHERE id = $1
          AND user_id = $2
        "#,
    )
    .bind(forum_uuid)
    .bind(local_user_uuid())
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    sqlx_core::query::query::<Postgres>(
        r#"
        DELETE FROM telegram_peer_credentials
        WHERE user_id = $1
          AND telegram_peer_id = $2
        "#,
    )
    .bind(local_user_uuid())
    .bind(peer_id)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let _ = sqlx_core::query::query::<Postgres>(
        r#"
        DELETE FROM telegram_dialogs
        WHERE user_id = $1
          AND telegram_peer_id = $2
        "#,
    )
    .bind(local_user_uuid())
    .bind(peer_id)
    .execute(pool)
    .await;

    Ok(mutation_ok(
        "Fórum excluído permanentemente.",
        Some(forum_uuid.to_string()),
        None,
    ))
}

async fn create_folder(
    State(state): State<AppState>,
    Json(request): Json<CreateFolderRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let clean_name = clean_mutation_name(&request.name)
        .map_err(|message| mutation_error(StatusCode::BAD_REQUEST, message))?;

    let parent_uuid = Uuid::parse_str(&request.parent_id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Pasta principal inválida."))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let Some(telegram) = &state.telegram else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram não está conectado.",
        ));
    };

    let parent_row = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            telegram_peer_id,
            name,
            is_forum
        FROM telegram_index_folders
        WHERE id = $1
          AND parent_id IS NULL
          AND telegram_topic_id = 0
          AND deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(parent_uuid)
    .fetch_optional(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let Some(parent_row) = parent_row else {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "A pasta principal não foi encontrada.",
        ));
    };

    let is_forum = parent_row.try_get::<bool, _>("is_forum").unwrap_or(false);

    if !is_forum {
        return Err(mutation_error(
            StatusCode::BAD_REQUEST,
            "Nova pasta é criada dentro de um fórum do TCloud.",
        ));
    }

    let peer_id = parent_row
        .try_get::<i64, _>("telegram_peer_id")
        .unwrap_or_default();

    let input_peer = mutation_input_peer(&telegram.client, pool, peer_id)
        .await
        .map_err(|message| mutation_error(StatusCode::BAD_GATEWAY, message))?;

    let random_id = Uuid::new_v4().as_u128() as u64 as i64;

    telegram
        .client
        .invoke(&tl::functions::messages::CreateForumTopic {
            title_missing: false,
            peer: input_peer.clone(),
            title: clean_name.clone(),
            icon_color: Some(0x6FB9F0),
            icon_emoji_id: None,
            random_id,
            send_as: None,
        })
        .await
        .map_err(|error| {
            mutation_error(
                StatusCode::BAD_GATEWAY,
                format!("Telegram não criou a pasta: {error}"),
            )
        })?;

    tokio::time::sleep(Duration::from_millis(350)).await;

    let mut topic_id = None::<i32>;

    for _attempt in 0..8 {
        let mut messages = telegram.client.iter_messages(input_peer.clone()).limit(200);

        loop {
            let message = match messages.next().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(error) => {
                    return Err(mutation_error(StatusCode::BAD_GATEWAY, error.to_string()));
                }
            };

            if let Some(tl::enums::MessageAction::TopicCreate(action)) = message.action() {
                if action.title.trim().eq_ignore_ascii_case(&clean_name) {
                    topic_id = Some(message.id());
                    break;
                }
            }
        }

        if topic_id.is_some() {
            break;
        }

        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    let Some(topic_id) = topic_id else {
        return Err(mutation_error(
            StatusCode::BAD_GATEWAY,
            "A pasta foi criada no Telegram, mas o identificador do tópico ainda não apareceu.",
        ));
    };

    let folder_id = sqlx_core::query_scalar::query_scalar::<Postgres, Uuid>(
        r#"
            INSERT INTO telegram_index_folders (
                id,
                user_id,
                parent_id,
                telegram_peer_id,
                telegram_topic_id,
                name,
                kind,
                source,
                is_forum,
                last_indexed_at
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                'folder',
                'web-topic',
                FALSE,
                NOW()
            )
            ON CONFLICT (
                user_id,
                telegram_peer_id,
                telegram_topic_id
            )
            DO UPDATE SET
                parent_id = EXCLUDED.parent_id,
                name = EXCLUDED.name,
                source = 'web-topic',
                deleted_at = NULL,
                updated_at = NOW()
            RETURNING id
            "#,
    )
    .bind(Uuid::new_v4())
    .bind(local_user_uuid())
    .bind(parent_uuid)
    .bind(peer_id)
    .bind(i64::from(topic_id))
    .bind(&clean_name)
    .fetch_one(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(mutation_ok(
        "Pasta criada.",
        Some(folder_id.to_string()),
        Some(parent_uuid.to_string()),
    ))
}

async fn upload_file(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    const MAX_WEB_UPLOAD: usize = 512 * 1024 * 1024;

    if body.is_empty() {
        return Err(mutation_error(
            StatusCode::BAD_REQUEST,
            "O arquivo está vazio.",
        ));
    }

    if body.len() > MAX_WEB_UPLOAD {
        return Err(mutation_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Nesta etapa, o envio Web aceita até 512 MB por arquivo.",
        ));
    }

    let parent_text = headers
        .get("x-tcloud-parent-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .trim();

    let encoded_name = headers
        .get("x-tcloud-file-name")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .trim();

    let file_name = clean_mutation_name(&percent_decode_header(encoded_name))
        .map_err(|message| mutation_error(StatusCode::BAD_REQUEST, message))?;

    let parent_uuid = Uuid::parse_str(parent_text)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Destino do upload inválido."))?;

    let mime = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("application/octet-stream")
        .to_string();

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let Some(telegram) = &state.telegram else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram não está conectado.",
        ));
    };

    let destination = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            telegram_peer_id,
            telegram_topic_id
        FROM telegram_index_folders
        WHERE id = $1
          AND deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(parent_uuid)
    .fetch_optional(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let Some(destination) = destination else {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Destino do upload não encontrado.",
        ));
    };

    let peer_id = destination
        .try_get::<i64, _>("telegram_peer_id")
        .unwrap_or_default();

    let topic_id = destination
        .try_get::<i64, _>("telegram_topic_id")
        .unwrap_or_default();

    let input_peer = mutation_input_peer(&telegram.client, pool, peer_id)
        .await
        .map_err(|message| mutation_error(StatusCode::BAD_GATEWAY, message))?;

    let mut cursor = Cursor::new(body.to_vec());

    let uploaded = telegram
        .client
        .upload_stream(&mut cursor, body.len(), file_name.clone())
        .await
        .map_err(|error| {
            mutation_error(
                StatusCode::BAD_GATEWAY,
                format!("Falha no upload Telegram: {error}"),
            )
        })?;

    let mut outgoing = InputMessage::new()
        .text(&file_name)
        .mime_type(&mime)
        .file(uploaded);

    if topic_id > 0 {
        let topic_i32 = i32::try_from(topic_id).map_err(|_| {
            mutation_error(StatusCode::BAD_REQUEST, "Identificador do tópico inválido.")
        })?;

        outgoing = outgoing.reply_to(Some(topic_i32));
    }

    let sent = telegram
        .client
        .send_message(input_peer, outgoing)
        .await
        .map_err(|error| {
            mutation_error(
                StatusCode::BAD_GATEWAY,
                format!("Falha ao publicar no Telegram: {error}"),
            )
        })?;

    let kind = classify_file_kind(&file_name, &mime);
    let file_id = Uuid::new_v4();

    let saved_id = sqlx_core::query_scalar::query_scalar::<Postgres, Uuid>(
        r#"
            INSERT INTO telegram_index_files (
                id,
                user_id,
                parent_id,
                telegram_peer_id,
                telegram_topic_id,
                telegram_message_id,
                name,
                kind,
                size_bytes,
                mime,
                sync_state,
                source,
                message_date,
                manual_parent_override,
                manual_trash
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                'online',
                'web-upload',
                $11,
                FALSE,
                FALSE
            )
            ON CONFLICT (
                user_id,
                telegram_peer_id,
                telegram_message_id
            )
            DO UPDATE SET
                parent_id = EXCLUDED.parent_id,
                telegram_topic_id =
                    EXCLUDED.telegram_topic_id,
                name = EXCLUDED.name,
                kind = EXCLUDED.kind,
                size_bytes = EXCLUDED.size_bytes,
                mime = EXCLUDED.mime,
                sync_state = 'online',
                source = 'web-upload',
                message_date = EXCLUDED.message_date,
                manual_parent_override = FALSE,
                manual_trash = FALSE,
                deleted_at = NULL,
                trashed_at = NULL,
                updated_at = NOW()
            RETURNING id
            "#,
    )
    .bind(file_id)
    .bind(local_user_uuid())
    .bind(parent_uuid)
    .bind(peer_id)
    .bind(topic_id)
    .bind(i64::from(sent.id()))
    .bind(&file_name)
    .bind(kind)
    .bind(body.len() as i64)
    .bind(&mime)
    .bind(sent.date())
    .fetch_one(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(mutation_ok(
        "Arquivo enviado.",
        Some(saved_id.to_string()),
        Some(parent_uuid.to_string()),
    ))
}

async fn rename_file(
    State(state): State<AppState>,
    Json(request): Json<FileNameMutationRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let file_uuid = Uuid::parse_str(&request.id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Arquivo inválido."))?;

    let clean_name = clean_mutation_name(&request.name)
        .map_err(|message| mutation_error(StatusCode::BAD_REQUEST, message))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let Some(telegram) = &state.telegram else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram não está conectado.",
        ));
    };

    let row = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            telegram_peer_id,
            telegram_message_id
        FROM telegram_index_files
        WHERE id = $1
          AND deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(file_uuid)
    .fetch_optional(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let Some(row) = row else {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Arquivo não encontrado.",
        ));
    };

    let peer_id = row
        .try_get::<i64, _>("telegram_peer_id")
        .unwrap_or_default();

    let message_id = row
        .try_get::<i64, _>("telegram_message_id")
        .unwrap_or_default();

    let message_i32 = i32::try_from(message_id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Mensagem Telegram inválida."))?;

    let input_peer = mutation_input_peer(&telegram.client, pool, peer_id)
        .await
        .map_err(|message| mutation_error(StatusCode::BAD_GATEWAY, message))?;

    telegram
        .client
        .edit_message(
            input_peer,
            message_i32,
            InputMessage::new().text(&clean_name),
        )
        .await
        .map_err(|error| {
            mutation_error(
                StatusCode::BAD_GATEWAY,
                format!("Telegram não renomeou o arquivo: {error}"),
            )
        })?;

    sqlx_core::query::query::<Postgres>(
        r#"
        UPDATE telegram_index_files
        SET
            name = $2,
            source = 'web-rename',
            updated_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(file_uuid)
    .bind(&clean_name)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(mutation_ok(
        "Arquivo renomeado.",
        Some(file_uuid.to_string()),
        None,
    ))
}

async fn move_file(
    State(state): State<AppState>,
    Json(request): Json<FileParentMutationRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let file_uuid = Uuid::parse_str(&request.id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Arquivo inválido."))?;

    let parent_uuid = Uuid::parse_str(&request.parent_id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Destino inválido."))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let target_topic = sqlx_core::query_scalar::query_scalar::<Postgres, i64>(
        r#"
            SELECT telegram_topic_id
            FROM telegram_index_folders
            WHERE id = $1
              AND deleted_at IS NULL
            LIMIT 1
            "#,
    )
    .bind(parent_uuid)
    .fetch_optional(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let Some(target_topic) = target_topic else {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Pasta de destino não encontrada.",
        ));
    };

    let result = sqlx_core::query::query::<Postgres>(
        r#"
        UPDATE telegram_index_files
        SET
            parent_id = $2,
            telegram_topic_id = $3,
            manual_parent_override = TRUE,
            source = 'web-move',
            updated_at = NOW()
        WHERE id = $1
          AND deleted_at IS NULL
        "#,
    )
    .bind(file_uuid)
    .bind(parent_uuid)
    .bind(target_topic)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    if result.rows_affected() == 0 {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Arquivo não encontrado.",
        ));
    }

    Ok(mutation_ok(
        "Arquivo movido no TCloud.",
        Some(file_uuid.to_string()),
        Some(parent_uuid.to_string()),
    ))
}

async fn trash_file(
    State(state): State<AppState>,
    Json(request): Json<FileIdMutationRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let file_uuid = Uuid::parse_str(&request.id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Arquivo inválido."))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let result = sqlx_core::query::query::<Postgres>(
        r#"
        UPDATE telegram_index_files
        SET
            original_parent_id = parent_id,
            manual_trash = TRUE,
            trashed_at = NOW(),
            deleted_at = NOW(),
            sync_state = 'trash',
            source = 'web-trash',
            updated_at = NOW()
        WHERE id = $1
          AND deleted_at IS NULL
        "#,
    )
    .bind(file_uuid)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    if result.rows_affected() == 0 {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Arquivo não encontrado.",
        ));
    }

    Ok(mutation_ok(
        "Arquivo enviado para a Lixeira.",
        Some(file_uuid.to_string()),
        None,
    ))
}

async fn restore_file(
    State(state): State<AppState>,
    Json(request): Json<FileIdMutationRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let file_uuid = Uuid::parse_str(&request.id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Arquivo inválido."))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let result = sqlx_core::query::query::<Postgres>(
        r#"
        UPDATE telegram_index_files
        SET
            parent_id =
                COALESCE(original_parent_id, parent_id),
            original_parent_id = NULL,
            manual_trash = FALSE,
            trashed_at = NULL,
            deleted_at = NULL,
            sync_state = 'online',
            source = 'web-restore',
            updated_at = NOW()
        WHERE id = $1
          AND manual_trash = TRUE
        "#,
    )
    .bind(file_uuid)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    if result.rows_affected() == 0 {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Arquivo não encontrado na Lixeira.",
        ));
    }

    Ok(mutation_ok(
        "Arquivo restaurado.",
        Some(file_uuid.to_string()),
        None,
    ))
}

async fn delete_file_permanently(
    State(state): State<AppState>,
    Json(request): Json<FileIdMutationRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let file_uuid = Uuid::parse_str(&request.id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Arquivo inválido."))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let Some(telegram) = &state.telegram else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram não está conectado.",
        ));
    };

    let row = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            telegram_peer_id,
            telegram_message_id
        FROM telegram_index_files
        WHERE id = $1
        LIMIT 1
        "#,
    )
    .bind(file_uuid)
    .fetch_optional(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let Some(row) = row else {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Arquivo não encontrado.",
        ));
    };

    let peer_id = row
        .try_get::<i64, _>("telegram_peer_id")
        .unwrap_or_default();

    let message_id = row
        .try_get::<i64, _>("telegram_message_id")
        .unwrap_or_default();

    let message_i32 = i32::try_from(message_id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Mensagem Telegram inválida."))?;

    let input_peer = mutation_input_peer(&telegram.client, pool, peer_id)
        .await
        .map_err(|message| mutation_error(StatusCode::BAD_GATEWAY, message))?;

    telegram
        .client
        .delete_messages(input_peer, &[message_i32])
        .await
        .map_err(|error| {
            mutation_error(
                StatusCode::BAD_GATEWAY,
                format!("Telegram não excluiu o arquivo: {error}"),
            )
        })?;

    sqlx_core::query::query::<Postgres>(
        r#"
        DELETE FROM telegram_index_files
        WHERE id = $1
        "#,
    )
    .bind(file_uuid)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(mutation_ok(
        "Arquivo excluído permanentemente.",
        Some(file_uuid.to_string()),
        None,
    ))
}

async fn delete_folder_permanently(
    State(state): State<AppState>,
    Json(request): Json<FileIdMutationRequest>,
) -> Result<Json<MutationResponse>, (StatusCode, Json<MutationResponse>)> {
    let folder_uuid = Uuid::parse_str(&request.id)
        .map_err(|_| mutation_error(StatusCode::BAD_REQUEST, "Pasta inválida."))?;

    let Some(pool) = &state.db else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PostgreSQL não está conectado.",
        ));
    };

    let Some(telegram) = &state.telegram else {
        return Err(mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram não está conectado.",
        ));
    };

    let row = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            parent_id,
            telegram_peer_id,
            telegram_topic_id
        FROM telegram_index_folders
        WHERE id = $1
          AND deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(folder_uuid)
    .fetch_optional(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let Some(row) = row else {
        return Err(mutation_error(
            StatusCode::NOT_FOUND,
            "Pasta não encontrada.",
        ));
    };

    let parent_id = row.try_get::<Option<Uuid>, _>("parent_id").unwrap_or(None);

    let peer_id = row
        .try_get::<i64, _>("telegram_peer_id")
        .unwrap_or_default();

    let topic_id = row
        .try_get::<i64, _>("telegram_topic_id")
        .unwrap_or_default();

    if parent_id.is_none() || topic_id <= 0 {
        return Err(mutation_error(
            StatusCode::BAD_REQUEST,
            "Somente uma pasta de tópico pode ser excluída por esta operação.",
        ));
    }

    let active_files = sqlx_core::query_scalar::query_scalar::<Postgres, i64>(
        r#"
            SELECT COUNT(*)
            FROM telegram_index_files
            WHERE parent_id = $1
              AND deleted_at IS NULL
            "#,
    )
    .bind(folder_uuid)
    .fetch_one(pool)
    .await
    .unwrap_or_default();

    if active_files > 0 {
        return Err(mutation_error(
            StatusCode::CONFLICT,
            "A pasta precisa estar vazia.",
        ));
    }

    let input_peer = mutation_input_peer(&telegram.client, pool, peer_id)
        .await
        .map_err(|message| mutation_error(StatusCode::BAD_GATEWAY, message))?;

    let topic_i32 = i32::try_from(topic_id).map_err(|_| {
        mutation_error(StatusCode::BAD_REQUEST, "Identificador de tópico inválido.")
    })?;

    telegram
        .client
        .invoke(&tl::functions::messages::DeleteTopicHistory {
            peer: input_peer,
            top_msg_id: topic_i32,
        })
        .await
        .map_err(|error| {
            mutation_error(
                StatusCode::BAD_GATEWAY,
                format!("Telegram não excluiu a pasta: {error}"),
            )
        })?;

    sqlx_core::query::query::<Postgres>(
        r#"
        DELETE FROM telegram_index_folders
        WHERE id = $1
        "#,
    )
    .bind(folder_uuid)
    .execute(pool)
    .await
    .map_err(|error| mutation_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(mutation_ok(
        "Pasta excluída.",
        Some(folder_uuid.to_string()),
        parent_id.map(|value| value.to_string()),
    ))
}

async fn list_trash(State(state): State<AppState>) -> Json<Vec<TCloudItem>> {
    let Some(pool) = &state.db else {
        return Json(Vec::new());
    };

    let rows = match sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            id::text AS id,
            original_parent_id::text AS parent_id,
            name,
            kind,
            size_bytes,
            mime,
            'trash'::text AS sync_state,
            updated_at,
            source
        FROM telegram_index_files
        WHERE manual_trash = TRUE
          AND deleted_at IS NOT NULL
        ORDER BY trashed_at DESC NULLS LAST, name
        LIMIT 50000
        "#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Json(Vec::new()),
    };

    Json(rows.into_iter().map(row_to_item).collect())
}

async fn list_files(State(state): State<AppState>) -> Json<Vec<TCloudItem>> {
    if let Some(pool) = &state.db {
        if let Ok(items) = database_files(pool).await {
            return Json(items);
        }
    }

    Json(state.fallback_files.as_ref().clone())
}

async fn get_file(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TCloudItem>, StatusCode> {
    if let Some(pool) = &state.db {
        if let Ok(Some(item)) = database_file(pool, &id).await {
            return Ok(Json(item));
        }
    }

    state
        .fallback_files
        .iter()
        .find(|item| item.id == id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn list_devices(State(state): State<AppState>) -> Json<Vec<DeviceSummary>> {
    let Some(pool) = &state.db else {
        return Json(Vec::new());
    };

    let rows = match sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            id::text AS id,
            name,
            platform,
            app_version,
            last_seen_at
        FROM devices
        ORDER BY last_seen_at DESC NULLS LAST, name
        LIMIT 100
        "#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Json(Vec::new()),
    };

    let devices = rows
        .into_iter()
        .map(|row| {
            let last_seen = row
                .try_get::<Option<DateTime<Utc>>, _>("last_seen_at")
                .ok()
                .flatten()
                .map(|value| value.to_rfc3339());

            DeviceSummary {
                id: row.try_get("id").unwrap_or_default(),
                name: row.try_get("name").unwrap_or_default(),
                platform: row.try_get("platform").unwrap_or_default(),
                app_version: row.try_get("app_version").ok(),
                last_seen_at: last_seen,
            }
        })
        .collect();

    Json(devices)
}

async fn database_files(pool: &PgPool) -> Result<Vec<TCloudItem>, sqlx_core::Error> {
    let rows = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            f.id::text AS id,
            f.parent_id::text AS parent_id,
            f.name,
            f.kind,
            f.size_bytes,
            f.mime,
            f.sync_state,
            f.updated_at,
            f.source,
            1::integer AS sort_order
        FROM telegram_index_files f
        WHERE f.deleted_at IS NULL

        UNION ALL

        SELECT
            d.id::text AS id,
            d.parent_id::text AS parent_id,
            d.name,
            'folder'::text AS kind,
            0::bigint AS size_bytes,
            'inode/directory'::text AS mime,
            'online'::text AS sync_state,
            d.updated_at,
            d.source,
            0::integer AS sort_order
        FROM telegram_index_folders d
        WHERE d.deleted_at IS NULL

        ORDER BY sort_order, name
        LIMIT 50000
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(row_to_item).collect())
}

async fn database_file(pool: &PgPool, id: &str) -> Result<Option<TCloudItem>, sqlx_core::Error> {
    let row = sqlx_core::query::query::<Postgres>(
        r#"
        SELECT
            id::text AS id,
            parent_id::text AS parent_id,
            name,
            kind,
            size_bytes,
            mime,
            sync_state,
            updated_at,
            source
        FROM telegram_index_files
        WHERE id::text = $1
          AND deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(row_to_item))
}

fn row_to_item(row: sqlx_postgres::PgRow) -> TCloudItem {
    let updated_at = row
        .try_get::<DateTime<Utc>, _>("updated_at")
        .map(|value| value.format("%d/%m/%Y %H:%M").to_string())
        .unwrap_or_else(|_| "Agora".to_string());

    let size = row
        .try_get::<i64, _>("size_bytes")
        .unwrap_or_default()
        .max(0) as u64;

    TCloudItem {
        id: row.try_get("id").unwrap_or_default(),
        parent_id: row.try_get("parent_id").ok(),
        name: row.try_get("name").unwrap_or_default(),
        kind: row.try_get("kind").unwrap_or_else(|_| "file".to_string()),
        size,
        mime: row
            .try_get("mime")
            .unwrap_or_else(|_| "application/octet-stream".to_string()),
        sync_state: row
            .try_get("sync_state")
            .unwrap_or_else(|_| "online".to_string()),
        modified_at: updated_at,
        source: row
            .try_get("source")
            .unwrap_or_else(|_| "database".to_string()),
    }
}

fn seed_fallback_files() -> Vec<TCloudItem> {
    Vec::new()
}

// TCLOUD_GLOBAL_IDENTITY_700
// Regra arquitetural:
// - telegram_index_files.id e telegram_index_folders.id sao IDs canonicos globais.
// - clientes nunca devem persistir IDs SQLite locais como identidade remota.
// - telegram_peer_id/topic_id/message_id sao identidade de transporte Telegram,
//   nao identidade primaria do objeto no cliente.

// TCLOUD_GLOBAL_IDENTITY_720
// File operations are addressed by telegram_index_files.id (canonical UUID).
// Clients should use that UUID for media/get/rename/move/trash/restore/delete.
// Legacy transport identity remains only as migration fallback.
