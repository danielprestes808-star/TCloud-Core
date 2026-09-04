use axum::{
    Json,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use constant_time_eq::constant_time_eq;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx_core::query::query;
use sqlx_postgres::PgPool;
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const TOKEN_ENV: &str = "TCLOUD_API_TOKEN";
const MIN_TOKEN_BYTES: usize = 32;
const MAX_BUCKETS: usize = 2_048;

#[derive(Clone)]
pub(crate) struct SecurityState {
    token: Option<Arc<[u8]>>,
    db: Option<PgPool>,
    buckets: Arc<Mutex<HashMap<String, RateBucket>>>,
}

struct RateBucket {
    started_at: Instant,
    requests: u32,
}

#[derive(Clone, Copy)]
struct RatePolicy {
    name: &'static str,
    limit: u32,
    window: Duration,
}

impl SecurityState {
    pub(crate) fn from_environment(
        cloud_runtime: bool,
        db: Option<PgPool>,
    ) -> Result<Self, String> {
        let token = env::var(TOKEN_ENV)
            .ok()
            .map(|value| value.trim().as_bytes().to_vec())
            .filter(|value| !value.is_empty());

        if let Some(value) = &token {
            if value.len() < MIN_TOKEN_BYTES {
                return Err(format!(
                    "{TOKEN_ENV} deve conter pelo menos {MIN_TOKEN_BYTES} bytes"
                ));
            }
        } else if cloud_runtime {
            return Err(format!(
                "{TOKEN_ENV} e obrigatorio quando o Core e executado na nuvem"
            ));
        }

        Ok(Self {
            token: token.map(Arc::<[u8]>::from),
            db,
            buckets: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn master_authorized(&self, authorization: Option<&str>) -> bool {
        let Some(expected) = &self.token else {
            return true;
        };
        let Some(received) = authorization.and_then(|value| value.strip_prefix("Bearer ")) else {
            return false;
        };
        let received = received.trim().as_bytes();
        received.len() == expected.len() && constant_time_eq(received, expected)
    }

    async fn authorized(&self, authorization: Option<&str>) -> bool {
        if self.master_authorized(authorization) {
            return true;
        }
        let Some(received) = authorization
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim)
            .filter(|value| value.starts_with("tcdev_") && value.len() >= 40)
        else {
            return false;
        };
        let Some(pool) = &self.db else {
            return false;
        };
        let token_hash = hex::encode(Sha256::digest(received.as_bytes()));
        let valid = query(
            r#"
            SELECT s.device_id
            FROM sessions s
            WHERE s.token_hash = $1
              AND s.revoked_at IS NULL
              AND s.expires_at > NOW()
            LIMIT 1
            "#,
        )
        .bind(token_hash)
        .fetch_optional(pool)
        .await;
        matches!(valid, Ok(Some(_)))
    }

    fn rate_limited(&self, client: &str, policy: RatePolicy) -> bool {
        let now = Instant::now();
        let key = format!("{}:{client}", policy.name);
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if buckets.len() >= MAX_BUCKETS && !buckets.contains_key(&key) {
            buckets.retain(|_, bucket| {
                now.duration_since(bucket.started_at) < Duration::from_secs(300)
            });
            if buckets.len() >= MAX_BUCKETS {
                return true;
            }
        }

        let bucket = buckets.entry(key).or_insert(RateBucket {
            started_at: now,
            requests: 0,
        });
        if now.duration_since(bucket.started_at) >= policy.window {
            bucket.started_at = now;
            bucket.requests = 0;
        }
        bucket.requests = bucket.requests.saturating_add(1);
        bucket.requests > policy.limit
    }
}

fn policy_for(path: &str, method: &str, authorized: bool) -> RatePolicy {
    if !authorized {
        return RatePolicy {
            name: "unauthorized",
            limit: 30,
            window: Duration::from_secs(60),
        };
    }
    if path.starts_with("/api/v1/auth/") && method == "POST" {
        return RatePolicy {
            name: "telegram-auth",
            limit: 10,
            window: Duration::from_secs(300),
        };
    }
    if path.starts_with("/api/v1/media/") && (method == "GET" || method == "HEAD") {
        return RatePolicy {
            name: "media-stream",
            limit: 5_000,
            window: Duration::from_secs(60),
        };
    }
    if path == "/api/v1/live/revision" && method == "GET" {
        return RatePolicy {
            name: "live-revision",
            limit: 1_200,
            window: Duration::from_secs(60),
        };
    }
    if method == "POST" {
        return RatePolicy {
            name: "mutation",
            limit: 60,
            window: Duration::from_secs(60),
        };
    }
    RatePolicy {
        name: "read",
        limit: 300,
        window: Duration::from_secs(60),
    }
}

fn client_key(request: &Request) -> String {
    request
        .headers()
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .chars()
        .take(64)
        .collect()
}

pub(crate) async fn protect(
    State(security): State<SecurityState>,
    request: Request,
    next: Next,
) -> Response {
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    let authorized = security.authorized(authorization).await;
    let policy = policy_for(request.uri().path(), request.method().as_str(), authorized);

    if security.rate_limited(&client_key(&request), policy) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(
                json!({"ok": false, "message": "Muitas requisicoes. Tente novamente mais tarde."}),
            ),
        )
            .into_response();
    }
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"ok": false, "message": "Credencial do TCloud Core ausente ou invalida."})),
        )
            .into_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policies_are_stricter_for_auth_and_mutations() {
        let auth = policy_for("/api/v1/auth/request-code", "POST", true);
        let mutation = policy_for("/api/v1/files/delete", "POST", true);
        let read = policy_for("/api/v1/files", "GET", true);
        assert!(auth.limit < mutation.limit);
        assert!(mutation.limit < read.limit);
    }

    #[test]
    fn bearer_comparison_rejects_missing_and_partial_tokens() {
        let state = SecurityState {
            token: Some(Arc::<[u8]>::from(
                b"01234567890123456789012345678901".as_slice(),
            )),
            db: None,
            buckets: Arc::new(Mutex::new(HashMap::new())),
        };
        assert!(!state.master_authorized(None));
        assert!(!state.master_authorized(Some("Bearer 0123456789")));
        assert!(state.master_authorized(Some("Bearer 01234567890123456789012345678901")));
    }
}
