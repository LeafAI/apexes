//! Authentication and audit primitives for Apexes' network control
//! surfaces (engine `/session/ws`, market-data gateway `/market`).
//!
//! - [`BearerToken`]: shared-secret `Authorization: Bearer <token>` check
//!   with a constant-time comparison.
//! - [`require_bearer`]: ready-made axum middleware for it.
//! - [`AuditLog`]: append-only JSON-lines record of who did what, synced
//!   to disk per record (control commands are rare and each matters).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};

/// A shared-secret bearer token. Only its SHA-256 digest is retained.
#[derive(Clone)]
pub struct BearerToken {
    digest: [u8; 32],
}

impl std::fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BearerToken({})", self.fingerprint())
    }
}

impl BearerToken {
    pub fn new(token: &str) -> Self {
        Self {
            digest: Sha256::digest(token.as_bytes()).into(),
        }
    }

    /// Reads a token from environment variable `var`; unset or empty
    /// yields `None`.
    pub fn from_env(var: &str) -> Option<Self> {
        std::env::var(var)
            .ok()
            .filter(|t| !t.trim().is_empty())
            .map(|t| Self::new(t.trim()))
    }

    /// Short non-reversible id for logs/audit ("who", without the secret).
    pub fn fingerprint(&self) -> String {
        hex::encode(&self.digest[..4])
    }

    /// Constant-time check of a presented raw token.
    pub fn verify(&self, presented: &str) -> bool {
        let other: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        self.digest
            .iter()
            .zip(other.iter())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
    }

    /// Checks an `Authorization: Bearer <token>` header.
    pub fn check_headers(&self, headers: &HeaderMap) -> bool {
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|presented| self.verify(presented.trim()))
    }
}

/// Builds an `Authorization` header value for clients.
pub fn bearer_header_value(token: &str) -> String {
    format!("Bearer {token}")
}

pub fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        "unauthorized\n",
    )
        .into_response()
}

/// axum middleware: rejects requests without a valid bearer token.
/// Use with `axum::middleware::from_fn_with_state(Arc<BearerToken>, require_bearer)`.
pub async fn require_bearer(
    State(token): State<Arc<BearerToken>>,
    request: Request,
    next: Next,
) -> Response {
    if token.check_headers(request.headers()) {
        next.run(request).await
    } else {
        tracing::warn!(path = %request.uri().path(), "rejected request: missing/invalid bearer token");
        unauthorized()
    }
}

/// Append-only JSON-lines audit trail.
pub struct AuditLog {
    file: Mutex<File>,
}

impl AuditLog {
    /// Opens (creating parent directories and the file as needed) for append.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// Records one event. Failures are logged, never panic: an action
    /// that already happened must not be turned into a crash by auditing.
    pub fn record(&self, actor: &str, action: &str, detail: serde_json::Value) {
        let line = serde_json::json!({
            "ts": chrono::Utc::now().to_rfc3339(),
            "actor": actor,
            "action": action,
            "detail": detail,
        });
        let mut file = self.file.lock().unwrap();
        let result = writeln!(file, "{line}").and_then(|()| file.sync_data());
        if let Err(error) = result {
            tracing::error!(?error, action, "FAILED to write audit record");
        }
        tracing::info!(target: "audit", actor, action, %detail, "audit");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::get;
    use tower::ServiceExt;

    #[test]
    fn verify_accepts_only_the_exact_token() {
        let t = BearerToken::new("s3cret");
        assert!(t.verify("s3cret"));
        assert!(!t.verify("s3cret "));
        assert!(!t.verify(""));
        assert!(!t.verify("S3CRET"));
        assert_eq!(t.fingerprint().len(), 8);
        assert!(!format!("{t:?}").contains("s3cret"));
    }

    #[test]
    fn header_parsing() {
        let t = BearerToken::new("abc");
        let mut h = HeaderMap::new();
        assert!(!t.check_headers(&h));
        h.insert(header::AUTHORIZATION, "Bearer abc".parse().unwrap());
        assert!(t.check_headers(&h));
        h.insert(header::AUTHORIZATION, "Basic abc".parse().unwrap());
        assert!(!t.check_headers(&h));
        h.insert(header::AUTHORIZATION, "Bearer nope".parse().unwrap());
        assert!(!t.check_headers(&h));
    }

    #[tokio::test]
    async fn middleware_gates_routes() {
        let token = Arc::new(BearerToken::new("tok"));
        let app = Router::new()
            .route("/x", get(|| async { "ok" }))
            .route_layer(axum::middleware::from_fn_with_state(token, require_bearer));

        let no_auth = app
            .clone()
            .oneshot(Request::get("/x").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(no_auth.status(), StatusCode::UNAUTHORIZED);
        assert!(no_auth.headers().contains_key(header::WWW_AUTHENTICATE));

        let ok = app
            .oneshot(
                Request::get("/x")
                    .header(header::AUTHORIZATION, "Bearer tok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
    }

    #[test]
    fn audit_log_appends_json_lines_across_reopen() {
        let dir = std::env::temp_dir().join(format!("apexes-audit-test-{}", std::process::id()));
        let path = dir.join("nested").join("audit.jsonl");
        {
            let log = AuditLog::open(&path).unwrap();
            log.record("token:abcd1234", "halt", serde_json::json!({"reason": "x"}));
        }
        {
            let log = AuditLog::open(&path).unwrap();
            log.record("anonymous", "unauthorized", serde_json::json!({}));
        }
        let lines: Vec<serde_json::Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["action"], "halt");
        assert_eq!(lines[0]["detail"]["reason"], "x");
        assert_eq!(lines[1]["actor"], "anonymous");
        assert!(lines[0]["ts"].is_string());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
