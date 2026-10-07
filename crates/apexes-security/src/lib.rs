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

/// What an authenticated caller may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Read-only: may connect, watch snapshots and scrape metrics, but every
    /// control command is refused.
    Viewer,
    /// May additionally send control commands (halt, flatten, stop, tune...).
    Operator,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
        }
    }

    pub fn can_command(self) -> bool {
        self == Role::Operator
    }
}

/// An authenticated caller: who (for the audit trail) and what they may do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub name: String,
    pub role: Role,
}

/// Name given to the legacy single shared token.
pub const SHARED_IDENTITY: &str = "shared";

/// Shortest token accepted from configuration.
pub const MIN_TOKEN_LEN: usize = 16;

/// A set of named bearer tokens. Only SHA-256 digests are retained.
#[derive(Clone)]
pub struct TokenSet {
    entries: Vec<(Identity, [u8; 32])>,
}

impl std::fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<_> = self.entries.iter().map(|(i, _)| i.name.as_str()).collect();
        write!(f, "TokenSet({names:?})")
    }
}

impl TokenSet {
    /// A single shared operator token (the legacy `APEXES_CONTROL_TOKEN`).
    /// No length check: use [`TokenSet::from_env`] for configuration.
    pub fn single(token: &str) -> Self {
        Self {
            entries: vec![(
                Identity {
                    name: SHARED_IDENTITY.to_string(),
                    role: Role::Operator,
                },
                Sha256::digest(token.as_bytes()).into(),
            )],
        }
    }

    /// Parses `name:role:token[,name:role:token...]`, role being `operator`
    /// or `viewer`. Rejects empty/duplicate/oddly-named entries, unknown
    /// roles and tokens shorter than [`MIN_TOKEN_LEN`] (a token may itself
    /// contain `:`).
    pub fn from_spec(spec: &str) -> Result<Self, String> {
        let mut entries: Vec<(Identity, [u8; 32])> = Vec::new();
        for item in spec.split(',').map(str::trim).filter(|i| !i.is_empty()) {
            let mut parts = item.splitn(3, ':');
            let (Some(name), Some(role), Some(token)) = (parts.next(), parts.next(), parts.next())
            else {
                return Err(format!(
                    "token entry {:?} must look like name:role:token",
                    item.split(':').next().unwrap_or("")
                ));
            };
            if name.is_empty()
                || !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
            {
                return Err(format!(
                    "invalid token name {name:?}: use letters, digits, '.', '_' or '-'"
                ));
            }
            if name == SHARED_IDENTITY {
                return Err(format!("the name {SHARED_IDENTITY:?} is reserved"));
            }
            let role = match role {
                "operator" => Role::Operator,
                "viewer" => Role::Viewer,
                other => {
                    return Err(format!(
                        "unknown role {other:?} for {name:?}: use operator or viewer"
                    ));
                }
            };
            if token.len() < MIN_TOKEN_LEN {
                return Err(format!(
                    "token for {name:?} is shorter than {MIN_TOKEN_LEN} characters"
                ));
            }
            if entries.iter().any(|(i, _)| i.name == name) {
                return Err(format!("duplicate token name {name:?}"));
            }
            entries.push((
                Identity {
                    name: name.to_string(),
                    role,
                },
                Sha256::digest(token.as_bytes()).into(),
            ));
        }
        if entries.is_empty() {
            return Err("no token entries".to_string());
        }
        Ok(Self { entries })
    }

    /// Combines the named tokens in `multi_var` with the legacy single token
    /// in `single_var`. `Ok(None)`: neither is set. A malformed or too-short
    /// token is an error, never silently ignored.
    pub fn from_env(single_var: &str, multi_var: &str) -> Result<Option<Self>, String> {
        let get = |var: &str| {
            std::env::var(var)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        Self::from_values(get(single_var).as_deref(), get(multi_var).as_deref())
            .map_err(|e| format!("{e} (from {single_var}/{multi_var})"))
    }

    /// Pure core of [`TokenSet::from_env`].
    pub fn from_values(single: Option<&str>, multi: Option<&str>) -> Result<Option<Self>, String> {
        let mut set: Option<Self> = None;
        if let Some(spec) = multi {
            set = Some(Self::from_spec(spec)?);
        }
        if let Some(token) = single {
            if token.len() < MIN_TOKEN_LEN {
                return Err(format!(
                    "the shared control token is shorter than {MIN_TOKEN_LEN} characters"
                ));
            }
            let single = Self::single(token);
            match &mut set {
                Some(existing) => existing.entries.extend(single.entries),
                None => set = Some(single),
            }
        }
        Ok(set)
    }

    /// Looks up a presented raw token. Compares against EVERY entry (no early
    /// exit) so timing does not reveal which entry, if any, matched.
    pub fn identify(&self, presented: &str) -> Option<&Identity> {
        let other: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        let mut found: Option<&Identity> = None;
        for (identity, digest) in &self.entries {
            let diff = digest
                .iter()
                .zip(other.iter())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b));
            if diff == 0 {
                found = Some(identity);
            }
        }
        found
    }

    /// Identifies the caller from an `Authorization: Bearer <token>` header.
    pub fn identify_headers(&self, headers: &HeaderMap) -> Option<&Identity> {
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .and_then(|presented| self.identify(presented.trim()))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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

    #[test]
    fn token_set_parses_names_roles_and_tokens_with_colons() {
        let set = TokenSet::from_spec(
            "alice:operator:aaaaaaaaaaaaaaaa, bob:viewer:bbbbbbbb:bbbbbbbb:bb ,",
        )
        .unwrap();
        assert_eq!(set.len(), 2);
        let alice = set.identify("aaaaaaaaaaaaaaaa").unwrap();
        assert_eq!((alice.name.as_str(), alice.role), ("alice", Role::Operator));
        let bob = set.identify("bbbbbbbb:bbbbbbbb:bb").unwrap();
        assert_eq!((bob.name.as_str(), bob.role), ("bob", Role::Viewer));
        assert!(!bob.role.can_command() && alice.role.can_command());
        assert!(set.identify("nope").is_none());
        assert!(set.identify("").is_none());
    }

    #[test]
    fn token_set_rejects_bad_specs() {
        let ok = "x".repeat(16);
        for (spec, why) in [
            ("".to_string(), "empty"),
            ("alice".to_string(), "missing fields"),
            (format!("alice:boss:{ok}"), "unknown role"),
            ("alice:operator:short".to_string(), "short token"),
            (format!("al ice:operator:{ok}"), "bad name"),
            (format!("shared:operator:{ok}"), "reserved name"),
            (format!("a:operator:{ok},a:viewer:{ok}y"), "duplicate name"),
        ] {
            assert!(TokenSet::from_spec(&spec).is_err(), "{why}: {spec}");
        }
    }

    #[test]
    fn token_set_combines_legacy_shared_token_with_named_ones() {
        let long = "s".repeat(20);
        let none = TokenSet::from_values(None, None).unwrap();
        assert!(none.is_none());

        let only_shared = TokenSet::from_values(Some(&long), None).unwrap().unwrap();
        let id = only_shared.identify(&long).unwrap();
        assert_eq!(
            (id.name.as_str(), id.role),
            (SHARED_IDENTITY, Role::Operator)
        );

        let both = TokenSet::from_values(
            Some(&long),
            Some(&format!("carol:viewer:{}", "c".repeat(16))),
        )
        .unwrap()
        .unwrap();
        assert_eq!(both.len(), 2);
        assert_eq!(both.identify(&long).unwrap().name, SHARED_IDENTITY);
        assert_eq!(both.identify(&"c".repeat(16)).unwrap().name, "carol");

        assert!(TokenSet::from_values(Some("short"), None).is_err());
    }

    #[test]
    fn token_set_identifies_from_headers_and_never_prints_secrets() {
        let set = TokenSet::from_spec(&format!("dave:operator:{}", "d".repeat(16))).unwrap();
        let mut h = HeaderMap::new();
        assert!(set.identify_headers(&h).is_none());
        h.insert(
            header::AUTHORIZATION,
            format!("Bearer {}", "d".repeat(16)).parse().unwrap(),
        );
        assert_eq!(set.identify_headers(&h).unwrap().name, "dave");
        h.insert(header::AUTHORIZATION, "Basic abc".parse().unwrap());
        assert!(set.identify_headers(&h).is_none());
        let shown = format!("{set:?}");
        assert!(shown.contains("dave") && !shown.contains(&"d".repeat(16)));
    }
}
