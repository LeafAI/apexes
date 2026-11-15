//! Request signing, behind a trait so the credential type is swappable.
//!
//! HMAC-SHA-256 is the default: the secret is in this process. Some Binance
//! APIs (and some operators) need Ed25519, where the private key lives
//! elsewhere, for example in an ssh-agent; such a signer implements the same
//! trait and nothing that consumes it changes.

use crate::signing::sign_query_string;

/// Produces the `signature` parameter for Binance signed requests.
pub trait ParamSigner: Send + Sync {
    /// The API key sent with each request (public, not a secret).
    fn api_key(&self) -> &str;

    /// Signs `payload`, the request parameters sorted by name and joined as
    /// `k=v&k=v`, exactly as Binance specifies. Returns the signature in the
    /// encoding Binance expects for this key type (lower-case hex for HMAC,
    /// base64 for Ed25519). The error text must never contain key material.
    fn sign(&self, payload: &str) -> Result<String, String>;
}

/// HMAC-SHA-256 with a secret held in memory.
#[derive(Clone)]
pub struct HmacSigner {
    api_key: String,
    api_secret: String,
}

impl HmacSigner {
    pub fn new(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            api_secret: api_secret.into(),
        }
    }
}

impl std::fmt::Debug for HmacSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmacSigner")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .finish()
    }
}

impl ParamSigner for HmacSigner {
    fn api_key(&self) -> &str {
        &self.api_key
    }

    fn sign(&self, payload: &str) -> Result<String, String> {
        Ok(sign_query_string(&self.api_secret, payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_signer_matches_the_documented_known_answer() {
        // Binance's documented example (see `signing::tests`).
        let signer = HmacSigner::new(
            "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
            "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j",
        );
        let payload = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        assert_eq!(
            signer.sign(payload).unwrap(),
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }

    #[test]
    fn debug_output_never_shows_the_key_or_secret() {
        let signer = HmacSigner::new("MY-API-KEY", "MY-SECRET");
        let shown = format!("{signer:?}");
        assert!(
            !shown.contains("MY-API-KEY") && !shown.contains("MY-SECRET"),
            "{shown}"
        );
    }
}
