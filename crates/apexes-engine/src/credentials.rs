//! The ONLY place in this plan that reads secrets from the environment.
//! `apexes-gateway-binance`'s own `BinanceGatewayConfig` never reads
//! `std::env` itself (an explicit, documented design decision in that
//! crate, see `crates/apexes-gateway-binance/src/config.rs`'s own doc
//! comment) -- this module is the upstream caller that does, per this
//! plan's design spec §4.4.
//!
//! # Format
//!
//! Given a `credentials_ref` (e.g. `"binance-main"`), this module reads
//! two environment variables, named by uppercasing `credentials_ref`
//! and replacing every `-` with `_`:
//! - `APEXES_CRED_<REF>_API_KEY`
//! - `APEXES_CRED_<REF>_API_SECRET`
//!
//! e.g. `credentials_ref = "binance-main"` reads
//! `APEXES_CRED_BINANCE_MAIN_API_KEY` /
//! `APEXES_CRED_BINANCE_MAIN_API_SECRET`. `apexes-ui`'s Task 11
//! session-launcher tab is expected to present the set of
//! `credentials_ref` values with both matching env vars actually set
//! in the environment it runs in as the selectable option list.
//!
//! `spot_base_url`/`futures_base_url`/`recv_window_ms` are NOT
//! per-`credentials_ref` -- they use `BinanceGatewayConfig`'s real
//! production defaults (`https://api.binance.com`/
//! `https://fapi.binance.com`, 5000ms) for every reference. A future
//! iteration could make these overridable too (e.g. for testnet
//! credentials) if a real need arises.

use apexes_gateway_binance::BinanceGatewayConfig;

const DEFAULT_SPOT_BASE_URL: &str = "https://api.binance.com";
const DEFAULT_FUTURES_BASE_URL: &str = "https://fapi.binance.com";
const DEFAULT_RECV_WINDOW_MS: u64 = 5000;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum CredentialsError {
    #[error(
        "no credentials found for credentials_ref {credentials_ref:?}: environment variable \
         {missing_var} is not set"
    )]
    MissingEnvVar {
        credentials_ref: String,
        missing_var: String,
    },
}

/// Converts a `credentials_ref` into the env-var-name fragment used by
/// both `APEXES_CRED_<REF>_API_KEY` and `APEXES_CRED_<REF>_API_SECRET`:
/// uppercased, with every `-` replaced by `_`.
fn env_var_fragment(credentials_ref: &str) -> String {
    credentials_ref.to_uppercase().replace('-', "_")
}

/// Resolves `credentials_ref` into a `BinanceGatewayConfig` by looking
/// up `APEXES_CRED_<REF>_API_KEY`/`APEXES_CRED_<REF>_API_SECRET` via
/// `lookup` -- a caller-supplied function rather than a direct
/// `std::env::var` call, so tests can supply a fake, process-env-free
/// lookup and never touch real process-global environment state
/// (avoiding the parallel-test env-var races `std::env::set_var`/
/// `remove_var` would otherwise risk). `main.rs` calls this with
/// `std::env::var` itself as `lookup`.
pub fn resolve_binance_credentials(
    credentials_ref: &str,
    lookup: impl Fn(&str) -> Result<String, std::env::VarError>,
) -> Result<BinanceGatewayConfig, CredentialsError> {
    let fragment = env_var_fragment(credentials_ref);
    let api_key_var = format!("APEXES_CRED_{fragment}_API_KEY");
    let api_secret_var = format!("APEXES_CRED_{fragment}_API_SECRET");

    let api_key = lookup(&api_key_var).map_err(|_| CredentialsError::MissingEnvVar {
        credentials_ref: credentials_ref.to_string(),
        missing_var: api_key_var.clone(),
    })?;
    let api_secret = lookup(&api_secret_var).map_err(|_| CredentialsError::MissingEnvVar {
        credentials_ref: credentials_ref.to_string(),
        missing_var: api_secret_var.clone(),
    })?;

    Ok(BinanceGatewayConfig {
        api_key,
        api_secret,
        spot_base_url: DEFAULT_SPOT_BASE_URL.to_string(),
        futures_base_url: DEFAULT_FUTURES_BASE_URL.to_string(),
        recv_window_ms: DEFAULT_RECV_WINDOW_MS,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Builds a `lookup` closure backed by an in-memory map instead of
    /// real process environment state -- this is the "fake lookup
    /// function" this module's own doc comment on
    /// `resolve_binance_credentials` calls for, avoiding
    /// `std::env::set_var`/`remove_var` (unsafe in Rust 2024, and
    /// racy across parallel tests regardless) entirely.
    fn fake_env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Result<String, std::env::VarError> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned().ok_or(std::env::VarError::NotPresent)
    }

    #[test]
    fn resolving_a_known_credentials_ref_succeeds() {
        let lookup = fake_env(&[
            ("APEXES_CRED_BINANCE_MAIN_API_KEY", "test-key"),
            ("APEXES_CRED_BINANCE_MAIN_API_SECRET", "test-secret"),
        ]);

        let config = resolve_binance_credentials("binance-main", lookup).unwrap();

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.api_secret, "test-secret");
        assert_eq!(config.spot_base_url, DEFAULT_SPOT_BASE_URL);
        assert_eq!(config.futures_base_url, DEFAULT_FUTURES_BASE_URL);
        assert_eq!(config.recv_window_ms, DEFAULT_RECV_WINDOW_MS);
    }

    #[test]
    fn resolving_an_unknown_credentials_ref_fails_with_a_clear_error() {
        let lookup = fake_env(&[]);

        let error = resolve_binance_credentials("unknown-ref", lookup).unwrap_err();

        assert_eq!(
            error,
            CredentialsError::MissingEnvVar {
                credentials_ref: "unknown-ref".to_string(),
                missing_var: "APEXES_CRED_UNKNOWN_REF_API_KEY".to_string(),
            }
        );
    }

    #[test]
    fn missing_secret_but_present_key_still_fails_with_a_clear_error() {
        let lookup = fake_env(&[("APEXES_CRED_PARTIAL_API_KEY", "test-key")]);

        let error = resolve_binance_credentials("partial", lookup).unwrap_err();

        assert_eq!(
            error,
            CredentialsError::MissingEnvVar {
                credentials_ref: "partial".to_string(),
                missing_var: "APEXES_CRED_PARTIAL_API_SECRET".to_string(),
            }
        );
    }

    #[test]
    fn env_var_fragment_uppercases_and_replaces_hyphens() {
        assert_eq!(env_var_fragment("binance-main"), "BINANCE_MAIN");
        assert_eq!(env_var_fragment("simple"), "SIMPLE");
    }
}
