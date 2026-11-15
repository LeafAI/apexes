//! The ONLY place in this plan that reads secrets from the environment.
//! `apexes-gateway-binance`'s own `BinanceGatewayConfig` never reads
//! `std::env` itself (an explicit, documented design decision in that
//! crate, see `crates/apexes-gateway-binance/src/config.rs`'s own doc
//! comment) -- this module is the upstream caller that does, per this
//! plan's design spec §4.4.
//!
//! # Format
//!
//! Given a `credentials_ref` (e.g. `"binance-main"`) and a
//! [`BinanceNetwork`], this module reads two environment variables, named
//! by uppercasing `credentials_ref`, replacing every `-` with `_`, and
//! inserting the network:
//! - `APEXES_CRED_<REF>_<NETWORK>_API_KEY`
//! - `APEXES_CRED_<REF>_<NETWORK>_API_SECRET`
//!
//! e.g. `("binance-main", Testnet)` reads
//! `APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY` /
//! `APEXES_CRED_BINANCE_MAIN_TESTNET_API_SECRET`. The network is part of
//! the name on purpose: a testnet session can never load mainnet keys,
//! and the old un-suffixed names are deliberately no longer read.
//!
//! Endpoints are not per-credential: they come from
//! [`BinanceNetwork::endpoints`], the only place Binance hostnames live.

use apexes_gateway_binance::{BinanceExecutionGateway, BinanceNetwork};

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum CredentialsError {
    #[error(
        "no {network} credentials found for credentials_ref {credentials_ref:?}: environment \
         variable {missing_var} is not set"
    )]
    MissingEnvVar {
        credentials_ref: String,
        network: BinanceNetwork,
        missing_var: String,
    },
}

/// Converts a `credentials_ref` into the env-var-name fragment: uppercased,
/// with every `-` replaced by `_`.
fn env_var_fragment(credentials_ref: &str) -> String {
    credentials_ref.to_uppercase().replace('-', "_")
}

/// The two variable names a session on `network` needs for
/// `credentials_ref`. Public so a launcher can check they are set (and
/// name the missing one) before spawning an engine.
pub fn credential_var_names(credentials_ref: &str, network: BinanceNetwork) -> (String, String) {
    let fragment = env_var_fragment(credentials_ref);
    let net = network.env_fragment();
    (
        format!("APEXES_CRED_{fragment}_{net}_API_KEY"),
        format!("APEXES_CRED_{fragment}_{net}_API_SECRET"),
    )
}

/// Resolves `credentials_ref` for `network` into a Spot-only
/// [`BinanceExecutionGateway`], reading the variables via `lookup` -- a
/// caller-supplied function rather than a direct `std::env::var` call, so
/// tests can supply a fake lookup and never touch process-global
/// environment state. `main.rs` passes `std::env::var`.
pub fn resolve_binance_credentials(
    credentials_ref: &str,
    network: BinanceNetwork,
    lookup: impl Fn(&str) -> Result<String, std::env::VarError>,
) -> Result<BinanceExecutionGateway, CredentialsError> {
    let (api_key_var, api_secret_var) = credential_var_names(credentials_ref, network);

    let fetch = |var: &String| {
        lookup(var).map_err(|_| CredentialsError::MissingEnvVar {
            credentials_ref: credentials_ref.to_string(),
            network,
            missing_var: var.clone(),
        })
    };
    let api_key = fetch(&api_key_var)?;
    let api_secret = fetch(&api_secret_var)?;

    Ok(BinanceExecutionGateway::for_network(
        network, api_key, api_secret,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A `lookup` closure backed by an in-memory map instead of real
    /// process environment (`set_var` is unsafe in Rust 2024 and racy).
    fn fake_env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Result<String, std::env::VarError> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned().ok_or(std::env::VarError::NotPresent)
    }

    #[test]
    fn var_names_include_the_network() {
        assert_eq!(
            credential_var_names("binance-main", BinanceNetwork::Testnet),
            (
                "APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY".to_string(),
                "APEXES_CRED_BINANCE_MAIN_TESTNET_API_SECRET".to_string()
            )
        );
        assert_eq!(
            credential_var_names("simple", BinanceNetwork::Mainnet).0,
            "APEXES_CRED_SIMPLE_MAINNET_API_KEY"
        );
    }

    #[test]
    fn resolving_a_known_ref_for_its_network_succeeds() {
        let lookup = fake_env(&[
            ("APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY", "test-key"),
            ("APEXES_CRED_BINANCE_MAIN_TESTNET_API_SECRET", "test-secret"),
        ]);
        assert!(
            resolve_binance_credentials("binance-main", BinanceNetwork::Testnet, lookup).is_ok()
        );
    }

    #[test]
    fn mainnet_keys_are_never_used_for_a_testnet_session() {
        let lookup = fake_env(&[
            ("APEXES_CRED_BINANCE_MAIN_MAINNET_API_KEY", "real-key"),
            ("APEXES_CRED_BINANCE_MAIN_MAINNET_API_SECRET", "real-secret"),
            ("APEXES_CRED_BINANCE_MAIN_API_KEY", "legacy-key"),
            ("APEXES_CRED_BINANCE_MAIN_API_SECRET", "legacy-secret"),
        ]);
        let error =
            match resolve_binance_credentials("binance-main", BinanceNetwork::Testnet, lookup) {
                Ok(_) => panic!("must not resolve without testnet-suffixed variables"),
                Err(error) => error,
            };
        assert_eq!(
            error,
            CredentialsError::MissingEnvVar {
                credentials_ref: "binance-main".to_string(),
                network: BinanceNetwork::Testnet,
                missing_var: "APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY".to_string(),
            }
        );
    }

    #[test]
    fn missing_secret_but_present_key_names_the_secret_variable() {
        let lookup = fake_env(&[("APEXES_CRED_PARTIAL_TESTNET_API_KEY", "test-key")]);
        let error = match resolve_binance_credentials("partial", BinanceNetwork::Testnet, lookup) {
            Ok(_) => panic!("must fail"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            CredentialsError::MissingEnvVar {
                credentials_ref: "partial".to_string(),
                network: BinanceNetwork::Testnet,
                missing_var: "APEXES_CRED_PARTIAL_TESTNET_API_SECRET".to_string(),
            }
        );
        assert!(
            error
                .to_string()
                .contains("APEXES_CRED_PARTIAL_TESTNET_API_SECRET")
        );
    }

    #[test]
    fn env_var_fragment_uppercases_and_replaces_hyphens() {
        assert_eq!(env_var_fragment("binance-main"), "BINANCE_MAIN");
        assert_eq!(env_var_fragment("simple"), "SIMPLE");
    }
}
