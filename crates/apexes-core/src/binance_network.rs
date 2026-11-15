//! [`BinanceNetwork`]: the single switch between Binance's production
//! Spot exchange and its Spot Test Network, and the ONLY place a Binance
//! Spot hostname is written down.
//!
//! The Spot Test Network (`testnet.binance.vision`) is a separate system
//! from a normal Binance account: its own accounts and API keys, virtual
//! funds, periodic resets to a blank state, and no Futures API. Keeping
//! every endpoint behind this one enum means a session is on exactly one
//! network, and a test (`no_binance_spot_hostname_outside_network_rs`)
//! fails if a hostname leaks elsewhere.
//!
//! Lives in `apexes-core` (not the Binance gateway crate) so that crates
//! which must not depend on the gateway -- notably `apexes-market-data`,
//! whose WebSocket server type needs a static URL -- can share it.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Which Binance Spot system a session talks to. Deliberately has no
/// `Default`: every caller must choose, so mainnet can never be selected
/// by omission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BinanceNetwork {
    Mainnet,
    Testnet,
}

/// Every Binance Spot endpoint a session needs, for one network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinanceEndpoints {
    pub spot_rest: &'static str,
    /// WebSocket API (orders, account, and the user-data stream). Binance
    /// removed the listenKey stream; user data now comes from here.
    pub spot_ws_api: &'static str,
    pub spot_market_ws: &'static str,
}

impl BinanceNetwork {
    pub fn endpoints(self) -> BinanceEndpoints {
        match self {
            Self::Mainnet => BinanceEndpoints {
                spot_rest: "https://api.binance.com",
                spot_ws_api: "wss://ws-api.binance.com:443/ws-api/v3",
                spot_market_ws: "wss://stream.binance.com:9443/ws",
            },
            Self::Testnet => BinanceEndpoints {
                spot_rest: "https://testnet.binance.vision",
                spot_ws_api: "wss://ws-api.testnet.binance.vision/ws-api/v3",
                spot_market_ws: "wss://stream.testnet.binance.vision/ws",
            },
        }
    }

    /// Lowercase name used on command lines and in the gateway handshake.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        }
    }

    /// Uppercase fragment used in credential environment variable names,
    /// e.g. `APEXES_CRED_<REF>_<TESTNET>_API_KEY`.
    pub fn env_fragment(self) -> &'static str {
        match self {
            Self::Mainnet => "MAINNET",
            Self::Testnet => "TESTNET",
        }
    }
}

impl fmt::Display for BinanceNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown Binance network {0:?}: expected \"mainnet\" or \"testnet\"")]
pub struct ParseNetworkError(String);

impl FromStr for BinanceNetwork {
    type Err = ParseNetworkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mainnet" => Ok(Self::Mainnet),
            "testnet" => Ok(Self::Testnet),
            other => Err(ParseNetworkError(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    #[test]
    fn endpoints_differ_per_network_and_testnet_is_the_test_network() {
        let main = BinanceNetwork::Mainnet.endpoints();
        let test = BinanceNetwork::Testnet.endpoints();
        assert_eq!(main.spot_rest, "https://api.binance.com");
        assert_eq!(test.spot_rest, "https://testnet.binance.vision");
        assert!(test.spot_ws_api.contains("ws-api.testnet.binance.vision"));
        assert!(main.spot_ws_api.contains("ws-api.binance.com"));
        assert!(test.spot_market_ws.contains("testnet.binance.vision"));
        assert!(!main.spot_market_ws.contains("testnet"));
    }

    #[test]
    fn serde_uses_lowercase_names_and_rejects_unknown() {
        assert_eq!(
            serde_json::to_string(&BinanceNetwork::Testnet).unwrap(),
            "\"testnet\""
        );
        let parsed: BinanceNetwork = serde_json::from_str("\"mainnet\"").unwrap();
        assert_eq!(parsed, BinanceNetwork::Mainnet);
        assert!(serde_json::from_str::<BinanceNetwork>("\"Mainnet\"").is_err());
        assert!(serde_json::from_str::<BinanceNetwork>("\"prod\"").is_err());
    }

    #[test]
    fn from_str_round_trips_and_names_the_bad_value() {
        for n in [BinanceNetwork::Mainnet, BinanceNetwork::Testnet] {
            assert_eq!(n.as_str().parse::<BinanceNetwork>().unwrap(), n);
        }
        let err = "prod".parse::<BinanceNetwork>().unwrap_err().to_string();
        assert!(err.contains("prod"), "{err}");
    }

    fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }

    /// The Spot hostnames live in this file only. Test code that points
    /// at a mock server or at the public testnet on purpose is exempt
    /// (it is under `tests/` or a `#[cfg(test)]` block that names the
    /// host for an assertion), so the scan covers non-test `src/`
    /// production code of the crates that talk to Binance Spot.
    #[test]
    fn no_binance_spot_hostname_outside_binance_network_rs() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        // Anchored on `//` so the Futures hosts (`fapi.`/`fstream.`), which
        // are out of scope here, do not match.
        let needles = [
            "//api.binance.com",
            "//stream.binance.com",
            "ws-api.binance.com",
            "testnet.binance.vision",
        ];
        let mut offenders = Vec::new();
        for crate_name in [
            "apexes-core",
            "apexes-gateway-binance",
            "apexes-engine",
            "apexes-market-data",
            "apexes-market-data-gateway",
            "apexes-live-trading",
            "apexes-supervisor",
        ] {
            let mut files = Vec::new();
            rust_files(&workspace.join(crate_name).join("src"), &mut files);
            for file in files {
                if file.ends_with("binance_network.rs") {
                    continue;
                }
                let Ok(text) = std::fs::read_to_string(&file) else {
                    continue;
                };
                let production = text.split("#[cfg(test)]").next().unwrap_or("");
                for (idx, line) in production.lines().enumerate() {
                    let trimmed = line.trim_start();
                    if trimmed.starts_with("//") {
                        continue;
                    }
                    if needles.iter().any(|n| line.contains(n)) {
                        offenders.push(format!("{}:{}", file.display(), idx + 1));
                    }
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "Binance Spot hostnames must only appear in binance_network.rs; found: {offenders:#?}"
        );
    }
}
