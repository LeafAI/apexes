//! [`LiveAccount`]: wraps a real `Arc<BinanceExecutionGateway>`,
//! implementing `Account` by maintaining a local cache (updated via
//! `refresh()`) for the synchronous balance/position/realized-pnl/
//! equity methods, and forwarding directly to the gateway for
//! deposit/withdrawal history, internal transfers, and reconciliation
//! (all of which need a fresh real network request every call). See
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §3.5.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use apexes_core::{ApexesError, FundMovement, WalletId};
use apexes_gateway_binance::BinanceExecutionGateway;
use apexes_strategy::Position;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;
use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};

/// Wraps a real `Arc<BinanceExecutionGateway>`. The synchronous
/// `Account` methods (`balances`/`positions`/`realized_pnl`/`equity`)
/// read a local cache populated by `refresh()` -- NOT a fresh network
/// call every time, matching `Account::balances`'s synchronous
/// signature. The cache uses `std::sync::RwLock`: reads block briefly
/// instead of silently returning an empty map under contention (a
/// `tokio::sync::RwLock` `try_read()` fallback would let `equity()`
/// return a wrong `0` and `reconcile_against_exchange` report spurious
/// discrepancies during a race with `refresh()`). This is safe in async
/// code because `refresh()` never holds a guard across an `.await`: the
/// gateway calls are awaited first, then the two `.write()` calls happen
/// on already-resolved values with no `.await` in between or after.
/// `reconcile_against_exchange` always issues a fresh
/// request (comparing against the cache, not refreshing it) -- the
/// whole point of reconciliation is catching a STALE cache, so it must
/// not silently refresh the cache first (that would make every
/// reconciliation trivially pass).
pub struct LiveAccount {
    gateway: Arc<BinanceExecutionGateway>,
    cached_balances: RwLock<HashMap<String, Decimal>>,
    cached_positions: RwLock<HashMap<String, Position>>,
}

impl LiveAccount {
    pub fn new(gateway: Arc<BinanceExecutionGateway>) -> Self {
        Self {
            gateway,
            cached_balances: RwLock::new(HashMap::new()),
            cached_positions: RwLock::new(HashMap::new()),
        }
    }

    /// Refreshes the local cache from real `account_balances`/
    /// `account_positions` calls. Callers (a future account-monitoring
    /// task, or `run_live_trading`) decide the polling interval -- this
    /// type performs no scheduling itself.
    pub async fn refresh(&self) -> Result<(), ApexesError> {
        let balances = self.gateway.account_balances().await.map_err(|reason| {
            ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            }
        })?;
        let positions = self.gateway.account_positions().await.map_err(|reason| {
            ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            }
        })?;

        *self
            .cached_balances
            .write()
            .unwrap_or_else(|p| p.into_inner()) = balances;
        *self
            .cached_positions
            .write()
            .unwrap_or_else(|p| p.into_inner()) = positions;
        Ok(())
    }
}

#[async_trait]
impl Account for LiveAccount {
    fn balances(&self) -> HashMap<String, Decimal> {
        self.cached_balances
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn positions(&self) -> HashMap<String, Position> {
        self.cached_positions
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn realized_pnl(&self) -> HashMap<String, Decimal> {
        // Binance exposes no single "account-level accumulated realized
        // PnL" field; deriving it from persisted Fill history (via
        // apexes-oms::OrderManager, per the design spec §3.5) is left
        // to a future enhancement once a concrete OrderManager wiring
        // point exists for this crate -- returning an empty map here is
        // a deliberate, explicitly-documented gap, not a silent one.
        HashMap::new()
    }

    fn equity(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        fx_provider: &dyn FxRateProvider,
        valuation_currency: &str,
    ) -> Result<Decimal, ApexesError> {
        let balances = self.balances();
        let mut total = Decimal::ZERO;
        for (asset, balance) in &balances {
            if balance.is_zero() {
                continue;
            }
            let price = if asset == valuation_currency {
                Decimal::ONE
            } else if let Some(price) = mark_prices.get(asset) {
                *price
            } else {
                fx_provider.fx_rate(asset, valuation_currency)?
            };
            total += balance * price;
        }
        Ok(total)
    }

    async fn deposit_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        self.gateway
            .deposit_history()
            .await
            .map_err(|reason| ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            })
    }

    async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        self.gateway
            .withdrawal_history()
            .await
            .map_err(|reason| ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            })
    }

    async fn internal_transfer(
        &self,
        from_wallet: WalletId,
        to_wallet: WalletId,
        asset: &str,
        amount: Decimal,
    ) -> Result<(), ApexesError> {
        self.gateway
            .internal_transfer(from_wallet, to_wallet, asset, amount)
            .await
            .map_err(|reason| ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            })
    }

    async fn reconcile_against_exchange(
        &self,
        tolerance: ReconciliationTolerance,
    ) -> Result<Vec<ReconciliationDiscrepancy>, ApexesError> {
        let exchange_balances = self.gateway.account_balances().await.map_err(|reason| {
            ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            }
        })?;
        let local_balances = self.balances();

        let mut discrepancies = Vec::new();
        let mut all_assets: std::collections::HashSet<&String> = local_balances.keys().collect();
        all_assets.extend(exchange_balances.keys());

        for asset in all_assets {
            let local_value = local_balances.get(asset).copied().unwrap_or(Decimal::ZERO);
            let exchange_value = exchange_balances
                .get(asset)
                .copied()
                .unwrap_or(Decimal::ZERO);
            if !tolerance.within(local_value, exchange_value) {
                discrepancies.push(ReconciliationDiscrepancy {
                    asset_or_symbol: asset.clone(),
                    local_value,
                    exchange_value,
                    difference: (local_value - exchange_value).abs(),
                });
            }
        }
        Ok(discrepancies)
    }
}

#[cfg(test)]
mod tests {
    use crate::account::Account;
    use crate::live_account::LiveAccount;
    use crate::reconciliation::ReconciliationTolerance;
    use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn gateway_against(mock_server: &MockServer) -> Arc<BinanceExecutionGateway> {
        Arc::new(BinanceExecutionGateway::new(BinanceGatewayConfig {
            api_key: "test-api-key".to_string(),
            api_secret: "test-api-secret".to_string(),
            spot_base_url: mock_server.uri(),
            futures_base_url: mock_server.uri(),
            recv_window_ms: 5000,
        }))
    }

    async fn mount_empty_balances_and_positions(mock_server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"balances": []})),
            )
            .mount(mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/balance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/positionRisk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(mock_server)
            .await;
    }

    #[tokio::test]
    async fn refresh_populates_balances_from_the_gateway() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "balances": [{"asset": "USDT", "free": "1000.0", "locked": "0.0"}]
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/balance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/positionRisk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);
        live_account.refresh().await.unwrap();

        let balances = Account::balances(&live_account);
        assert_eq!(*balances.get("USDT").unwrap(), dec!(1000));
    }

    #[tokio::test]
    async fn balances_before_first_refresh_are_empty() {
        let mock_server = MockServer::start().await;
        mount_empty_balances_and_positions(&mock_server).await;
        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);

        assert!(Account::balances(&live_account).is_empty());
    }

    #[tokio::test]
    async fn deposit_history_forwards_to_the_gateway() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/sapi/v1/capital/deposit/hisrec"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": "dep-1", "amount": "10.0", "coin": "USDT", "status": 1, "insertTime": 1700000000000i64}
            ])))
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);

        let history = Account::deposit_history(&live_account).await.unwrap();
        assert_eq!(history.len(), 1);
    }

    #[tokio::test]
    async fn internal_transfer_forwards_to_the_gateway() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/sapi/v1/asset/transfer"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"tranId": 1})),
            )
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);

        let result = Account::internal_transfer(
            &live_account,
            apexes_core::WalletId::Spot,
            apexes_core::WalletId::UsdsFutures,
            "USDT",
            dec!(50),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn reconcile_against_exchange_reports_discrepancy_beyond_tolerance() {
        let mock_server = MockServer::start().await;
        // Local cache will be empty (no refresh() called); exchange
        // reports a real USDT balance -- a large discrepancy.
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "balances": [{"asset": "USDT", "free": "1000.0", "locked": "0.0"}]
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/balance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/positionRisk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);
        let tolerance = ReconciliationTolerance {
            absolute: dec!(1),
            relative_fraction: dec!(0.01),
        };

        let discrepancies = Account::reconcile_against_exchange(&live_account, tolerance)
            .await
            .unwrap();
        assert_eq!(discrepancies.len(), 1);
        assert_eq!(discrepancies[0].asset_or_symbol, "USDT");
        assert_eq!(discrepancies[0].exchange_value, dec!(1000));
    }

    #[tokio::test]
    async fn reconcile_against_exchange_reports_nothing_within_tolerance() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "balances": [{"asset": "USDT", "free": "1000.0", "locked": "0.0"}]
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/balance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/positionRisk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);
        live_account.refresh().await.unwrap();

        let tolerance = ReconciliationTolerance {
            absolute: dec!(1),
            relative_fraction: dec!(0.01),
        };
        let discrepancies = Account::reconcile_against_exchange(&live_account, tolerance)
            .await
            .unwrap();
        assert!(discrepancies.is_empty());
    }
}
