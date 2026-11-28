//! Strict Spot recovery queries. Never use the lossy reconciliation API.
use super::*;
use apexes_core::{Instrument, Side, Venue};
use apexes_oms::{
    RemoteTerminalEvidence, SpotEvidenceBalance, SpotEvidenceSource, TerminalOrderEvidence,
};
use sha2::{Digest, Sha256};

impl BinanceExecutionGateway {
    /// Same stable local owner encoding used by the engine. Refuse
    /// unknown endpoints rather than accepting a caller's network label.
    pub fn spot_inventory_owner(
        &self,
        strategy_ref: &serde_json::Value,
        instruments: &[Instrument],
    ) -> Result<String, String> {
        let network = self
            .rest
            .spot_network()
            .ok_or("unqualified Spot REST endpoint")?;
        if !self.spot_only
            || !strategy_ref.is_object()
            || instruments.is_empty()
            || instruments.len() > 128
            || instruments
                .iter()
                .any(|i| i.venue != Venue::Binance || i.asset_class != AssetClass::Spot)
        {
            return Err("unqualified Spot owner scope".into());
        }
        let mut scope: Vec<_> = instruments
            .iter()
            .map(|i| (&i.symbol, &i.base_currency, &i.quote_currency))
            .collect();
        scope.sort();
        if scope.windows(2).any(|w| w[0].0 == w[1].0) {
            return Err("duplicate Spot instrument".into());
        }
        Ok(serde_json::json!({
            "version": 1, "network": network.to_string(),
            "credential": self.credential_fingerprint(),
            "strategy": strategy_ref, "instruments": scope,
        })
        .to_string())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TerminalResponse {
    order_id: i64,
    client_order_id: String,
    symbol: String,
    side: String,
    status: String,
    executed_qty: Decimal,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrictTrade {
    id: i64,
    order_id: i64,
    symbol: String,
    is_buyer: bool,
    price: Decimal,
    qty: Decimal,
    commission: Decimal,
    commission_asset: String,
    time: i64,
}

fn parse_terminal(body: &str, order: &Order) -> Result<TerminalResponse, String> {
    let r: TerminalResponse =
        serde_json::from_str(body).map_err(|_| "invalid terminal response")?;
    if r.order_id <= 0
        || r.client_order_id != order.id.0.to_string()
        || r.symbol != order.symbol
        || r.side
            != match order.side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            }
        || order
            .exchange_order_id
            .as_ref()
            .is_none_or(|id| *id != r.order_id.to_string())
        || r.executed_qty < Decimal::ZERO
        || !matches!(
            parse_binance_status(&r.status)?,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
        )
    {
        return Err("terminal order identity or state mismatch".into());
    }
    Ok(r)
}

fn parse_trades(
    body: &str,
    order: &Order,
    remote: &TerminalResponse,
) -> Result<Vec<ExchangeFill>, String> {
    let entries: Vec<StrictTrade> =
        serde_json::from_str(body).map_err(|_| "invalid trade response")?;
    // No pagination inference: a saturated response is unqualified.
    if entries.len() >= 1000 {
        return Err("trade page saturated".into());
    }
    let mut total = Decimal::ZERO;
    let mut ids = std::collections::HashSet::new();
    let mut trades = Vec::new();
    for e in entries {
        if e.id < 0
            || !ids.insert(e.id)
            || e.order_id != remote.order_id
            || e.symbol != order.symbol
            || e.is_buyer != (order.side == Side::Buy)
            || e.price <= Decimal::ZERO
            || e.qty <= Decimal::ZERO
            || e.commission < Decimal::ZERO
            || e.commission_asset.is_empty()
            || e.commission_asset.len() > 24
            || !e
                .commission_asset
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return Err("trade identity or amount mismatch".into());
        }
        total = total.checked_add(e.qty).ok_or("trade quantity overflow")?;
        trades.push(ExchangeFill {
            exchange_trade_id: e.id.to_string(),
            price: e.price,
            quantity: e.qty,
            fee: e.commission,
            fee_currency: Some(e.commission_asset),
            filled_at: millis_to_datetime(e.time)?,
        });
    }
    if total != remote.executed_qty {
        return Err("incomplete trade coverage".into());
    }
    Ok(trades)
}

#[async_trait]
impl SpotEvidenceSource for BinanceExecutionGateway {
    fn owner_fingerprint(
        &self,
        strategy_ref: &serde_json::Value,
        instruments: &[Instrument],
    ) -> Result<String, String> {
        Ok(format!(
            "{:x}",
            Sha256::digest(
                self.spot_inventory_owner(strategy_ref, instruments)?
                    .as_bytes()
            )
        ))
    }

    async fn terminal_order(&self, order: &Order) -> Result<TerminalOrderEvidence, String> {
        if order.asset_class != AssetClass::Spot
            || order.venue != Venue::Binance
            || order.id.0.is_nil()
            || order.symbol.is_empty()
            || order.symbol.len() > 128
            || !order
                .symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return Err("unsupported terminal order scope".into());
        }
        let params = format!("symbol={}&origClientOrderId={}", order.symbol, order.id.0);
        let (status, body) = self
            .rest
            .signed_request("GET", ApiFamily::Spot, "/api/v3/order", &params)
            .await?;
        let remote = if status == reqwest::StatusCode::BAD_REQUEST && is_order_does_not_exist(&body)
        {
            RemoteTerminalEvidence::NotFound
        } else {
            if !status.is_success() {
                return Err("terminal lookup failed".into());
            }
            let parsed = parse_terminal(&body, order)?;
            let params = format!(
                "symbol={}&orderId={}&limit=1000",
                order.symbol, parsed.order_id
            );
            let (status, body) = self
                .rest
                .signed_request("GET", ApiFamily::Spot, "/api/v3/myTrades", &params)
                .await?;
            if !status.is_success() {
                return Err("terminal trades lookup failed".into());
            }
            let trades = parse_trades(&body, order, &parsed)?;
            RemoteTerminalEvidence::Found {
                exchange_order_id: parsed.order_id.to_string(),
                execution: apexes_oms::OrderExecutionSnapshot {
                    status: parse_binance_status(&parsed.status)?,
                    executed_quantity: parsed.executed_qty,
                },
                trades,
            }
        };
        Ok(TerminalOrderEvidence {
            client_order_id: order.id,
            venue: order.venue,
            asset_class: order.asset_class,
            symbol: order.symbol.clone(),
            side: order.side,
            remote,
        })
    }

    async fn account_open_order_count(&self) -> Result<usize, String> {
        Ok(self.fetch_open_order_entries(AssetClass::Spot).await?.len())
    }

    async fn account_wallet(&self) -> Result<HashMap<String, SpotEvidenceBalance>, String> {
        // Explicitly request zero rows too; never synthesize a missing asset.
        let (status, body) = self
            .rest
            .signed_request(
                "GET",
                ApiFamily::Spot,
                "/api/v3/account",
                "omitZeroBalances=false",
            )
            .await?;
        if !status.is_success() {
            return Err("Spot wallet lookup failed".into());
        }
        let parsed: SpotAccountResponse =
            serde_json::from_str(&body).map_err(|_| "invalid Spot wallet response")?;
        if parsed.balances.len() > 1024 {
            return Err("Spot wallet exceeds evidence capacity".into());
        }
        let mut balances = HashMap::new();
        for e in parsed.balances {
            if e.asset.is_empty()
                || e.asset.len() > 24
                || !e
                    .asset
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                || e.free < Decimal::ZERO
                || e.locked < Decimal::ZERO
                || balances
                    .insert(
                        e.asset,
                        SpotEvidenceBalance {
                            free: e.free,
                            locked: e.locked,
                        },
                    )
                    .is_some()
            {
                return Err("invalid or duplicate wallet balance".into());
            }
        }
        Ok(balances)
    }
}
