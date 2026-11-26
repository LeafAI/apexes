//! F02.03: public fee-only pricing, separate from OMS and trading events.
//! Rebuild attribution from the durable chronological journal. No trade
//! intent, account mutation, or private endpoint is needed.
use apexes_backtest::spot_accounting::{LifetimeSpotPnl, SpotAccounting};
use apexes_core::{Fill, Instrument};
use apexes_gateway_binance::BinanceExecutionGateway;
use apexes_oms::{FeeConversion, FeeJournal};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::sync::watch;

const HISTORICAL_BUDGET: usize = 16;
const MARK_BUDGET: usize = 32;
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
const REFRESH: Duration = Duration::from_secs(30);

pub fn spawn(
    journal: FeeJournal,
    gateway: Arc<BinanceExecutionGateway>,
    instruments: Vec<Instrument>,
) -> (
    tokio::task::JoinHandle<()>,
    watch::Receiver<Option<LifetimeSpotPnl>>,
) {
    let (tx, rx) = watch::channel(None);
    let task = tokio::spawn(async move {
        let mut worker = Worker {
            journal,
            gateway,
            instruments: instruments
                .into_iter()
                .map(|i| (i.symbol.clone(), i))
                .collect(),
            next_missing: 0,
        };
        loop {
            if tx.is_closed() {
                break;
            }
            match worker.refresh().await {
                Ok(snapshot) => {
                    tx.send_replace(Some(snapshot));
                }
                Err(_) => {
                    tracing::warn!("durable fee attribution unavailable; will retry");
                    tx.send_replace(None);
                }
            }
            tokio::time::sleep(REFRESH).await;
        }
    });
    (task, rx)
}

struct Worker {
    journal: FeeJournal,
    gateway: Arc<BinanceExecutionGateway>,
    instruments: HashMap<String, Instrument>,
    next_missing: usize,
}

type History = Vec<(Fill, Option<FeeConversion>)>;

fn assets<'a>(
    fill: &Fill,
    instruments: &'a HashMap<String, Instrument>,
) -> Result<(&'a str, &'a str), &'static str> {
    let i = instruments
        .get(&fill.symbol)
        .ok_or("unknown journal instrument")?;
    if i.venue != fill.venue {
        return Err("journal venue mismatch");
    }
    Ok((
        i.base_currency.as_deref().ok_or("missing base")?,
        i.quote_currency.as_deref().ok_or("missing quote")?,
    ))
}

fn fee_value(
    fill: &Fill,
    base: &str,
    quote: &str,
    evidence: Option<&FeeConversion>,
) -> Result<Option<Decimal>, &'static str> {
    let asset = fill.fee_currency.as_deref().unwrap_or(quote);
    if let Some(e) = evidence
        && (e.asset != asset || e.quote != quote || asset == base || asset == quote)
    {
        return Err("journal conversion pair mismatch");
    }
    Ok(if fill.fee.is_zero() {
        Some(Decimal::ZERO)
    } else if asset == quote {
        Some(fill.fee)
    } else if asset == base {
        Some(fill.fee * fill.price)
    } else {
        evidence.map(|e| fill.fee * e.rate)
    })
}

impl Worker {
    async fn lookup(&self, asset: &str, quote: &str, at: DateTime<Utc>) -> Option<FeeConversion> {
        tokio::time::timeout(
            LOOKUP_TIMEOUT,
            self.gateway.fee_conversion(asset, quote, at),
        )
        .await
        .ok()?
        .ok()
    }

    async fn resolve_history(&mut self, history: &mut History) -> Result<(), &'static str> {
        let mut missing = Vec::new();
        for (index, (fill, evidence)) in history.iter().enumerate() {
            let (base, quote) = assets(fill, &self.instruments)?;
            if fee_value(fill, base, quote, evidence.as_ref())?.is_none() {
                missing.push(index);
            }
        }
        if missing.is_empty() {
            self.next_missing = 0;
            return Ok(());
        }
        // Round-robin retry: an unavailable old pair must not starve newer fills.
        let start = self.next_missing % missing.len();
        let count = HISTORICAL_BUDGET.min(missing.len());
        let mut cache = HashMap::new();
        for n in 0..count {
            let index = missing[(start + n) % missing.len()];
            let (fill, evidence) = &mut history[index];
            let (_, quote) = assets(fill, &self.instruments)?;
            let asset = fill.fee_currency.as_deref().ok_or("missing fee asset")?;
            let key = (
                asset.to_owned(),
                quote.to_owned(),
                fill.filled_at.timestamp_millis().div_euclid(60_000),
            );
            let conversion = if let Some(cached) = cache.get(&key) {
                Option::<FeeConversion>::clone(cached)
            } else {
                let found = self.lookup(asset, quote, fill.filled_at).await;
                cache.insert(key, found.clone());
                found
            };
            // A cached candle must still satisfy each fill's causal window.
            if let Some(c) = conversion
                && c.observed_at <= fill.filled_at
                && fill.filled_at - c.observed_at <= chrono::TimeDelta::minutes(5)
            {
                self.journal
                    .persist(fill.id, &c)
                    .await
                    .map_err(|_| "persist conversion failed")?;
                *evidence = Some(c);
            }
        }
        self.next_missing = start + count;
        Ok(())
    }

    async fn refresh(&mut self) -> Result<LifetimeSpotPnl, &'static str> {
        let mut history = self
            .journal
            .history()
            .await
            .map_err(|_| "read journal failed")?;
        self.resolve_history(&mut history).await?;
        let as_of = Utc::now();
        let (mut accounting, pairs) = rebuild(&history, &self.instruments)?;
        // Separate fee-only marks: never subscribe these assets to strategy
        // market data, and never place orders or mutate wallet balances.
        for (asset, quote) in pairs.into_iter().take(MARK_BUDGET) {
            if let Some(mark) = self.lookup(&asset, &quote, as_of).await {
                accounting.observe(
                    apexes_core::Venue::Binance,
                    &asset,
                    &quote,
                    mark.observed_at,
                    mark.rate,
                );
            }
        }
        Ok(LifetimeSpotPnl {
            as_of,
            fill_count: history.len(),
            per_symbol: accounting.summaries(),
        })
    }
}

type Pairs = BTreeSet<(String, String)>;

fn rebuild(
    history: &History,
    instruments: &HashMap<String, Instrument>,
) -> Result<(SpotAccounting, Pairs), &'static str> {
    let mut accounting = SpotAccounting::default();
    let mut pairs = BTreeSet::new();
    for (fill, evidence) in history {
        let (base, quote) = assets(fill, instruments)?;
        let cost = fee_value(fill, base, quote, evidence.as_ref())?;
        accounting.record_valued(fill, base, quote, false, cost);
        pairs.insert((base.into(), quote.into()));
        let asset = fill.fee_currency.as_deref().unwrap_or(quote);
        if !fill.fee.is_zero() && asset != quote {
            pairs.insert((asset.into(), quote.into()));
        }
    }
    Ok((accounting, pairs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, FillId, MarketType, OrderId, Side, Venue};
    use rust_decimal_macros::dec;
    use uuid::Uuid;
    #[test]
    fn lifetime_rebuild_is_independent_of_wallet_and_uses_durable_cost() {
        let at = Utc::now();
        let instrument = Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".into()),
            Some("USDT".into()),
            dec!(0.01),
            dec!(0.001),
            dec!(1),
        )
        .unwrap();
        let fill = Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0.01),
            fee_currency: Some("BNB".into()),
            filled_at: at,
            leverage: None,
            margin_mode: None,
        };
        let evidence = FeeConversion {
            asset: "BNB".into(),
            quote: "USDT".into(),
            observed_at: at,
            rate: dec!(300),
        };
        let history = vec![(fill, Some(evidence))];
        let instruments = HashMap::from([("BTCUSDT".into(), instrument)]);
        for _ in 0..2 {
            let (mut ledger, pairs) = rebuild(&history, &instruments).unwrap();
            assert!(pairs.contains(&("BNB".into(), "USDT".into())));
            ledger.observe(Venue::Binance, "BTC", "USDT", at, dec!(110));
            ledger.observe(Venue::Binance, "BNB", "USDT", at, dec!(400));
            let pnl = &ledger.summaries()["BTCUSDT"];
            assert_eq!(pnl.fee_at_execution, Some(dec!(3)));
            assert_eq!(pnl.net_pnl, Some(dec!(6)));
        }
    }
}
