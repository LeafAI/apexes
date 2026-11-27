//! Opt-in public network check. No API keys, accounts, orders or database.
use apexes_core::MarketEvent;
use apexes_market_data::{provider::BinanceExchangeKind, raw_depth_capture::capture};
use std::time::Duration;
use tokio::{
    sync::mpsc,
    time::{Instant, timeout_at},
};

async fn probe(market: BinanceExchangeKind) -> Result<(usize, usize), String> {
    let (tx, mut rx) = mpsc::channel(8192);
    let capture = capture("BTCUSDT", market, tx);
    let observe = async move {
        let deadline = Instant::now() + Duration::from_secs(20);
        let (mut snapshots, mut increments) = (0, 0);
        while let Ok(Some(event)) = timeout_at(deadline, rx.recv()).await {
            let MarketEvent::Depth(depth) = event else {
                return Err("unexpected event".into());
            };
            depth.validate_source_range()?;
            if depth.is_invalidation() {
                continue;
            }
            if depth.is_snapshot {
                snapshots += 1;
            } else {
                if depth.source_range.is_none() {
                    return Err("missing native range".into());
                }
                increments += 1;
            }
        }
        rx.close();
        Ok((snapshots, increments))
    };
    let (captured, observed) = tokio::join!(capture, observe);
    captured?;
    observed
}

#[tokio::test]
#[ignore = "requires public Binance REST and WebSocket connectivity"]
async fn public_spot_and_usd_m_depth_publish_native_ranges() {
    let (spot, usd_m) = tokio::join!(
        probe(BinanceExchangeKind::Spot),
        probe(BinanceExchangeKind::Perpetual)
    );
    let spot = spot.unwrap();
    let usd_m = usd_m.unwrap();
    eprintln!("Spot snapshots/deltas: {spot:?}; USD-M snapshots/deltas: {usd_m:?}");
    assert!(
        spot.0 > 0 && spot.1 > 0,
        "no synchronized Spot observations"
    );
    assert!(
        usd_m.0 > 0 && usd_m.1 > 0,
        "no synchronized USD-M observations"
    );
}
