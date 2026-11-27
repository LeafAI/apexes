//! Decode recorded Binance L2 frames without losing source identifiers.
//! This is an offline/capture adapter, not a replacement for the supervised
//! live book synchronizer. Callers must establish source and market scope.
use crate::provider::BinanceExchangeKind;
use apexes_core::{DepthSourceRange, DepthUpdate, PriceLevel, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Deserialize)]
struct DeltaFrame {
    #[serde(rename = "e")]
    event: String,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "E", with = "chrono::serde::ts_milliseconds")]
    event_time: DateTime<Utc>,
    #[serde(rename = "U")]
    first: u64,
    #[serde(rename = "u")]
    last: u64,
    #[serde(rename = "pu")]
    previous_final: Option<u64>,
    #[serde(rename = "b")]
    bids: Vec<(Decimal, Decimal)>,
    #[serde(rename = "a")]
    asks: Vec<(Decimal, Decimal)>,
}

#[derive(Deserialize)]
struct SnapshotFrame {
    #[serde(rename = "lastUpdateId")]
    last: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

fn levels(
    raw: Vec<(Decimal, Decimal)>,
    bid: bool,
    snapshot: bool,
) -> Result<Vec<PriceLevel>, String> {
    let mut seen = HashSet::new();
    let mut levels = Vec::with_capacity(raw.len());
    for (price, quantity) in raw {
        if price <= Decimal::ZERO
            || quantity < Decimal::ZERO
            || (snapshot && quantity == Decimal::ZERO)
            || !seen.insert(price)
        {
            return Err("invalid or duplicate depth level".into());
        }
        levels.push(PriceLevel { price, quantity });
    }
    levels.sort_by(|a, b| {
        if bid {
            b.price.cmp(&a.price)
        } else {
            a.price.cmp(&b.price)
        }
    });
    Ok(levels)
}

fn validate_symbol(symbol: &str) -> Result<(), String> {
    if symbol.is_empty()
        || !symbol
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return Err("expected canonical uppercase exchange symbol".into());
    }
    Ok(())
}

/// Decode one unwrapped depthUpdate payload. E is the original event
/// timestamp. Preserve U/u/pu exactly; never synthesize missing endpoints.
pub fn decode_delta(
    raw: &str,
    symbol: &str,
    exchange: BinanceExchangeKind,
) -> Result<DepthUpdate, String> {
    validate_symbol(symbol)?;
    let frame: DeltaFrame = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    if frame.event != "depthUpdate" || frame.symbol != symbol {
        return Err("unexpected depth event or symbol".into());
    }
    if (exchange == BinanceExchangeKind::Spot && frame.previous_final.is_some())
        || (exchange == BinanceExchangeKind::Perpetual && frame.previous_final.is_none())
    {
        return Err("previous-final field differs from declared source market".into());
    }
    let update = DepthUpdate {
        symbol: symbol.into(),
        venue: Venue::Binance,
        bids: levels(frame.bids, true, false)?,
        asks: levels(frame.asks, false, false)?,
        sequence: Some(frame.last),
        source_range: Some(DepthSourceRange {
            first: frame.first,
            last: frame.last,
            previous_final: frame.previous_final,
        }),
        is_snapshot: false,
        timestamp: frame.event_time,
    };
    update.validate_source_range()?;
    Ok(update)
}

/// Decode a delta using an explicitly recorded availability timestamp
/// instead of exchange event time. For buffered deltas, use the time the
/// synchronized stream actually published the event, not arrival before
/// snapshot acquisition. This lets historical replay use one causal clock.
pub fn decode_delta_at(
    raw: &str,
    symbol: &str,
    exchange: BinanceExchangeKind,
    observed_at: DateTime<Utc>,
) -> Result<DepthUpdate, String> {
    let mut update = decode_delta(raw, symbol, exchange)?;
    update.timestamp = observed_at;
    Ok(update)
}

/// Decode a recorded REST snapshot. Its observation timestamp is supplied
/// by the capture process (when the response became available), not now()
/// or a guessed time derived from the last update ID.
pub fn decode_snapshot(
    raw: &str,
    symbol: &str,
    observed_at: DateTime<Utc>,
) -> Result<DepthUpdate, String> {
    validate_symbol(symbol)?;
    let frame: SnapshotFrame = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    let bids = levels(frame.bids, true, true)?;
    let asks = levels(frame.asks, false, true)?;
    if bids
        .first()
        .zip(asks.first())
        .is_some_and(|(b, a)| b.price >= a.price)
    {
        return Err("crossed depth snapshot".into());
    }
    Ok(DepthUpdate {
        symbol: symbol.into(),
        venue: Venue::Binance,
        bids,
        asks,
        sequence: Some(frame.last),
        source_range: None,
        is_snapshot: true,
        timestamp: observed_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    const SPOT: &str = r#"{"e":"depthUpdate","E":1700000000123,"s":"BTCUSDT","U":99,"u":103,"b":[["99","0"],["100","2"]],"a":[["102","3"]]}"#;
    const FUTURES: &str = r#"{"e":"depthUpdate","E":1700000000123,"s":"BTCUSDT","U":108,"u":110,"pu":103,"b":[],"a":[["102","3"]]}"#;

    #[test]
    fn spot_preserves_original_range_zero_deletions_and_time() {
        let d = decode_delta(SPOT, "BTCUSDT", BinanceExchangeKind::Spot).unwrap();
        assert_eq!(d.sequence, Some(103));
        assert_eq!(
            d.source_range,
            Some(DepthSourceRange {
                first: 99,
                last: 103,
                previous_final: None
            })
        );
        assert_eq!(d.timestamp.timestamp_millis(), 1700000000123);
        assert_eq!(d.bids[0].price, dec!(100));
        assert_eq!(d.bids[1].quantity, Decimal::ZERO);
    }
    #[test]
    fn futures_preserves_previous_final_and_refuses_wrong_market() {
        let d = decode_delta(FUTURES, "BTCUSDT", BinanceExchangeKind::Perpetual).unwrap();
        assert_eq!(
            d.source_range,
            Some(DepthSourceRange {
                first: 108,
                last: 110,
                previous_final: Some(103)
            })
        );
        assert!(decode_delta(FUTURES, "BTCUSDT", BinanceExchangeKind::Spot).is_err());
        assert!(decode_delta(SPOT, "BTCUSDT", BinanceExchangeKind::Perpetual).is_err());
    }
    #[test]
    fn refuses_missing_endpoints_wrong_symbol_and_bad_levels() {
        for raw in [
            SPOT.replace("\"U\":99,", ""),
            SPOT.replace("\"U\":99", "\"U\":104"),
            SPOT.replace("\"100\",\"2\"", "\"99\",\"2\""),
            SPOT.replace("\"100\",\"2\"", "\"100\",\"-2\""),
        ] {
            assert!(decode_delta(&raw, "BTCUSDT", BinanceExchangeKind::Spot).is_err());
        }
        assert!(decode_delta(SPOT, "ETHUSDT", BinanceExchangeKind::Spot).is_err());
    }
    #[test]
    fn buffered_delta_can_preserve_recorded_publication_time() {
        let at = DateTime::from_timestamp_millis(1700000001000).unwrap();
        let d = decode_delta_at(SPOT, "BTCUSDT", BinanceExchangeKind::Spot, at).unwrap();
        assert_eq!(d.timestamp, at);
        assert_eq!(
            d.source_range,
            Some(DepthSourceRange {
                first: 99,
                last: 103,
                previous_final: None
            })
        );
    }

    #[test]
    fn snapshot_uses_observation_time_and_only_original_last_id() {
        let at = DateTime::from_timestamp_millis(1700000000000).unwrap();
        let s = decode_snapshot(
            r#"{"lastUpdateId":98,"bids":[["99","1"]],"asks":[["101","1"]]}"#,
            "BTCUSDT",
            at,
        )
        .unwrap();
        assert_eq!(s.timestamp, at);
        assert_eq!(s.sequence, Some(98));
        assert_eq!(s.source_range, None);
        assert!(s.is_snapshot);
        assert!(
            decode_snapshot(
                r#"{"lastUpdateId":98,"bids":[["102","1"]],"asks":[["101","1"]]}"#,
                "BTCUSDT",
                at
            )
            .is_err()
        );
    }
}
