//! F02.05: passive conformance and fail-closed validation of managed Spot orders against
//! fresh exchange filters. The exchange remains the final authority.
use super::ApiFamily;
use super::{BinanceExecutionGateway, wire_decimal};
use apexes_core::{AssetClass, OrderType, Side, Venue};
use apexes_oms::{SpotCommission, SpotPreflight};
use apexes_strategy::OrderIntent;
use rust_decimal::Decimal;
use serde_json::Value;

fn number(filter: &Value, key: &str) -> Result<Decimal, String> {
    filter[key]
        .as_str()
        .and_then(|s| s.parse::<Decimal>().ok())
        .filter(|v| *v >= Decimal::ZERO)
        .ok_or_else(|| format!("missing or invalid trading filter {key}"))
}

fn bounds(value: Decimal, min: Decimal, max: Decimal, step: Decimal) -> Result<(), String> {
    if value <= Decimal::ZERO
        || value < min
        || (max > Decimal::ZERO && value > max)
        || (step > Decimal::ZERO && value.checked_rem(step) != Some(Decimal::ZERO))
    {
        return Err("order violates current exchange bounds or increment".into());
    }
    // Persisted quantity and price must be exactly what goes on the wire.
    if wire_decimal(value)?.parse::<Decimal>().ok() != Some(value) {
        return Err("order exceeds supported wire precision".into());
    }
    Ok(())
}

fn filter<'a>(row: &'a Value, name: &str) -> Result<&'a Value, String> {
    row["filters"]
        .as_array()
        .and_then(|filters| filters.iter().find(|f| f["filterType"] == name))
        .ok_or_else(|| format!("missing required {name} filter"))
}

fn round_step(value: Decimal, step: Decimal, up: bool) -> Result<Decimal, String> {
    if value <= Decimal::ZERO {
        return Err("non-positive order quantity or price".into());
    }
    if step.is_zero() {
        return Ok(value);
    }
    let rem = value.checked_rem(step).ok_or("invalid order increment")?;
    let floor = value.checked_sub(rem).ok_or("order rounding overflow")?;
    if up && !rem.is_zero() {
        floor
            .checked_add(step)
            .ok_or_else(|| "order rounding overflow".into())
    } else {
        Ok(floor)
    }
}

fn conform_shape(intent: &mut OrderIntent, row: &Value) -> Result<(), String> {
    if intent.venue != Venue::Binance
        || intent.asset_class != AssetClass::Spot
        || intent.leverage.is_some()
        || intent.margin_mode.is_some()
        || intent.stop_price.is_some()
    {
        return Err("unsupported managed spot intent".into());
    }
    let kind = match (intent.order_type, intent.price) {
        (OrderType::Market, None) => "MARKET",
        (OrderType::Limit, Some(_)) => "LIMIT",
        _ => return Err("managed spot preflight supports market and limit orders only".into()),
    };
    if row["status"] != "TRADING"
        || row["isSpotTradingAllowed"] != true
        || !row["orderTypes"]
            .as_array()
            .is_some_and(|kinds| kinds.iter().any(|k| k == kind))
    {
        return Err("spot symbol or order type is not currently tradable".into());
    }
    let lot = filter(row, "LOT_SIZE")?;
    let mut step = number(lot, "stepSize")?;
    if intent.order_type == OrderType::Market {
        let other = number(filter(row, "MARKET_LOT_SIZE")?, "stepSize")?;
        let (large, small) = (step.max(other), step.min(other));
        if !small.is_zero() && large.checked_rem(small) != Some(Decimal::ZERO) {
            return Err("incompatible market and general lot increments".into());
        }
        step = large;
    }
    intent.quantity = round_step(intent.quantity, step, false)?;
    bounds(
        intent.quantity,
        number(lot, "minQty")?,
        number(lot, "maxQty")?,
        number(lot, "stepSize")?,
    )?;
    if intent.order_type == OrderType::Market {
        // MARKET_LOT_SIZE may disable its increment with zero.
        let lot = filter(row, "MARKET_LOT_SIZE")?;
        bounds(
            intent.quantity,
            number(lot, "minQty")?,
            number(lot, "maxQty")?,
            number(lot, "stepSize")?,
        )?;
    }
    if let Some(price) = intent.price {
        let p = filter(row, "PRICE_FILTER")?;
        let price = round_step(price, number(p, "tickSize")?, intent.side == Side::Sell)?;
        intent.price = Some(price);
        bounds(
            price,
            number(p, "minPrice")?,
            number(p, "maxPrice")?,
            number(p, "tickSize")?,
        )?;
    }
    Ok(())
}

fn notional_limits(row: &Value, market: bool) -> Result<Vec<(Decimal, Decimal, u64)>, String> {
    let filters = row["filters"]
        .as_array()
        .ok_or("missing exchange filters")?;
    let mut limits = Vec::new();
    for f in filters {
        let (min, max) = match f["filterType"].as_str() {
            Some("MIN_NOTIONAL") => {
                let apply = !market
                    || f["applyToMarket"]
                        .as_bool()
                        .ok_or("invalid applyToMarket")?;
                (
                    if apply {
                        number(f, "minNotional")?
                    } else {
                        Decimal::ZERO
                    },
                    Decimal::ZERO,
                )
            }
            Some("NOTIONAL") => {
                let min = !market
                    || f["applyMinToMarket"]
                        .as_bool()
                        .ok_or("invalid applyMinToMarket")?;
                let max = !market
                    || f["applyMaxToMarket"]
                        .as_bool()
                        .ok_or("invalid applyMaxToMarket")?;
                (
                    if min {
                        number(f, "minNotional")?
                    } else {
                        Decimal::ZERO
                    },
                    if max {
                        number(f, "maxNotional")?
                    } else {
                        Decimal::ZERO
                    },
                )
            }
            _ => continue,
        };
        let mins = if market && (min > Decimal::ZERO || max > Decimal::ZERO) {
            f["avgPriceMins"]
                .as_u64()
                .ok_or("invalid market notional averaging window")?
        } else {
            0
        };
        limits.push((min, max, mins));
    }
    if limits.is_empty() {
        return Err("missing exchange notional filter".into());
    }
    Ok(limits)
}

fn commission_evidence(body: &Value, symbol: &str, side: Side) -> Result<SpotCommission, String> {
    if body["symbol"].as_str() != Some(symbol) {
        return Err("commission symbol mismatch".into());
    }
    let side_key = if side == Side::Buy { "buyer" } else { "seller" };
    let mut maker = Decimal::ZERO;
    let mut taker = Decimal::ZERO;
    for category in ["standardCommission", "specialCommission", "taxCommission"] {
        let row = &body[category];
        let side_rate = number(row, side_key)?;
        maker = maker
            .checked_add(number(row, "maker")?)
            .and_then(|r| r.checked_add(side_rate))
            .ok_or("commission overflow")?;
        taker = taker
            .checked_add(number(row, "taker")?)
            .and_then(|r| r.checked_add(side_rate))
            .ok_or("commission overflow")?;
    }
    let rate = maker.max(taker);
    if rate >= Decimal::ONE {
        return Err("invalid combined commission rate".into());
    }
    let discount = &body["discount"];
    let account = discount["enabledForAccount"]
        .as_bool()
        .ok_or("missing account fee discount state")?;
    let symbol_enabled = discount["enabledForSymbol"]
        .as_bool()
        .ok_or("missing symbol fee discount state")?;
    let asset = discount["discountAsset"]
        .as_str()
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 24
                && s.bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        })
        .ok_or("invalid discount asset")?;
    let discount_rate = number(discount, "discount")?;
    if discount_rate > Decimal::ONE {
        return Err("invalid discount rate".into());
    }
    Ok(SpotCommission {
        rate,
        observed_at: chrono::Utc::now(),
        discount_asset: (account && symbol_enabled).then(|| asset.to_owned()),
    })
}

impl BinanceExecutionGateway {
    async fn spot_commission(&self, symbol: &str, side: Side) -> Result<SpotCommission, String> {
        let (status, body) = self
            .rest
            .signed_request(
                "GET",
                ApiFamily::Spot,
                "/api/v3/account/commission",
                &format!("symbol={symbol}"),
            )
            .await
            .map_err(|_| "spot commission request failed")?;
        if !status.is_success() {
            return Err("spot commission unavailable".into());
        }
        let value: Value =
            serde_json::from_str(&body).map_err(|_| "invalid commission response")?;
        commission_evidence(&value, symbol, side)
    }

    pub(super) async fn check_spot_intent(
        &self,
        intent: &OrderIntent,
    ) -> Result<SpotPreflight, String> {
        let symbol = &intent.symbol;
        if symbol.is_empty()
            || !symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return Err("invalid spot symbol".into());
        }
        let body = self
            .rest
            .public_get("/api/v3/exchangeInfo", &format!("symbol={symbol}"))
            .await?;
        let info: Value = serde_json::from_str(&body).map_err(|_| "invalid exchange metadata")?;
        let row = info["symbols"]
            .as_array()
            .and_then(|rows| rows.iter().find(|r| r["symbol"].as_str() == Some(symbol)))
            .ok_or("spot symbol unavailable")?;
        let mut intent = intent.clone();
        conform_shape(&mut intent, row)?;
        let base = row["baseAsset"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or("missing base identity")?;
        let quote = row["quoteAsset"]
            .as_str()
            .filter(|s| !s.is_empty() && *s != base)
            .ok_or("missing quote identity")?;
        let market = intent.order_type == OrderType::Market;
        let reference_price = match intent.price {
            Some(price) => price,
            None => self.last_price(symbol).await?,
        };
        // Preserve base-quantity semantics: a managed market buy is
        // dispatched as a LIMIT IOC at a tick-aligned cap no greater
        // than the observed last price. No price-slippage allowance.
        let market_buy_cap = if market && intent.side == Side::Buy {
            let mut bounded = intent.clone();
            bounded.order_type = OrderType::Limit;
            bounded.price = Some(reference_price);
            conform_shape(&mut bounded, row)?;
            if bounded.quantity != intent.quantity {
                return Err("bounded buy quantity changed after market conformance".into());
            }
            let cap = bounded.price.ok_or("missing bounded buy limit price")?;
            for (min, max, _) in notional_limits(row, false)? {
                let value = intent
                    .quantity
                    .checked_mul(cap)
                    .ok_or("bounded buy notional overflow")?;
                if value < min || (max > Decimal::ZERO && value > max) {
                    return Err("bounded buy violates limit notional bounds".into());
                }
            }
            Some(cap)
        } else {
            None
        };
        let limits = notional_limits(row, market)?;
        for (min, max, mins) in limits {
            if min.is_zero() && max.is_zero() {
                continue;
            }
            let price = if mins == 0 {
                reference_price
            } else {
                let body = self
                    .rest
                    .public_get("/api/v3/avgPrice", &format!("symbol={symbol}"))
                    .await?;
                let average: Value =
                    serde_json::from_str(&body).map_err(|_| "invalid average price")?;
                if average["mins"].as_u64() != Some(mins) {
                    return Err("exchange averaging window unavailable".into());
                }
                let close = average["closeTime"]
                    .as_i64()
                    .ok_or("missing average price time")?;
                let age = chrono::Utc::now().timestamp_millis().saturating_sub(close);
                if !(-5000..=30000).contains(&age) {
                    return Err("stale average price".into());
                }
                number(&average, "price")?
            };
            let value = intent
                .quantity
                .checked_mul(price)
                .ok_or("notional overflow")?;
            if price <= Decimal::ZERO || value < min || (max > Decimal::ZERO && value > max) {
                return Err("order violates current exchange notional limits".into());
            }
        }
        let commission = self.spot_commission(symbol, intent.side).await?;
        let balances = self.spot_balances().await?;
        Ok(SpotPreflight {
            commission,
            base: base.into(),
            quote: quote.into(),
            reference_price: market_buy_cap.unwrap_or(reference_price),
            market_buy_cap,
            quantity: intent.quantity,
            price: intent.price,
            free_base: balances.get(base).map_or(Decimal::ZERO, |b| b.free),
            free_quote: balances.get(quote).map_or(Decimal::ZERO, |b| b.free),
        })
    }
}
