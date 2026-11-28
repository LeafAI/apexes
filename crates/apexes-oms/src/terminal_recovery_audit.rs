//! Offline comparison of independently collected terminal execution evidence.
//! No gateway requests, order mutations, callback replay, or startup authority.
use crate::{CoherentRecoverySnapshot, ExchangeFill, OrderExecutionSnapshot};
use apexes_core::{AssetClass, OrderId, OrderStatus, Side, Venue};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};

const MAX_ORDERS: usize = 2048;
const MAX_FILLS: usize = 8192;

/// Evidence from a separately qualified lookup by client order identity.
/// NotFound must mean a definite negative lookup, never a timeout or
/// unsupported lookup. The collector is responsible for completeness.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RemoteTerminalEvidence {
    NotFound,
    Found {
        exchange_order_id: String,
        execution: OrderExecutionSnapshot,
        trades: Vec<ExchangeFill>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TerminalOrderEvidence {
    pub client_order_id: OrderId,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub symbol: String,
    pub side: Side,
    pub remote: RemoteTerminalEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TerminalAuditError {
    #[error("terminal evidence exceeds inspection bounds")]
    Capacity,
    #[error("invalid local terminal journal")]
    LocalJournal,
    #[error("duplicate or unexpected order evidence")]
    UnexpectedOrder,
    #[error("required terminal order evidence is missing")]
    MissingOrder,
    #[error("terminal order identity or scope mismatch")]
    Identity,
    #[error("terminal status or cumulative executed quantity mismatch")]
    Execution,
    #[error("terminal trade set or trade economics mismatch")]
    Trade,
}

/// Compare an exact selected journal with a complete supplied evidence set.
/// Success means only equality of these inputs, not proof of evidence
/// freshness, an authoritative network audit, or permission to trade.
pub fn compare_terminal_evidence(
    snapshot: &CoherentRecoverySnapshot,
    evidence: &[TerminalOrderEvidence],
) -> Result<(), TerminalAuditError> {
    use TerminalAuditError as E;
    if snapshot.orders.len() > MAX_ORDERS
        || evidence.len() > MAX_ORDERS
        || snapshot.fills.len() > MAX_FILLS
        || evidence
            .iter()
            .try_fold(0usize, |count, e| match &e.remote {
                RemoteTerminalEvidence::NotFound => Some(count),
                RemoteTerminalEvidence::Found { trades, .. } => count.checked_add(trades.len()),
            })
            .is_none_or(|count| count > MAX_FILLS)
    {
        return Err(E::Capacity);
    }

    let mut orders = HashMap::new();
    let mut exchange_ids = HashSet::new();
    for order in &snapshot.orders {
        if order.id.0.is_nil()
            || order.venue != Venue::Binance
            || order.asset_class != AssetClass::Spot
            || order.symbol.is_empty()
            || order.leverage.is_some()
            || order.margin_mode.is_some()
            || order.quantity <= Decimal::ZERO
            || order.filled_quantity < Decimal::ZERO
            || order.filled_quantity > order.quantity
            || !matches!(
                order.status,
                OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
            )
            || (order.status == OrderStatus::Filled && order.filled_quantity != order.quantity)
            || orders.insert(order.id, order).is_some()
        {
            return Err(E::LocalJournal);
        }
        match &order.exchange_order_id {
            Some(id)
                if !id.is_empty()
                    && exchange_ids.insert((order.venue, order.symbol.as_str(), id.as_str())) => {}
            None if order.status == OrderStatus::Rejected && order.filled_quantity.is_zero() => {}
            _ => return Err(E::LocalJournal),
        }
    }

    let mut fills = HashMap::<OrderId, HashMap<&str, &crate::schema::FillRow>>::new();
    let mut totals = HashMap::<OrderId, Decimal>::new();
    let mut fill_ids = HashSet::new();
    let mut trade_ids = HashSet::new();
    for fill in &snapshot.fills {
        let id = OrderId(fill.order_id);
        let order = orders.get(&id).ok_or(E::LocalJournal)?;
        if order.exchange_order_id.is_none()
            || fill.id.is_nil()
            || !fill_ids.insert(fill.id)
            || fill.exchange_trade_id.is_empty()
            || fill.symbol != order.symbol
            || fill.venue != format!("{:?}", order.venue)
            || fill.side != format!("{:?}", order.side)
            || fill.price <= Decimal::ZERO
            || fill.quantity <= Decimal::ZERO
            || fill.fee < Decimal::ZERO
            || !trade_ids.insert((
                order.venue,
                order.symbol.as_str(),
                fill.exchange_trade_id.as_str(),
            ))
            || fills
                .entry(id)
                .or_default()
                .insert(&fill.exchange_trade_id, fill)
                .is_some()
        {
            return Err(E::LocalJournal);
        }
        let total = totals.entry(id).or_default();
        *total = total.checked_add(fill.quantity).ok_or(E::LocalJournal)?;
    }
    if orders
        .values()
        .any(|o| totals.get(&o.id).copied().unwrap_or_default() != o.filled_quantity)
    {
        return Err(E::LocalJournal);
    }

    for item in evidence {
        let order = orders
            .remove(&item.client_order_id)
            .ok_or(E::UnexpectedOrder)?;
        if item.venue != order.venue
            || item.asset_class != order.asset_class
            || item.symbol != order.symbol
            || item.side != order.side
        {
            return Err(E::Identity);
        }
        let mut expected = fills.remove(&order.id).unwrap_or_default();
        match (&order.exchange_order_id, &item.remote) {
            (None, RemoteTerminalEvidence::NotFound) if expected.is_empty() => {}
            (
                Some(id),
                RemoteTerminalEvidence::Found {
                    exchange_order_id,
                    execution,
                    trades,
                },
            ) => {
                if id != exchange_order_id {
                    return Err(E::Identity);
                }
                if execution.status != order.status
                    || execution.executed_quantity != order.filled_quantity
                {
                    return Err(E::Execution);
                }
                for trade in trades {
                    let prior = expected
                        .remove(trade.exchange_trade_id.as_str())
                        .ok_or(E::Trade)?;
                    // PostgreSQL stores microsecond timestamps. Do not
                    // compare precision that was never retained.
                    if prior.price != trade.price
                        || prior.quantity != trade.quantity
                        || prior.fee != trade.fee
                        || prior.fee_currency != trade.fee_currency
                        || prior.filled_at.timestamp_micros() != trade.filled_at.timestamp_micros()
                    {
                        return Err(E::Trade);
                    }
                }
                if !expected.is_empty() {
                    return Err(E::Trade);
                }
            }
            _ => return Err(E::Identity),
        }
    }
    if !orders.is_empty() || !fills.is_empty() {
        return Err(E::MissingOrder);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Order, OrderType};
    use chrono::DateTime;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn fixture() -> (CoherentRecoverySnapshot, Vec<TerminalOrderEvidence>) {
        let at = DateTime::from_timestamp(1_800_000_000, 123_000).unwrap();
        let order = |status, exchange_id: Option<&str>, filled| Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: exchange_id.map(str::to_owned),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: filled,
            status,
            created_at: at,
            updated_at: at,
        };
        let orders = vec![
            order(OrderStatus::Filled, Some("one"), dec!(1)),
            order(OrderStatus::Cancelled, Some("two"), dec!(0)),
            order(OrderStatus::Rejected, None, dec!(0)),
            order(OrderStatus::Cancelled, Some("three"), dec!(0.5)),
        ];
        let fill = |order: &Order, id: &str, quantity| crate::schema::FillRow {
            id: Uuid::new_v4(),
            order_id: order.id.0,
            exchange_trade_id: id.into(),
            symbol: order.symbol.clone(),
            venue: "Binance".into(),
            side: "Buy".into(),
            price: dec!(100),
            quantity,
            fee: dec!(0.001),
            fee_currency: Some("BTC".into()),
            filled_at: at,
        };
        let fills = vec![
            fill(&orders[0], "11", dec!(0.4)),
            fill(&orders[0], "12", dec!(0.6)),
            fill(&orders[3], "13", dec!(0.5)),
        ];
        let evidence = orders
            .iter()
            .map(|o| TerminalOrderEvidence {
                client_order_id: o.id,
                venue: o.venue,
                asset_class: o.asset_class,
                symbol: o.symbol.clone(),
                side: o.side,
                remote: match &o.exchange_order_id {
                    None => RemoteTerminalEvidence::NotFound,
                    Some(id) => RemoteTerminalEvidence::Found {
                        exchange_order_id: id.clone(),
                        execution: OrderExecutionSnapshot {
                            status: o.status,
                            executed_quantity: o.filled_quantity,
                        },
                        trades: fills
                            .iter()
                            .filter(|f| f.order_id == o.id.0)
                            .map(|f| ExchangeFill {
                                exchange_trade_id: f.exchange_trade_id.clone(),
                                price: f.price,
                                quantity: f.quantity,
                                fee: f.fee,
                                fee_currency: f.fee_currency.clone(),
                                filled_at: f.filled_at,
                            })
                            .collect(),
                    },
                },
            })
            .collect();
        (
            CoherentRecoverySnapshot {
                capture: None,
                envelope: vec![],
                orders,
                fills,
            },
            evidence,
        )
    }

    fn trades(evidence: &mut [TerminalOrderEvidence]) -> &mut Vec<ExchangeFill> {
        let RemoteTerminalEvidence::Found { trades, .. } = &mut evidence[0].remote else {
            panic!("fixture");
        };
        trades
    }

    #[test]
    fn exact_terminal_evidence_is_order_independent_and_includes_zero_fill_orders() {
        let (snapshot, mut evidence) = fixture();
        compare_terminal_evidence(&snapshot, &evidence).unwrap();
        trades(&mut evidence).reverse();
        evidence.reverse();
        compare_terminal_evidence(&snapshot, &evidence).unwrap();
    }

    #[test]
    fn missing_duplicate_unexpected_and_wrong_scope_evidence_fails() {
        let (snapshot, evidence) = fixture();
        assert_eq!(
            compare_terminal_evidence(&snapshot, &evidence[..3]),
            Err(TerminalAuditError::MissingOrder)
        );
        let mut duplicate = evidence.clone();
        duplicate.push(evidence[0].clone());
        assert_eq!(
            compare_terminal_evidence(&snapshot, &duplicate),
            Err(TerminalAuditError::UnexpectedOrder)
        );
        let mut wrong = evidence.clone();
        wrong[0].symbol = "ETHUSDT".into();
        assert_eq!(
            compare_terminal_evidence(&snapshot, &wrong),
            Err(TerminalAuditError::Identity)
        );
        let mut unknown = evidence.clone();
        unknown[0].client_order_id = OrderId(Uuid::new_v4());
        assert_eq!(
            compare_terminal_evidence(&snapshot, &unknown),
            Err(TerminalAuditError::UnexpectedOrder)
        );
        let mut wrong = evidence.clone();
        wrong[0].side = Side::Sell;
        assert_eq!(
            compare_terminal_evidence(&snapshot, &wrong),
            Err(TerminalAuditError::Identity)
        );
    }

    #[test]
    fn terminal_lookup_and_execution_changes_fail() {
        let (snapshot, evidence) = fixture();
        let mut changed = evidence.clone();
        changed[0].remote = RemoteTerminalEvidence::NotFound;
        assert_eq!(
            compare_terminal_evidence(&snapshot, &changed),
            Err(TerminalAuditError::Identity)
        );
        let mut changed = evidence.clone();
        changed[2].remote = evidence[1].remote.clone();
        assert_eq!(
            compare_terminal_evidence(&snapshot, &changed),
            Err(TerminalAuditError::Identity)
        );
        let mut changed = evidence.clone();
        if let RemoteTerminalEvidence::Found {
            exchange_order_id, ..
        } = &mut changed[0].remote
        {
            *exchange_order_id = "other".into();
        }
        assert_eq!(
            compare_terminal_evidence(&snapshot, &changed),
            Err(TerminalAuditError::Identity)
        );
        let mut changed = evidence.clone();
        if let RemoteTerminalEvidence::Found { execution, .. } = &mut changed[3].remote {
            execution.executed_quantity = dec!(0.6);
        }
        assert_eq!(
            compare_terminal_evidence(&snapshot, &changed),
            Err(TerminalAuditError::Execution)
        );
        let mut changed = evidence.clone();
        if let RemoteTerminalEvidence::Found { execution, .. } = &mut changed[1].remote {
            execution.status = OrderStatus::New;
        }
        assert_eq!(
            compare_terminal_evidence(&snapshot, &changed),
            Err(TerminalAuditError::Execution)
        );
    }

    #[test]
    fn exact_trade_set_refuses_duplicate_missing_late_and_repriced_fills() {
        let (snapshot, evidence) = fixture();
        for case in 0..8 {
            let mut changed = evidence.clone();
            let trades = trades(&mut changed);
            match case {
                0 => {
                    trades.pop();
                }
                1 => trades.push(trades[0].clone()),
                2 => {
                    let mut late = trades[0].clone();
                    late.exchange_trade_id = "late".into();
                    trades.push(late);
                }
                3 => trades[0].price = dec!(101),
                4 => trades[0].quantity = dec!(0.3),
                5 => trades[0].fee = dec!(0.002),
                6 => trades[0].fee_currency = Some("USDT".into()),
                _ => trades[0].filled_at += chrono::TimeDelta::microseconds(1),
            }
            assert_eq!(
                compare_terminal_evidence(&snapshot, &changed),
                Err(TerminalAuditError::Trade),
                "case {case}"
            );
        }
    }

    #[test]
    fn invalid_local_journal_is_not_made_valid_by_matching_evidence() {
        let (mut snapshot, evidence) = fixture();
        snapshot.fills[0].side = "Sell".into();
        assert_eq!(
            compare_terminal_evidence(&snapshot, &evidence),
            Err(TerminalAuditError::LocalJournal)
        );
        let (mut snapshot, evidence) = fixture();
        snapshot.fills[0].quantity = dec!(0.1);
        assert_eq!(
            compare_terminal_evidence(&snapshot, &evidence),
            Err(TerminalAuditError::LocalJournal)
        );
        let (mut snapshot, evidence) = fixture();
        snapshot.fills[2].exchange_trade_id = snapshot.fills[0].exchange_trade_id.clone();
        assert_eq!(
            compare_terminal_evidence(&snapshot, &evidence),
            Err(TerminalAuditError::LocalJournal)
        );
        let (mut snapshot, evidence) = fixture();
        snapshot.orders[1].status = OrderStatus::New;
        assert_eq!(
            compare_terminal_evidence(&snapshot, &evidence),
            Err(TerminalAuditError::LocalJournal)
        );
    }

    #[test]
    fn bounded_trade_evidence_and_storage_timestamp_precision() {
        let (snapshot, mut evidence) = fixture();
        trades(&mut evidence)[0].filled_at += chrono::TimeDelta::nanoseconds(999);
        compare_terminal_evidence(&snapshot, &evidence).unwrap();
        let trades = trades(&mut evidence);
        let trade = trades[0].clone();
        trades.resize(MAX_FILLS + 1, trade);
        assert_eq!(
            compare_terminal_evidence(&snapshot, &evidence),
            Err(TerminalAuditError::Capacity)
        );
    }
}
