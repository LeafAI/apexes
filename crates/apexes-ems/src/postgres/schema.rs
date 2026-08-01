//! PostgreSQL table schemas for `PostgresEms`. Mirrors
//! `apexes_oms::schema`'s pattern (DDL constants + Row structs +
//! conversions).

use apexes_strategy::{ExecutionAlgoSpec, OrderRouting, ParentOrderId, ParentOrderStatus};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::EmsError;

pub const PARENT_ORDERS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS parent_orders
(
    id                  UUID PRIMARY KEY,
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    side                TEXT NOT NULL,
    total_quantity      NUMERIC(38, 10) NOT NULL,
    filled_quantity     NUMERIC(38, 10) NOT NULL,
    algo_spec           JSONB NOT NULL,
    routing             JSONB NOT NULL,
    status              TEXT NOT NULL,
    failure_reason      TEXT,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
)
";

pub const PARENT_ORDER_CHILDREN_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS parent_order_children
(
    parent_order_id     UUID NOT NULL REFERENCES parent_orders(id),
    child_order_id      UUID NOT NULL,
    sequence_number     INT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (parent_order_id, child_order_id)
)
";

/// A JSON-serializable mirror of `ExecutionAlgoSpec`, used only for the
/// `algo_spec` JSONB column -- `ExecutionAlgoSpec` itself does not
/// derive `Serialize`/`Deserialize` (it lives in `apexes-strategy`,
/// which has no `serde` dependency), so this crate defines its own
/// serializable shadow type and converts between them explicitly.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind")]
pub enum AlgoSpecJson {
    Twap {
        duration_secs: u64,
        slices: u32,
    },
    Vwap {
        duration_secs: u64,
        lookback_days: u32,
    },
    Iceberg {
        visible_qty: Decimal,
    },
    Pov {
        participation_rate: Decimal,
    },
}

impl From<&ExecutionAlgoSpec> for AlgoSpecJson {
    fn from(spec: &ExecutionAlgoSpec) -> Self {
        match spec {
            ExecutionAlgoSpec::Twap { duration, slices } => AlgoSpecJson::Twap {
                duration_secs: duration.as_secs(),
                slices: *slices,
            },
            ExecutionAlgoSpec::Vwap {
                duration,
                lookback_days,
            } => AlgoSpecJson::Vwap {
                duration_secs: duration.as_secs(),
                lookback_days: *lookback_days,
            },
            ExecutionAlgoSpec::Iceberg { visible_qty } => AlgoSpecJson::Iceberg {
                visible_qty: *visible_qty,
            },
            ExecutionAlgoSpec::Pov { participation_rate } => AlgoSpecJson::Pov {
                participation_rate: *participation_rate,
            },
        }
    }
}

impl From<AlgoSpecJson> for ExecutionAlgoSpec {
    fn from(json: AlgoSpecJson) -> Self {
        match json {
            AlgoSpecJson::Twap {
                duration_secs,
                slices,
            } => ExecutionAlgoSpec::Twap {
                duration: std::time::Duration::from_secs(duration_secs),
                slices,
            },
            AlgoSpecJson::Vwap {
                duration_secs,
                lookback_days,
            } => ExecutionAlgoSpec::Vwap {
                duration: std::time::Duration::from_secs(duration_secs),
                lookback_days,
            },
            AlgoSpecJson::Iceberg { visible_qty } => ExecutionAlgoSpec::Iceberg { visible_qty },
            AlgoSpecJson::Pov { participation_rate } => {
                ExecutionAlgoSpec::Pov { participation_rate }
            }
        }
    }
}

/// A JSON-serializable mirror of `OrderRouting`, same rationale as
/// `AlgoSpecJson`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind")]
pub enum RoutingJson {
    Fixed,
    RouteToBest,
    Split { candidates: Vec<String> },
}

impl From<&OrderRouting> for RoutingJson {
    fn from(routing: &OrderRouting) -> Self {
        match routing {
            OrderRouting::Fixed => RoutingJson::Fixed,
            OrderRouting::RouteToBest => RoutingJson::RouteToBest,
            OrderRouting::Split(candidates) => RoutingJson::Split {
                candidates: candidates.iter().map(|c| format!("{c:?}")).collect(),
            },
        }
    }
}

impl RoutingJson {
    /// The inverse of `From<&OrderRouting>` -- kept for symmetry with
    /// the serialization direction and for future consumers that need
    /// to rehydrate routing decisions from a persisted row (none of
    /// this task's scheduling loops do: child orders are always
    /// submitted `Fixed`, see `PostgresEms`'s module docs).
    #[allow(dead_code)]
    pub fn into_order_routing(self) -> Result<OrderRouting, EmsError> {
        match self {
            RoutingJson::Fixed => Ok(OrderRouting::Fixed),
            RoutingJson::RouteToBest => Ok(OrderRouting::RouteToBest),
            RoutingJson::Split { candidates } => {
                let parsed = candidates
                    .iter()
                    .map(|s| parse_asset_class(s))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(OrderRouting::Split(parsed))
            }
        }
    }
}

fn parse_asset_class(s: &str) -> Result<apexes_core::AssetClass, EmsError> {
    match s {
        "Spot" => Ok(apexes_core::AssetClass::Spot),
        "Perpetual" => Ok(apexes_core::AssetClass::Perpetual),
        "Future" => Ok(apexes_core::AssetClass::Future),
        "Option" => Ok(apexes_core::AssetClass::Option),
        "Equity" => Ok(apexes_core::AssetClass::Equity),
        other => Err(EmsError::QueryFailed {
            reason: format!("unknown persisted AssetClass: {other:?}"),
        }),
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ParentOrderRow {
    pub id: Uuid,
    pub symbol: String,
    pub venue: String,
    pub side: String,
    pub total_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub algo_spec: serde_json::Value,
    /// Persisted for audit/debugging completeness (and future
    /// routing-aware scheduling); none of this task's scheduling loops
    /// read it back -- child orders are always submitted `Fixed`.
    #[allow(dead_code)]
    pub routing: serde_json::Value,
    pub status: String,
    pub failure_reason: Option<String>,
    /// Audit timestamps, persisted but not read by the scheduling
    /// loops (which pace themselves off real elapsed time instead).
    #[allow(dead_code)]
    pub created_at: DateTime<Utc>,
    #[allow(dead_code)]
    pub updated_at: DateTime<Utc>,
}

impl ParentOrderRow {
    /// The inverse of `parse_status` -- kept for symmetry; none of this
    /// task's code paths write a terminal status through it (they use
    /// raw SQL UPDATEs with literal status strings instead).
    #[allow(dead_code)]
    pub fn status_json(status: &ParentOrderStatus) -> (&'static str, Option<String>) {
        match status {
            ParentOrderStatus::Scheduling => ("Scheduling", None),
            ParentOrderStatus::Completed => ("Completed", None),
            ParentOrderStatus::Cancelled => ("Cancelled", None),
            ParentOrderStatus::Failed { reason } => ("Failed", Some(reason.clone())),
        }
    }

    pub fn parse_status(&self) -> Result<ParentOrderStatus, EmsError> {
        match self.status.as_str() {
            "Scheduling" => Ok(ParentOrderStatus::Scheduling),
            "Completed" => Ok(ParentOrderStatus::Completed),
            "Cancelled" => Ok(ParentOrderStatus::Cancelled),
            "Failed" => Ok(ParentOrderStatus::Failed {
                reason: self.failure_reason.clone().unwrap_or_default(),
            }),
            other => Err(EmsError::QueryFailed {
                reason: format!("unknown persisted parent order status: {other:?}"),
            }),
        }
    }

    pub fn parent_id(&self) -> ParentOrderId {
        ParentOrderId(self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn algo_spec_json_round_trips_all_variants() {
        let specs = vec![
            ExecutionAlgoSpec::Twap {
                duration: std::time::Duration::from_secs(300),
                slices: 10,
            },
            ExecutionAlgoSpec::Vwap {
                duration: std::time::Duration::from_secs(3600),
                lookback_days: 20,
            },
            ExecutionAlgoSpec::Iceberg {
                visible_qty: dec!(0.5),
            },
            ExecutionAlgoSpec::Pov {
                participation_rate: dec!(0.1),
            },
        ];
        for spec in specs {
            let json = serde_json::to_value(AlgoSpecJson::from(&spec)).unwrap();
            let back: ExecutionAlgoSpec =
                serde_json::from_value::<AlgoSpecJson>(json).unwrap().into();
            assert_eq!(back, spec);
        }
    }

    #[test]
    fn routing_json_round_trips_all_variants() {
        let routings = vec![
            OrderRouting::Fixed,
            OrderRouting::RouteToBest,
            OrderRouting::Split(vec![
                apexes_core::AssetClass::Spot,
                apexes_core::AssetClass::Perpetual,
            ]),
        ];
        for routing in routings {
            let json = serde_json::to_value(RoutingJson::from(&routing)).unwrap();
            let back = serde_json::from_value::<RoutingJson>(json)
                .unwrap()
                .into_order_routing()
                .unwrap();
            assert_eq!(back, routing);
        }
    }

    #[test]
    fn parse_status_recovers_every_persisted_variant() {
        let row = ParentOrderRow {
            id: Uuid::nil(),
            symbol: "BTCUSDT".to_string(),
            venue: "Binance".to_string(),
            side: "Buy".to_string(),
            total_quantity: dec!(1),
            filled_quantity: dec!(0),
            algo_spec: serde_json::json!({"kind": "Fixed"}),
            routing: serde_json::json!({"kind": "Fixed"}),
            status: "Failed".to_string(),
            failure_reason: Some("boom".to_string()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert_eq!(
            row.parse_status().unwrap(),
            ParentOrderStatus::Failed {
                reason: "boom".to_string()
            }
        );
        assert_eq!(row.parent_id(), ParentOrderId(Uuid::nil()));
        assert_eq!(
            ParentOrderRow::status_json(&ParentOrderStatus::Cancelled),
            ("Cancelled", None)
        );
    }
}
