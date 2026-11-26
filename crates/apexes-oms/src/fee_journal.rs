//! Durable pricing evidence for the managed Spot fill journal. Public
//! pricing I/O belongs to a separate task, not the order manager.
use apexes_core::{Fill, FillId};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;

use crate::{OmsError, schema::FillRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeConversion {
    pub asset: String,
    pub quote: String,
    pub observed_at: DateTime<Utc>,
    pub rate: Decimal,
}

#[derive(Clone)]
pub struct FeeJournal {
    pub(crate) pool: PgPool,
}

#[derive(sqlx::FromRow)]
struct PricedRow {
    #[sqlx(flatten)]
    fill: FillRow,
    fee_conversion_asset: Option<String>,
    fee_conversion_quote: Option<String>,
    fee_conversion_at: Option<DateTime<Utc>>,
    fee_conversion_rate: Option<Decimal>,
}

impl FeeJournal {
    pub(crate) async fn migrate(pool: &PgPool) -> Result<(), OmsError> {
        sqlx::query(
            "ALTER TABLE fills
            ADD COLUMN IF NOT EXISTS fee_conversion_asset TEXT,
            ADD COLUMN IF NOT EXISTS fee_conversion_quote TEXT,
            ADD COLUMN IF NOT EXISTS fee_conversion_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS fee_conversion_rate NUMERIC",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Consistent, chronological snapshot of the dedicated journal.
    pub async fn history(&self) -> Result<Vec<(Fill, Option<FeeConversion>)>, OmsError> {
        let rows: Vec<PricedRow> = sqlx::query_as(
            "SELECT f.* FROM fills f JOIN orders o ON o.id=f.order_id
             WHERE o.asset_class='Spot'
             ORDER BY f.filled_at, f.venue, f.symbol, length(f.exchange_trade_id), f.exchange_trade_id, f.id")
            .fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                let fill = Fill::try_from(row.fill)?;
                let conversion = match (
                    row.fee_conversion_asset,
                    row.fee_conversion_quote,
                    row.fee_conversion_at,
                    row.fee_conversion_rate,
                ) {
                    (None, None, None, None) => None,
                    (Some(asset), Some(quote), Some(observed_at), Some(rate)) => {
                        let evidence = FeeConversion {
                            asset,
                            quote,
                            observed_at,
                            rate,
                        };
                        if !valid(&fill, &evidence) {
                            return Err(invalid());
                        }
                        Some(evidence)
                    }
                    _ => return Err(invalid()),
                };
                Ok((fill, conversion))
            })
            .collect()
    }

    /// First valid historical evidence is immutable. A retry cannot
    /// overwrite it with a different price or a future observation.
    pub async fn persist(&self, fill_id: FillId, evidence: &FeeConversion) -> Result<(), OmsError> {
        let result = sqlx::query(
            "UPDATE fills SET fee_conversion_asset=$2, fee_conversion_quote=$3,
             fee_conversion_at=$4, fee_conversion_rate=$5
             WHERE id=$1 AND fee_currency=$2 AND fee>0 AND $2<>$3
             AND $5>0 AND $4<=filled_at AND $4>=filled_at-interval '5 minutes'
             AND fee_conversion_rate IS NULL",
        )
        .bind(fill_id.0)
        .bind(&evidence.asset)
        .bind(&evidence.quote)
        .bind(evidence.observed_at)
        .bind(evidence.rate)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        let existing: Option<(String, String, DateTime<Utc>, Decimal)> = sqlx::query_as(
            "SELECT fee_conversion_asset,fee_conversion_quote,fee_conversion_at,fee_conversion_rate
             FROM fills WHERE id=$1 AND fee_conversion_rate IS NOT NULL",
        )
        .bind(fill_id.0)
        .fetch_optional(&self.pool)
        .await?;
        if existing
            == Some((
                evidence.asset.clone(),
                evidence.quote.clone(),
                evidence.observed_at,
                evidence.rate,
            ))
        {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}

fn invalid() -> OmsError {
    OmsError::InvalidRow {
        reason: "invalid or conflicting durable fee conversion evidence".into(),
    }
}

fn valid(fill: &Fill, evidence: &FeeConversion) -> bool {
    fill.fee > Decimal::ZERO
        && fill.fee_currency.as_deref() == Some(evidence.asset.as_str())
        && evidence.asset != evidence.quote
        && !evidence.quote.is_empty()
        && evidence.rate > Decimal::ZERO
        && evidence.observed_at <= fill.filled_at
        && fill.filled_at - evidence.observed_at <= chrono::TimeDelta::minutes(5)
}
