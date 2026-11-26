//! [`VolumeCurveProvider`]: an EMS-local abstraction over
//! `apexes_storage::StorageReader::intraday_volume_curve`, existing
//! solely so VWAP scheduling logic can be unit-tested without a live
//! ClickHouse connection (`StorageReader::new` requires one to
//! construct at all). Production code (Task 17) uses the
//! `apexes_storage::StorageReader` impl below; tests use a trivial mock.

use apexes_core::Venue;
use apexes_storage::reader::VolumeCurve;
use async_trait::async_trait;

use crate::error::EmsError;

#[async_trait]
pub trait VolumeCurveProvider: Send + Sync {
    async fn intraday_volume_curve(
        &self,
        symbol: &str,
        venue: Venue,
        lookback_days: u32,
    ) -> Result<VolumeCurve, EmsError>;
    /// A causal provider must explicitly implement historical cutoff semantics.
    async fn intraday_volume_curve_as_of(
        &self,
        _symbol: &str,
        _venue: Venue,
        _lookback_days: u32,
        _at: chrono::DateTime<chrono::Utc>,
    ) -> Result<VolumeCurve, EmsError> {
        Err(EmsError::InsufficientVwapHistory {
            reason: "provider has no causal history support".into(),
        })
    }
}

#[async_trait]
impl VolumeCurveProvider for apexes_storage::StorageReader {
    async fn intraday_volume_curve_as_of(
        &self,
        symbol: &str,
        venue: Venue,
        lookback_days: u32,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<VolumeCurve, EmsError> {
        self.intraday_volume_curve_as_of(symbol, venue, lookback_days, at)
            .await
            .map_err(|error| EmsError::InsufficientVwapHistory {
                reason: error.to_string(),
            })
    }
    async fn intraday_volume_curve(
        &self,
        symbol: &str,
        venue: Venue,
        lookback_days: u32,
    ) -> Result<VolumeCurve, EmsError> {
        // Calls StorageReader's own inherent method of the same name
        // (inherent methods take priority over trait methods on an
        // exact-name call through `self`, so this does not recurse).
        self.intraday_volume_curve(symbol, venue, lookback_days)
            .await
            .map_err(|error| EmsError::InsufficientVwapHistory {
                reason: error.to_string(),
            })
    }
}
