//! Apexes technical indicator library: a shared streaming `Indicator`
//! trait plus built-in implementations (SMA, EMA, RSI, MACD, Bollinger
//! Bands, ATR, rolling standard deviation). Every indicator is
//! incremental (`update` is O(1) amortized per call, never a
//! recompute-from-scratch over history), matching the project's
//! backtest/live logic-parity requirement.

pub mod atr;
pub mod bollinger;
pub mod ema;
pub mod indicator;
pub mod macd;
pub mod rsi;
pub mod sma;
pub mod stddev;

pub use atr::{Atr, AtrInput};
pub use bollinger::{Bollinger, BollingerOutput};
pub use ema::Ema;
pub use indicator::Indicator;
pub use macd::{Macd, MacdOutput};
pub use rsi::Rsi;
pub use sma::Sma;
pub use stddev::RollingStdDev;

/// Validated snapshots for bounded positive-price scoped indicators.
pub mod checkpoint;
