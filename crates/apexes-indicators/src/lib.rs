//! Apexes technical indicator library: a shared streaming `Indicator`
//! trait plus built-in implementations (SMA, EMA, RSI, MACD, Bollinger
//! Bands, ATR, rolling standard deviation). Every indicator is
//! incremental (`update` is O(1) amortized per call, never a
//! recompute-from-scratch over history), matching the project's
//! backtest/live logic-parity requirement.

pub mod ema;
pub mod indicator;
pub mod rsi;
pub mod sma;
pub mod stddev;

pub use ema::Ema;
pub use indicator::Indicator;
pub use rsi::Rsi;
pub use sma::Sma;
pub use stddev::RollingStdDev;
