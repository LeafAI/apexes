//! The shared streaming indicator interface implemented by every
//! technical indicator in this crate.

/// A streaming (incremental) technical indicator: internal state is
/// updated one data point at a time (`update`), never recomputed from
/// scratch over a historical window. This matches the project's
/// non-negotiable backtest/live logic-parity requirement -- the exact
/// same `update` call sequence drives both a historical replay
/// (`apexes-backtest`) and a live/paper trading loop.
pub trait Indicator {
    /// The data point type this indicator consumes per `update` call.
    /// Price-only indicators (SMA/EMA/RSI/MACD/Bollinger/stddev) use
    /// `rust_decimal::Decimal` (typically a bar's close price, but the
    /// indicator itself has no opinion on which price a caller feeds
    /// it). Indicators that structurally need more than one price field
    /// per bar (ATR needs high/low/close) use a dedicated input type
    /// instead.
    type Input;

    /// The value(s) produced by one `update` call. Single-value
    /// indicators use `Option<Decimal>` (`None` during warm-up, before
    /// enough data points have arrived to produce a meaningful value).
    /// Multi-value indicators (MACD, Bollinger Bands) use their own
    /// dedicated `Option<Struct>` output type, following the same
    /// warm-up convention.
    type Output;

    /// Feeds one new data point into the indicator's internal state and
    /// returns the updated value. O(1) amortized per call for every
    /// indicator in this crate.
    fn update(&mut self, input: Self::Input) -> Self::Output;
}
