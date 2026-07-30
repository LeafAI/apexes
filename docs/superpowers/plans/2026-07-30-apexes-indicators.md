# Apexes-Indicators: Factor/Technical Indicator Library Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a new `apexes-indicators` crate providing a streaming (incremental) `Indicator`
trait plus 7 built-in technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands, ATR, rolling
stddev), then wire them into `apexes-strategy`'s Drift Script environment as a new `indicators`
global object so `.drift` scripts can create and update indicator instances across ticks.

**Architecture:** `apexes-indicators` is a pure-Rust crate (no `drift-script` dependency) that
depends only on `apexes-core` (for the `Bar` type, via a `From` conversion) and `rust_decimal`.
Each indicator implements a shared `Indicator` trait with associated `Input`/`Output` types,
maintaining O(1)-amortized internal state (ring buffers, running sums) rather than recomputing
over history. `apexes-strategy` gets a new `drift_dsl::indicators` binding module (parallel to
the existing `drift_dsl::bindings` for `ctx.*`) that registers factory functions
(`indicators.sma(period)` etc.) returning `Arc<Mutex<...>>`-backed script objects with a single
`update` method, so indicator state persists correctly across `on_market_event` calls when a
script creates the indicator once at its top level.

**Tech Stack:** Rust edition 2024, Cargo workspace, `rust_decimal` (with `MathematicalOps` for
`.sqrt()`, already enabled workspace-wide via the `maths` feature), `drift-script` (for the
binding layer only, in `apexes-strategy`), `thiserror`.

## Global Constraints

- Design spec of record: `docs/superpowers/specs/2026-07-30-apexes-indicators-design.md`. If you
  find a mismatch between this plan and that spec while implementing, the spec wins — stop and
  flag it rather than improvising.
- Rust edition `2024`, matching `apexes`' workspace (`Cargo.toml` `[workspace.package] edition =
  "2024"`).
- Every indicator's `update` must be O(1) amortized per call — no recompute-from-scratch over a
  historical window. Streaming/incremental only; no batch (slice-in-slice-out) API in this plan
  (design spec §一, explicitly excluded).
- `Output` types are `Option<...>` (or an `Option<Struct>` for MACD/Bollinger) — `None` during
  warm-up, before enough data points have arrived. No implicit "error placeholder" values.
- Rust-side constructors (`Sma::new(0)` etc.) `panic!` on structurally invalid parameters
  (contract violation, not runtime data error) — design spec §六 decision 3. The Drift Script
  binding layer must NOT let a script trigger that panic: it validates parameters itself and
  returns a script-level error object instead.
- Every commit in the `apexes` repo uses `git commit -S -s` (signed + signed-off), matching the
  project's established convention. Direct commits to `master` (no branches/PRs), matching the
  project's established convention.
- `cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets
  -- -D warnings` / `cargo fmt --check` must all pass clean after every task that touches
  compiled code.

---

### Task 1: Scaffold the `apexes-indicators` crate and implement the `Indicator` trait + `Sma`

**Files:**
- Modify: `Cargo.toml` (workspace root) — add `"crates/apexes-indicators"` to `members`
- Create: `crates/apexes-indicators/Cargo.toml`
- Create: `crates/apexes-indicators/src/lib.rs`
- Create: `crates/apexes-indicators/src/indicator.rs`
- Create: `crates/apexes-indicators/src/sma.rs`

**Interfaces:**
- Produces: `pub trait Indicator { type Input; type Output; fn update(&mut self, input:
  Self::Input) -> Self::Output; }`, `pub struct Sma` implementing `Indicator<Input = Decimal,
  Output = Option<Decimal>>` with `pub fn new(period: usize) -> Self`. Both re-exported from
  `apexes_indicators::{Indicator, Sma}` for later tasks and for `apexes-strategy`.

- [x] **Step 1: Add the new crate to the workspace**

Read the current workspace root `Cargo.toml`:

```bash
cd ~/src/oss/apexes
cat Cargo.toml
```

Edit the `members` list to add `"crates/apexes-indicators"` (keep existing members and array
formatting style — single line, comma-separated):

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators"]
```

- [x] **Step 2: Create `crates/apexes-indicators/Cargo.toml`**

```toml
[package]
name = "apexes-indicators"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
rust_decimal = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
```

- [x] **Step 3: Create `crates/apexes-indicators/src/indicator.rs`**

```rust
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
```

- [x] **Step 4: Write `Sma`'s failing tests**

Create `crates/apexes-indicators/src/sma.rs`:

```rust
//! Simple Moving Average.

use std::collections::VecDeque;

use rust_decimal::Decimal;

use crate::indicator::Indicator;

/// Simple Moving Average over a fixed-size trailing window.
pub struct Sma {
    period: usize,
    window: VecDeque<Decimal>,
    sum: Decimal,
}

impl Sma {
    /// Creates a new `Sma` with the given window size.
    ///
    /// # Panics
    /// Panics if `period == 0` -- a zero-length window is a construction-time
    /// contract violation, not a runtime data error (design spec §六 decision 3).
    pub fn new(period: usize) -> Self {
        assert!(period > 0, "Sma::new: period must be > 0, got {period}");
        Self { period, window: VecDeque::with_capacity(period), sum: Decimal::ZERO }
    }
}

impl Indicator for Sma {
    type Input = Decimal;
    type Output = Option<Decimal>;

    fn update(&mut self, price: Decimal) -> Option<Decimal> {
        self.window.push_back(price);
        self.sum += price;
        if self.window.len() > self.period {
            if let Some(oldest) = self.window.pop_front() {
                self.sum -= oldest;
            }
        }
        if self.window.len() == self.period {
            Some(self.sum / Decimal::from(self.period as u64))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_during_warmup() {
        let mut sma = Sma::new(3);
        assert_eq!(sma.update(dec!(10)), None);
        assert_eq!(sma.update(dec!(11)), None);
    }

    #[test]
    fn returns_average_once_window_is_full() {
        let mut sma = Sma::new(3);
        sma.update(dec!(10));
        sma.update(dec!(11));
        assert_eq!(sma.update(dec!(12)), Some(dec!(11)));
    }

    #[test]
    fn slides_window_after_full() {
        let mut sma = Sma::new(3);
        sma.update(dec!(10));
        sma.update(dec!(11));
        sma.update(dec!(12));
        assert_eq!(sma.update(dec!(13)), Some(dec!(12)));
        assert_eq!(sma.update(dec!(14)), Some(dec!(13)));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Sma::new(0);
    }
}
```

- [x] **Step 5: Create `crates/apexes-indicators/src/lib.rs`**

```rust
//! Apexes technical indicator library: a shared streaming `Indicator`
//! trait plus built-in implementations (SMA, EMA, RSI, MACD, Bollinger
//! Bands, ATR, rolling standard deviation). Every indicator is
//! incremental (`update` is O(1) amortized per call, never a
//! recompute-from-scratch over history), matching the project's
//! backtest/live logic-parity requirement.

pub mod indicator;
pub mod sma;

pub use indicator::Indicator;
pub use sma::Sma;
```

- [x] **Step 6: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-indicators
```
Expected: `4 passed` (3 behavior tests + 1 `#[should_panic]` test).

- [x] **Step 7: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-indicators --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs. If `cargo fmt --check` reports diffs, run `cargo
fmt` (auto-fix) and re-run both commands to confirm clean.

- [x] **Step 8: Commit**

```bash
cd ~/src/oss/apexes
git add Cargo.toml crates/apexes-indicators
git commit -S -s -m "feat: scaffold apexes-indicators crate with Indicator trait and Sma"
git push origin master
```

---

### Task 2: Implement `Ema` and `RollingStdDev`

**Files:**
- Create: `crates/apexes-indicators/src/ema.rs`
- Create: `crates/apexes-indicators/src/stddev.rs`
- Modify: `crates/apexes-indicators/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_indicators::Indicator` (Task 1).
- Produces: `pub struct Ema` implementing `Indicator<Input = Decimal, Output = Option<Decimal>>`
  with `pub fn new(period: usize) -> Self`; `pub struct RollingStdDev` implementing
  `Indicator<Input = Decimal, Output = Option<Decimal>>` with `pub fn new(period: usize) ->
  Self`. Both consumed by `Bollinger` in Task 4 and by the Drift Script binding layer in Task 6.

- [x] **Step 1: Write `Ema`**

Create `crates/apexes-indicators/src/ema.rs`:

```rust
//! Exponential Moving Average.

use rust_decimal::Decimal;

use crate::indicator::Indicator;

/// Exponential Moving Average with smoothing factor `alpha = 2 / (period + 1)`.
///
/// The first data point seeds the internal value (so a meaningful average
/// requires seeing at least `period` data points, matching `Sma`'s
/// warm-up length) but does not itself count toward the returned
/// warm-up-complete average -- see `returns_none_until_period_points_seen`
/// below for the exact boundary.
pub struct Ema {
    period: usize,
    alpha: Decimal,
    value: Option<Decimal>,
    count: usize,
}

impl Ema {
    /// # Panics
    /// Panics if `period == 0`.
    pub fn new(period: usize) -> Self {
        assert!(period > 0, "Ema::new: period must be > 0, got {period}");
        let alpha = Decimal::from(2) / Decimal::from((period + 1) as u64);
        Self { period, alpha, value: None, count: 0 }
    }
}

impl Indicator for Ema {
    type Input = Decimal;
    type Output = Option<Decimal>;

    fn update(&mut self, price: Decimal) -> Option<Decimal> {
        self.value = Some(match self.value {
            None => price,
            Some(prev) => self.alpha * price + (Decimal::ONE - self.alpha) * prev,
        });
        self.count += 1;
        if self.count >= self.period { self.value } else { None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_until_period_points_seen() {
        let mut ema = Ema::new(3);
        assert_eq!(ema.update(dec!(10)), None);
        assert_eq!(ema.update(dec!(11)), None);
    }

    #[test]
    fn returns_seeded_ema_once_warm() {
        // Golden values hand-derived: alpha = 2/4 = 0.5, seed = 10,
        // then 0.5*11 + 0.5*10 = 10.5, then 0.5*12 + 0.5*10.5 = 11.25.
        let mut ema = Ema::new(3);
        ema.update(dec!(10));
        ema.update(dec!(11));
        assert_eq!(ema.update(dec!(12)), Some(dec!(11.25)));
        assert_eq!(ema.update(dec!(13)), Some(dec!(12.125)));
        assert_eq!(ema.update(dec!(14)), Some(dec!(13.0625)));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Ema::new(0);
    }
}
```

- [x] **Step 2: Write `RollingStdDev`**

Create `crates/apexes-indicators/src/stddev.rs`:

```rust
//! Rolling (population) standard deviation over a fixed-size trailing window.

use std::collections::VecDeque;

use rust_decimal::{Decimal, MathematicalOps};

use crate::indicator::Indicator;

/// Rolling population standard deviation: `sqrt(mean(x^2) - mean(x)^2)`
/// over the current trailing window, updated in O(1) via running sums.
pub struct RollingStdDev {
    period: usize,
    window: VecDeque<Decimal>,
    sum: Decimal,
    sum_sq: Decimal,
}

impl RollingStdDev {
    /// # Panics
    /// Panics if `period == 0`.
    pub fn new(period: usize) -> Self {
        assert!(period > 0, "RollingStdDev::new: period must be > 0, got {period}");
        Self {
            period,
            window: VecDeque::with_capacity(period),
            sum: Decimal::ZERO,
            sum_sq: Decimal::ZERO,
        }
    }
}

impl Indicator for RollingStdDev {
    type Input = Decimal;
    type Output = Option<Decimal>;

    fn update(&mut self, price: Decimal) -> Option<Decimal> {
        self.window.push_back(price);
        self.sum += price;
        self.sum_sq += price * price;
        if self.window.len() > self.period {
            if let Some(oldest) = self.window.pop_front() {
                self.sum -= oldest;
                self.sum_sq -= oldest * oldest;
            }
        }
        if self.window.len() == self.period {
            let n = Decimal::from(self.period as u64);
            let mean = self.sum / n;
            let variance = self.sum_sq / n - mean * mean;
            Some(variance.sqrt().unwrap_or(Decimal::ZERO))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_during_warmup() {
        let mut std = RollingStdDev::new(2);
        assert_eq!(std.update(dec!(10)), None);
    }

    #[test]
    fn returns_constant_stddev_for_alternating_series() {
        // prices [10, 14, 10, 14], period=2 -> mean=12, stddev=2.0 at every full window.
        let mut std = RollingStdDev::new(2);
        std.update(dec!(10));
        assert_eq!(std.update(dec!(14)), Some(dec!(2)));
        assert_eq!(std.update(dec!(10)), Some(dec!(2)));
        assert_eq!(std.update(dec!(14)), Some(dec!(2)));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        RollingStdDev::new(0);
    }
}
```

- [x] **Step 3: Update `lib.rs` re-exports**

```rust
pub mod ema;
pub mod indicator;
pub mod sma;
pub mod stddev;

pub use ema::Ema;
pub use indicator::Indicator;
pub use sma::Sma;
pub use stddev::RollingStdDev;
```

- [x] **Step 4: Run tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-indicators
```
Expected: `10 passed` (4 from Task 1 + 3 `Ema` + 3 `RollingStdDev`).

- [x] **Step 5: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-indicators --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-verify both commands).

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-indicators
git commit -S -s -m "feat: add Ema and RollingStdDev indicators"
git push origin master
```

---

### Task 3: Implement `Rsi`

**Files:**
- Create: `crates/apexes-indicators/src/rsi.rs`
- Modify: `crates/apexes-indicators/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_indicators::Indicator` (Task 1).
- Produces: `pub struct Rsi` implementing `Indicator<Input = Decimal, Output = Option<Decimal>>`
  with `pub fn new(period: usize) -> Self`.

- [x] **Step 1: Write `Rsi`**

Create `crates/apexes-indicators/src/rsi.rs`:

```rust
//! Relative Strength Index, using Wilder's smoothing method.

use rust_decimal::Decimal;

use crate::indicator::Indicator;

/// Relative Strength Index (Wilder's smoothing): the first `period`
/// price changes are averaged with a simple mean to seed `avg_gain`/
/// `avg_loss`; every change after that uses Wilder's recursive smoothing
/// `avg = (avg * (period - 1) + current) / period`. When `avg_loss` is
/// zero (the price has only risen over the tracked history), RSI is
/// defined as `100` to avoid division by zero.
pub struct Rsi {
    period: usize,
    prev_price: Option<Decimal>,
    avg_gain: Decimal,
    avg_loss: Decimal,
    /// Number of price *changes* folded into `avg_gain`/`avg_loss` so far
    /// (not the number of prices seen -- the first price has no prior
    /// price to diff against).
    change_count: usize,
}

impl Rsi {
    /// # Panics
    /// Panics if `period == 0`.
    pub fn new(period: usize) -> Self {
        assert!(period > 0, "Rsi::new: period must be > 0, got {period}");
        Self {
            period,
            prev_price: None,
            avg_gain: Decimal::ZERO,
            avg_loss: Decimal::ZERO,
            change_count: 0,
        }
    }

    fn rsi_from_averages(&self) -> Decimal {
        if self.avg_loss.is_zero() {
            return Decimal::from(100);
        }
        let rs = self.avg_gain / self.avg_loss;
        Decimal::from(100) - Decimal::from(100) / (Decimal::ONE + rs)
    }
}

impl Indicator for Rsi {
    type Input = Decimal;
    type Output = Option<Decimal>;

    fn update(&mut self, price: Decimal) -> Option<Decimal> {
        let Some(prev) = self.prev_price else {
            self.prev_price = Some(price);
            return None;
        };
        self.prev_price = Some(price);

        let change = price - prev;
        let gain = change.max(Decimal::ZERO);
        let loss = (-change).max(Decimal::ZERO);

        if self.change_count < self.period {
            let n = Decimal::from(self.change_count as u64);
            self.avg_gain = (self.avg_gain * n + gain) / (n + Decimal::ONE);
            self.avg_loss = (self.avg_loss * n + loss) / (n + Decimal::ONE);
            self.change_count += 1;
            if self.change_count < self.period {
                return None;
            }
        } else {
            let period_minus_one = Decimal::from((self.period - 1) as u64);
            let period = Decimal::from(self.period as u64);
            self.avg_gain = (self.avg_gain * period_minus_one + gain) / period;
            self.avg_loss = (self.avg_loss * period_minus_one + loss) / period;
        }

        Some(self.rsi_from_averages())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_until_period_changes_seen() {
        // period=2 needs 2 price changes (3 prices) before a value.
        let mut rsi = Rsi::new(2);
        assert_eq!(rsi.update(dec!(10)), None); // seeds prev_price, no change yet
        assert_eq!(rsi.update(dec!(12)), None); // 1st change, still warming up
    }

    #[test]
    fn matches_hand_derived_golden_values() {
        // prices [10, 12, 11, 13], period=2.
        // change1: +2 (gain=2,loss=0); change2: -1 (gain=0,loss=1) -> seeds
        //   avg_gain=(2+0)/2=1, avg_loss=(0+1)/2=0.5 -> RS=2 -> RSI=100-100/3=66.666...
        // change3: +2 (gain=2,loss=0) -> avg_gain=(1*1+2)/2=1.5, avg_loss=(0.5*1+0)/2=0.25
        //   -> RS=6 -> RSI=100-100/7=85.714285...
        let mut rsi = Rsi::new(2);
        rsi.update(dec!(10));
        rsi.update(dec!(12));
        let third = rsi.update(dec!(11)).expect("warm after 2 changes");
        assert!((third - dec!(66.666666666666667)).abs() < dec!(0.0001));
        let fourth = rsi.update(dec!(13)).expect("still warm");
        assert!((fourth - dec!(85.714285714285714)).abs() < dec!(0.0001));
    }

    #[test]
    fn all_gains_yields_100() {
        let mut rsi = Rsi::new(2);
        rsi.update(dec!(10));
        rsi.update(dec!(11));
        assert_eq!(rsi.update(dec!(12)), Some(dec!(100)));
        assert_eq!(rsi.update(dec!(13)), Some(dec!(100)));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Rsi::new(0);
    }
}
```

- [x] **Step 2: Update `lib.rs`**

```rust
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
```

- [x] **Step 3: Run tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-indicators
```
Expected: `14 passed` (10 from Tasks 1-2 + 4 `Rsi`). If
`matches_hand_derived_golden_values` fails due to `Decimal` precision/rounding differing from
the plan's tolerance-based assertion, print the actual computed values (`dbg!(third, fourth)`
temporarily) and widen the tolerance constant if the discrepancy is within expected
floating/decimal rounding noise (sub-0.001 magnitude) — do not change the RSI algorithm itself
without re-deriving the golden values by hand first.

- [x] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-indicators --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-verify both commands).

- [x] **Step 5: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-indicators
git commit -S -s -m "feat: add Rsi indicator (Wilder's smoothing)"
git push origin master
```

---

### Task 4: Implement `Macd` and `Bollinger`

**Files:**
- Create: `crates/apexes-indicators/src/macd.rs`
- Create: `crates/apexes-indicators/src/bollinger.rs`
- Modify: `crates/apexes-indicators/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_indicators::{Indicator, Ema, Sma, RollingStdDev}` (Tasks 1-2).
- Produces: `pub struct Macd` implementing `Indicator<Input = Decimal, Output =
  Option<MacdOutput>>`, `pub struct MacdOutput { pub macd: Decimal, pub signal: Decimal, pub
  histogram: Decimal }`; `pub struct Bollinger` implementing `Indicator<Input = Decimal, Output =
  Option<BollingerOutput>>`, `pub struct BollingerOutput { pub upper: Decimal, pub middle:
  Decimal, pub lower: Decimal }`. Both consumed by the Drift Script binding layer in Task 6.

- [x] **Step 1: Write `Macd`**

Create `crates/apexes-indicators/src/macd.rs`:

```rust
//! Moving Average Convergence Divergence.

use rust_decimal::Decimal;

use crate::ema::Ema;
use crate::indicator::Indicator;

/// The three values produced by one `Macd::update` call once warm.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MacdOutput {
    /// `fast_ema - slow_ema`.
    pub macd: Decimal,
    /// EMA of `macd` over `signal_period`.
    pub signal: Decimal,
    /// `macd - signal`.
    pub histogram: Decimal,
}

/// MACD: the difference between a fast and slow EMA, plus an EMA of that
/// difference (the signal line). `update` only starts returning
/// `Some(MacdOutput)` once both underlying EMAs AND the signal EMA have
/// all warmed up.
pub struct Macd {
    fast_ema: Ema,
    slow_ema: Ema,
    signal_ema: Ema,
}

impl Macd {
    /// # Panics
    /// Panics if `fast_period == 0`, `slow_period == 0`, or `signal_period == 0`
    /// (via the underlying `Ema::new` calls).
    pub fn new(fast_period: usize, slow_period: usize, signal_period: usize) -> Self {
        Self {
            fast_ema: Ema::new(fast_period),
            slow_ema: Ema::new(slow_period),
            signal_ema: Ema::new(signal_period),
        }
    }
}

impl Indicator for Macd {
    type Input = Decimal;
    type Output = Option<MacdOutput>;

    fn update(&mut self, price: Decimal) -> Option<MacdOutput> {
        let fast = self.fast_ema.update(price);
        let slow = self.slow_ema.update(price);
        let (fast, slow) = (fast?, slow?);
        let macd = fast - slow;
        let signal = self.signal_ema.update(macd)?;
        Some(MacdOutput { macd, signal, histogram: macd - signal })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_during_warmup() {
        let mut macd = Macd::new(2, 3, 2);
        assert_eq!(macd.update(dec!(10)), None);
        assert_eq!(macd.update(dec!(12)), None);
    }

    #[test]
    fn matches_hand_derived_golden_values() {
        // prices [10, 12, 11, 15, 13, 20], fast=2 slow=3 signal=2.
        // Cross-checked against an independent Python re-implementation of
        // the same recursive EMA formulas.
        let mut macd = Macd::new(2, 3, 2);
        macd.update(dec!(10));
        macd.update(dec!(12));
        macd.update(dec!(11));
        let fourth = macd.update(dec!(15)).expect("warm at 4th price");
        assert!((fourth.macd - dec!(0.7037037037037042)).abs() < dec!(0.0001));
        assert!((fourth.signal - dec!(0.506172839506173)).abs() < dec!(0.0001));
        assert!((fourth.histogram - dec!(0.1975308641975312)).abs() < dec!(0.0001));

        let fifth = macd.update(dec!(13)).expect("still warm");
        assert!((fifth.macd - dec!(0.23456790123456805)).abs() < dec!(0.0001));
        assert!((fifth.signal - dec!(0.3251028806584364)).abs() < dec!(0.0001));
        assert!((fifth.histogram - dec!(-0.09053497942386834)).abs() < dec!(0.0001));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_fast_period_panics() {
        Macd::new(0, 3, 2);
    }
}
```

- [x] **Step 2: Write `Bollinger`**

Create `crates/apexes-indicators/src/bollinger.rs`:

```rust
//! Bollinger Bands.

use rust_decimal::Decimal;

use crate::indicator::Indicator;
use crate::sma::Sma;
use crate::stddev::RollingStdDev;

/// The three band values produced by one `Bollinger::update` call once warm.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BollingerOutput {
    pub upper: Decimal,
    /// Equal to the underlying SMA.
    pub middle: Decimal,
    pub lower: Decimal,
}

/// Bollinger Bands: an SMA (`middle`) plus upper/lower bands offset by
/// `num_std` rolling standard deviations, over the same trailing window.
pub struct Bollinger {
    sma: Sma,
    stddev: RollingStdDev,
    num_std: Decimal,
}

impl Bollinger {
    /// # Panics
    /// Panics if `period == 0` (via the underlying `Sma::new`/`RollingStdDev::new` calls).
    pub fn new(period: usize, num_std: Decimal) -> Self {
        Self { sma: Sma::new(period), stddev: RollingStdDev::new(period), num_std }
    }
}

impl Indicator for Bollinger {
    type Input = Decimal;
    type Output = Option<BollingerOutput>;

    fn update(&mut self, price: Decimal) -> Option<BollingerOutput> {
        let middle = self.sma.update(price)?;
        let std = self.stddev.update(price)?;
        let offset = self.num_std * std;
        Some(BollingerOutput { upper: middle + offset, middle, lower: middle - offset })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_during_warmup() {
        let mut boll = Bollinger::new(2, dec!(2));
        assert_eq!(boll.update(dec!(10)), None);
    }

    #[test]
    fn matches_hand_derived_golden_values() {
        // prices [10, 14, 10, 14], period=2, num_std=2 -> constant
        // mean=12, stddev=2.0, so bands are constant (16, 12, 8) at every
        // full window (alternating series has the same pairwise stats
        // each step).
        let mut boll = Bollinger::new(2, dec!(2));
        boll.update(dec!(10));
        let expected = BollingerOutput { upper: dec!(16), middle: dec!(12), lower: dec!(8) };
        assert_eq!(boll.update(dec!(14)), Some(expected));
        assert_eq!(boll.update(dec!(10)), Some(expected));
        assert_eq!(boll.update(dec!(14)), Some(expected));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Bollinger::new(0, dec!(2));
    }
}
```

- [x] **Step 3: Update `lib.rs`**

```rust
pub mod bollinger;
pub mod ema;
pub mod indicator;
pub mod macd;
pub mod rsi;
pub mod sma;
pub mod stddev;

pub use bollinger::{Bollinger, BollingerOutput};
pub use ema::Ema;
pub use indicator::Indicator;
pub use macd::{Macd, MacdOutput};
pub use rsi::Rsi;
pub use sma::Sma;
pub use stddev::RollingStdDev;
```

- [x] **Step 4: Run tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-indicators
```
Expected: `20 passed` (14 from Tasks 1-3 + 3 `Macd` + 3 `Bollinger`).

- [x] **Step 5: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-indicators --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-verify both commands).

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-indicators
git commit -S -s -m "feat: add Macd and Bollinger indicators"
git push origin master
```

---

### Task 5: Implement `Atr` and `AtrInput`

**Files:**
- Create: `crates/apexes-indicators/src/atr.rs`
- Modify: `crates/apexes-indicators/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_indicators::Indicator` (Task 1), `apexes_core::Bar` (already defined).
- Produces: `pub struct AtrInput { pub high: Decimal, pub low: Decimal, pub close: Decimal }`
  with `impl From<&apexes_core::Bar> for AtrInput`; `pub struct Atr` implementing
  `Indicator<Input = AtrInput, Output = Option<Decimal>>` with `pub fn new(period: usize) ->
  Self`. `AtrInput` consumed directly by the Drift Script binding layer in Task 6 (constructed
  from three validated `Decimal` fields, not via the `Bar` conversion).

- [x] **Step 1: Write `Atr`**

Create `crates/apexes-indicators/src/atr.rs`:

```rust
//! Average True Range.

use rust_decimal::Decimal;

use crate::indicator::Indicator;

/// The subset of OHLC fields `Atr` actually needs (high/low/close --
/// `Atr` never uses `open` or `volume`). `apexes_core::Bar` implements
/// `From<&Bar> for AtrInput` (defined here, not in `apexes-core`, to
/// avoid a reverse dependency) so Rust callers holding a real `Bar` can
/// pass it directly via `.into()`; the Drift Script binding layer
/// constructs an `AtrInput` straight from a script's `high`/`low`/`close`
/// fields without fabricating an entire fake `Bar`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtrInput {
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
}

impl From<&apexes_core::Bar> for AtrInput {
    fn from(bar: &apexes_core::Bar) -> Self {
        Self { high: bar.high, low: bar.low, close: bar.close }
    }
}

/// Average True Range (Wilder's smoothing), the only indicator in this
/// crate that needs more than a single price per update.
pub struct Atr {
    period: usize,
    prev_close: Option<Decimal>,
    avg_tr: Option<Decimal>,
    /// Number of true-range values folded into `avg_tr` so far.
    count: usize,
}

impl Atr {
    /// # Panics
    /// Panics if `period == 0`.
    pub fn new(period: usize) -> Self {
        assert!(period > 0, "Atr::new: period must be > 0, got {period}");
        Self { period, prev_close: None, avg_tr: None, count: 0 }
    }

    fn true_range(&self, input: AtrInput) -> Decimal {
        let base = input.high - input.low;
        match self.prev_close {
            None => base,
            Some(prev_close) => {
                let hc = (input.high - prev_close).abs();
                let lc = (input.low - prev_close).abs();
                base.max(hc).max(lc)
            }
        }
    }
}

impl Indicator for Atr {
    type Input = AtrInput;
    type Output = Option<Decimal>;

    fn update(&mut self, input: AtrInput) -> Option<Decimal> {
        let tr = self.true_range(input);
        self.prev_close = Some(input.close);

        if self.count < self.period {
            self.avg_tr = Some(match self.avg_tr {
                None => tr,
                Some(prev) => {
                    let n = Decimal::from(self.count as u64);
                    (prev * n + tr) / (n + Decimal::ONE)
                }
            });
            self.count += 1;
            if self.count < self.period {
                return None;
            }
        } else {
            let period_minus_one = Decimal::from((self.period - 1) as u64);
            let period = Decimal::from(self.period as u64);
            self.avg_tr = self
                .avg_tr
                .map(|prev| (prev * period_minus_one + tr) / period);
        }

        self.avg_tr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn bar(high: Decimal, low: Decimal, close: Decimal) -> AtrInput {
        AtrInput { high, low, close }
    }

    #[test]
    fn returns_none_during_warmup() {
        let mut atr = Atr::new(2);
        assert_eq!(atr.update(bar(dec!(10), dec!(8), dec!(9))), None);
    }

    #[test]
    fn matches_hand_derived_golden_values() {
        // bars: (10,8,9), (12,9,11), (11,9,10); period=2.
        // tr1 = 10-8 = 2 (no prev_close)
        // tr2 = max(12-9, |12-9|, |9-9|) = 3 -> avg_tr seeded=(2+3)/2=2.5
        // tr3 = max(11-9, |11-11|, |9-11|) = 2 -> avg_tr=(2.5*1+2)/2=2.25
        let mut atr = Atr::new(2);
        assert_eq!(atr.update(bar(dec!(10), dec!(8), dec!(9))), None);
        assert_eq!(atr.update(bar(dec!(12), dec!(9), dec!(11))), Some(dec!(2.5)));
        assert_eq!(atr.update(bar(dec!(11), dec!(9), dec!(10))), Some(dec!(2.25)));
    }

    #[test]
    fn from_bar_extracts_high_low_close_only() {
        let bar = apexes_core::Bar {
            symbol: "BTCUSDT".to_string(),
            venue: apexes_core::Venue::Binance,
            open: dec!(100),
            high: dec!(110),
            low: dec!(90),
            close: dec!(105),
            volume: dec!(1000),
            interval: apexes_core::BarInterval::OneMinute,
            open_time: chrono::Utc::now(),
            close_time: chrono::Utc::now(),
        };
        let input: AtrInput = (&bar).into();
        assert_eq!(input, AtrInput { high: dec!(110), low: dec!(90), close: dec!(105) });
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Atr::new(0);
    }
}
```

**Note:** the `Bar` literal in `from_bar_extracts_high_low_close_only` uses the full field list
confirmed by reading `crates/apexes-core/src/market_data.rs` directly (`symbol`, `venue`,
`open`, `high`, `low`, `close`, `volume`, `interval`, `open_time`, `close_time`) — `Venue` and
`BarInterval` are re-exported from `apexes_core`'s crate root alongside `Bar` (confirm via
`cargo doc` or `crates/apexes-core/src/lib.rs` if the import path differs; adjust the `use`/path
prefix to match, the field values themselves do not change).

- [x] **Step 2: Update `lib.rs`**

```rust
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
```

- [x] **Step 3: Run tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-indicators
```
Expected: `24 passed` (20 from Tasks 1-4 + 4 `Atr`). If `from_bar_extracts_high_low_close_only`
fails to compile because `apexes_core::Venue`/`BarInterval` aren't re-exported at the crate
root, check `crates/apexes-core/src/lib.rs` for the actual re-export path (e.g.
`apexes_core::instrument::Venue`) and fix the `use`/inline path accordingly — the field values
and assertions themselves are unaffected.

- [x] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-indicators --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-verify both commands).

- [x] **Step 5: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-indicators
git commit -S -s -m "feat: add Atr indicator and AtrInput"
git push origin master
```

---

### Task 6: Wire `indicators.*` into `apexes-strategy`'s Drift Script environment

**Files:**
- Modify: `crates/apexes-strategy/Cargo.toml` — add `apexes-indicators = { path =
  "../apexes-indicators" }` dependency
- Create: `crates/apexes-strategy/src/drift_dsl/indicators.rs`
- Modify: `crates/apexes-strategy/src/drift_dsl/mod.rs`
- Modify: `crates/apexes-strategy/src/drift_dsl/strategy.rs` — register the new binding

**Interfaces:**
- Consumes: `apexes_indicators::{Indicator, Sma, Ema, Rsi, Macd, MacdOutput, Bollinger,
  BollingerOutput, Atr, AtrInput, RollingStdDev}` (Tasks 1-5), `drift_script::{DriftValue,
  Interpreter, NativeFn}`.
- Produces: `pub fn register_to(interp: &Interpreter)` in `drift_dsl::indicators`, called from
  `DriftScriptStrategy::from_file` alongside the existing `drift_script_math::register_to` call.

- [x] **Step 1: Add the `apexes-indicators` dependency**

Edit `crates/apexes-strategy/Cargo.toml`, adding one line to `[dependencies]` (after
`apexes-core`):

```toml
[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-indicators = { path = "../apexes-indicators" }
drift-script = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "14af039f01436e25984b493ad9b27c1d5bc16a21" }
drift-script-math = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "14af039f01436e25984b493ad9b27c1d5bc16a21" }
tokio = { workspace = true }
thiserror = { workspace = true }
rust_decimal = { workspace = true }
chrono = { workspace = true }
tracing = { workspace = true }
```

- [x] **Step 2: Write `crates/apexes-strategy/src/drift_dsl/indicators.rs`**

```rust
//! `indicators.*` bindings exposed to Drift Script strategy scripts:
//! factory functions (`sma`/`ema`/`rsi`/`macd`/`bollinger`/`atr`/
//! `stddev`) that each construct an `apexes_indicators::*` instance and
//! return a script-facing object with a single `update` method. Unlike
//! `ctx.*` (rebuilt fresh on every `on_market_event` call, see
//! `bindings.rs`), these factories are meant to be called ONCE at a
//! script's top level (`let sma = indicators.sma(20);`) so the returned
//! object's `Arc<Mutex<_>>`-held state persists across ticks -- calling a
//! factory function itself is what constructs the indicator; the
//! returned `update` closure only ever mutates that one shared instance.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apexes_indicators::{
    Atr, AtrInput, Bollinger, BollingerOutput, Ema, Indicator, Macd, MacdOutput, RollingStdDev,
    Rsi, Sma,
};
use drift_script::{DriftValue, Interpreter, NativeFn};
use rust_decimal::Decimal;

/// Installs the `indicators` global object into `interp`'s environment.
pub fn register_to(interp: &Interpreter) {
    let mut indicators_map: HashMap<String, DriftValue> = HashMap::new();
    indicators_map.insert("sma".to_string(), DriftValue::NativeFunction(sma_factory()));
    indicators_map.insert("ema".to_string(), DriftValue::NativeFunction(ema_factory()));
    indicators_map.insert("rsi".to_string(), DriftValue::NativeFunction(rsi_factory()));
    indicators_map.insert("macd".to_string(), DriftValue::NativeFunction(macd_factory()));
    indicators_map
        .insert("bollinger".to_string(), DriftValue::NativeFunction(bollinger_factory()));
    indicators_map.insert("atr".to_string(), DriftValue::NativeFunction(atr_factory()));
    indicators_map.insert("stddev".to_string(), DriftValue::NativeFunction(stddev_factory()));
    interp.env().define_const("indicators".to_string(), DriftValue::Object(indicators_map));
}

fn error_object(message: String) -> DriftValue {
    let mut map = HashMap::new();
    map.insert("error".to_string(), DriftValue::String(Arc::new(message)));
    DriftValue::Object(map)
}

/// Extracts a positive `usize` period from `args[index]`, or an
/// `error_object` describing what went wrong (missing arg, non-number,
/// non-positive, non-integer).
fn require_positive_usize(args: &[DriftValue], index: usize, fn_name: &str) -> Result<usize, DriftValue> {
    match args.get(index) {
        Some(DriftValue::Number(n)) if *n > 0.0 && n.fract() == 0.0 => Ok(*n as usize),
        Some(DriftValue::Number(n)) => Err(error_object(format!(
            "{fn_name}: argument at position {index} must be a positive integer, got {n}"
        ))),
        Some(other) => Err(error_object(format!(
            "{fn_name}: argument at position {index} must be a number, got {}",
            other.type_name()
        ))),
        None => Err(error_object(format!("{fn_name}: missing argument at position {index}"))),
    }
}

fn require_number(args: &[DriftValue], index: usize, fn_name: &str) -> Result<Decimal, DriftValue> {
    match args.get(index) {
        Some(DriftValue::Number(n)) => Decimal::try_from(*n).map_err(|_| {
            error_object(format!(
                "{fn_name}: argument at position {index} is not a finite number ({n})"
            ))
        }),
        Some(other) => Err(error_object(format!(
            "{fn_name}: argument at position {index} must be a number, got {}",
            other.type_name()
        ))),
        None => Err(error_object(format!("{fn_name}: missing argument at position {index}"))),
    }
}

fn optional_decimal_field(obj: &DriftValue, key: &str, fn_name: &str) -> Result<Decimal, String> {
    let DriftValue::Object(map) = obj else {
        return Err(format!("{fn_name}: expected an object argument, got {}", obj.type_name()));
    };
    match map.get(key) {
        Some(DriftValue::Number(n)) => Decimal::try_from(*n)
            .map_err(|_| format!("{fn_name}: field {key:?} is not a finite number ({n})")),
        Some(other) => Err(format!(
            "{fn_name}: field {key:?} must be a number, got {}",
            other.type_name()
        )),
        None => Err(format!("{fn_name}: missing required field {key:?}")),
    }
}

/// Wraps a single-value indicator's `update` in a `DriftValue::Object`
/// with one `update` method: `Option<Decimal>` becomes `Number`/`Null`.
fn make_update_object<I>(indicator: I) -> DriftValue
where
    I: Indicator<Input = Decimal, Output = Option<Decimal>> + Send + 'static,
{
    let state = Arc::new(Mutex::new(indicator));
    let update: NativeFn = Arc::new(move |args| {
        let state = Arc::clone(&state);
        Box::pin(async move {
            let price = match require_number(&args, 0, "indicator.update") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let Ok(mut guard) = state.lock() else {
                return error_object("indicator.update: internal lock poisoned".to_string());
            };
            match guard.update(price) {
                Some(v) => DriftValue::Number(v.try_into().unwrap_or(f64::NAN)),
                None => DriftValue::Null,
            }
        })
    });
    let mut obj = HashMap::new();
    obj.insert("update".to_string(), DriftValue::NativeFunction(update));
    DriftValue::Object(obj)
}

fn sma_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_positive_usize(&args, 0, "indicators.sma") {
                Ok(period) => make_update_object(Sma::new(period)),
                Err(e) => e,
            }
        })
    })
}

fn ema_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_positive_usize(&args, 0, "indicators.ema") {
                Ok(period) => make_update_object(Ema::new(period)),
                Err(e) => e,
            }
        })
    })
}

fn rsi_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_positive_usize(&args, 0, "indicators.rsi") {
                Ok(period) => make_update_object(Rsi::new(period)),
                Err(e) => e,
            }
        })
    })
}

fn stddev_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_positive_usize(&args, 0, "indicators.stddev") {
                Ok(period) => make_update_object(RollingStdDev::new(period)),
                Err(e) => e,
            }
        })
    })
}

fn macd_output_to_drift_value(output: MacdOutput) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert("macd".to_string(), DriftValue::Number(output.macd.try_into().unwrap_or(f64::NAN)));
    obj.insert(
        "signal".to_string(),
        DriftValue::Number(output.signal.try_into().unwrap_or(f64::NAN)),
    );
    obj.insert(
        "histogram".to_string(),
        DriftValue::Number(output.histogram.try_into().unwrap_or(f64::NAN)),
    );
    DriftValue::Object(obj)
}

fn macd_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let fast = match require_positive_usize(&args, 0, "indicators.macd") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let slow = match require_positive_usize(&args, 1, "indicators.macd") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let signal = match require_positive_usize(&args, 2, "indicators.macd") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let state = Arc::new(Mutex::new(Macd::new(fast, slow, signal)));
            let update: NativeFn = Arc::new(move |args| {
                let state = Arc::clone(&state);
                Box::pin(async move {
                    let price = match require_number(&args, 0, "macd.update") {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let Ok(mut guard) = state.lock() else {
                        return error_object("macd.update: internal lock poisoned".to_string());
                    };
                    match guard.update(price) {
                        Some(output) => macd_output_to_drift_value(output),
                        None => DriftValue::Null,
                    }
                })
            });
            let mut obj = HashMap::new();
            obj.insert("update".to_string(), DriftValue::NativeFunction(update));
            DriftValue::Object(obj)
        })
    })
}

fn bollinger_output_to_drift_value(output: BollingerOutput) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert("upper".to_string(), DriftValue::Number(output.upper.try_into().unwrap_or(f64::NAN)));
    obj.insert(
        "middle".to_string(),
        DriftValue::Number(output.middle.try_into().unwrap_or(f64::NAN)),
    );
    obj.insert("lower".to_string(), DriftValue::Number(output.lower.try_into().unwrap_or(f64::NAN)));
    DriftValue::Object(obj)
}

fn bollinger_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let period = match require_positive_usize(&args, 0, "indicators.bollinger") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let num_std = match require_number(&args, 1, "indicators.bollinger") {
                Ok(n) => n,
                Err(e) => return e,
            };
            let state = Arc::new(Mutex::new(Bollinger::new(period, num_std)));
            let update: NativeFn = Arc::new(move |args| {
                let state = Arc::clone(&state);
                Box::pin(async move {
                    let price = match require_number(&args, 0, "bollinger.update") {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let Ok(mut guard) = state.lock() else {
                        return error_object("bollinger.update: internal lock poisoned".to_string());
                    };
                    match guard.update(price) {
                        Some(output) => bollinger_output_to_drift_value(output),
                        None => DriftValue::Null,
                    }
                })
            });
            let mut obj = HashMap::new();
            obj.insert("update".to_string(), DriftValue::NativeFunction(update));
            DriftValue::Object(obj)
        })
    })
}

fn atr_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let period = match require_positive_usize(&args, 0, "indicators.atr") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let state = Arc::new(Mutex::new(Atr::new(period)));
            let update: NativeFn = Arc::new(move |args| {
                let state = Arc::clone(&state);
                Box::pin(async move {
                    let Some(bar_obj) = args.first() else {
                        return error_object("atr.update: missing bar argument".to_string());
                    };
                    let high = match optional_decimal_field(bar_obj, "high", "atr.update") {
                        Ok(v) => v,
                        Err(msg) => return error_object(msg),
                    };
                    let low = match optional_decimal_field(bar_obj, "low", "atr.update") {
                        Ok(v) => v,
                        Err(msg) => return error_object(msg),
                    };
                    let close = match optional_decimal_field(bar_obj, "close", "atr.update") {
                        Ok(v) => v,
                        Err(msg) => return error_object(msg),
                    };
                    let Ok(mut guard) = state.lock() else {
                        return error_object("atr.update: internal lock poisoned".to_string());
                    };
                    match guard.update(AtrInput { high, low, close }) {
                        Some(v) => DriftValue::Number(v.try_into().unwrap_or(f64::NAN)),
                        None => DriftValue::Null,
                    }
                })
            });
            let mut obj = HashMap::new();
            obj.insert("update".to_string(), DriftValue::NativeFunction(update));
            DriftValue::Object(obj)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(f: &NativeFn, args: Vec<DriftValue>) -> DriftValue {
        futures::executor::block_on(f(args))
    }

    #[test]
    fn sma_factory_rejects_zero_period() {
        let factory = sma_factory();
        let result = call(&factory, vec![DriftValue::Number(0.0)]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn sma_update_returns_null_during_warmup_then_number() {
        let factory = sma_factory();
        let sma_obj = call(&factory, vec![DriftValue::Number(3.0)]);
        let DriftValue::Object(obj) = sma_obj else { panic!("expected Object") };
        let DriftValue::NativeFunction(update) = obj.get("update").unwrap().clone() else {
            panic!("expected update NativeFunction")
        };
        assert!(matches!(call(&update, vec![DriftValue::Number(10.0)]), DriftValue::Null));
        assert!(matches!(call(&update, vec![DriftValue::Number(11.0)]), DriftValue::Null));
        let third = call(&update, vec![DriftValue::Number(12.0)]);
        assert!(matches!(third, DriftValue::Number(n) if (n - 11.0).abs() < 1e-9));
    }

    #[test]
    fn macd_update_returns_object_with_three_fields_once_warm() {
        let factory = macd_factory();
        let macd_obj = call(
            &factory,
            vec![DriftValue::Number(2.0), DriftValue::Number(3.0), DriftValue::Number(2.0)],
        );
        let DriftValue::Object(obj) = macd_obj else { panic!("expected Object") };
        let DriftValue::NativeFunction(update) = obj.get("update").unwrap().clone() else {
            panic!("expected update NativeFunction")
        };
        for price in [10.0, 12.0, 11.0] {
            call(&update, vec![DriftValue::Number(price)]);
        }
        let fourth = call(&update, vec![DriftValue::Number(15.0)]);
        let DriftValue::Object(output) = fourth else { panic!("expected Object output") };
        assert!(output.contains_key("macd"));
        assert!(output.contains_key("signal"));
        assert!(output.contains_key("histogram"));
    }

    #[test]
    fn atr_update_reads_high_low_close_from_object_argument() {
        let factory = atr_factory();
        let atr_obj = call(&factory, vec![DriftValue::Number(2.0)]);
        let DriftValue::Object(obj) = atr_obj else { panic!("expected Object") };
        let DriftValue::NativeFunction(update) = obj.get("update").unwrap().clone() else {
            panic!("expected update NativeFunction")
        };
        let bar_arg = |h: f64, l: f64, c: f64| {
            let mut m = HashMap::new();
            m.insert("high".to_string(), DriftValue::Number(h));
            m.insert("low".to_string(), DriftValue::Number(l));
            m.insert("close".to_string(), DriftValue::Number(c));
            DriftValue::Object(m)
        };
        assert!(matches!(call(&update, vec![bar_arg(10.0, 8.0, 9.0)]), DriftValue::Null));
        let second = call(&update, vec![bar_arg(12.0, 9.0, 11.0)]);
        assert!(matches!(second, DriftValue::Number(n) if (n - 2.5).abs() < 1e-9));
    }

    #[test]
    fn atr_update_rejects_non_object_argument() {
        let factory = atr_factory();
        let atr_obj = call(&factory, vec![DriftValue::Number(2.0)]);
        let DriftValue::Object(obj) = atr_obj else { panic!("expected Object") };
        let DriftValue::NativeFunction(update) = obj.get("update").unwrap().clone() else {
            panic!("expected update NativeFunction")
        };
        let result = call(&update, vec![DriftValue::Number(9.0)]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }
}
```

- [x] **Step 3: Run the new module's tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy drift_dsl::indicators:: -- --nocapture
```
Expected: this will FAIL TO COMPILE at this point, because `mod.rs` doesn't yet declare
`indicators` as a submodule and `strategy.rs` doesn't register it. That's expected — proceed to
Step 4.

- [x] **Step 4: Wire the new module into `mod.rs`**

Read `crates/apexes-strategy/src/drift_dsl/mod.rs` (current content from the prior plan):

```rust
mod bindings;
mod convert;
mod strategy;

pub use strategy::DriftScriptStrategy;
```

Edit to add the new module (it has no public re-export of its own — `register_to` is called
internally by `strategy.rs`, not exposed to `apexes-strategy`'s own public API):

```rust
mod bindings;
mod convert;
mod indicators;
mod strategy;

pub use strategy::DriftScriptStrategy;
```

- [x] **Step 5: Register `indicators::register_to` in `DriftScriptStrategy::from_file`**

In `crates/apexes-strategy/src/drift_dsl/strategy.rs`, find this existing line:

```rust
        let mut interpreter = Interpreter::new();
        drift_script_math::register_to(&interpreter);
```

Replace it with:

```rust
        let mut interpreter = Interpreter::new();
        drift_script_math::register_to(&interpreter);
        crate::drift_dsl::indicators::register_to(&interpreter);
```

- [x] **Step 6: Run the new module's tests again**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy drift_dsl::indicators:: -- --nocapture
```
Expected: `5 passed` (`sma_factory_rejects_zero_period`,
`sma_update_returns_null_during_warmup_then_number`,
`macd_update_returns_object_with_three_fields_once_warm`,
`atr_update_reads_high_low_close_from_object_argument`,
`atr_update_rejects_non_object_argument`).

- [x] **Step 7: Run the full `apexes-strategy` test suite**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy
```
Expected: all prior tests (25 unit + 7 integration from the previous plan) plus the 5 new
`indicators` tests = `30 passed` in the unit-test binary, `7 passed` in the integration-test
binary, 0 failed.

- [x] **Step 8: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings. If `cargo fmt --check` reports diffs (very likely for the long match
arms/nested closures in `indicators.rs`), run `cargo fmt` and re-run `cargo test -p
apexes-strategy` + both clippy/fmt commands again to confirm nothing regressed and formatting is
clean.

- [x] **Step 9: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/Cargo.toml crates/apexes-strategy/src/drift_dsl
git commit -S -s -m "feat: wire apexes-indicators into DriftScriptStrategy as the indicators.* binding"
git push origin master
```

---

### Task 7: End-to-end fixture test proving cross-tick indicator state persistence

**Files:**
- Create: `crates/apexes-strategy/tests/fixtures/uses_indicators.drift`
- Modify: `crates/apexes-strategy/tests/drift_script_strategy.rs`

**Interfaces:**
- Consumes: `apexes_strategy::drift_dsl::DriftScriptStrategy`, `apexes_strategy::{Strategy,
  StrategyContext}` (all pre-existing from the prior plan).
- Produces: nothing new consumed by later tasks — this is the plan's final behavioral proof
  point.

- [x] **Step 1: Create the fixture script**

```bash
mkdir -p ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures
cat > ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures/uses_indicators.drift << 'EOF'
let sma = indicators.sma(3);

function on_market_event(event, ctx) {
  // drift-script does not support `??` (nullish coalescing) -- its parser
  // accepts the syntax but the lowering pass explicitly rejects it with
  // "nullish coalescing not supported" (confirmed by reading
  // `lower_logical_expression` in drift-dsl's `crates/drift-script/src/parser.rs`).
  // Use a ternary keyed off the `event.type` tag field that
  // `convert.rs`'s `market_event_to_drift_value` always sets instead.
  let price = event.type == "bar" ? event.close : event.price;
  let sma_value = sma.update(price);

  if (sma_value == null) {
    // Still warming up -- not enough data points yet.
    return [];
  }

  if (price > sma_value) {
    return [{
      symbol: event.symbol,
      venue: event.venue,
      side: "buy",
      order_type: "market",
      quantity: 1,
    }];
  }

  return [];
}
EOF
```

This exercises exactly the behavior the design's warm-up/persistence model depends on: the
first two `on_market_event` calls must return `[]` (SMA(3) not yet warm), and only the third
call onward can produce an order -- which is only possible if the `sma` object created once at
script top level is the SAME instance mutated across all three `on_market_event` invocations
(proving the `Arc<Mutex<_>>`-backed top-level-script-state design from decision 1 actually
works end to end, not just at the unit-test level).

- [x] **Step 2: Add the driver tests**

Read `crates/apexes-strategy/tests/drift_script_strategy.rs` (from the prior plan) to confirm
its existing `with_runtime`/`fixture_path`/`sample_trade_event` helpers are still present, then
append these two new `#[test]` functions at the end of the file (same sync-`#[test]` +
`with_runtime` pattern as the existing 7 tests, for the same
`Handle::current().block_on()`-inside-`#[tokio::test]` panic reason documented on
`with_runtime`'s doc comment):

```rust
#[test]
fn uses_indicators_sma_stays_flat_during_warmup_then_buys() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_indicators.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };

        // SMA(3) warm-up: first two calls, regardless of price, must
        // return no orders (sma.update returns null on the script side).
        let first_event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(10),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        assert_eq!(strategy.on_market_event(&first_event, &ctx).len(), 0);

        let second_event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(11),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        assert_eq!(strategy.on_market_event(&second_event, &ctx).len(), 0);

        // Third call: SMA(3) over [10, 11, 12] = 11; price 12 > 11 -> buy.
        let third_event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(12),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        let intents = strategy.on_market_event(&third_event, &ctx);
        assert_eq!(intents.len(), 1, "SMA should be warm by the 3rd tick and trigger a buy");
        assert_eq!(intents[0].side, Side::Buy);
    });
}

#[test]
fn uses_indicators_sma_state_is_independent_per_strategy_instance() {
    with_runtime(|| {
        // A second, freshly-constructed DriftScriptStrategy must start
        // its own SMA warm-up from scratch -- proving indicator state is
        // NOT accidentally shared across separate script instances (e.g.
        // via a process-global registry), only within one instance's
        // persistent top-level environment.
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_indicators.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(999),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        assert_eq!(
            strategy.on_market_event(&event, &ctx).len(),
            0,
            "a brand-new strategy instance must start its own warm-up, not inherit state"
        );
    });
}
```

- [x] **Step 3: Run the new tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy --test drift_script_strategy -- --nocapture
```
Expected: `9 passed` (7 from the prior plan + 2 new). If
`uses_indicators_sma_stays_flat_during_warmup_then_buys` fails on the third call's assertion,
check whether `event.type == "bar" ? event.close : event.price` resolved to the right field for
a `TradeEvent` (which has `type: "trade"` and a `price` field, not `close` -- confirm via
`crates/apexes-strategy/src/drift_dsl/convert.rs`'s `MarketEvent::Trade` conversion branch that
the object's `"type"` field is exactly the string `"trade"`, so the ternary's condition is
`false` and it correctly evaluates to `event.price`) before changing the fixture script.

- [x] **Step 4: Run the full `apexes-strategy` test suite one more time**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy
```
Expected: `30 passed` (unit tests) + `9 passed` (integration tests), 0 failed.

- [x] **Step 5: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-verify both commands).

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/tests
git commit -S -s -m "test: add end-to-end fixture proving indicator state persists across ticks"
git push origin master
```

---

### Task 8: Final whole-effort verification

**Files:** None created/modified — this task only runs verification commands, updates
`docs/FEATURES.md`, and confirms the end state.

- [x] **Step 1: Full workspace build**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 2: Full workspace test suite**

```bash
cd ~/src/oss/apexes
cargo test --workspace
```
Expected: all tests across all crates pass, including the new `apexes-indicators` crate's 24
unit tests, and `apexes-strategy`'s now-30 unit + 9 integration tests. Report the exact total
test count broken down by crate, and explicitly confirm `apexes-backtest`'s pre-existing tests
are unaffected (they don't touch `apexes-indicators` at all in this plan).

- [x] **Step 3: Full workspace clippy**

```bash
cd ~/src/oss/apexes
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: 0 warnings.

- [x] **Step 4: Full workspace fmt check**

```bash
cd ~/src/oss/apexes
cargo fmt --check
```
Expected: 0 diffs.

- [x] **Step 5: Update `docs/FEATURES.md`**

Read `docs/FEATURES.md`, find row `2.2` (`Factor library & technical indicator library`,
currently `🔲 Not Started`). Update its Status to `✅ Done` and its Notes column to:
```
apexes-indicators crate: streaming Indicator trait (SMA/EMA/RSI/MACD/Bollinger/ATR/rolling stddev, all O(1) amortized per update); wired into DriftScriptStrategy as the indicators.* Drift Script binding (indicators.sma(period).update(price) etc.), with cross-tick state persistence proven end to end.
```

Update the Summary counts table at the bottom of the file (increment `✅ Done` by 1, decrement
`🔲 Not Started` by 1, keep `**Total**` unchanged) and the "Current active work" paragraph to
note sub-project `#4`'s second phase (`2.2`) is now also complete, alongside `2.1` from the
prior plan; leave the "what's next" decision unstated (per PyO3 bindings being explicitly
deferred to the end of `#4`, and `2.3`/`2.4`/`2.5`/`#5` all still open — don't guess which one
the user picks next).

- [x] **Step 6: Commit the FEATURES.md update**

```bash
cd ~/src/oss/apexes
git add docs/FEATURES.md
git commit -S -s -m "docs: mark factor/technical-indicator library complete in FEATURES.md"
git push origin master
```

- [x] **Step 7: Report final summary**

Summarize: `apexes-indicators` crate's file list and line counts, total test count (broken
down: unit tests per indicator file), `apexes-strategy`'s updated test count (unit +
integration), confirmation that `apexes-backtest`'s and `apexes-market-data`'s and
`apexes-storage`'s and `apexes-core`'s pre-existing tests still pass unchanged, and the
`apexes` commit SHAs for Tasks 1-8. No further commit needed for this verification-only task.

---

## Plan Self-Review Notes

**Spec coverage:** Design spec §一 (scope: new `apexes-indicators` crate, `Indicator` trait, 7
indicators, Drift Script `indicators.*` binding, explicitly no PyO3/batch-API/other-indicators)
→ Tasks 1-6 cover the crate + binding, Task 8 confirms nothing extra was added. §二 (crate
structure) → Task 1 Step 1-5 (file layout matches exactly: `indicator.rs`, one file per
indicator). §三 (`Indicator` trait) → Task 1 Step 3. §四·4.1-4.7 (all 7 indicators, including
exact recursive formulas for EMA/RSI/MACD/ATR) → Tasks 1 (SMA), 2 (EMA, stddev), 3 (RSI), 4
(MACD, Bollinger), 5 (ATR + `AtrInput`). §五 (Drift Script binding: factory functions, `update`
method, `Arc<Mutex<_>>` persistence, MACD/Bollinger struct outputs, ATR reading
high/low/close from the script's `event` object) → Task 6. §六 decision 1 (why `Arc<Mutex<_>>`
+ top-level script state, not per-call rebuild like `ctx.*`) → directly informs Task 6's
`make_update_object` design and Task 7's persistence-proof test. §六 decision 2 (`AtrInput` not
`Bar`) → Task 5. §六 decision 3 (Rust `panic!` vs script-level `error_object`) → Task 1-5's
`#[should_panic]` tests (Rust side) and Task 6's `require_positive_usize`/`require_number`
(script side, never calling the panicking constructors with unvalidated input). §七 (testing
strategy: unit tests with golden values, DSL binding unit tests, end-to-end fixture proving
cross-tick persistence, full workspace verification) → Tasks 1-5 (golden-value unit tests), Task
6 Step 2 (binding unit tests), Task 7 (end-to-end persistence proof), Task 8 (full workspace).
§八 (non-goals: no PyO3, no batch API, no other indicators, no hot-reload, no persistence/cache)
— confirmed none of Tasks 1-8 do any of these.

**Placeholder scan:** No "TBD"/"TODO" strings in any task. Every code step shows complete,
copy-pasteable code, including full golden-value derivations shown inline as code comments
(SMA/EMA/RSI/MACD/Bollinger/ATR test values were independently computed via a Python
reference implementation during plan-writing, not guessed).

**Type consistency:** `Indicator::{Input, Output}` associated types are used identically across
every indicator (Tasks 1-5) and consumed identically in the binding layer (Task 6's
`make_update_object<I: Indicator<Input = Decimal, Output = Option<Decimal>>>` generic function,
and MACD/Bollinger/ATR's individually-inlined equivalents since their `Output` types differ).
`MacdOutput{macd,signal,histogram}` and `BollingerOutput{upper,middle,lower}` field names
defined in Task 4 are referenced identically in Task 6's `macd_output_to_drift_value`/
`bollinger_output_to_drift_value`. `AtrInput{high,low,close}` defined in Task 5 is constructed
identically in Task 6's `atr_factory`'s `update` closure. `apexes_indicators::Indicator` (the
trait name) matches its `pub use indicator::Indicator;` re-export path used by every later
task's `use apexes_indicators::{Indicator, ...}` import. The Drift Script binding's
`register_to(interp: &Interpreter)` signature (Task 6 Step 2) matches its call site in Task 6
Step 5 (`crate::drift_dsl::indicators::register_to(&interpreter)`), mirroring the exact same
pattern already used for `drift_script_math::register_to(&interpreter)` one line above it.
