# GPU K-line + Tick-line Chart Engine (Sub-project #13b) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in `docs/superpowers/specs/2026-08-05-apexes-ui-chart-engine-design.md`: a new `apexes-chart` crate providing a hand-written `wgpu`-rendered `CandlestickChart` and `TickLineChart`, embedded into `apexes-ui`'s existing dashboard below the bid/ask/last-trade table, both interactive (scroll-zoom, drag-pan, sharing one visible time range) and fed by real-time `MarketEvent`s -- `Trade`s aggregated into OHLCV bars via `apexes_market_data::BarAggregator`, `Tick`s accumulated into a time-windowed bid/ask ring buffer.

**Architecture:** Seven sequential tasks. Task 1 extends `apexes_market_data::BarAggregator` with a `current_open_bar` accessor (needed so the K-line chart can show the still-forming candle, not just closed ones). Task 2 scaffolds `apexes-chart` and its pure-logic `view_state` module (zoom/pan/auto-fit math, no GPU/egui deps). Task 3 adds `geometry.rs` (bar/tick-to-vertex conversion, still pure logic). Task 4 builds the shared `wgpu` pipeline plumbing and the `CandlestickChart` widget. Task 5 builds `TickLineChart`, reusing Task 4's pipeline infrastructure. Task 6 wires both widgets into `apexes-ui`'s dashboard (per-symbol `BarAggregator` + tick ring buffer, symbol/interval selectors, shared `ViewState`). Task 7 is a full-workspace verification checkpoint plus the final whole-branch review and `docs/FEATURES.md` update.

**Tech Stack:** Rust 2024, new workspace dependencies `egui-wgpu` (0.35, pinned to match the already-pinned `egui`/`eframe` 0.35), `wgpu` (29.0, the exact version `egui-wgpu` 0.35.0 itself depends on), `bytemuck` (1.25, matching `egui-wgpu`'s own pinned version). Existing workspace/crate deps reused: `apexes-core`, `apexes-market-data` (`BarAggregator`, `Bar`, `BarInterval`), `apexes-ui`'s existing `app_state`/`client`/`ui` modules, `chrono`, `rust_decimal`, `egui_kittest` (dev-dependency, now also needs its `wgpu` feature).

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/`cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production code (test code may use them freely).
- Every commit signed: `git commit -S -s`.
- No new `ApexesError` variants -- `apexes-chart` is a pure rendering crate with no fallible network/IO operations of its own.
- **Real hand-written `wgpu` rendering, not `egui_plot`.** Both `CandlestickChart` and `TickLineChart` implement `egui_wgpu::CallbackTrait` with their own `wgpu::RenderPipeline`, vertex/fragment shaders (WGSL), and vertex buffers -- `egui_plot`'s interaction conventions (scroll-to-zoom, drag-to-pan) are the UX reference point, not its rendering path.
- **No ClickHouse historical backfill.** Both charts start empty at process launch and fill in as real-time data arrives -- no historical query code in this plan.
- **Red for close > open, green for close <= open** (this project's chosen candle-color convention -- the reverse of TradingView's default).
- **Y-axis auto-fits independently per chart** (candlestick fits visible bars' high/low; tick-line fits visible ticks' bid/ask range) while both charts share one `ViewState`'s X-axis (time) range.
- **Tick ring buffer retains only the last 1 hour**, evicted on every push (not on a timer), so it can never grow unbounded regardless of tick rate.
- **`ViewState.user_adjusted` semantics**: once `zoom`/`pan` is called, auto-advance-to-follow-new-data stops permanently for that `ViewState` instance (switching symbols constructs a fresh one, which is the only way back to "follow live" mode).
- A `wgpu` shader compile or pipeline creation failure must panic at `apexes-ui` startup with a clear message (the same fail-fast convention 13a established for WebSocket bind/subscribe failures) -- never a silent fallback to a blank chart.
- **Known environment dependency, not a blocker for this plan:** the `egui_kittest` `wgpu`-feature headless render tests (Tasks 4/5) require a working `wgpu` backend at test-run time -- confirmed working on the current macOS development environment (Metal backend). If this project's tests are ever run on a GPU-less Linux CI machine, a software rasterizer (e.g. Mesa `llvmpipe`) would need to be installed for those two tests to pass.
- Full design rationale (why hand-written `wgpu` over `egui_plot`, why both charts share one `ViewState`, why 13b has no historical backfill) is in the design spec -- read it before starting if anything below is unclear on *why*, not just *what*.

---

### Task 1: `apexes_market_data::BarAggregator::current_open_bar`

**Files:**
- Modify: `crates/apexes-market-data/src/bar_aggregator.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `BarAggregator::current_open_bar(&self, interval: BarInterval) -> Option<apexes_core::Bar>` (used by Task 6's `apexes-ui` integration, so the K-line chart can show the still-forming candle in addition to closed ones).

- [ ] **Step 1: Write the failing test**

```rust
// crates/apexes-market-data/src/bar_aggregator.rs -- add to the
// existing #[cfg(test)] mod tests block, after the last existing test
// function (all_six_intervals_are_maintained_independently):

#[test]
fn current_open_bar_returns_none_before_any_trade() {
    let agg = BarAggregator::new("BTCUSDT", Venue::Binance);
    assert_eq!(agg.current_open_bar(BarInterval::OneMinute), None);
}

#[test]
fn current_open_bar_reflects_the_still_forming_bucket() {
    let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
    agg.on_trade(dec!(100), dec!(1), t(0));
    agg.on_trade(dec!(110), dec!(2), t(10));

    let open_bar = agg
        .current_open_bar(BarInterval::OneMinute)
        .expect("a trade landed inside the [0,60) OneMinute bucket, which has not closed yet");

    assert_eq!(open_bar.open, dec!(100));
    assert_eq!(open_bar.high, dec!(110));
    assert_eq!(open_bar.low, dec!(100));
    assert_eq!(open_bar.close, dec!(110));
    assert_eq!(open_bar.volume, dec!(3));
    assert_eq!(open_bar.symbol, "BTCUSDT");
    assert_eq!(open_bar.venue, Venue::Binance);
    assert_eq!(open_bar.interval, BarInterval::OneMinute);
}

#[test]
fn current_open_bar_is_none_for_an_interval_with_no_trades_yet() {
    let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
    agg.on_trade(dec!(100), dec!(1), t(0));
    // OneDay's bucket exists (every interval accumulates in parallel per
    // on_trade's own doc comment), so this asserts current_open_bar
    // still correctly reports Some for it, not just OneMinute.
    assert!(agg.current_open_bar(BarInterval::OneDay).is_some());
}

#[test]
fn current_open_bar_does_not_consume_the_open_bucket() {
    // Calling current_open_bar must not close/remove the bucket --
    // on_trade must still be able to accumulate into it afterward.
    let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
    agg.on_trade(dec!(100), dec!(1), t(0));
    let _ = agg.current_open_bar(BarInterval::OneMinute);
    let _ = agg.current_open_bar(BarInterval::OneMinute);
    agg.on_trade(dec!(120), dec!(1), t(10));

    let open_bar = agg.current_open_bar(BarInterval::OneMinute).unwrap();
    assert_eq!(open_bar.high, dec!(120));
    assert_eq!(open_bar.volume, dec!(2));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p apexes-market-data current_open_bar`
Expected: FAIL with "no method named `current_open_bar` found" -- expected; Step 3 implements it.

- [ ] **Step 3: Implement `current_open_bar`**

```rust
// crates/apexes-market-data/src/bar_aggregator.rs -- add this method
// to the existing `impl BarAggregator` block, immediately after
// `on_trade`'s closing brace (before the block's own closing brace):

    /// Returns the still-forming (not yet closed) bar for `interval`,
    /// if any trade has landed in the current bucket -- `None` if no
    /// trade has arrived for this interval yet. Does not consume or
    /// modify the open bucket; `on_trade` can still accumulate into
    /// it afterward. Used by the K-line chart (sub-project #13b) to
    /// show the currently-forming candle alongside closed ones,
    /// rather than only ever showing history.
    pub fn current_open_bar(&self, interval: BarInterval) -> Option<Bar> {
        self.open_bars.get(&interval).map(|open_bar| Bar {
            symbol: self.symbol.clone(),
            venue: self.venue,
            open: open_bar.open,
            high: open_bar.high,
            low: open_bar.low,
            close: open_bar.close,
            volume: open_bar.volume,
            interval,
            open_time: open_bar.open_time,
            close_time: open_bar.close_time,
        })
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p apexes-market-data current_open_bar`
Expected: all 4 new tests pass.

- [ ] **Step 5: Run this crate's full test suite, clippy, fmt, workspace check**

Run: `cargo test -p apexes-market-data`
Expected: all pass (existing tests + 4 new).

Run: `cargo clippy -p apexes-market-data --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-market-data/src/bar_aggregator.rs
git commit -S -s -m "feat(apexes-market-data): BarAggregator::current_open_bar -- expose the still-forming candle (sub-project #13b)"
```

---

### Task 2: `apexes-chart` crate scaffold + `view_state.rs`

**Files:**
- Modify: `Cargo.toml` (workspace `members` + new `[workspace.dependencies]` entries)
- Create: `crates/apexes-chart/Cargo.toml`
- Create: `crates/apexes-chart/src/lib.rs`
- Create: `crates/apexes-chart/src/view_state.rs`

**Interfaces:**
- Produces: `apexes_chart::view_state::ViewState` -- `ViewState::new_following_live() -> Self` (constructs with the default "now minus 5 minutes" to "now" range, `user_adjusted: false`), `ViewState::zoom(&mut self, cursor_fraction: f32, delta: f32)`, `ViewState::pan(&mut self, delta_fraction: f32)`, `ViewState::advance_if_following_live(&mut self, new_data_time: chrono::DateTime<chrono::Utc>)`, `ViewState::visible_range(&self) -> &std::ops::Range<chrono::DateTime<chrono::Utc>>`, `ViewState::y_axis_range_for(&self, values: impl Iterator<Item = rust_decimal::Decimal>) -> Option<std::ops::Range<rust_decimal::Decimal>>`. Used by Task 3's `geometry.rs`, Tasks 4/5's chart widgets, and Task 6's `apexes-ui` integration.

- [ ] **Step 1: Add the new crate to the workspace and pin the new dependencies**

```toml
# Cargo.toml -- change the workspace members list (add
# "crates/apexes-chart" at the end) and add three new entries to
# [workspace.dependencies] (keep every existing line exactly as-is,
# only append):

[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators", "crates/apexes-optimize", "crates/apexes-strategy-registry", "crates/apexes-risk", "crates/apexes-paper", "crates/apexes-oms", "crates/apexes-gateway-binance", "crates/apexes-gateway-xtp", "crates/apexes-gateway-ibkr", "crates/apexes-live-trading", "crates/apexes-ems", "crates/apexes-account", "crates/apexes-market-data-chain", "crates/apexes-market-data-ashare", "crates/apexes-market-data-hshare", "crates/apexes-market-data-usequity", "crates/apexes-message-bus", "crates/apexes-ui", "crates/apexes-chart"]

[workspace.package]
edition = "2024"
version = "0.1.0"
license = "MIT"

[workspace.dependencies]
rust_decimal = { version = "1.42", features = ["serde", "maths"] }
serde = { version = "1.0", features = ["derive"] }
thiserror = "2.0"
chrono = { version = "0.4", features = ["serde"] }
tokio = { version = "1.53", features = ["rt-multi-thread", "macros"] }
futures = "0.3"
async-trait = "0.1.91"
uuid = { version = "1.24", features = ["v4", "serde"] }
clap = { version = "4.6", features = ["derive"] }
serde_json = "1.0"
barter-data = "0.11"
barter-instrument = "0.3"
tracing = "0.1"
clickhouse = { version = "0.15", features = ["chrono"] }
fixnum = { version = "0.9", features = ["i128", "serde"] }
barter = "0.12"
sha2 = "0.10"
rand = "0.10"
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres", "chrono", "rust_decimal", "uuid"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
tokio-tungstenite = { version = "0.26", features = ["rustls-tls-webpki-roots"] }
hmac = "0.12"
hex = "0.4"
axum = "0.8"
tracing-subscriber = "0.3"
eframe = "0.35"
egui = "0.35"
egui_kittest = { version = "0.35", features = ["eframe"] }
egui-wgpu = "0.35"
wgpu = "29.0"
bytemuck = { version = "1.25", features = ["derive"] }
```

- [ ] **Step 2: Create the crate manifest**

```toml
# crates/apexes-chart/Cargo.toml
[package]
name = "apexes-chart"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
egui = { workspace = true }
egui-wgpu = { workspace = true }
wgpu = { workspace = true }
bytemuck = { workspace = true }
chrono = { workspace = true }
rust_decimal = { workspace = true }

[dev-dependencies]
egui_kittest = { workspace = true, features = ["wgpu"] }
rust_decimal_macros = "1.40"
```

- [ ] **Step 3: Write the failing tests for `view_state.rs`**

```rust
// crates/apexes-chart/src/view_state.rs -- create the file with this
// content:

//! Pure-logic shared view state for both chart widgets: the visible
//! time range, zoom/pan interaction math, and Y-axis auto-fit
//! computation. No `wgpu`/`egui` dependency -- unit-testable directly.
//! One `ViewState` is constructed by the caller (`apexes-ui`) and
//! shared by mutable reference between `CandlestickChart::show` and
//! `TickLineChart::show` on every frame, keeping both charts'
//! visible time range synchronized.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::ops::Range;

/// How much history the default "follow live" view shows.
const DEFAULT_WINDOW_SECONDS: i64 = 5 * 60;

/// Fractional padding added above/below an auto-fit Y range so
/// candle wicks / line peaks don't touch the viewport edge.
const Y_AXIS_PADDING_FRACTION: f64 = 0.05;

pub struct ViewState {
    visible_range: Range<DateTime<Utc>>,
    /// Once `zoom`/`pan` is called, this becomes `true` and stays
    /// `true` for the lifetime of this `ViewState` instance --
    /// `advance_if_following_live` becomes a no-op from then on, so
    /// new data does not yank the user's chosen view of history out
    /// from under them. Switching symbols (constructing a fresh
    /// `ViewState`) is the only way back to "follow live" mode.
    user_adjusted: bool,
}

impl ViewState {
    /// Constructs a `ViewState` in "follow live" mode: visible range
    /// is `now - DEFAULT_WINDOW_SECONDS` to `now`, and will keep
    /// advancing to track new data via `advance_if_following_live`
    /// until the user manually zooms or pans.
    pub fn new_following_live() -> Self {
        let now = Utc::now();
        Self {
            visible_range: (now - chrono::Duration::seconds(DEFAULT_WINDOW_SECONDS))..now,
            user_adjusted: false,
        }
    }

    pub fn visible_range(&self) -> &Range<DateTime<Utc>> {
        &self.visible_range
    }

    /// If new data arrives at `new_data_time` and this `ViewState` is
    /// still in "follow live" mode (no manual zoom/pan yet), advances
    /// the visible range's right edge to `new_data_time` and shifts
    /// the left edge by the same delta, preserving the range's width
    /// (and therefore the current zoom level). A no-op once
    /// `user_adjusted` is `true`, and a no-op if `new_data_time` is
    /// not after the current right edge (out-of-order data must not
    /// move the view backwards).
    pub fn advance_if_following_live(&mut self, new_data_time: DateTime<Utc>) {
        if self.user_adjusted {
            return;
        }
        if new_data_time <= self.visible_range.end {
            return;
        }
        let delta = new_data_time - self.visible_range.end;
        self.visible_range = (self.visible_range.start + delta)..new_data_time;
    }

    /// Narrows/widens the visible range around a point (`cursor_fraction`,
    /// 0.0-1.0 across the current visible width). `delta` follows
    /// `egui`'s scroll-delta convention: positive zooms in (narrows),
    /// negative zooms out (widens). Marks this `ViewState` as
    /// manually adjusted, permanently stopping "follow live"
    /// auto-advance. The resulting range is clamped to a minimum
    /// width of 1 second so repeated zoom-in cannot collapse it to a
    /// zero or negative-width range.
    pub fn zoom(&mut self, cursor_fraction: f32, delta: f32) {
        self.user_adjusted = true;
        let cursor_fraction = cursor_fraction.clamp(0.0, 1.0) as f64;
        let width_seconds = (self.visible_range.end - self.visible_range.start).num_milliseconds() as f64 / 1000.0;
        let zoom_factor = (1.0 - (delta as f64) * 0.1).clamp(0.1, 10.0);
        let new_width_seconds = (width_seconds * zoom_factor).max(1.0);
        let cursor_time = self.visible_range.start
            + chrono::Duration::milliseconds((width_seconds * cursor_fraction * 1000.0) as i64);
        let new_start_offset_seconds = (cursor_time - self.visible_range.start).num_milliseconds() as f64 / 1000.0
            * (new_width_seconds / width_seconds);
        let new_start = cursor_time - chrono::Duration::milliseconds((new_start_offset_seconds * 1000.0) as i64);
        let new_end = new_start + chrono::Duration::milliseconds((new_width_seconds * 1000.0) as i64);
        self.visible_range = new_start..new_end;
    }

    /// Shifts the visible range by `delta_fraction` of its current
    /// width (positive shifts forward in time / pans right, negative
    /// shifts backward / pans left). Marks this `ViewState` as
    /// manually adjusted, permanently stopping "follow live"
    /// auto-advance.
    pub fn pan(&mut self, delta_fraction: f32) {
        self.user_adjusted = true;
        let width_seconds = (self.visible_range.end - self.visible_range.start).num_milliseconds() as f64 / 1000.0;
        let shift_ms = (width_seconds * delta_fraction as f64 * 1000.0) as i64;
        let shift = chrono::Duration::milliseconds(shift_ms);
        self.visible_range = (self.visible_range.start + shift)..(self.visible_range.end + shift);
    }

    /// Computes an auto-fit Y range from `values` (already filtered
    /// by the caller to the currently-visible data), with
    /// `Y_AXIS_PADDING_FRACTION` padding above/below. Returns `None`
    /// if `values` is empty (nothing visible to fit to).
    pub fn y_axis_range_for(&self, values: impl Iterator<Item = Decimal>) -> Option<Range<Decimal>> {
        let mut min: Option<Decimal> = None;
        let mut max: Option<Decimal> = None;
        for value in values {
            min = Some(min.map_or(value, |m| m.min(value)));
            max = Some(max.map_or(value, |m| m.max(value)));
        }
        let (min, max) = (min?, max?);
        if min == max {
            // A single distinct value (or all-equal values): pad by a
            // small fixed fraction of the value itself so the range
            // isn't zero-width.
            let padding = (min.abs() * Decimal::new(5, 2)).max(Decimal::new(1, 8));
            return Some((min - padding)..(max + padding));
        }
        let span = max - min;
        let padding = span * Decimal::try_from(Y_AXIS_PADDING_FRACTION).expect("Y_AXIS_PADDING_FRACTION is a valid finite constant");
        Some((min - padding)..(max + padding))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn t(epoch_secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
    }

    fn state_with_range(start: i64, end: i64) -> ViewState {
        ViewState {
            visible_range: t(start)..t(end),
            user_adjusted: false,
        }
    }

    #[test]
    fn advance_if_following_live_shifts_the_whole_window_forward() {
        let mut state = state_with_range(0, 300);
        state.advance_if_following_live(t(360));
        assert_eq!(*state.visible_range(), t(60)..t(360));
    }

    #[test]
    fn advance_if_following_live_is_a_noop_after_a_manual_zoom() {
        let mut state = state_with_range(0, 300);
        state.zoom(0.5, 1.0);
        let range_after_zoom = state.visible_range().clone();
        state.advance_if_following_live(t(1000));
        assert_eq!(*state.visible_range(), range_after_zoom);
    }

    #[test]
    fn advance_if_following_live_is_a_noop_after_a_manual_pan() {
        let mut state = state_with_range(0, 300);
        state.pan(0.5);
        let range_after_pan = state.visible_range().clone();
        state.advance_if_following_live(t(1000));
        assert_eq!(*state.visible_range(), range_after_pan);
    }

    #[test]
    fn advance_if_following_live_ignores_out_of_order_data() {
        let mut state = state_with_range(0, 300);
        state.advance_if_following_live(t(100)); // before the current right edge
        assert_eq!(*state.visible_range(), t(0)..t(300));
    }

    #[test]
    fn zoom_in_narrows_the_visible_range() {
        let mut state = state_with_range(0, 300);
        state.zoom(0.5, 2.0);
        let width = (state.visible_range().end - state.visible_range().start).num_seconds();
        assert!(width < 300, "zooming in with positive delta should narrow the range, got width {width}");
    }

    #[test]
    fn zoom_out_widens_the_visible_range() {
        let mut state = state_with_range(0, 300);
        state.zoom(0.5, -2.0);
        let width = (state.visible_range().end - state.visible_range().start).num_seconds();
        assert!(width > 300, "zooming out with negative delta should widen the range, got width {width}");
    }

    #[test]
    fn zoom_marks_the_state_as_user_adjusted() {
        let mut state = state_with_range(0, 300);
        assert!(!state.user_adjusted);
        state.zoom(0.5, 1.0);
        assert!(state.user_adjusted);
    }

    #[test]
    fn pan_shifts_the_range_by_the_requested_fraction_forward() {
        let mut state = state_with_range(0, 300);
        state.pan(0.5); // shift forward by 50% of the 300s width = 150s
        assert_eq!(*state.visible_range(), t(150)..t(450));
    }

    #[test]
    fn pan_shifts_the_range_backward_for_a_negative_fraction() {
        let mut state = state_with_range(300, 600);
        state.pan(-0.5);
        assert_eq!(*state.visible_range(), t(150)..t(450));
    }

    #[test]
    fn pan_marks_the_state_as_user_adjusted() {
        let mut state = state_with_range(0, 300);
        state.pan(0.1);
        assert!(state.user_adjusted);
    }

    #[test]
    fn y_axis_range_for_empty_iterator_is_none() {
        let state = state_with_range(0, 300);
        assert_eq!(state.y_axis_range_for(std::iter::empty()), None);
    }

    #[test]
    fn y_axis_range_for_pads_above_and_below_the_min_max() {
        let state = state_with_range(0, 300);
        let range = state
            .y_axis_range_for(vec![dec!(100), dec!(110), dec!(90)].into_iter())
            .unwrap();
        assert!(range.start < dec!(90));
        assert!(range.end > dec!(110));
    }

    #[test]
    fn y_axis_range_for_a_single_distinct_value_is_not_zero_width() {
        let state = state_with_range(0, 300);
        let range = state.y_axis_range_for(vec![dec!(100), dec!(100), dec!(100)].into_iter()).unwrap();
        assert!(range.start < range.end);
    }
}
```

- [ ] **Step 4: Create `lib.rs`**

```rust
// crates/apexes-chart/src/lib.rs
pub mod view_state;
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p apexes-chart view_state`
Expected: all 13 tests pass.

- [ ] **Step 6: Run clippy, fmt, workspace check**

Run: `cargo clippy -p apexes-chart --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/apexes-chart/Cargo.toml crates/apexes-chart/src/lib.rs crates/apexes-chart/src/view_state.rs
git commit -S -s -m "feat(apexes-chart): crate scaffold + ViewState (zoom/pan/auto-fit) (sub-project #13b)"
```

---

### Task 3: `geometry.rs` -- bar/tick-to-vertex conversion

**Files:**
- Create: `crates/apexes-chart/src/geometry.rs`
- Modify: `crates/apexes-chart/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_chart::view_state::ViewState` (Task 2), `apexes_core::Bar` (existing).
- Produces: `apexes_chart::geometry::{Vertex, candlestick_vertices, tick_line_vertices, TickPoint}`. `Vertex { position: [f32; 2], color: [f32; 4] }` (implements `bytemuck::Pod`/`bytemuck::Zeroable`). `TickPoint { pub timestamp: chrono::DateTime<chrono::Utc>, pub bid: rust_decimal::Decimal, pub ask: rust_decimal::Decimal }`. Used by Tasks 4/5's chart widgets and Task 6's `apexes-ui` integration (which constructs the `TickPoint` ring buffer).

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-chart/src/geometry.rs -- create the file with this
// content (both candlestick_vertices and tick_line_vertices are
// implemented fully here, with their own test coverage together --
// they share the time_to_ndc_x/price_to_ndc_y/push_quad helpers, so
// there is no meaningful way to land one without the other already
// compiling):

//! Pure-logic conversion from chart data (`Bar`s, tick points) plus
//! the current `ViewState` and Y-axis range into GPU vertex data. No
//! `wgpu` dependency -- outputs plain `Vertex` structs, testable
//! without a GPU context. `candlestick_vertices` and
//! `tick_line_vertices` both filter their input to
//! `view.visible_range()` first, then map each remaining point's
//! `(time, price)` into normalized device coordinates (x, y both in
//! [-1.0, 1.0], y-up) via linear interpolation against
//! `view.visible_range()` (X) and `y_range` (Y).

use crate::view_state::ViewState;
use apexes_core::Bar;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::ops::Range;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Vertex {
    pub position: [f32; 2],
    pub color: [f32; 4],
}

/// Red for close > open (bullish, per this project's chosen
/// convention -- the reverse of TradingView's default), green
/// otherwise.
const BULLISH_COLOR: [f32; 4] = [0.85, 0.1, 0.1, 1.0];
const BEARISH_COLOR: [f32; 4] = [0.1, 0.7, 0.2, 1.0];
const BID_LINE_COLOR: [f32; 4] = [0.2, 0.6, 1.0, 1.0];
const ASK_LINE_COLOR: [f32; 4] = [1.0, 0.6, 0.2, 1.0];

/// Half-width, in normalized-device-coordinate X units, of a
/// candle's body and of a tick-line's line-as-quad thickness. A
/// fixed NDC width (not pixel width) keeps candles/lines a
/// consistent visual proportion of the chart regardless of viewport
/// size, matching this crate's decision to keep `geometry.rs` free
/// of any viewport-pixel-size parameter (viewport size only matters
/// for aspect-ratio-correct rendering, which the vertex shader --
/// not this module -- is responsible for).
const CANDLE_HALF_WIDTH_FRACTION: f64 = 0.15;
const LINE_HALF_THICKNESS_NDC: f32 = 0.006;

fn time_to_ndc_x(time: chrono::DateTime<chrono::Utc>, range: &Range<chrono::DateTime<chrono::Utc>>) -> f64 {
    let total = (range.end - range.start).num_milliseconds() as f64;
    if total <= 0.0 {
        return 0.0;
    }
    let offset = (time - range.start).num_milliseconds() as f64;
    (offset / total) * 2.0 - 1.0
}

fn price_to_ndc_y(price: Decimal, y_range: &Range<Decimal>) -> f32 {
    let span = y_range.end - y_range.start;
    if span <= Decimal::ZERO {
        return 0.0;
    }
    let fraction = ((price - y_range.start) / span)
        .to_f64()
        .unwrap_or(0.5);
    ((fraction * 2.0 - 1.0) as f32).clamp(-1.0, 1.0)
}

fn push_quad(vertices: &mut Vec<Vertex>, left: f32, right: f32, bottom: f32, top: f32, color: [f32; 4]) {
    vertices.push(Vertex { position: [left, bottom], color });
    vertices.push(Vertex { position: [right, bottom], color });
    vertices.push(Vertex { position: [right, top], color });
    vertices.push(Vertex { position: [left, bottom], color });
    vertices.push(Vertex { position: [right, top], color });
    vertices.push(Vertex { position: [left, top], color });
}

/// Converts `bars` (any mix of closed bars and the current
/// `BarAggregator::current_open_bar` result) into candlestick
/// geometry: a filled rectangle per candle body (two triangles, six
/// vertices) plus a thin rectangle per wick (six more vertices)
/// spanning `low` to `high`. Bars entirely outside
/// `view.visible_range()` are skipped.
pub fn candlestick_vertices(bars: &[Bar], view: &ViewState, y_range: &Range<Decimal>) -> Vec<Vertex> {
    let range = view.visible_range();
    let mut vertices = Vec::new();

    for bar in bars {
        if bar.close_time < range.start || bar.open_time > range.end {
            continue;
        }
        let color = if bar.close > bar.open { BULLISH_COLOR } else { BEARISH_COLOR };
        let center_x = time_to_ndc_x(bar.open_time, range);
        let half_width = CANDLE_HALF_WIDTH_FRACTION
            * (2.0 / ((range.end - range.start).num_milliseconds() as f64).max(1.0))
            * ((bar.close_time - bar.open_time).num_milliseconds() as f64).max(1.0);
        let left = center_x - half_width;
        let right = center_x + half_width;
        let open_y = price_to_ndc_y(bar.open, y_range);
        let close_y = price_to_ndc_y(bar.close, y_range);
        let (body_bottom, body_top) = if close_y >= open_y { (open_y, close_y) } else { (close_y, open_y) };

        push_quad(&mut vertices, left as f32, right as f32, body_bottom, body_top, color);

        let high_y = price_to_ndc_y(bar.high, y_range);
        let low_y = price_to_ndc_y(bar.low, y_range);
        let wick_half_width = half_width * 0.15;
        push_quad(
            &mut vertices,
            (center_x - wick_half_width) as f32,
            (center_x + wick_half_width) as f32,
            low_y,
            high_y,
            color,
        );
    }

    vertices
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TickPoint {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub bid: Decimal,
    pub ask: Decimal,
}

/// Converts `ticks` into two polylines (bid, ask), each rendered as
/// a strip of thick-line-as-quads connecting consecutive points.
/// Ticks entirely outside `view.visible_range()` are skipped, but one
/// point immediately before and after the visible range is kept if
/// present (so a line segment crossing into the visible range from
/// outside it still renders, rather than starting mid-air at the
/// viewport edge) -- callers pass the FULL tick buffer, not a
/// pre-filtered slice, so this function can apply that one-point
/// look-around itself.
pub fn tick_line_vertices(ticks: &[TickPoint], view: &ViewState, y_range: &Range<Decimal>) -> Vec<Vertex> {
    let range = view.visible_range();
    let mut vertices = Vec::new();

    let relevant_indices: Vec<usize> = ticks
        .iter()
        .enumerate()
        .filter(|(_, t)| t.timestamp >= range.start && t.timestamp <= range.end)
        .map(|(i, _)| i)
        .collect();

    if relevant_indices.is_empty() {
        return vertices;
    }

    let start_index = relevant_indices[0].saturating_sub(1);
    let end_index = (*relevant_indices.last().unwrap() + 1).min(ticks.len() - 1);
    let visible = &ticks[start_index..=end_index];

    push_line_strip(&mut vertices, visible, range, y_range, |t| t.bid, BID_LINE_COLOR);
    push_line_strip(&mut vertices, visible, range, y_range, |t| t.ask, ASK_LINE_COLOR);

    vertices
}

fn push_line_strip(
    vertices: &mut Vec<Vertex>,
    points: &[TickPoint],
    range: &Range<chrono::DateTime<chrono::Utc>>,
    y_range: &Range<Decimal>,
    value_of: impl Fn(&TickPoint) -> Decimal,
    color: [f32; 4],
) {
    for pair in points.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        let ax = time_to_ndc_x(a.timestamp, range) as f32;
        let ay = price_to_ndc_y(value_of(a), y_range);
        let bx = time_to_ndc_x(b.timestamp, range) as f32;
        let by = price_to_ndc_y(value_of(b), y_range);

        // A thick line-as-quad connecting (ax, ay) to (bx, by):
        // offset perpendicular to the segment direction by
        // LINE_HALF_THICKNESS_NDC on each side.
        let (dx, dy) = (bx - ax, by - ay);
        let length = (dx * dx + dy * dy).sqrt().max(1e-6);
        let (nx, ny) = (-dy / length * LINE_HALF_THICKNESS_NDC, dx / length * LINE_HALF_THICKNESS_NDC);

        vertices.push(Vertex { position: [ax + nx, ay + ny], color });
        vertices.push(Vertex { position: [ax - nx, ay - ny], color });
        vertices.push(Vertex { position: [bx - nx, by - ny], color });
        vertices.push(Vertex { position: [ax + nx, ay + ny], color });
        vertices.push(Vertex { position: [bx - nx, by - ny], color });
        vertices.push(Vertex { position: [bx + nx, by + ny], color });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{BarInterval, Venue};
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn t(epoch_secs: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
    }

    fn state_with_range(start: i64, end: i64) -> ViewState {
        // Constructed via new_following_live then adjusted, since
        // ViewState's fields are private to this crate's view_state
        // module (not pub outside it) -- geometry.rs's own tests
        // build a real ViewState via pan/zoom from a known baseline
        // rather than constructing the struct literal directly (that
        // literal-construction shortcut is only available inside
        // view_state.rs's own #[cfg(test)] module via `use super::*`).
        let mut state = ViewState::new_following_live();
        // Force a deterministic range: pan so that visible_range()
        // becomes exactly [t(start), t(end)). new_following_live's
        // own default window is DEFAULT_WINDOW_SECONDS (300s) wide,
        // so first zoom to the target width, then pan to the target
        // start.
        let default_width = 300.0;
        let target_width = (end - start) as f32;
        state.zoom(0.0, 1.0 - target_width / default_width);
        let current_start = state.visible_range().start;
        let target_start = t(start);
        let shift_seconds = (target_start - current_start).num_seconds() as f32;
        let current_width = (state.visible_range().end - state.visible_range().start).num_seconds() as f32;
        state.pan(shift_seconds / current_width);
        state
    }

    fn bar(open_time: i64, close_time: i64, open: Decimal, high: Decimal, low: Decimal, close: Decimal) -> Bar {
        Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open,
            high,
            low,
            close,
            volume: dec!(1),
            interval: BarInterval::OneMinute,
            open_time: t(open_time),
            close_time: t(close_time),
        }
    }

    #[test]
    fn candlestick_vertices_produces_twelve_vertices_per_bar() {
        let bars = vec![bar(0, 60, dec!(100), dec!(110), dec!(90), dec!(105))];
        let view = state_with_range(0, 300);
        let y_range = dec!(80)..dec!(120);
        let vertices = candlestick_vertices(&bars, &view, &y_range);
        assert_eq!(vertices.len(), 12, "6 vertices for the body quad + 6 for the wick quad");
    }

    #[test]
    fn candlestick_vertices_uses_bullish_color_when_close_exceeds_open() {
        let bars = vec![bar(0, 60, dec!(100), dec!(110), dec!(90), dec!(105))];
        let view = state_with_range(0, 300);
        let y_range = dec!(80)..dec!(120);
        let vertices = candlestick_vertices(&bars, &view, &y_range);
        assert_eq!(vertices[0].color, BULLISH_COLOR);
    }

    #[test]
    fn candlestick_vertices_uses_bearish_color_when_close_is_at_or_below_open() {
        let bars = vec![bar(0, 60, dec!(105), dec!(110), dec!(90), dec!(100))];
        let view = state_with_range(0, 300);
        let y_range = dec!(80)..dec!(120);
        let vertices = candlestick_vertices(&bars, &view, &y_range);
        assert_eq!(vertices[0].color, BEARISH_COLOR);
    }

    #[test]
    fn candlestick_vertices_skips_bars_entirely_outside_the_visible_range() {
        let bars = vec![bar(1000, 1060, dec!(100), dec!(110), dec!(90), dec!(105))];
        let view = state_with_range(0, 300);
        let y_range = dec!(80)..dec!(120);
        let vertices = candlestick_vertices(&bars, &view, &y_range);
        assert!(vertices.is_empty());
    }

    #[test]
    fn tick_line_vertices_produces_six_vertices_per_segment_per_line() {
        let ticks = vec![
            TickPoint { timestamp: t(0), bid: dec!(100), ask: dec!(101) },
            TickPoint { timestamp: t(60), bid: dec!(102), ask: dec!(103) },
            TickPoint { timestamp: t(120), bid: dec!(101), ask: dec!(102) },
        ];
        let view = state_with_range(0, 300);
        let y_range = dec!(95)..dec!(110);
        let vertices = tick_line_vertices(&ticks, &view, &y_range);
        // 3 points = 2 segments per line, 2 lines (bid, ask), 6 vertices
        // per segment = 2 * 2 * 6 = 24.
        assert_eq!(vertices.len(), 24);
    }

    #[test]
    fn tick_line_vertices_is_empty_when_no_ticks_are_in_the_visible_range() {
        let ticks = vec![TickPoint { timestamp: t(1000), bid: dec!(100), ask: dec!(101) }];
        let view = state_with_range(0, 300);
        let y_range = dec!(95)..dec!(110);
        let vertices = tick_line_vertices(&ticks, &view, &y_range);
        assert!(vertices.is_empty());
    }

    #[test]
    fn tick_line_vertices_includes_one_point_before_the_visible_range_for_a_crossing_segment() {
        // A tick just before the visible range's start, plus one
        // inside it: the segment between them crosses into view and
        // must still render (not be silently dropped for starting
        // outside range).
        let ticks = vec![
            TickPoint { timestamp: t(50), bid: dec!(100), ask: dec!(101) }, // just before range start (100)
            TickPoint { timestamp: t(150), bid: dec!(102), ask: dec!(103) }, // inside range
        ];
        let view = state_with_range(100, 300);
        let y_range = dec!(95)..dec!(110);
        let vertices = tick_line_vertices(&ticks, &view, &y_range);
        assert!(!vertices.is_empty(), "the segment crossing into the visible range must still render");
    }
}
```

- [ ] **Step 2: Register the module**

```rust
// crates/apexes-chart/src/lib.rs
pub mod geometry;
pub mod view_state;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p apexes-chart`
Expected: all tests pass (13 `view_state` + 4 candlestick + 3 tick-line = 20 tests).

- [ ] **Step 4: Run clippy, fmt, workspace check**

Run: `cargo clippy -p apexes-chart --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-chart/src/geometry.rs crates/apexes-chart/src/lib.rs
git commit -S -s -m "feat(apexes-chart): geometry.rs -- bar/tick-to-vertex conversion (sub-project #13b)"
```

---

### Task 4: Shared `wgpu` pipeline plumbing + `CandlestickChart`

**Files:**
- Create: `crates/apexes-chart/src/shaders/candlestick.wgsl`
- Create: `crates/apexes-chart/src/pipeline.rs`
- Create: `crates/apexes-chart/src/candlestick.rs`
- Modify: `crates/apexes-chart/src/lib.rs`
- Create: `crates/apexes-chart/tests/candlestick_renders_without_panicking.rs`

**Interfaces:**
- Consumes: `apexes_chart::geometry::{Vertex, candlestick_vertices}` (Task 3), `apexes_chart::view_state::ViewState` (Task 2), `apexes_core::Bar` (existing).
- Produces: `apexes_chart::pipeline::create_solid_color_pipeline(device: &wgpu::Device, target_format: wgpu::TextureFormat, shader_source: &str) -> wgpu::RenderPipeline` (shared by Task 5's `TickLineChart`). `apexes_chart::candlestick::CandlestickChart` -- `CandlestickChart::new(device: &wgpu::Device, target_format: wgpu::TextureFormat) -> Self`, `CandlestickChart::show(&self, ui: &mut egui::Ui, bars: &[apexes_core::Bar], view: &mut ViewState, desired_size: egui::Vec2)`.

- [ ] **Step 1: Write the WGSL shader**

```wgsl
// crates/apexes-chart/src/shaders/candlestick.wgsl
// Solid-fill triangle shader shared by CandlestickChart and (via the
// same create_solid_color_pipeline function, Task 5) TickLineChart --
// both submit pre-positioned normalized-device-coordinate vertices
// with an embedded per-vertex color, so no uniform buffer or
// transform matrix is needed.

struct VertexInput {
    @location(0) position: vec2<f32>,
    @location(1) color: vec4<f32>,
};

struct VertexOutput {
    @builtin(position) clip_position: vec4<f32>,
    @location(0) color: vec4<f32>,
};

@vertex
fn vs_main(in: VertexInput) -> VertexOutput {
    var out: VertexOutput;
    out.clip_position = vec4<f32>(in.position, 0.0, 1.0);
    out.color = in.color;
    return out;
}

@fragment
fn fs_main(in: VertexOutput) -> @location(0) vec4<f32> {
    return in.color;
}
```

- [ ] **Step 2: Write `pipeline.rs`**

```rust
// crates/apexes-chart/src/pipeline.rs
//! Shared `wgpu::RenderPipeline` construction for both
//! `CandlestickChart` and `TickLineChart` -- both charts submit
//! pre-positioned, pre-colored vertices with no transform (see
//! `shaders/candlestick.wgsl`'s own doc comment), so one pipeline
//! shape serves both.

use crate::geometry::Vertex;

/// Creates a `wgpu::RenderPipeline` for `shader_source` (WGSL text),
/// rendering `Vertex` (`[f32; 2]` position + `[f32; 4]` color)
/// triangle lists with no depth/stencil testing and standard alpha
/// blending, targeting `target_format`. A `wgpu` pipeline-creation
/// failure (e.g. malformed WGSL) panics with a clear message rather
/// than silently producing a chart that never renders -- this
/// happens once at `CandlestickChart::new`/`TickLineChart::new` time
/// (called from `apexes-ui`'s `main.rs` at startup), matching this
/// project's established fail-fast-at-startup convention.
pub fn create_solid_color_pipeline(
    device: &wgpu::Device,
    target_format: wgpu::TextureFormat,
    shader_source: &str,
) -> wgpu::RenderPipeline {
    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("apexes-chart solid color shader"),
        source: wgpu::ShaderSource::Wgsl(shader_source.into()),
    });

    let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
        label: Some("apexes-chart solid color pipeline layout"),
        bind_group_layouts: &[],
        push_constant_ranges: &[],
    });

    let vertex_attributes = wgpu::vertex_attr_array![0 => Float32x2, 1 => Float32x4];

    device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
        label: Some("apexes-chart solid color pipeline"),
        layout: Some(&pipeline_layout),
        vertex: wgpu::VertexState {
            module: &shader,
            entry_point: Some("vs_main"),
            compilation_options: wgpu::PipelineCompilationOptions::default(),
            buffers: &[wgpu::VertexBufferLayout {
                array_stride: std::mem::size_of::<Vertex>() as wgpu::BufferAddress,
                step_mode: wgpu::VertexStepMode::Vertex,
                attributes: &vertex_attributes,
            }],
        },
        fragment: Some(wgpu::FragmentState {
            module: &shader,
            entry_point: Some("fs_main"),
            compilation_options: wgpu::PipelineCompilationOptions::default(),
            targets: &[Some(wgpu::ColorTargetState {
                format: target_format,
                blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                write_mask: wgpu::ColorWrites::ALL,
            })],
        }),
        primitive: wgpu::PrimitiveState::default(),
        depth_stencil: None,
        multisample: wgpu::MultisampleState::default(),
        multiview_mask: None,
        cache: None,
    })
}
```

- [ ] **Step 3: Write `candlestick.rs`**

```rust
// crates/apexes-chart/src/candlestick.rs
//! `CandlestickChart`: a hand-written-`wgpu`-rendered K-line chart
//! widget, embedded into an `egui` frame via `egui_wgpu::CallbackTrait`.
//! Owns its own `wgpu::RenderPipeline` (built once at construction
//! time, per `pipeline.rs`), and on every `show` call builds a fresh
//! vertex buffer from the current `bars`/`ViewState`/Y-range via
//! `geometry::candlestick_vertices`, then issues an
//! `egui_wgpu::Callback::new_paint_callback` to draw it. Also reads
//! scroll/drag input from the allocated response rect each frame and
//! feeds it into the shared `ViewState`'s `zoom`/`pan`, mirroring
//! `egui_plot`'s interaction conventions (see the design spec §4.4).

use crate::geometry::{Vertex, candlestick_vertices};
use crate::pipeline::create_solid_color_pipeline;
use crate::view_state::ViewState;
use apexes_core::Bar;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Arc;
use wgpu::util::DeviceExt;

const SHADER_SOURCE: &str = include_str!("shaders/candlestick.wgsl");

pub struct CandlestickChart {
    pipeline: Arc<wgpu::RenderPipeline>,
}

impl CandlestickChart {
    pub fn new(device: &wgpu::Device, target_format: wgpu::TextureFormat) -> Self {
        Self {
            pipeline: Arc::new(create_solid_color_pipeline(device, target_format, SHADER_SOURCE)),
        }
    }

    /// Renders the chart for `bars` (any mix of closed bars and the
    /// current still-forming bar) into a `desired_size`-sized area of
    /// `ui`, reading scroll/drag input from that area to update
    /// `view` (shared with `TickLineChart` by the caller).
    pub fn show(&self, ui: &mut egui::Ui, bars: &[Bar], view: &mut ViewState, desired_size: egui::Vec2) {
        let (rect, response) = ui.allocate_exact_size(desired_size, egui::Sense::click_and_drag());

        if response.hovered() {
            let scroll_delta = ui.input(|i| i.smooth_scroll_delta.y);
            if scroll_delta != 0.0
                && let Some(hover_pos) = response.hover_pos()
            {
                let cursor_fraction = ((hover_pos.x - rect.left()) / rect.width()).clamp(0.0, 1.0);
                view.zoom(cursor_fraction, scroll_delta * 0.01);
            }
        }
        if response.dragged() {
            let drag_delta_fraction = -response.drag_delta().x / rect.width();
            view.pan(drag_delta_fraction);
        }

        let y_range = view
            .y_axis_range_for(bars.iter().flat_map(|b| [b.high, b.low].into_iter()))
            .unwrap_or(Decimal::ZERO..Decimal::ONE);
        let vertices = candlestick_vertices(bars, view, &y_range);

        let pipeline = Arc::clone(&self.pipeline);
        let callback = egui_wgpu::Callback::new_paint_callback(rect, CandlestickPaintCallback { pipeline, vertices });
        ui.painter().add(callback);
    }
}

struct CandlestickPaintCallback {
    pipeline: Arc<wgpu::RenderPipeline>,
    vertices: Vec<Vertex>,
}

impl egui_wgpu::CallbackTrait for CandlestickPaintCallback {
    fn prepare(
        &self,
        device: &wgpu::Device,
        _queue: &wgpu::Queue,
        _screen_descriptor: &egui_wgpu::ScreenDescriptor,
        _egui_encoder: &mut wgpu::CommandEncoder,
        callback_resources: &mut egui_wgpu::CallbackResources,
    ) -> Vec<wgpu::CommandBuffer> {
        let buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("apexes-chart candlestick vertex buffer"),
            contents: bytemuck::cast_slice(&self.vertices),
            usage: wgpu::BufferUsages::VERTEX,
        });
        callback_resources.insert(CandlestickFrameBuffer {
            buffer,
            vertex_count: self.vertices.len() as u32,
        });
        Vec::new()
    }

    fn paint(&self, _info: egui_wgpu::PaintCallbackInfo, render_pass: &mut wgpu::RenderPass<'static>, callback_resources: &egui_wgpu::CallbackResources) {
        let Some(frame_buffer) = callback_resources.get::<CandlestickFrameBuffer>() else {
            return;
        };
        if frame_buffer.vertex_count == 0 {
            return;
        }
        render_pass.set_pipeline(&self.pipeline);
        render_pass.set_vertex_buffer(0, frame_buffer.buffer.slice(..));
        render_pass.draw(0..frame_buffer.vertex_count, 0..1);
    }
}

struct CandlestickFrameBuffer {
    buffer: wgpu::Buffer,
    vertex_count: u32,
}
```

- [ ] **Step 4: Register the modules**

```rust
// crates/apexes-chart/src/lib.rs
pub mod candlestick;
pub mod geometry;
pub mod pipeline;
pub mod view_state;
```

- [ ] **Step 5: Write the headless render test**

```rust
// crates/apexes-chart/tests/candlestick_renders_without_panicking.rs
//! Drives one `egui_kittest` headless render pass (using its real
//! `wgpu`-feature `WgpuTestRenderer`, which prefers a software
//! rasterizer where available) over `CandlestickChart::show` with
//! synthetic `Bar` data, asserting no panic. Known environment
//! dependency: requires a working `wgpu` backend at test-run time
//! (Metal on the current macOS dev environment; would need Mesa
//! llvmpipe on a GPU-less Linux CI machine) -- see the design spec
//! §7 for the full rationale.

use apexes_chart::candlestick::CandlestickChart;
use apexes_chart::view_state::ViewState;
use apexes_core::{Bar, BarInterval, Venue};
use chrono::{TimeZone, Utc};
use egui_kittest::Harness;
use rust_decimal_macros::dec;

fn t(epoch_secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
}

fn sample_bars() -> Vec<Bar> {
    vec![
        Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(65000),
            high: dec!(65200),
            low: dec!(64900),
            close: dec!(65100),
            volume: dec!(10),
            interval: BarInterval::OneMinute,
            open_time: t(0),
            close_time: t(60),
        },
        Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(65100),
            high: dec!(65150),
            low: dec!(64800),
            close: dec!(64850),
            volume: dec!(8),
            interval: BarInterval::OneMinute,
            open_time: t(60),
            close_time: t(120),
        },
    ]
}

struct TestApp {
    chart: CandlestickChart,
    bars: Vec<Bar>,
    view: ViewState,
}

impl eframe::App for TestApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.chart.show(ui, &self.bars, &mut self.view, egui::Vec2::new(400.0, 200.0));
    }
}

#[test]
fn candlestick_chart_renders_one_frame_without_panicking() {
    let mut harness = Harness::builder().wgpu().build_eframe(|cc| {
        let render_state = cc.wgpu_render_state.as_ref().expect("wgpu render state should be present when the wgpu harness feature is used");
        let chart = CandlestickChart::new(&render_state.device, render_state.target_format);
        TestApp {
            chart,
            bars: sample_bars(),
            view: ViewState::new_following_live(),
        }
    });

    harness.run();
}
```

- [ ] **Step 6: Run the test**

Run: `cargo test -p apexes-chart --test candlestick_renders_without_panicking`
Expected: PASS (may take a moment to initialize the `wgpu` backend on first run).

- [ ] **Step 7: Run this crate's full test suite, clippy, fmt, workspace check**

Run: `cargo test -p apexes-chart`
Expected: all pass (20 existing + 1 new = 21).

Run: `cargo clippy -p apexes-chart --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-chart/src/shaders/candlestick.wgsl crates/apexes-chart/src/pipeline.rs crates/apexes-chart/src/candlestick.rs crates/apexes-chart/src/lib.rs crates/apexes-chart/tests/candlestick_renders_without_panicking.rs
git commit -S -s -m "feat(apexes-chart): wgpu pipeline plumbing + CandlestickChart widget (sub-project #13b)"
```

---

### Task 5: `TickLineChart`

**Files:**
- Create: `crates/apexes-chart/src/tick_line.rs`
- Modify: `crates/apexes-chart/src/lib.rs`
- Create: `crates/apexes-chart/tests/tick_line_renders_without_panicking.rs`

**Interfaces:**
- Consumes: `apexes_chart::geometry::{Vertex, TickPoint, tick_line_vertices}` (Task 3), `apexes_chart::pipeline::create_solid_color_pipeline` (Task 4), `apexes_chart::view_state::ViewState` (Task 2).
- Produces: `apexes_chart::tick_line::TickLineChart` -- `TickLineChart::new(device: &wgpu::Device, target_format: wgpu::TextureFormat) -> Self`, `TickLineChart::show(&self, ui: &mut egui::Ui, ticks: &[apexes_chart::geometry::TickPoint], view: &mut ViewState, desired_size: egui::Vec2)`. Used by Task 6's `apexes-ui` integration.

- [ ] **Step 1: Write `tick_line.rs`**

The shader is reused as-is from Task 4 (`shaders/candlestick.wgsl` -- despite its filename, it is a generic solid-color-triangle shader per its own doc comment, not candlestick-specific; no new WGSL file is needed).

```rust
// crates/apexes-chart/src/tick_line.rs
//! `TickLineChart`: a hand-written-`wgpu`-rendered bid/ask line chart
//! widget, embedded into an `egui` frame via `egui_wgpu::CallbackTrait`.
//! Structurally identical to `CandlestickChart` (see that module's
//! own doc comment for the shared rationale) -- reuses the same
//! `pipeline.rs`/`shaders/candlestick.wgsl` (a generic solid-color
//! shader, not actually candlestick-specific) since both widgets
//! submit pre-positioned, pre-colored triangle-list vertices with no
//! transform.

use crate::geometry::{TickPoint, Vertex, tick_line_vertices};
use crate::pipeline::create_solid_color_pipeline;
use crate::view_state::ViewState;
use rust_decimal::Decimal;
use std::sync::Arc;
use wgpu::util::DeviceExt;

const SHADER_SOURCE: &str = include_str!("shaders/candlestick.wgsl");

pub struct TickLineChart {
    pipeline: Arc<wgpu::RenderPipeline>,
}

impl TickLineChart {
    pub fn new(device: &wgpu::Device, target_format: wgpu::TextureFormat) -> Self {
        Self {
            pipeline: Arc::new(create_solid_color_pipeline(device, target_format, SHADER_SOURCE)),
        }
    }

    /// Renders the chart for `ticks` into a `desired_size`-sized area
    /// of `ui`, reading scroll/drag input from that area to update
    /// `view` (shared with `CandlestickChart` by the caller).
    pub fn show(&self, ui: &mut egui::Ui, ticks: &[TickPoint], view: &mut ViewState, desired_size: egui::Vec2) {
        let (rect, response) = ui.allocate_exact_size(desired_size, egui::Sense::click_and_drag());

        if response.hovered() {
            let scroll_delta = ui.input(|i| i.smooth_scroll_delta.y);
            if scroll_delta != 0.0
                && let Some(hover_pos) = response.hover_pos()
            {
                let cursor_fraction = ((hover_pos.x - rect.left()) / rect.width()).clamp(0.0, 1.0);
                view.zoom(cursor_fraction, scroll_delta * 0.01);
            }
        }
        if response.dragged() {
            let drag_delta_fraction = -response.drag_delta().x / rect.width();
            view.pan(drag_delta_fraction);
        }

        let y_range = view
            .y_axis_range_for(ticks.iter().flat_map(|t| [t.bid, t.ask].into_iter()))
            .unwrap_or(Decimal::ZERO..Decimal::ONE);
        let vertices = tick_line_vertices(ticks, view, &y_range);

        let pipeline = Arc::clone(&self.pipeline);
        let callback = egui_wgpu::Callback::new_paint_callback(rect, TickLinePaintCallback { pipeline, vertices });
        ui.painter().add(callback);
    }
}

struct TickLinePaintCallback {
    pipeline: Arc<wgpu::RenderPipeline>,
    vertices: Vec<Vertex>,
}

impl egui_wgpu::CallbackTrait for TickLinePaintCallback {
    fn prepare(
        &self,
        device: &wgpu::Device,
        _queue: &wgpu::Queue,
        _screen_descriptor: &egui_wgpu::ScreenDescriptor,
        _egui_encoder: &mut wgpu::CommandEncoder,
        callback_resources: &mut egui_wgpu::CallbackResources,
    ) -> Vec<wgpu::CommandBuffer> {
        let buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("apexes-chart tick-line vertex buffer"),
            contents: bytemuck::cast_slice(&self.vertices),
            usage: wgpu::BufferUsages::VERTEX,
        });
        callback_resources.insert(TickLineFrameBuffer {
            buffer,
            vertex_count: self.vertices.len() as u32,
        });
        Vec::new()
    }

    fn paint(&self, _info: egui_wgpu::PaintCallbackInfo, render_pass: &mut wgpu::RenderPass<'static>, callback_resources: &egui_wgpu::CallbackResources) {
        let Some(frame_buffer) = callback_resources.get::<TickLineFrameBuffer>() else {
            return;
        };
        if frame_buffer.vertex_count == 0 {
            return;
        }
        render_pass.set_pipeline(&self.pipeline);
        render_pass.set_vertex_buffer(0, frame_buffer.buffer.slice(..));
        render_pass.draw(0..frame_buffer.vertex_count, 0..1);
    }
}

struct TickLineFrameBuffer {
    buffer: wgpu::Buffer,
    vertex_count: u32,
}
```

- [ ] **Step 2: Register the module**

```rust
// crates/apexes-chart/src/lib.rs
pub mod candlestick;
pub mod geometry;
pub mod pipeline;
pub mod tick_line;
pub mod view_state;
```

- [ ] **Step 3: Write the headless render test**

```rust
// crates/apexes-chart/tests/tick_line_renders_without_panicking.rs
//! Mirrors `candlestick_renders_without_panicking.rs` (Task 4) for
//! `TickLineChart` -- see that file's own doc comment for the shared
//! rationale and the known `wgpu`-backend environment dependency.

use apexes_chart::geometry::TickPoint;
use apexes_chart::tick_line::TickLineChart;
use apexes_chart::view_state::ViewState;
use chrono::{TimeZone, Utc};
use egui_kittest::Harness;
use rust_decimal_macros::dec;

fn t(epoch_secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
}

fn sample_ticks() -> Vec<TickPoint> {
    vec![
        TickPoint { timestamp: t(0), bid: dec!(64990), ask: dec!(65010) },
        TickPoint { timestamp: t(30), bid: dec!(65000), ask: dec!(65020) },
        TickPoint { timestamp: t(60), bid: dec!(64980), ask: dec!(65005) },
    ]
}

struct TestApp {
    chart: TickLineChart,
    ticks: Vec<TickPoint>,
    view: ViewState,
}

impl eframe::App for TestApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.chart.show(ui, &self.ticks, &mut self.view, egui::Vec2::new(400.0, 200.0));
    }
}

#[test]
fn tick_line_chart_renders_one_frame_without_panicking() {
    let mut harness = Harness::builder().wgpu().build_eframe(|cc| {
        let render_state = cc.wgpu_render_state.as_ref().expect("wgpu render state should be present when the wgpu harness feature is used");
        let chart = TickLineChart::new(&render_state.device, render_state.target_format);
        TestApp {
            chart,
            ticks: sample_ticks(),
            view: ViewState::new_following_live(),
        }
    });

    harness.run();
}
```

- [ ] **Step 4: Run the test**

Run: `cargo test -p apexes-chart --test tick_line_renders_without_panicking`
Expected: PASS.

- [ ] **Step 5: Run this crate's full test suite, clippy, fmt, workspace check**

Run: `cargo test -p apexes-chart`
Expected: all pass (21 existing + 1 new = 22).

Run: `cargo clippy -p apexes-chart --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-chart/src/tick_line.rs crates/apexes-chart/src/lib.rs crates/apexes-chart/tests/tick_line_renders_without_panicking.rs
git commit -S -s -m "feat(apexes-chart): TickLineChart widget (sub-project #13b)"
```

---

### Task 6: `apexes-ui` integration -- per-symbol aggregation + dashboard wiring

**Files:**
- Modify: `crates/apexes-ui/Cargo.toml`
- Modify: `crates/apexes-ui/src/ui.rs`
- Create: `crates/apexes-ui/src/chart_state.rs`

**Interfaces:**
- Consumes: `apexes_chart::{candlestick::CandlestickChart, tick_line::TickLineChart, view_state::ViewState, geometry::TickPoint}` (Tasks 2-5), `apexes_market_data::bar_aggregator::BarAggregator` (existing, extended in Task 1), `apexes_core::{Bar, BarInterval, MarketEvent}` (existing).
- Produces: `apexes_ui::chart_state::{ChartState, TickRingBuffer}` -- `ChartState::new(symbols: &[String]) -> Self`, `ChartState::apply_event(&mut self, event: &MarketEvent) -> Vec<Bar>` (returns any bars that closed as a result of this event, empty otherwise -- the caller is responsible for retaining closed-bar history across calls, since neither `ChartState` nor `BarAggregator` retain it themselves), `ChartState::current_open_bar_for(&self, symbol: &str, interval: BarInterval) -> Option<Bar>`, `ChartState::ticks_for(&self, symbol: &str) -> &[TickPoint]`. Used only by `ui.rs`'s `DashboardApp`, which combines `apply_event`'s accumulated closed-bar history with `current_open_bar_for`'s result into the full bar list a chart needs (see Step 4's `bars_for_chart` method).

- [ ] **Step 1: Add the `apexes-chart`/`apexes-market-data` dependencies**

```toml
# crates/apexes-ui/Cargo.toml -- add two lines to [dependencies]
# (apexes-market-data is not yet a dependency of this crate; it is
# needed for BarAggregator):
apexes-chart = { path = "../apexes-chart" }
```

```toml
# crates/apexes-ui/Cargo.toml -- the full [dependencies] section
# after this task's edits (apexes-market-data was already a
# dependency from sub-project #13a's main.rs use of
# BinanceMarketDataProvider -- confirm this before editing; if it is
# already present, only add the apexes-chart line above and do not
# duplicate apexes-market-data):
[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-message-bus = { path = "../apexes-message-bus" }
apexes-market-data = { path = "../apexes-market-data" }
apexes-chart = { path = "../apexes-chart" }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "sync"] }
axum = { workspace = true, features = ["ws"] }
tokio-tungstenite = { workspace = true }
futures = { workspace = true }
serde_json = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
clap = { workspace = true }
eframe = { workspace = true }
egui = { workspace = true }
rust_decimal = { workspace = true }
```

- [ ] **Step 2: Write the failing tests for `chart_state.rs`**

```rust
// crates/apexes-ui/src/chart_state.rs -- create the file with this
// content:

//! Per-symbol chart data accumulation for the K-line/tick-line
//! widgets (sub-project #13b): one `BarAggregator` and one
//! time-windowed tick ring buffer per hardcoded symbol, fed by every
//! incoming `MarketEvent`. Pure logic -- no `egui`/`wgpu` dependency,
//! unit-testable directly.

use apexes_chart::geometry::TickPoint;
use apexes_core::{Bar, BarInterval, MarketEvent, Venue};
use apexes_market_data::bar_aggregator::BarAggregator;
use chrono::Duration;
use std::collections::HashMap;

/// How much tick history each symbol's ring buffer retains, per the
/// design spec's explicit choice (retain by time window, not by a
/// fixed item count).
const TICK_RETENTION: Duration = Duration::hours(1);

pub struct TickRingBuffer {
    points: Vec<TickPoint>,
}

impl Default for TickRingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl TickRingBuffer {
    pub fn new() -> Self {
        Self { points: Vec::new() }
    }

    /// Appends `point`, then evicts every point older than
    /// `TICK_RETENTION` relative to `point`'s own timestamp (not
    /// relative to "now") -- eviction runs on every push, never on a
    /// timer, so the buffer can never grow unbounded regardless of
    /// tick rate.
    pub fn push(&mut self, point: TickPoint) {
        let cutoff = point.timestamp - TICK_RETENTION;
        self.points.push(point);
        self.points.retain(|p| p.timestamp >= cutoff);
    }

    pub fn points(&self) -> &[TickPoint] {
        &self.points
    }
}

pub struct ChartState {
    aggregators: HashMap<String, BarAggregator>,
    tick_buffers: HashMap<String, TickRingBuffer>,
}

impl ChartState {
    pub fn new(symbols: &[String]) -> Self {
        let mut aggregators = HashMap::new();
        let mut tick_buffers = HashMap::new();
        for symbol in symbols {
            aggregators.insert(symbol.clone(), BarAggregator::new(symbol.clone(), Venue::Binance));
            tick_buffers.insert(symbol.clone(), TickRingBuffer::new());
        }
        Self { aggregators, tick_buffers }
    }

    /// Feeds `event` into the relevant symbol's `BarAggregator`
    /// (`Trade` events) or tick ring buffer (`Tick` events). Events
    /// for a symbol not in the pre-populated set are ignored, same
    /// convention as `app_state::AppState::apply_event`. Returns any
    /// bars that closed as a result of this event (empty for
    /// `Tick`/non-matching events) -- `BarAggregator`/`ChartState` do
    /// not retain closed-bar history themselves (`on_trade` only ever
    /// hands back bars AT THE MOMENT they close), so the caller
    /// (`DashboardApp`, Step 4 below) is responsible for accumulating
    /// this method's returned bars into its own history across calls.
    pub fn apply_event(&mut self, event: &MarketEvent) -> Vec<Bar> {
        match event {
            MarketEvent::Trade(trade) => {
                if let Some(aggregator) = self.aggregators.get_mut(&trade.symbol) {
                    return aggregator.on_trade(trade.price, trade.quantity, trade.timestamp);
                }
                Vec::new()
            }
            MarketEvent::Tick(tick) => {
                if let Some(buffer) = self.tick_buffers.get_mut(&tick.symbol) {
                    buffer.push(TickPoint {
                        timestamp: tick.timestamp,
                        bid: tick.bid_price,
                        ask: tick.ask_price,
                    });
                }
                Vec::new()
            }
            MarketEvent::Depth(_) | MarketEvent::Bar(_) | MarketEvent::MarkPrice(_) => Vec::new(),
        }
    }

    /// Returns `symbol`'s currently still-forming (not yet closed)
    /// bar for `interval`, if any -- the caller (`DashboardApp`) is
    /// responsible for prepending its own retained closed-bar history
    /// (accumulated from `apply_event`'s return values) before this
    /// method's result, matching this task's own `Interfaces`
    /// contract of "closed bars... plus its current open bar, if any,
    /// appended last".
    pub fn current_open_bar_for(&self, symbol: &str, interval: BarInterval) -> Option<Bar> {
        self.aggregators.get(symbol)?.current_open_bar(interval)
    }

    pub fn ticks_for(&self, symbol: &str) -> &[TickPoint] {
        self.tick_buffers
            .get(symbol)
            .map(|buffer| buffer.points())
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{TradeEvent, Tick};
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn t(epoch_secs: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
    }

    fn symbols() -> Vec<String> {
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
    }

    #[test]
    fn tick_ring_buffer_evicts_points_older_than_the_retention_window() {
        let mut buffer = TickRingBuffer::new();
        buffer.push(TickPoint { timestamp: t(0), bid: dec!(100), ask: dec!(101) });
        buffer.push(TickPoint { timestamp: t(3600 + 1), bid: dec!(102), ask: dec!(103) }); // 1 second past the 1h retention window
        assert_eq!(buffer.points().len(), 1);
        assert_eq!(buffer.points()[0].bid, dec!(102));
    }

    #[test]
    fn tick_ring_buffer_keeps_points_within_the_retention_window() {
        let mut buffer = TickRingBuffer::new();
        buffer.push(TickPoint { timestamp: t(0), bid: dec!(100), ask: dec!(101) });
        buffer.push(TickPoint { timestamp: t(3000), bid: dec!(102), ask: dec!(103) }); // within 1h
        assert_eq!(buffer.points().len(), 2);
    }

    #[test]
    fn apply_event_trade_feeds_the_correct_symbols_aggregator() {
        let mut state = ChartState::new(&symbols());
        state.apply_event(&MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t(0),
        }));
        let open_bar = state.current_open_bar_for("BTCUSDT", BarInterval::OneMinute);
        assert!(open_bar.is_some());
        assert_eq!(state.current_open_bar_for("ETHUSDT", BarInterval::OneMinute), None);
    }

    #[test]
    fn apply_event_tick_feeds_the_correct_symbols_ring_buffer() {
        let mut state = ChartState::new(&symbols());
        state.apply_event(&MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64990),
            bid_size: dec!(1),
            ask_price: dec!(65010),
            ask_size: dec!(1),
            timestamp: t(0),
        }));
        assert_eq!(state.ticks_for("BTCUSDT").len(), 1);
        assert_eq!(state.ticks_for("ETHUSDT").len(), 0);
    }

    #[test]
    fn apply_event_for_an_unknown_symbol_is_ignored() {
        let mut state = ChartState::new(&symbols());
        state.apply_event(&MarketEvent::Trade(TradeEvent {
            symbol: "DOGEUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t(0),
        }));
        assert_eq!(state.current_open_bar_for("DOGEUSDT", BarInterval::OneMinute), None);
    }
}
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p apexes-ui chart_state`
Expected: all 5 tests pass.

- [ ] **Step 4: Wire `ChartState` and the two chart widgets into `DashboardApp`**

```rust
// crates/apexes-ui/src/ui.rs -- replace the ENTIRE file's content
// with (this adds chart state, a symbol/interval selector, and the
// two chart widgets below the existing table; the table/connection-status
// section is otherwise unchanged from 13a):

//! `eframe::App` implementation: a live-updating table of
//! bid/ask/last-trade price for the hardcoded instrument list, a
//! connection-status indicator, and (sub-project #13b) a K-line
//! candlestick chart plus a bid/ask tick-line chart for a
//! user-selected symbol, sharing one `ViewState` so both charts'
//! visible time range stays synchronized. Drains any pending
//! `MarketEvent`s from the client task's channel on every `ui()`
//! call and folds them into both `app_state` (table) and
//! `chart_state` (charts), then requests a repaint shortly afterward
//! so everything keeps updating even with no user input.

use crate::app_state::AppState;
use crate::chart_state::ChartState;
use crate::client::ConnectionStatus;
use apexes_chart::candlestick::CandlestickChart;
use apexes_chart::tick_line::TickLineChart;
use apexes_chart::view_state::ViewState;
use apexes_core::{Bar, BarInterval, MarketEvent};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// How often the app requests a repaint while idle, so newly-arrived
/// `MarketEvent`s (which arrive on a background tokio task, not from
/// user interaction) get picked up and rendered promptly.
const REPAINT_INTERVAL: Duration = Duration::from_millis(200);

/// The curated K-line granularities offered in the dropdown, per the
/// design spec's explicit choice not to expose all six
/// `BarInterval` variants `BarAggregator` tracks internally.
const AVAILABLE_INTERVALS: [BarInterval; 4] = [
    BarInterval::OneMinute,
    BarInterval::FiveMinutes,
    BarInterval::FifteenMinutes,
    BarInterval::OneHour,
];

fn interval_label(interval: BarInterval) -> &'static str {
    match interval {
        BarInterval::OneSecond => "1s",
        BarInterval::OneMinute => "1m",
        BarInterval::FiveMinutes => "5m",
        BarInterval::FifteenMinutes => "15m",
        BarInterval::OneHour => "1h",
        BarInterval::OneDay => "1d",
    }
}

pub struct DashboardApp {
    app_state: AppState,
    chart_state: ChartState,
    /// Per-(symbol, interval) accumulated CLOSED bar history --
    /// `ChartState`/`BarAggregator` only ever hand back bars that
    /// close AT THE MOMENT they close (`on_trade`'s return value),
    /// so this history must be accumulated here as events are
    /// drained, not re-derived on every frame.
    closed_bars: HashMap<(String, BarInterval), Vec<Bar>>,
    connection_status: ConnectionStatus,
    event_rx: Option<mpsc::UnboundedReceiver<MarketEvent>>,
    status_rx: Option<watch::Receiver<ConnectionStatus>>,
    symbols: Vec<String>,
    selected_symbol: String,
    selected_interval: BarInterval,
    view: ViewState,
    candlestick_chart: Option<CandlestickChart>,
    tick_line_chart: Option<TickLineChart>,
}

impl DashboardApp {
    /// Production constructor: wires the app up to the real client
    /// task's channels. The two chart widgets are constructed lazily
    /// on the first `ui()` call (`eframe::App::ui`, below), once a real
    /// `wgpu::Device`/`TextureFormat` is available via
    /// `eframe::Frame`'s `wgpu_render_state` -- they cannot be built
    /// in this constructor, which runs before `eframe` has finished
    /// its own `wgpu` setup.
    pub fn new(
        symbols: &[String],
        event_rx: mpsc::UnboundedReceiver<MarketEvent>,
        status_rx: watch::Receiver<ConnectionStatus>,
    ) -> Self {
        let selected_symbol = symbols.first().cloned().unwrap_or_default();
        Self {
            app_state: AppState::new(symbols),
            chart_state: ChartState::new(symbols),
            closed_bars: HashMap::new(),
            connection_status: ConnectionStatus::default(),
            event_rx: Some(event_rx),
            status_rx: Some(status_rx),
            symbols: symbols.to_vec(),
            selected_symbol,
            selected_interval: BarInterval::OneMinute,
            view: ViewState::new_following_live(),
            candlestick_chart: None,
            tick_line_chart: None,
        }
    }

    /// Test-only constructor: builds the app directly from
    /// already-populated `AppState`/`ChartState` and a fixed
    /// `ConnectionStatus`, with no channels -- used by
    /// `tests/ui_renders_without_panicking.rs` so the headless render
    /// test doesn't need a real client task running. The two chart
    /// widgets are still constructed lazily (same as the production
    /// path), since this constructor also runs before any
    /// `wgpu_render_state` is available.
    pub fn new_for_test(app_state: AppState, connection_status: ConnectionStatus) -> Self {
        Self {
            app_state,
            chart_state: ChartState::new(&[]),
            closed_bars: HashMap::new(),
            connection_status,
            event_rx: None,
            status_rx: None,
            symbols: Vec::new(),
            selected_symbol: String::new(),
            selected_interval: BarInterval::OneMinute,
            view: ViewState::new_following_live(),
            candlestick_chart: None,
            tick_line_chart: None,
        }
    }

    fn drain_updates(&mut self) {
        if let Some(event_rx) = self.event_rx.as_mut() {
            let mut drained = Vec::new();
            while let Ok(event) = event_rx.try_recv() {
                drained.push(event);
            }
            for event in &drained {
                self.app_state.apply_event(event);
                let newly_closed = self.chart_state.apply_event(event);
                if let MarketEvent::Trade(trade) = event {
                    for bar in newly_closed {
                        self.closed_bars
                            .entry((trade.symbol.clone(), bar.interval))
                            .or_default()
                            .push(bar);
                    }
                    if self.symbols.contains(&trade.symbol) {
                        self.view.advance_if_following_live(trade.timestamp);
                    }
                }
            }
        }
        if let Some(status_rx) = self.status_rx.as_mut() {
            self.connection_status = *status_rx.borrow();
        }
    }

    /// Combines this symbol/interval's retained closed-bar history
    /// with the current still-forming bar (if any), per this task's
    /// own `Interfaces` contract.
    fn bars_for_chart(&self, symbol: &str, interval: BarInterval) -> Vec<Bar> {
        let mut bars = self
            .closed_bars
            .get(&(symbol.to_string(), interval))
            .cloned()
            .unwrap_or_default();
        if let Some(open_bar) = self.chart_state.current_open_bar_for(symbol, interval) {
            bars.push(open_bar);
        }
        bars
    }
}

impl eframe::App for DashboardApp {
    fn ui(&mut self, ui: &mut egui::Ui, frame: &mut eframe::Frame) {
        self.drain_updates();
        ui.ctx().request_repaint_after(REPAINT_INTERVAL);

        if self.candlestick_chart.is_none()
            && let Some(render_state) = frame.wgpu_render_state.as_ref()
        {
            self.candlestick_chart = Some(CandlestickChart::new(&render_state.device, render_state.target_format));
            self.tick_line_chart = Some(TickLineChart::new(&render_state.device, render_state.target_format));
        }

        let (label, color) = match self.connection_status {
            ConnectionStatus::Connected => ("Connected", egui::Color32::GREEN),
            ConnectionStatus::Reconnecting => ("Reconnecting...", egui::Color32::YELLOW),
            ConnectionStatus::Disconnected => ("Disconnected", egui::Color32::RED),
        };
        ui.horizontal(|ui| {
            ui.colored_label(color, "\u{25CF}");
            ui.label(label);
        });

        ui.separator();

        egui::Grid::new("dashboard_grid")
            .striped(true)
            .show(ui, |ui| {
                ui.strong("Symbol");
                ui.strong("Bid");
                ui.strong("Ask");
                ui.strong("Last Trade");
                ui.end_row();

                for (symbol, row) in self.app_state.rows() {
                    ui.label(symbol);
                    ui.label(row.bid.map_or_else(|| "-".to_string(), |v| v.to_string()));
                    ui.label(row.ask.map_or_else(|| "-".to_string(), |v| v.to_string()));
                    ui.label(
                        row.last_trade
                            .map_or_else(|| "-".to_string(), |v| v.to_string()),
                    );
                    ui.end_row();
                }
            });

        ui.separator();

        ui.horizontal(|ui| {
            ui.label("Chart symbol:");
            egui::ComboBox::from_id_salt("chart_symbol_selector")
                .selected_text(&self.selected_symbol)
                .show_ui(ui, |ui| {
                    for symbol in &self.symbols {
                        ui.selectable_value(&mut self.selected_symbol, symbol.clone(), symbol);
                    }
                });

            ui.label("Interval:");
            egui::ComboBox::from_id_salt("chart_interval_selector")
                .selected_text(interval_label(self.selected_interval))
                .show_ui(ui, |ui| {
                    for interval in AVAILABLE_INTERVALS {
                        ui.selectable_value(&mut self.selected_interval, interval, interval_label(interval));
                    }
                });
        });

        let bars = self.bars_for_chart(&self.selected_symbol, self.selected_interval);
        if let Some(chart) = self.candlestick_chart.as_ref() {
            chart.show(ui, &bars, &mut self.view, egui::Vec2::new(ui.available_width(), 250.0));
        }

        let ticks = self.chart_state.ticks_for(&self.selected_symbol).to_vec();
        if let Some(chart) = self.tick_line_chart.as_ref() {
            chart.show(ui, &ticks, &mut self.view, egui::Vec2::new(ui.available_width(), 150.0));
        }
    }
}
```

- [ ] **Step 5: Run the crate's full test suite**

Run: `cargo test -p apexes-ui`
Expected: all pass (existing 13a tests + 5 new `chart_state` tests). If `ui_renders_without_panicking.rs` (from 13a) fails to compile because `DashboardApp::new_for_test`'s signature is unchanged but `ui.rs`'s internal structure changed, re-read that test file and confirm it still compiles as-is (it should -- `new_for_test`'s public signature is unchanged by this task).

- [ ] **Step 6: Confirm the binary builds**

Run: `cargo build -p apexes-ui --bin apexes-ui`
Expected: builds successfully with 0 errors.

- [ ] **Step 7: Run clippy, fmt, workspace check**

Run: `cargo clippy -p apexes-ui --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-ui/Cargo.toml crates/apexes-ui/src/ui.rs crates/apexes-ui/src/chart_state.rs
git commit -S -s -m "feat(apexes-ui): wire CandlestickChart/TickLineChart into the dashboard (sub-project #13b)"
```

---

### Task 7: Full-workspace verification checkpoint + final whole-branch review + `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

- [ ] **Step 1: Full-workspace verification**

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo test --workspace`
Expected: 0 failed (existing tests unaffected; `apexes-chart`'s 22 tests and `apexes-ui`'s new `chart_state` tests included).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 2: Generate the whole-branch review package and dispatch a final reviewer**

Use the base commit recorded right before Task 1 started (the tip of `master` immediately after this plan document itself was committed) and HEAD (the tip after Task 6). Dispatch a final reviewer (per this project's established subagent-tiering convention, a top-tier profile for final whole-branch reviews) covering:
- Scope match against the design spec: does `apexes-chart` genuinely use hand-written `wgpu` rendering (real `RenderPipeline`/shaders/vertex buffers via `CallbackTrait`), not `egui_plot` or any CPU-tessellation shortcut?
- Do both charts genuinely share one `ViewState` (verify `DashboardApp` constructs exactly one `ViewState` and passes `&mut self.view` to both `chart.show(...)` calls, not two separate instances)?
- Is the "follow live" / `user_adjusted` semantics correctly implemented and tested (does a manual zoom/pan genuinely stop auto-advance permanently, per the design spec's explicit requirement)?
- Is the tick ring buffer's 1-hour retention genuinely enforced on every push (not on a timer), with no code path that could let it grow unbounded?
- Does `BarAggregator::current_open_bar` (Task 1) correctly avoid consuming/mutating the open bucket (re-verify the "does not consume" test genuinely proves this, not just that it doesn't panic)?
- Are both `egui_kittest` `wgpu`-feature render tests (Tasks 4/5) genuine proofs (do they actually exercise the real `CallbackTrait`/pipeline code path, not just construct-and-discard the chart)?
- Whether `docs/FEATURES.md` row `9.2` should move to `✅ Done` per the design spec §8's own recommendation, or whether the reviewer finds a reason it should stay `🚧 In Progress` instead (e.g. if any Critical/Important finding is scope-affecting) -- the reviewer's own reasoned recommendation should be followed unless the user directs otherwise.

- [ ] **Step 3: Process findings**

Fix any Critical/Important findings directly (the controller applies straightforward fixes itself rather than re-dispatching for trivial issues, re-verifying independently afterward). Accept Minor/advisory findings as documented scope decisions unless the user directs otherwise.

- [ ] **Step 4: Update `docs/FEATURES.md`**

Update row `9.2`'s status (per Step 2's final determination) and Notes to record that both a K-line candlestick chart and a bid/ask tick-line chart are genuinely GPU-rendered via a hand-written `wgpu` pipeline (`apexes-chart` crate), interactive (scroll-zoom, drag-pan, shared visible time range), fed by real-time `MarketEvent`s (no ClickHouse historical backfill -- explicitly deferred, matching the design spec's own Non-Goals). Recompute the Summary table's status counts via `grep -c` (never estimated) for each status symbol, subtracting for the legend row and the Summary table's own self-referencing row exactly as done for every prior sub-project's `FEATURES.md` update in this project. Update the "Current active work" narrative paragraph at the bottom of the file to describe this work's completion, avoiding any raw status-emoji string inside prose.

Run: `cargo build --workspace` and `cargo test --workspace` one more time after any Step 3 fixes, to reconfirm a clean state before committing.

- [ ] **Step 5: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: update FEATURES.md row 9.2 (GPU K-line + tick-line chart engine, sub-project #13b)"
```

- [ ] **Step 6: Append a `.superpowers/sdd/progress.md` ledger entry**

Follow this project's established ledger format (one entry per task, plus a final summary entry), then declare this plan's work complete.
