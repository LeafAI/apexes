# Apexes: GPU K-line + Tick-line Chart Engine (Sub-project #13b) Design

## 1. Context and Scope

Sub-project `#13` (UI) is split into sequential phases, tracked in `docs/superpowers/specs/2026-08-04-apexes-ui-foundation-design.md` §1. Phase 13a (complete: `apexes-ui` crate -- a real `MarketEventBus`/`BinanceMarketDataProvider` subscription bridged over a real localhost WebSocket into an `egui`/`eframe` desktop table of bid/ask/last-trade price for three hardcoded Binance Spot instruments) is the foundation this phase builds on.

This spec covers **13b**: a hand-rolled `wgpu`-rendered chart engine, embedded into `apexes-ui`'s existing dashboard, adding two chart widgets below the existing table for the currently-selected symbol:

1. A **K-line (candlestick) chart**, fed by real-time OHLCV aggregation of the incoming `Trade` stream.
2. A **tick-line chart**, showing bid/ask price as two continuous lines over time, fed directly by the incoming `Tick` stream.

Per the roadmap doc's own committed technology direction (`docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md` §2.4: "GPU 渲染 K线/Tick 图表引擎（wgpu）"), both charts are rendered via a genuinely hand-written `wgpu` pipeline (vertex/fragment shaders, vertex buffers, render passes) embedded into egui's frame via `egui_wgpu::CallbackTrait` -- not `egui_plot` (egui's own CPU-tessellation-based plotting library), though `egui_plot`'s interaction conventions (scroll-to-zoom, drag-to-pan, auto-fitting axes) are used as the API/UX reference point.

## 2. Goals

- Ship a genuinely GPU-rendered candlestick chart and tick-line chart, both real-time, both interactive (zoom/pan), sharing one visible time range.
- Establish `apexes-chart` as a reusable rendering crate other future UI phases (13c-13f) can draw on for their own chart/visualization needs, rather than one-off code embedded directly in `apexes-ui`.
- Keep scope deliberately minimal beyond that: no historical backfill, no crosshair/tooltip/order-marker overlays, a curated (not exhaustive) set of K-line granularities.

## 3. Non-Goals (explicitly deferred)

- ClickHouse historical backfill for chart cold-start -- both charts start empty at process launch and fill in as new data arrives in real time. This is a deliberate scope reduction to keep 13b minimal, not an oversight; a future phase may add historical backfill if needed.
- Any interaction beyond scroll-wheel zoom and drag-to-pan (no crosshair, no hover tooltips, no order/fill markers on the chart).
- More than four K-line granularities (1 minute / 5 minutes / 15 minutes / 1 hour) in the granularity selector, even though `apexes_market_data::BarAggregator` (already in the workspace) tracks all six `BarInterval` variants internally.
- Multi-symbol simultaneous chart display -- one K-line chart and one tick-line chart are shown at a time, for whichever symbol is currently selected in a shared dropdown; switching the dropdown switches what both charts display.
- Order/position/risk overlays on the chart (that is sub-project `#13d`'s scope).

## 4. Architecture

### 4.1 New crate: `apexes-chart`

A reusable `wgpu`-rendering crate, depended on by `apexes-ui` (and available to future UI phases). Two widgets, sharing common infrastructure:

```
crates/apexes-chart/
  Cargo.toml
  src/
    lib.rs
    view_state.rs   -- pure logic: shared visible-time-range state,
                        zoom/pan, Y-axis auto-fit. No wgpu/egui deps.
    geometry.rs      -- pure logic: (data, ViewState, viewport pixel
                         size) -> Vec<Vertex>. Separate functions for
                         candlestick geometry (body rectangles + wick
                         line segments) and tick-line geometry (two
                         polylines, bid and ask). No wgpu deps --
                         plain vertex structs in, testable without a
                         GPU context.
    candlestick.rs   -- CandlestickChart widget: egui_wgpu::CallbackTrait
                         impl owning the candle-rendering wgpu pipeline,
                         calls geometry.rs each frame.
    tick_line.rs      -- TickLineChart widget: egui_wgpu::CallbackTrait
                         impl owning the line-rendering wgpu pipeline,
                         calls geometry.rs each frame.
    shaders/
      candlestick.wgsl -- vertex + fragment shader, solid-fill triangles
                           (candle bodies as rectangles, wicks as thin
                           rectangles), red for close > open, green for
                           close <= open (per this project's chosen
                           convention -- explicitly the reverse of the
                           TradingView-style "green up / red down"
                           convention some readers may expect).
      tick_line.wgsl    -- vertex + fragment shader for the two
                            polylines (bid, ask), rendered as
                            thick-line-as-quads (a single-pixel-wide
                            wgpu line primitive is visually too thin
                            and has inconsistent width across
                            backends; quads give predictable,
                            controllable line width).
  tests/
    candlestick_renders_without_panicking.rs -- egui_kittest, wgpu
                                                  feature, synthetic
                                                  Bar data
    tick_line_renders_without_panicking.rs    -- egui_kittest, wgpu
                                                  feature, synthetic
                                                  tick data
```

`ViewState` (from `view_state.rs`) is constructed once by the caller (`apexes-ui`) and passed by mutable reference to both `CandlestickChart::show` and `TickLineChart::show` on every frame -- this is what keeps the two charts' visible time range synchronized: whichever widget's interaction (scroll/drag) runs first in a given frame updates the single shared `ViewState`, and the other widget reads the already-updated range when it renders immediately after. Y-axis auto-fit is computed independently per chart (candlestick fits to visible bars' high/low; tick-line fits to visible ticks' bid/ask range) since the two data series have unrelated value ranges, even though they share the same X-axis (time) range.

### 4.2 `view_state.rs` (pure logic, no `wgpu`/`egui` dependency)

```rust
pub struct ViewState {
    pub visible_range: std::ops::Range<chrono::DateTime<chrono::Utc>>,
}
```

- Constructed with an initial default range of "now minus 5 minutes" to "now" when a symbol is first selected. On every new `Bar`/`TickPoint` arrival, if the user has not manually panned or zoomed since the range was last auto-advanced (tracked via a `user_adjusted: bool` flag on `ViewState`, set by `zoom`/`pan` and never cleared automatically), the range's right edge is advanced to the new data's timestamp and the left edge advances by the same delta (keeping the range's width, and therefore the current zoom level, unchanged) -- this keeps new data continuously visible by default ("follow live" behavior). Once the user manually zooms or pans, `user_adjusted` becomes `true` and auto-advance stops permanently for that `ViewState` instance, so the user's chosen view of history is not yanked out from under them by new data -- switching symbols (which constructs a fresh `ViewState`) is the only way to return to "follow live" mode. This matches `egui_plot`'s own established UX convention of not fighting user-initiated navigation.
- `zoom(&mut self, cursor_fraction: f32, delta: f32)`: narrows/widens `visible_range` around a point (as a 0.0-1.0 fraction across the current visible width, corresponding to the mouse cursor's X position at scroll time), mirroring `egui_plot`'s scroll-to-zoom-toward-cursor behavior.
- `pan(&mut self, delta_fraction: f32)`: shifts `visible_range` by a fraction of its current width, driven by drag-delta-in-pixels converted to a time-delta by the caller (widget code, not `view_state.rs` itself, since only the widget knows the current pixel-to-time-range mapping at render time).
- `y_axis_range_for(&self, values: impl Iterator<Item = Decimal>) -> Range<Decimal>`: computes an auto-fit Y range (min/max of the given values, mapped from `visible_range`-filtered data by the caller) with a small fixed-fraction padding above/below so candle wicks/line peaks don't touch the viewport edge.

### 4.3 `geometry.rs` (pure logic, no `wgpu` dependency)

```rust
pub struct Vertex {
    pub position: [f32; 2], // normalized device coordinates
    pub color: [f32; 4],
}

pub fn candlestick_vertices(
    bars: &[apexes_core::Bar],
    view: &ViewState,
    y_range: &std::ops::Range<rust_decimal::Decimal>,
    viewport_size: [f32; 2],
) -> Vec<Vertex>;

pub fn tick_line_vertices(
    ticks: &[TickPoint], // (DateTime<Utc>, bid: Decimal, ask: Decimal)
    view: &ViewState,
    y_range: &std::ops::Range<rust_decimal::Decimal>,
    viewport_size: [f32; 2],
) -> Vec<Vertex>;
```

Both functions filter their input to `view.visible_range` first, then map each remaining point's `(time, price)` into normalized device coordinates via straightforward linear interpolation against `view.visible_range` (X) and `y_range` (Y). `candlestick_vertices` emits two triangles per candle body (a filled rectangle, red if `close > open` else green) plus two thin rectangles per candle for the high/low wick. `tick_line_vertices` emits a thick-line-as-quads strip per line (bid, ask), each quad connecting two consecutive tick points.

### 4.4 `candlestick.rs` / `tick_line.rs` (the only modules touching `wgpu` directly)

Each implements `egui_wgpu::CallbackTrait`'s `prepare`/`paint` methods (per `egui-wgpu` 0.35's documented callback mechanism): `prepare` calls the corresponding `geometry.rs` function to build the current frame's vertex buffer and uploads it to the GPU; `paint` binds the pipeline and vertex buffer and issues the draw call into the render pass egui provides. Both widgets expose a `show(ui: &mut egui::Ui, ...)` method (mirroring `egui_plot::Plot::show`'s API shape) that allocates a response rect, reads scroll/drag input from that rect via `egui::Ui`'s response object, calls into the shared `ViewState` (`zoom`/`pan`), and issues the `egui_wgpu` paint callback.

### 4.5 `apexes-ui` integration

- New per-symbol state in `ui.rs`: a `BarAggregator` (from `apexes_market_data::bar_aggregator`, already in the workspace) fed every incoming `MarketEvent::Trade` for that symbol, and a time-windowed ring buffer of `TickPoint { timestamp, bid, ask }` fed every incoming `MarketEvent::Tick`, retaining only points from the last 1 hour (older points evicted on each push, by dropping from the front while the oldest point's timestamp is more than 1 hour before the newest).
- A single shared `ViewState`, a symbol-selector dropdown (BTCUSDT/ETHUSDT/BNBUSDT, matching the existing table), and a `BarInterval` dropdown (1 minute / 5 minutes / 15 minutes / 1 hour) are added above the two new chart widgets. Selecting a different symbol swaps which `BarAggregator`'s bars and which ring buffer's ticks feed the two charts (their own accumulated data keeps running in the background for all three symbols regardless of which is currently displayed, matching the existing table's already-established "always tracks all three, display shows what's selected" pattern).
- Layout, top to bottom: existing bid/ask/last-trade table (unchanged from 13a) -> symbol/interval selector row -> `CandlestickChart` -> `TickLineChart`.

## 5. Dependencies (new workspace-level additions)

- `egui-wgpu` (the `egui_wgpu::CallbackTrait` custom-rendering integration point; `eframe`/`egui` already depend on it transitively, but this is the first crate to use it directly).
- `wgpu` (direct dependency for the hand-written pipeline/shader code in `apexes-chart`).
- `bytemuck` (standard `wgpu` companion crate for safely casting `Vertex` structs to raw GPU buffer bytes -- confirm exact version against what `wgpu`/`egui-wgpu` 0.35 already pull in as a transitive dependency before pinning a new version, to avoid a duplicate-version resolution warning).

No other new dependencies -- `apexes_market_data::BarAggregator`, `apexes_core::{Bar, BarInterval, MarketEvent}`, `chrono`, `rust_decimal` are all already available in the workspace.

## 6. Error Handling and Resilience

- `wgpu` pipeline/shader compilation failures are startup-time, not per-frame -- a shader compile error should surface as a clear panic/log at `apexes-ui` startup (the same class of "fail fast with a clear message" behavior 13a established for WebSocket bind/subscribe failures), not a silent fallback to a blank chart.
- No new `ApexesError` variants -- `apexes-chart` is a pure rendering crate with no fallible network/IO operations of its own; its only failure modes are GPU/shader-level, handled per the point above.
- The tick ring buffer's 1-hour eviction must never allow unbounded growth regardless of tick rate -- eviction runs on every push, not on a timer, so there is no window where the buffer can grow past its time-based bound even under a burst of ticks.

## 7. Testing Strategy

Mirrors 13a's two-tier strategy:

1. **Unit tests, no I/O, no `wgpu`/`egui`:** `view_state.rs`'s zoom/pan math and Y-axis auto-fit calculation; `geometry.rs`'s bar-to-vertex and tick-to-vertex conversion (including the red/green color-assignment rule and the visible-range filtering); the tick ring buffer's push/evict logic in `apexes-ui`.
2. **Integration tests, `egui_kittest` `wgpu` feature:** one test per widget (`candlestick_renders_without_panicking.rs`, `tick_line_renders_without_panicking.rs`), each constructing synthetic data (a handful of `Bar`s / `TickPoint`s) and driving one `egui_kittest::Harness` render pass using its real `WgpuTestRenderer` (per `egui_kittest` 0.35's `wgpu` feature, which prefers a software rasterizer adapter where available), asserting no panic.
   - **Known environment dependency, documented honestly rather than silently assumed:** this requires a working `wgpu` backend at test-run time. Verified working on the current macOS development environment (Metal backend, available by default). If this project's tests are ever run on a GPU-less Linux CI machine, a software rasterizer (e.g. Mesa `llvmpipe`) must be installed for these two tests to pass -- this is a real, not hypothetical, constraint worth tracking if/when this project sets up Linux CI, not a blocker for current development.
3. No test depends on a real Binance connection or real market data -- both chart tests use hand-constructed synthetic `Bar`/`TickPoint` values.

## 8. Documentation Honesty

`docs/FEATURES.md` row `9.2` ("High-performance K-line/tick chart engine (GPU rendering, wgpu)") should be marked `✅ Done` once this phase lands, since 13b delivers exactly what row 9.2 describes: both a K-line and a tick chart, genuinely GPU-rendered via hand-written `wgpu`. Row `9.1`'s own remaining gaps (Level-2 depth visualization, tick-by-tick trade heatmap -- noted as still open in 13a's own `FEATURES.md` update) are unaffected by this phase and remain tracked separately under row 9.1, not folded into 9.2's completion.
