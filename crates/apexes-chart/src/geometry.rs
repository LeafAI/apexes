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

fn time_to_ndc_x(
    time: chrono::DateTime<chrono::Utc>,
    range: &Range<chrono::DateTime<chrono::Utc>>,
) -> f64 {
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
    let fraction = ((price - y_range.start) / span).to_f64().unwrap_or(0.5);
    ((fraction * 2.0 - 1.0) as f32).clamp(-1.0, 1.0)
}

fn push_quad(
    vertices: &mut Vec<Vertex>,
    left: f32,
    right: f32,
    bottom: f32,
    top: f32,
    color: [f32; 4],
) {
    vertices.push(Vertex {
        position: [left, bottom],
        color,
    });
    vertices.push(Vertex {
        position: [right, bottom],
        color,
    });
    vertices.push(Vertex {
        position: [right, top],
        color,
    });
    vertices.push(Vertex {
        position: [left, bottom],
        color,
    });
    vertices.push(Vertex {
        position: [right, top],
        color,
    });
    vertices.push(Vertex {
        position: [left, top],
        color,
    });
}

/// Converts `bars` (any mix of closed bars and the current
/// `BarAggregator::current_open_bar` result) into candlestick
/// geometry: a filled rectangle per candle body (two triangles, six
/// vertices) plus a thin rectangle per wick (six more vertices)
/// spanning `low` to `high`. Bars entirely outside
/// `view.visible_range()` are skipped.
pub fn candlestick_vertices(
    bars: &[Bar],
    view: &ViewState,
    y_range: &Range<Decimal>,
) -> Vec<Vertex> {
    let range = view.visible_range();
    let mut vertices = Vec::new();

    for bar in bars {
        if bar.close_time < range.start || bar.open_time > range.end {
            continue;
        }
        let color = if bar.close > bar.open {
            BULLISH_COLOR
        } else {
            BEARISH_COLOR
        };
        let center_x = time_to_ndc_x(bar.open_time, range);
        let half_width = CANDLE_HALF_WIDTH_FRACTION
            * (2.0 / ((range.end - range.start).num_milliseconds() as f64).max(1.0))
            * ((bar.close_time - bar.open_time).num_milliseconds() as f64).max(1.0);
        let left = center_x - half_width;
        let right = center_x + half_width;
        let open_y = price_to_ndc_y(bar.open, y_range);
        let close_y = price_to_ndc_y(bar.close, y_range);
        let (body_bottom, body_top) = if close_y >= open_y {
            (open_y, close_y)
        } else {
            (close_y, open_y)
        };

        push_quad(
            &mut vertices,
            left as f32,
            right as f32,
            body_bottom,
            body_top,
            color,
        );

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
pub fn tick_line_vertices(
    ticks: &[TickPoint],
    view: &ViewState,
    y_range: &Range<Decimal>,
) -> Vec<Vertex> {
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

    push_line_strip(
        &mut vertices,
        visible,
        range,
        y_range,
        |t| t.bid,
        BID_LINE_COLOR,
    );
    push_line_strip(
        &mut vertices,
        visible,
        range,
        y_range,
        |t| t.ask,
        ASK_LINE_COLOR,
    );

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
        let (nx, ny) = (
            -dy / length * LINE_HALF_THICKNESS_NDC,
            dx / length * LINE_HALF_THICKNESS_NDC,
        );

        vertices.push(Vertex {
            position: [ax + nx, ay + ny],
            color,
        });
        vertices.push(Vertex {
            position: [ax - nx, ay - ny],
            color,
        });
        vertices.push(Vertex {
            position: [bx - nx, by - ny],
            color,
        });
        vertices.push(Vertex {
            position: [ax + nx, ay + ny],
            color,
        });
        vertices.push(Vertex {
            position: [bx - nx, by - ny],
            color,
        });
        vertices.push(Vertex {
            position: [bx + nx, by + ny],
            color,
        });
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
        let current_width =
            (state.visible_range().end - state.visible_range().start).num_seconds() as f32;
        state.pan(shift_seconds / current_width);
        state
    }

    fn bar(
        open_time: i64,
        close_time: i64,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
    ) -> Bar {
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
        assert_eq!(
            vertices.len(),
            12,
            "6 vertices for the body quad + 6 for the wick quad"
        );
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
            TickPoint {
                timestamp: t(0),
                bid: dec!(100),
                ask: dec!(101),
            },
            TickPoint {
                timestamp: t(60),
                bid: dec!(102),
                ask: dec!(103),
            },
            TickPoint {
                timestamp: t(120),
                bid: dec!(101),
                ask: dec!(102),
            },
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
        let ticks = vec![TickPoint {
            timestamp: t(1000),
            bid: dec!(100),
            ask: dec!(101),
        }];
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
            TickPoint {
                timestamp: t(50),
                bid: dec!(100),
                ask: dec!(101),
            }, // just before range start (100)
            TickPoint {
                timestamp: t(150),
                bid: dec!(102),
                ask: dec!(103),
            }, // inside range
        ];
        let view = state_with_range(100, 300);
        let y_range = dec!(95)..dec!(110);
        let vertices = tick_line_vertices(&ticks, &view, &y_range);
        assert!(
            !vertices.is_empty(),
            "the segment crossing into the visible range must still render"
        );
    }
}
