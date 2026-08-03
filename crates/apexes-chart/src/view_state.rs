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
        let width_seconds =
            (self.visible_range.end - self.visible_range.start).num_milliseconds() as f64 / 1000.0;
        let zoom_factor = (1.0 - (delta as f64) * 0.1).clamp(0.1, 10.0);
        let new_width_seconds = (width_seconds * zoom_factor).max(1.0);
        let cursor_time = self.visible_range.start
            + chrono::Duration::milliseconds((width_seconds * cursor_fraction * 1000.0) as i64);
        let new_start_offset_seconds =
            (cursor_time - self.visible_range.start).num_milliseconds() as f64 / 1000.0
                * (new_width_seconds / width_seconds);
        let new_start = cursor_time
            - chrono::Duration::milliseconds((new_start_offset_seconds * 1000.0) as i64);
        let new_end =
            new_start + chrono::Duration::milliseconds((new_width_seconds * 1000.0) as i64);
        self.visible_range = new_start..new_end;
    }

    /// Shifts the visible range by `delta_fraction` of its current
    /// width (positive shifts forward in time / pans right, negative
    /// shifts backward / pans left). Marks this `ViewState` as
    /// manually adjusted, permanently stopping "follow live"
    /// auto-advance.
    pub fn pan(&mut self, delta_fraction: f32) {
        self.user_adjusted = true;
        let width_seconds =
            (self.visible_range.end - self.visible_range.start).num_milliseconds() as f64 / 1000.0;
        let shift_ms = (width_seconds * delta_fraction as f64 * 1000.0) as i64;
        let shift = chrono::Duration::milliseconds(shift_ms);
        self.visible_range = (self.visible_range.start + shift)..(self.visible_range.end + shift);
    }

    /// Computes an auto-fit Y range from `values` (already filtered
    /// by the caller to the currently-visible data), with
    /// `Y_AXIS_PADDING_FRACTION` padding above/below. Returns `None`
    /// if `values` is empty (nothing visible to fit to).
    pub fn y_axis_range_for(
        &self,
        values: impl Iterator<Item = Decimal>,
    ) -> Option<Range<Decimal>> {
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
        let padding = span
            * Decimal::try_from(Y_AXIS_PADDING_FRACTION)
                .expect("Y_AXIS_PADDING_FRACTION is a valid finite constant");
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
        assert!(
            width < 300,
            "zooming in with positive delta should narrow the range, got width {width}"
        );
    }

    #[test]
    fn zoom_out_widens_the_visible_range() {
        let mut state = state_with_range(0, 300);
        state.zoom(0.5, -2.0);
        let width = (state.visible_range().end - state.visible_range().start).num_seconds();
        assert!(
            width > 300,
            "zooming out with negative delta should widen the range, got width {width}"
        );
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
        let range = state
            .y_axis_range_for(vec![dec!(100), dec!(100), dec!(100)].into_iter())
            .unwrap();
        assert!(range.start < range.end);
    }
}
