//! How the operator console arranges itself for the width it has.
//!
//! The console was one long single column: a thirteen-row list of numbers
//! down the left edge of a 1,500-point window with nothing beside it. Wide
//! windows now put key figures in a row, related cards side by side, and the
//! strategy parameters in several columns; narrow ones fall back to stacking.
//! All of that is decided here, as plain functions of the available width, so
//! it is tested without a window.

use crate::widgets::GAP;

/// Narrowest a key-figure tile may be before it would clip its number.
pub const MIN_TILE_WIDTH: f32 = 170.0;
/// Most key-figure tiles shown in one row.
pub const MAX_TILES_PER_ROW: usize = 6;
/// Width from which the details card and the risk-limits card share a row.
pub const SPLIT_MID_FROM: f32 = 640.0;
/// Width from which positions/orders and balances share a row.
pub const SPLIT_BOTTOM_FROM: f32 = 700.0;
/// Narrowest a strategy-parameter column may be.
pub const MIN_PARAM_COLUMN: f32 = 330.0;
pub const MAX_PARAM_COLUMNS: usize = 3;

/// Width of the left part of the mid row when it is split (the details card),
/// as a share of the row. The risk card gets the rest.
pub const MID_LEFT_SHARE: f32 = 0.58;
/// The same for the bottom row: positions and open orders (the wider one).
pub const BOTTOM_LEFT_SHARE: f32 = 0.64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsoleLayout {
    /// Key-figure tiles per row, balanced so rows are as even as possible.
    pub tiles_per_row: usize,
    pub split_mid: bool,
    pub split_bottom: bool,
    pub param_columns: usize,
}

/// Tiles per row for `count` tiles in `width`: as many as fit, then evened out
/// so the last row is not left with one orphan (five tiles at four-a-row make
/// 3 + 2, not 4 + 1).
pub fn balanced_tiles_per_row(width: f32, count: usize) -> usize {
    let fit = crate::widgets::tiles_per_row(width, MIN_TILE_WIDTH, MAX_TILES_PER_ROW);
    if count == 0 {
        return fit;
    }
    let rows = count.div_ceil(fit);
    count.div_ceil(rows)
}

pub fn param_columns(width: f32) -> usize {
    if !width.is_finite() || width <= 0.0 {
        return 1;
    }
    (((width + GAP) / (MIN_PARAM_COLUMN + GAP)).floor() as usize).clamp(1, MAX_PARAM_COLUMNS)
}

pub fn layout(width: f32, tile_count: usize) -> ConsoleLayout {
    ConsoleLayout {
        tiles_per_row: balanced_tiles_per_row(width, tile_count),
        split_mid: width >= SPLIT_MID_FROM,
        split_bottom: width >= SPLIT_BOTTOM_FROM,
        param_columns: param_columns(width),
    }
}

/// Splits `width` into two columns by `left_share`, with the gap between.
pub fn two_columns(width: f32, left_share: f32) -> (f32, f32) {
    let usable = (width - GAP).max(0.0);
    let left = usable * left_share.clamp(0.0, 1.0);
    (left, usable - left)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn six_key_figures_make_two_even_rows_when_they_do_not_all_fit() {
        // The default window leaves about 650 points beside the orders panel:
        // four tiles would fit, but 4 + 2 looks lopsided.
        assert_eq!(balanced_tiles_per_row(650.0, 6), 3);
        assert_eq!(balanced_tiles_per_row(1260.0, 6), 6, "all fit in one row");
        assert_eq!(balanced_tiles_per_row(380.0, 6), 2);
        assert_eq!(balanced_tiles_per_row(150.0, 6), 1, "one tile still shows");
    }

    #[test]
    fn five_tiles_make_three_and_two_never_four_and_one() {
        assert_eq!(balanced_tiles_per_row(740.0, 5), 3, "fit 4 -> 3 + 2");
        assert_eq!(balanced_tiles_per_row(1100.0, 5), 5);
    }

    #[test]
    fn a_row_never_holds_more_than_fit_and_never_fewer_than_one() {
        for width in [0.0, 50.0, 200.0, 400.0, 650.0, 900.0, 1260.0, 3000.0] {
            for count in 0..=9usize {
                let per_row = balanced_tiles_per_row(width, count);
                let fit = crate::widgets::tiles_per_row(width, MIN_TILE_WIDTH, MAX_TILES_PER_ROW);
                assert!(
                    (1..=fit).contains(&per_row),
                    "w{width} n{count}: {per_row} (fit {fit})"
                );
            }
        }
    }

    #[test]
    fn no_tile_is_narrower_than_the_minimum_when_the_window_allows_it() {
        for width in [400.0, 650.0, 900.0, 1260.0] {
            let per_row = balanced_tiles_per_row(width, 6);
            let tile = crate::widgets::tile_width(width, per_row);
            assert!(tile >= MIN_TILE_WIDTH, "w{width}: tile {tile}");
        }
    }

    #[test]
    fn parameters_use_one_to_three_columns_by_width() {
        assert_eq!(param_columns(300.0), 1);
        assert_eq!(param_columns(700.0), 2);
        assert_eq!(param_columns(1100.0), 3);
        assert_eq!(param_columns(5000.0), MAX_PARAM_COLUMNS);
        assert_eq!(param_columns(0.0), 1);
        assert_eq!(param_columns(f32::NAN), 1);
    }

    #[test]
    fn cards_share_a_row_only_when_each_has_room() {
        let narrow = layout(500.0, 6);
        assert!(!narrow.split_mid && !narrow.split_bottom, "narrow: stacked");
        let medium = layout(660.0, 6);
        assert!(medium.split_mid && !medium.split_bottom);
        let wide = layout(1260.0, 6);
        assert!(wide.split_mid && wide.split_bottom);
    }

    #[test]
    fn the_minimum_window_still_gets_a_usable_layout() {
        // 760 wide minus margins, orders panel below: about 720 usable.
        let l = layout(720.0, 6);
        assert!(l.tiles_per_row >= 3);
        assert!(l.split_mid && l.split_bottom);
        assert_eq!(l.param_columns, 2);
    }

    #[test]
    fn two_columns_split_the_width_exactly_with_the_gap_between() {
        for width in [0.0, 300.0, 640.0, 1260.0] {
            let (left, right) = two_columns(width, 0.58);
            if width > GAP {
                assert!((left + right + GAP - width).abs() < 0.01, "w{width}");
            }
            assert!(left >= 0.0 && right >= 0.0);
        }
        let (l, r) = two_columns(1000.0, 5.0);
        assert!(l <= 990.0 + 0.01 && r >= 0.0, "a bad share is clamped");
    }
}
