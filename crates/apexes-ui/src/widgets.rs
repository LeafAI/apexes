//! The small building blocks of the console's look: cards, section titles,
//! KPI tiles and pills.
//!
//! One implementation of each, so every part of the window has the same
//! margins, the same type sizes and the same colours, in both themes. What is
//! a *decision* (which colour a number gets, how many tiles fit a row) is a
//! plain function here and is tested; the egui drawing only renders it.

use egui::{Color32, RichText};
use rust_decimal::Decimal;

use crate::theme::Palette;

/// Inner margin of a card, in points.
pub const CARD_MARGIN: i8 = 12;
/// Gap between cards and between tiles.
pub const GAP: f32 = 10.0;
/// Corner radius for cards and pills.
pub const RADIUS: u8 = 8;

/// Type sizes, in points. The default egui sizes made every heading the same
/// weight as body text; a hierarchy needs steps people can see.
pub const SIZE_KPI: f32 = 24.0;
pub const SIZE_TITLE: f32 = 15.0;
pub const SIZE_CAPTION: f32 = 12.0;

// The type scale must have steps people can see. Checked when the code is
// compiled, so a careless edit to a size does not build.
const _: () = {
    assert!(
        SIZE_KPI >= SIZE_TITLE * 1.4,
        "a key figure must stand out from a title"
    );
    assert!(
        SIZE_TITLE > SIZE_CAPTION,
        "a title is bigger than a caption"
    );
    assert!(SIZE_CAPTION >= 11.0, "captions stay legible");
};

/// How a signed number is coloured: gains good, losses bad, zero and anything
/// unknown neutral. Colour only reinforces the sign, which is always printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sign {
    Gain,
    Loss,
    Flat,
}

pub fn sign_of(value: Decimal) -> Sign {
    // Two decimals is what is shown; a value that rounds to 0.00 is flat.
    let shown = value.round_dp(2);
    if shown > Decimal::ZERO {
        Sign::Gain
    } else if shown < Decimal::ZERO {
        Sign::Loss
    } else {
        Sign::Flat
    }
}

pub fn sign_color(palette: &Palette, sign: Sign, neutral: Color32) -> Color32 {
    match sign {
        Sign::Gain => palette.good,
        Sign::Loss => palette.danger,
        Sign::Flat => neutral,
    }
}

/// How many equal tiles fit a row of `width`, at least `min_tile` wide each and
/// never more than `max`. At least one, so a very narrow window still shows
/// something.
pub fn tiles_per_row(width: f32, min_tile: f32, max: usize) -> usize {
    if !width.is_finite() || width <= 0.0 || min_tile <= 0.0 {
        return 1;
    }
    let fit = ((width + GAP) / (min_tile + GAP)).floor() as usize;
    fit.clamp(1, max.max(1))
}

/// The width of one of `count` tiles that fill `width` with `GAP` between.
pub fn tile_width(width: f32, count: usize) -> f32 {
    let count = count.max(1) as f32;
    ((width - GAP * (count - 1.0)) / count).max(0.0)
}

/// A card: a surface a step off the window background with a hairline border.
pub fn card<R>(
    ui: &mut egui::Ui,
    palette: &Palette,
    add_contents: impl FnOnce(&mut egui::Ui) -> R,
) -> R {
    egui::Frame::new()
        .fill(palette.card_fill)
        .stroke(egui::Stroke::new(1.0, palette.card_stroke))
        .corner_radius(RADIUS)
        .inner_margin(CARD_MARGIN)
        .show(ui, |ui| {
            // Fill the width the caller gave: a card must not shrink to its
            // content, or neighbours would not line up.
            ui.set_width(ui.available_width());
            // Always top-down. A card is usually placed inside a horizontal row
            // (tiles, side-by-side cards) and a child ui inherits its parent's
            // direction, which laid every card's contents out sideways.
            ui.with_layout(egui::Layout::top_down(egui::Align::Min), add_contents)
                .inner
        })
        .inner
}

/// A section title: bigger than body text, in the strong colour, with a
/// little air under it.
pub fn section_title(ui: &mut egui::Ui, text: &str) {
    ui.label(RichText::new(text).size(SIZE_TITLE).strong());
    ui.add_space(4.0);
}

/// A caption: small, quiet text such as the label under a number.
pub fn caption(palette: &Palette, text: &str) -> RichText {
    RichText::new(text).size(SIZE_CAPTION).color(palette.label)
}

/// A key figure: a caption over a large number, in a card of exact width.
pub fn kpi_tile(
    ui: &mut egui::Ui,
    palette: &Palette,
    width: f32,
    label: &str,
    value: &str,
    value_color: Option<Color32>,
    note: Option<&str>,
) {
    ui.allocate_ui(egui::vec2(width, 0.0), |ui| {
        card(ui, palette, |ui| {
            ui.label(caption(palette, label));
            let mut text = RichText::new(value).size(SIZE_KPI).strong();
            if let Some(color) = value_color {
                text = text.color(color);
            }
            ui.label(text);
            match note {
                Some(note) => ui.label(caption(palette, note)),
                // Keep every tile the same height whether or not it has a note.
                None => ui.label(caption(palette, " ")),
            };
        });
    });
}

/// Height of one label/value row.
pub const ROW_HEIGHT: f32 = 22.0;

/// Which type a row's label is set in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelStyle {
    Normal,
    /// Smaller and quieter: a sub-item of the row above it.
    Sub,
}

/// One row of `width`: the label at the left, the value at the right edge, each
/// a real label placed in an exact rectangle.
///
/// Exact, because a layout (`right_to_left`, a grid cell) takes whatever width it
/// is offered, which made cards wider than their column and pushed the page under
/// its neighbour. Real labels, not `painter.text`, because painted text creates
/// no accessibility node: a screen reader (and the test harness) would not see
/// the number at all.
pub fn pair_row(
    ui: &mut egui::Ui,
    palette: &Palette,
    width: f32,
    label: &str,
    value: &str,
    style: LabelStyle,
) {
    let width = width.max(0.0);
    let (rect, _) = ui.allocate_exact_size(egui::vec2(width, ROW_HEIGHT), egui::Sense::hover());
    let (label_text, indent) = match style {
        LabelStyle::Normal => (RichText::new(label), 0.0),
        LabelStyle::Sub => (caption(palette, label), 12.0),
    };
    let (label_rect, value_rect) = pair_rects(rect, indent);
    place(ui, label_rect, egui::Align::Min, label_text);
    place(
        ui,
        value_rect,
        egui::Align::Max,
        RichText::new(value).monospace(),
    );
}

/// The two rectangles of a row: the label gets the left 60%, the value the right
/// 40%, so a long label cannot push its value off the card. Never overlapping,
/// whatever the width.
pub fn pair_rects(row: egui::Rect, indent: f32) -> (egui::Rect, egui::Rect) {
    let width = row.width().max(0.0);
    let split = row.min.x + width * 0.6;
    let label = egui::Rect::from_min_max(
        egui::pos2((row.min.x + indent).min(split), row.min.y),
        egui::pos2(split, row.max.y),
    );
    let value = egui::Rect::from_min_max(egui::pos2(split, row.min.y), row.max);
    (label, value)
}

/// A real, clipped label centred vertically in `rect` and aligned to one edge.
fn place(ui: &mut egui::Ui, rect: egui::Rect, align: egui::Align, text: RichText) {
    let layout = match align {
        egui::Align::Max => egui::Layout::right_to_left(egui::Align::Center),
        _ => egui::Layout::left_to_right(egui::Align::Center),
    };
    let mut cell = ui.new_child(egui::UiBuilder::new().max_rect(rect).layout(layout));
    cell.set_clip_rect(rect.intersect(ui.clip_rect()));
    cell.add(egui::Label::new(text).truncate());
}

/// A pill: a short status word on a tinted rounded background. The word, not
/// the colour, carries the meaning.
pub fn pill(ui: &mut egui::Ui, text: &str, color: Color32, text_color: Color32) {
    egui::Frame::new()
        .fill(color)
        .corner_radius(RADIUS)
        .inner_margin(egui::Margin::symmetric(8, 2))
        .show(ui, |ui| {
            ui.label(
                RichText::new(text)
                    .size(SIZE_CAPTION)
                    .strong()
                    .color(text_color),
            );
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn gains_are_good_losses_bad_and_anything_that_rounds_to_zero_is_flat() {
        assert_eq!(sign_of(dec!(931.29)), Sign::Gain);
        assert_eq!(sign_of(dec!(-271.99)), Sign::Loss);
        assert_eq!(sign_of(dec!(0)), Sign::Flat);
        assert_eq!(
            sign_of(dec!(0.004)),
            Sign::Flat,
            "shows as 0.00, so not a gain"
        );
        assert_eq!(sign_of(dec!(-0.004)), Sign::Flat, "and not a loss");
        assert_eq!(sign_of(dec!(0.006)), Sign::Gain, "rounds to 0.01");
        assert_eq!(sign_of(dec!(-0.006)), Sign::Loss, "rounds to -0.01");
    }

    /// The colour must agree with the printed number, whatever the rounding
    /// rule does at the edge (banker's rounding makes 0.005 show as 0.00).
    #[test]
    fn the_colour_always_agrees_with_what_the_number_shows() {
        for raw in [
            "0.004", "0.005", "0.006", "-0.005", "-0.006", "0", "12.345", "-12.345",
        ] {
            let value: Decimal = raw.parse().unwrap();
            let shown = crate::number_format::money(value);
            let printed_negative =
                shown.starts_with('-') && shown.trim_start_matches('-') != "0.00";
            let printed_zero = shown.trim_start_matches('-') == "0.00";
            match sign_of(value) {
                Sign::Loss => assert!(printed_negative, "{raw} -> {shown}"),
                Sign::Flat => assert!(printed_zero, "{raw} -> {shown}"),
                Sign::Gain => assert!(!printed_negative && !printed_zero, "{raw} -> {shown}"),
            }
        }
    }

    #[test]
    fn a_sign_picks_the_themes_colour_and_flat_keeps_the_neutral_one() {
        for theme in [egui::Theme::Light, egui::Theme::Dark] {
            let p = Palette::for_theme(theme);
            let neutral = Color32::from_gray(10);
            assert_eq!(sign_color(&p, Sign::Gain, neutral), p.good);
            assert_eq!(sign_color(&p, Sign::Loss, neutral), p.danger);
            assert_eq!(sign_color(&p, Sign::Flat, neutral), neutral);
        }
    }

    #[test]
    fn tiles_per_row_follows_the_width_and_never_drops_below_one() {
        assert_eq!(tiles_per_row(1800.0, 200.0, 6), 6, "capped at the maximum");
        assert_eq!(tiles_per_row(1000.0, 200.0, 6), 4);
        assert_eq!(tiles_per_row(430.0, 200.0, 6), 2);
        assert_eq!(tiles_per_row(150.0, 200.0, 6), 1, "narrower than one tile");
        assert_eq!(tiles_per_row(0.0, 200.0, 6), 1);
        assert_eq!(tiles_per_row(f32::NAN, 200.0, 6), 1);
        assert_eq!(
            tiles_per_row(500.0, 0.0, 6),
            1,
            "a bad minimum cannot divide by zero"
        );
        assert_eq!(
            tiles_per_row(1000.0, 200.0, 0),
            1,
            "a zero maximum still shows one"
        );
    }

    #[test]
    fn tiles_exactly_fill_the_row_with_the_gap_between_them() {
        for count in 1..=6usize {
            let width = 1000.0;
            let tile = tile_width(width, count);
            let total = tile * count as f32 + GAP * (count as f32 - 1.0);
            assert!((total - width).abs() < 0.001, "{count} tiles: {total}");
        }
        assert_eq!(tile_width(5.0, 6), 0.0, "never negative");
        assert!(
            tile_width(100.0, 0).is_finite(),
            "zero tiles does not divide by zero"
        );
    }

    #[test]
    fn a_rows_label_and_value_never_overlap_and_stay_inside_the_row() {
        for width in [0.0, 40.0, 200.0, 640.0, 2000.0] {
            for indent in [0.0, 12.0] {
                let row =
                    egui::Rect::from_min_size(egui::pos2(10.0, 5.0), egui::vec2(width, ROW_HEIGHT));
                let (label, value) = pair_rects(row, indent);
                assert!(
                    label.max.x <= value.min.x + 0.001,
                    "w{width}: label overlaps value"
                );
                assert!(label.min.x >= row.min.x && value.max.x <= row.max.x + 0.001);
                assert!(label.width() >= 0.0 && value.width() >= 0.0);
                assert_eq!((label.min.y, label.max.y), (row.min.y, row.max.y));
            }
        }
    }

    #[test]
    fn a_wide_row_gives_the_value_room_for_a_long_number() {
        let row = egui::Rect::from_min_size(egui::pos2(0.0, 0.0), egui::vec2(400.0, ROW_HEIGHT));
        let (_, value) = pair_rects(row, 0.0);
        // 9940.847907 in 13.5pt monospace is about 90 points.
        assert!(value.width() >= 150.0, "{}", value.width());
    }
}
