//! The "Recent orders" panel: what the strategy has sent and how each order
//! ended, so an operator can see the first order and every one since.
//!
//! It is a **panel**, never a modal dialog. A modal blocks the window under
//! it, and the console must always keep HALT reachable (AGENTS.md, invariant
//! 12); an operator watching orders during a bad moment must not have to
//! dismiss a dialog first.
//!
//! Where it goes is decided by [`placement`]: beside the main column when the
//! window is wide enough (the whitespace to the right of the tables), below
//! the tables when it is not. Everything that is a decision is plain data
//! here so it is tested without a window.

use apexes_backtest::OrderRecord;
use apexes_backtest::order_log;
use chrono::{DateTime, Local, TimeZone, Utc};
use rust_decimal::Decimal;

use crate::number_format;

/// Window width at which the panel moves beside the main column. Below it the
/// tables need the whole width, so the panel goes underneath.
pub const SIDE_BY_SIDE_MIN_WIDTH: f32 = 1100.0;
/// Narrowest the side panel is allowed to be dragged to.
pub const SIDE_PANEL_MIN_WIDTH: f32 = 380.0;
pub const SIDE_PANEL_DEFAULT_WIDTH: f32 = 500.0;
/// Widest the side panel may be dragged to, so it can never push the main
/// column (where HALT and the evaluation live) below a usable width.
pub const SIDE_PANEL_MAX_WIDTH: f32 = 640.0;

/// Orders shown unless the operator asks for all of them.
pub const DEFAULT_SHOWN: usize = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// A resizable panel to the right of the tables.
    Side,
    /// A section under the tables.
    Below,
}

pub fn placement(window_width: f32) -> Placement {
    if window_width >= SIDE_BY_SIDE_MIN_WIDTH {
        Placement::Side
    } else {
        Placement::Below
    }
}

/// How a row is coloured. The words always carry the meaning; colour only
/// reinforces them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Good,
    Neutral,
    Warn,
    Bad,
}

pub fn tone(status: &str) -> Tone {
    match status {
        order_log::FILLED => Tone::Good,
        order_log::WORKING | order_log::PARTIAL => Tone::Neutral,
        order_log::CLOSED => Tone::Warn,
        order_log::REJECTED => Tone::Bad,
        _ => Tone::Neutral,
    }
}

/// One table row, already formatted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub time: String,
    pub side: String,
    pub symbol: String,
    pub quantity: String,
    /// Fill price when it filled, else the limit price, else `-`.
    pub price: String,
    pub status: String,
    pub tone: Tone,
    /// Why, for a refused or closed order; shown on hover and under the row.
    pub detail: Option<String>,
}

/// Local clock time `HH:MM:SS`, with the date added once it is not today (a
/// panel that spans midnight must not make 23:59 and 00:01 look adjacent).
pub fn time_label<Tz: TimeZone>(zone: &Tz, at: DateTime<Utc>, today: chrono::NaiveDate) -> String
where
    Tz::Offset: std::fmt::Display,
{
    let local = at.with_timezone(zone);
    if local.date_naive() == today {
        local.format("%H:%M:%S").to_string()
    } else {
        local.format("%m-%d %H:%M:%S").to_string()
    }
}

pub fn row<Tz: TimeZone>(zone: &Tz, record: &OrderRecord, today: chrono::NaiveDate) -> Row
where
    Tz::Offset: std::fmt::Display,
{
    let filled = record.filled_quantity;
    let quantity = if filled > Decimal::ZERO && filled < record.quantity {
        format!(
            "{}/{}",
            number_format::amount(filled),
            number_format::amount(record.quantity)
        )
    } else {
        number_format::amount(record.quantity)
    };
    let price = record
        .avg_fill_price
        .or(record.limit_price)
        .map_or_else(|| "-".to_string(), number_format::price);
    Row {
        time: time_label(zone, record.at, today),
        side: record.side.clone(),
        symbol: record.symbol.clone(),
        quantity,
        price,
        status: record.status.clone(),
        tone: tone(&record.status),
        detail: record.detail.clone(),
    }
}

/// The rows to draw: newest first, at most `shown`. Also how many were left
/// out, so the panel can offer "show all".
pub fn rows<Tz: TimeZone>(
    zone: &Tz,
    records: &[OrderRecord],
    shown: usize,
    today: chrono::NaiveDate,
) -> (Vec<Row>, usize)
where
    Tz::Offset: std::fmt::Display,
{
    let rows: Vec<Row> = records
        .iter()
        .take(shown)
        .map(|record| row(zone, record, today))
        .collect();
    (rows, records.len().saturating_sub(shown))
}

/// The panel's one-line summary: how the orders ended.
pub fn summary(records: &[OrderRecord]) -> String {
    if records.is_empty() {
        return "no orders yet".to_string();
    }
    let count = |status: &str| records.iter().filter(|r| r.status == status).count();
    let mut parts = vec![format!("{} orders", records.len())];
    for (label, status) in [
        ("filled", order_log::FILLED),
        ("working", order_log::WORKING),
        ("partial", order_log::PARTIAL),
        ("closed", order_log::CLOSED),
        ("rejected", order_log::REJECTED),
    ] {
        let n = count(status);
        if n > 0 {
            parts.push(format!("{n} {label}"));
        }
    }
    parts.join(", ")
}

/// The local zone's today, for [`row`].
pub fn today_local() -> chrono::NaiveDate {
    Local::now().date_naive()
}

/// Draws the panel's contents. `show_all` is the operator's "show all"
/// choice, kept by the caller so it survives redraws and tab switches.
pub fn show(ui: &mut egui::Ui, snapshot: &apexes_backtest::SessionSnapshot, show_all: &mut bool) {
    let palette = crate::theme::palette_of(ui.ctx());
    ui.heading("Recent orders");
    ui.weak(summary(&snapshot.recent_orders));
    if snapshot.recent_orders.is_empty() {
        ui.add_space(4.0);
        ui.weak("Orders the strategy sends appear here, newest first, whether they fill or are refused.");
        return;
    }
    let zone = crate::time_axis::local_zone();
    let today = today_local();
    let limit = if *show_all { usize::MAX } else { DEFAULT_SHOWN };
    let (rows, hidden) = rows(&zone, &snapshot.recent_orders, limit, today);
    egui::ScrollArea::vertical()
        .id_salt("recent_orders_scroll")
        .auto_shrink([false, true])
        .show(ui, |ui| {
            // One small grid per order, not one grid for all: a refusal's
            // reason needs the panel's full width, and inside a shared grid it
            // is squeezed into the narrow first column.
            // The panel is resizable: columns are laid out in the width it
            // currently has, never asking for more (asking made it grow every
            // frame).
            // Rows are laid out in exactly the width the panel has now.
            ui.spacing_mut().item_spacing.y = 1.0;
            let width = ui.available_width();
            header_row(ui, width);
            for (i, r) in rows.iter().enumerate() {
                order_row(ui, i, r, &palette, width);
            }
            if hidden > 0 && ui.small_button(format!("Show {hidden} older")).clicked() {
                *show_all = true;
            }
            if *show_all
                && snapshot.recent_orders.len() > DEFAULT_SHOWN
                && ui.small_button("Show fewer").clicked()
            {
                *show_all = false;
            }
        });
}

/// Column widths as fractions of the panel, so the columns line up between the
/// header and every row. They sum to 1: a row is exactly the panel wide.
const COLUMNS: [f32; 6] = [0.22, 0.08, 0.21, 0.15, 0.19, 0.15];

/// Height of one table line.
const LINE: f32 = 20.0;

/// Draws one line of six cells across exactly `width`, with an optional
/// background. Every cell is an exact rectangle (never sized by its text), and
/// the line is allocated once, so the panel cannot grow or shrink with its
/// content: an earlier version used layouts and frames whose margins and
/// spacing made each row wider than the panel, which then grew every frame.
fn line(
    ui: &mut egui::Ui,
    width: f32,
    background: Option<egui::Color32>,
    cells: [(egui::RichText, Option<&str>); 6],
) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(width, LINE), egui::Sense::hover());
    if let Some(color) = background {
        ui.painter().rect_filled(rect, 2.0, color);
    }
    let mut x = rect.min.x;
    for ((text, hover), share) in cells.into_iter().zip(COLUMNS) {
        let w = width * share;
        let cell_rect =
            egui::Rect::from_min_size(egui::pos2(x + 3.0, rect.min.y), egui::vec2(w - 3.0, LINE));
        let mut cell = ui.new_child(
            egui::UiBuilder::new()
                .max_rect(cell_rect)
                .layout(egui::Layout::left_to_right(egui::Align::Center)),
        );
        cell.set_clip_rect(cell_rect.intersect(ui.clip_rect()));
        let response = cell.add(egui::Label::new(text).truncate());
        if let Some(hover) = hover {
            response.on_hover_text(hover);
        }
        x += w;
    }
}

fn header_row(ui: &mut egui::Ui, width: f32) {
    let head = |name: &str| (egui::RichText::new(name).strong(), None);
    line(
        ui,
        width,
        None,
        [
            head("Time"),
            head("Side"),
            head("Qty"),
            head("Price"),
            head("Status"),
            head("Symbol"),
        ],
    );
}

fn order_row(
    ui: &mut egui::Ui,
    index: usize,
    r: &Row,
    palette: &crate::theme::Palette,
    width: f32,
) {
    let stripe = index.is_multiple_of(2).then(|| ui.visuals().faint_bg_color);
    let color = match r.tone {
        Tone::Good => palette.good,
        Tone::Warn => palette.warn,
        Tone::Bad => palette.danger,
        Tone::Neutral => ui.visuals().text_color(),
    };
    let plain = |text: &str| (egui::RichText::new(text), None);
    line(
        ui,
        width,
        stripe,
        [
            (egui::RichText::new(&r.time), Some(r.time.as_str())),
            plain(&r.side),
            plain(&r.quantity),
            plain(&r.price),
            (
                egui::RichText::new(&r.status).color(color),
                r.detail.as_deref(),
            ),
            plain(&r.symbol),
        ],
    );
    if let Some(detail) = &r.detail {
        // A refusal's reason is the point of listing it: shown in full, under
        // its row, not only on hover.
        ui.add(egui::Label::new(egui::RichText::new(detail).weak().small()).wrap());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::FixedOffset;
    use rust_decimal_macros::dec;

    fn kst() -> FixedOffset {
        FixedOffset::east_opt(9 * 3600).unwrap()
    }

    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        // Given in KST, stored in UTC.
        Utc.with_ymd_and_hms(2026, 11, 16, h, m, s).unwrap() - chrono::Duration::hours(9)
    }

    fn today() -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2026, 11, 16).unwrap()
    }

    fn record(status: &str, filled: Decimal) -> OrderRecord {
        OrderRecord {
            id: "x".to_string(),
            at: at(23, 14, 14),
            symbol: "BTCUSDT".to_string(),
            side: "Buy".to_string(),
            order_type: "Market".to_string(),
            quantity: dec!(0.00070000),
            limit_price: None,
            filled_quantity: filled,
            avg_fill_price: (filled > dec!(0)).then_some(dec!(64401.85000000)),
            status: status.to_string(),
            detail: None,
        }
    }

    #[test]
    fn wide_windows_get_a_side_panel_and_narrow_ones_a_section_below() {
        assert_eq!(placement(1180.0), Placement::Side, "the default window");
        assert_eq!(placement(SIDE_BY_SIDE_MIN_WIDTH), Placement::Side);
        assert_eq!(placement(SIDE_BY_SIDE_MIN_WIDTH - 1.0), Placement::Below);
        assert_eq!(placement(760.0), Placement::Below, "the minimum window");
        assert_eq!(placement(0.0), Placement::Below);
    }

    #[test]
    fn the_default_window_is_wide_enough_for_the_side_panel() {
        // If the default window were narrower than the threshold the panel
        // would never be seen beside the tables on a fresh install.
        assert!(
            crate::chrome::native_options()
                .viewport
                .inner_size
                .is_some_and(|size| size.x >= SIDE_BY_SIDE_MIN_WIDTH)
        );
    }

    #[test]
    fn a_filled_order_shows_its_clean_quantity_price_and_local_time() {
        let r = row(&kst(), &record(order_log::FILLED, dec!(0.0007)), today());
        assert_eq!(r.time, "23:14:14");
        assert_eq!(r.quantity, "0.0007", "no trailing zeros");
        assert_eq!(r.price, "64401.85");
        assert_eq!((r.side.as_str(), r.status.as_str()), ("Buy", "Filled"));
        assert_eq!(r.tone, Tone::Good);
    }

    #[test]
    fn a_partly_filled_order_shows_filled_over_total() {
        let r = row(&kst(), &record(order_log::PARTIAL, dec!(0.0002)), today());
        assert_eq!(r.quantity, "0.0002/0.0007");
        assert_eq!(r.tone, Tone::Neutral);
    }

    #[test]
    fn an_unfilled_limit_order_shows_its_limit_price() {
        let mut rec = record(order_log::WORKING, dec!(0));
        rec.order_type = "Limit".to_string();
        rec.limit_price = Some(dec!(64000));
        let r = row(&kst(), &rec, today());
        assert_eq!(r.price, "64000.00");
        assert_eq!(
            row(&kst(), &record(order_log::REJECTED, dec!(0)), today()).price,
            "-"
        );
    }

    #[test]
    fn the_reason_for_a_refusal_travels_with_the_row() {
        let mut rec = record(order_log::REJECTED, dec!(0));
        rec.detail = Some("order notional 32000 exceeds per-order notional limit 100".into());
        let r = row(&kst(), &rec, today());
        assert_eq!(r.tone, Tone::Bad);
        assert!(r.detail.as_deref().unwrap().contains("notional limit"));
    }

    #[test]
    fn yesterdays_orders_carry_the_date_so_midnight_is_not_hidden() {
        let yesterday = Utc.with_ymd_and_hms(2026, 11, 15, 14, 59, 0).unwrap(); // 23:59 KST on the 15th
        assert_eq!(time_label(&kst(), yesterday, today()), "11-15 23:59:00");
        assert_eq!(time_label(&kst(), at(0, 1, 5), today()), "00:01:05");
    }

    #[test]
    fn every_status_has_a_tone_and_an_unknown_one_is_neutral_not_a_panic() {
        assert_eq!(tone(order_log::FILLED), Tone::Good);
        assert_eq!(tone(order_log::CLOSED), Tone::Warn);
        assert_eq!(tone(order_log::REJECTED), Tone::Bad);
        assert_eq!(tone(order_log::WORKING), Tone::Neutral);
        assert_eq!(tone("Something a newer engine invented"), Tone::Neutral);
    }

    #[test]
    fn only_the_newest_are_drawn_and_the_rest_are_counted() {
        let records: Vec<OrderRecord> = (0..45)
            .map(|i| {
                let mut r = record(order_log::FILLED, dec!(0.0007));
                r.id = i.to_string();
                r
            })
            .collect();
        let (shown, hidden) = rows(&kst(), &records, DEFAULT_SHOWN, today());
        assert_eq!((shown.len(), hidden), (30, 15));
        let (all, hidden) = rows(&kst(), &records, usize::MAX, today());
        assert_eq!((all.len(), hidden), (45, 0));
        let (none, hidden) = rows(&kst(), &[], DEFAULT_SHOWN, today());
        assert_eq!((none.len(), hidden), (0, 0));
    }

    #[test]
    fn the_summary_counts_how_orders_ended_and_omits_empty_buckets() {
        assert_eq!(summary(&[]), "no orders yet");
        let records = vec![
            record(order_log::FILLED, dec!(0.0007)),
            record(order_log::FILLED, dec!(0.0007)),
            record(order_log::REJECTED, dec!(0)),
        ];
        assert_eq!(summary(&records), "3 orders, 2 filled, 1 rejected");
    }
}
