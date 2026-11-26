//! The console's chart card: one chart with a selector.
//!
//! "Equity" is the account's value over time. Each traded symbol has its own
//! price chart, and "All" overlays every symbol as percent change since its
//! first price, which is the only fair way to draw BTC at 65,000 and BNB at
//! 590 on one axis. What can be selected, what a stale selection falls back to
//! and how a series is normalised are decided here as plain functions; the
//! egui drawing at the bottom only renders them.

use crate::theme::{Palette, palette_of};
use crate::time_axis;
use crate::value_axis;
use apexes_backtest::SessionSnapshot;
use chrono::{DateTime, Utc};
use egui::Color32;
use egui_plot::{Line, Plot, PlotPoints, Points};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

pub const CHART_HEIGHT: f32 = 230.0;

/// What the chart is showing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ChartView {
    #[default]
    Equity,
    Symbol(String),
    /// Every symbol, as percent change since its first price.
    All,
}

impl ChartView {
    fn label(&self) -> String {
        match self {
            ChartView::Equity => "Equity".to_string(),
            ChartView::Symbol(symbol) => symbol.clone(),
            ChartView::All => "All".to_string(),
        }
    }
}

type History = [(DateTime<Utc>, Decimal)];

/// The symbols that have at least one price, in a fixed (alphabetical) order.
pub fn priced_symbols(snapshot: &SessionSnapshot) -> Vec<String> {
    let mut symbols: Vec<String> = snapshot
        .price_history
        .iter()
        .filter(|(_, history)| !history.is_empty())
        .map(|(symbol, _)| symbol.clone())
        .collect();
    symbols.sort();
    symbols
}

/// What the operator can choose between: equity, each priced symbol, and, when
/// there are two or more to compare, all of them together.
pub fn options(snapshot: &SessionSnapshot) -> Vec<ChartView> {
    let symbols = priced_symbols(snapshot);
    let mut options = vec![ChartView::Equity];
    options.extend(symbols.iter().cloned().map(ChartView::Symbol));
    if symbols.len() >= 2 {
        options.push(ChartView::All);
    }
    options
}

/// The selection to actually show: the chosen one if it is still on offer,
/// else equity (a symbol that vanished, or "All" after the session shrank to
/// one symbol, must not leave an empty chart behind).
pub fn resolve(selected: &ChartView, snapshot: &SessionSnapshot) -> ChartView {
    if options(snapshot).contains(selected) {
        selected.clone()
    } else {
        ChartView::Equity
    }
}

/// Percent change of each point against the first: `[x, (p / first - 1) * 100]`.
/// Empty when the first price is not positive (nothing to compare against).
pub fn percent_change_points(history: &History) -> Vec<[f64; 2]> {
    let Some((_, first)) = history.first() else {
        return Vec::new();
    };
    if *first <= Decimal::ZERO {
        return Vec::new();
    }
    history
        .iter()
        .map(|(at, price)| {
            let change = (*price / *first - Decimal::ONE) * Decimal::ONE_HUNDRED;
            [time_axis::to_x(*at), change.to_f64().unwrap_or(0.0)]
        })
        .collect()
}

/// `[unix seconds, price]` for a price line.
pub fn price_points(history: &History) -> Vec<[f64; 2]> {
    history
        .iter()
        .map(|(at, price)| [time_axis::to_x(*at), price.to_f64().unwrap_or(0.0)])
        .collect()
}

/// A price with the places its size needs: 65,000.12 and 0.081234 both read
/// right without the same number of decimals.
pub fn price_text(price: f64) -> String {
    let places = if price.abs() >= 100.0 {
        2
    } else if price.abs() >= 1.0 {
        4
    } else {
        6
    };
    format!("{price:.places$}")
}

/// The series colour of the `index`th symbol. Stable per symbol (its place in
/// the sorted list), so BNB keeps its colour in "All" and on its own chart.
pub fn series_color(palette: &Palette, index: usize) -> Color32 {
    palette.series[index % palette.series.len()]
}

fn symbol_color(palette: &Palette, snapshot: &SessionSnapshot, symbol: &str) -> Color32 {
    let index = priced_symbols(snapshot)
        .iter()
        .position(|s| s == symbol)
        .unwrap_or(0);
    series_color(palette, index)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum YKind {
    Money,
    Percent,
}

struct Series {
    name: String,
    color: Color32,
    points: Vec<[f64; 2]>,
    dashed: bool,
    fill: bool,
}

/// Draws the card: the selector, then the chart it selects.
pub fn chart_card(ui: &mut egui::Ui, selected: &mut ChartView, snapshot: &SessionSnapshot) {
    let palette = palette_of(ui.ctx());
    let options = options(snapshot);
    *selected = resolve(selected, snapshot);
    crate::widgets::card(ui, &palette, |ui| {
        ui.horizontal_wrapped(|ui| {
            for option in &options {
                ui.selectable_value(selected, option.clone(), option.label());
            }
        });
        ui.add_space(4.0);
        let (series, kind) = series_for(selected, snapshot, &palette);
        ui.horizontal_wrapped(|ui| {
            ui.label(crate::widgets::caption(&palette, &caption(selected)));
            if *selected == ChartView::All {
                colour_key(ui, &series);
            }
        });
        draw(ui, &palette, selected, series, kind);
    });
}

/// Which colour is which symbol, written in the colours themselves. A legend
/// drawn inside the plot sat on top of the data in the corner where the
/// lines end up, so the key lives beside the caption instead.
fn colour_key(ui: &mut egui::Ui, series: &[Series]) {
    for s in series {
        ui.label(egui::RichText::new(&s.name).color(s.color).strong().small());
    }
}

fn caption(view: &ChartView) -> String {
    match view {
        ChartView::Equity => "account value; dashed line: running peak".to_string(),
        ChartView::Symbol(_) => "last trade price".to_string(),
        ChartView::All => "percent change since each symbol's first price".to_string(),
    }
}

fn series_for(
    view: &ChartView,
    snapshot: &SessionSnapshot,
    palette: &Palette,
) -> (Vec<Series>, YKind) {
    match view {
        ChartView::Equity => (equity_series(snapshot, palette), YKind::Money),
        ChartView::Symbol(symbol) => {
            let history = snapshot
                .price_history
                .get(symbol)
                .map_or(&[][..], Vec::as_slice);
            let series = Series {
                name: symbol.clone(),
                color: symbol_color(palette, snapshot, symbol),
                points: price_points(history),
                dashed: false,
                fill: true,
            };
            (vec![series], YKind::Money)
        }
        ChartView::All => {
            let series = priced_symbols(snapshot)
                .into_iter()
                .map(|symbol| Series {
                    color: symbol_color(palette, snapshot, &symbol),
                    points: percent_change_points(&snapshot.price_history[&symbol]),
                    name: symbol,
                    dashed: false,
                    fill: false,
                })
                .collect();
            (series, YKind::Percent)
        }
    }
}

fn equity_series(snapshot: &SessionSnapshot, palette: &Palette) -> Vec<Series> {
    let history = &snapshot.equity_history;
    vec![
        Series {
            name: "peak".to_string(),
            color: palette.plot_peak,
            points: crate::evaluation_view::peak_points(history),
            dashed: true,
            fill: false,
        },
        Series {
            name: "equity".to_string(),
            color: palette.info,
            points: crate::evaluation_view::plot_points(history),
            dashed: false,
            fill: true,
        },
    ]
}

fn draw(ui: &mut egui::Ui, palette: &Palette, view: &ChartView, series: Vec<Series>, kind: YKind) {
    if series.iter().all(|s| s.points.is_empty()) {
        let text = match view {
            ChartView::Equity => "collecting equity history...",
            _ => "waiting for the first trade price...",
        };
        ui.add_space(8.0);
        ui.label(crate::widgets::caption(palette, text));
        ui.add_space(8.0);
        return;
    }
    let zone = time_axis::local_zone();
    let last_x = series
        .iter()
        .flat_map(|s| s.points.last())
        .map(|p| p[0])
        .fold(f64::NEG_INFINITY, f64::max);
    let title = time_axis::from_x(last_x)
        .map(|at| time_axis::axis_title(&zone, at))
        .unwrap_or_default();
    let id = format!("chart_{}", view.label());
    let plot = Plot::new(id)
        .height(CHART_HEIGHT)
        .allow_scroll(false)
        .show_background(false)
        .grid_color(palette.grid)
        .x_axis_label(title)
        .y_grid_spacer(|input| value_axis::grid_marks(input.bounds, input.base_step_size))
        .y_axis_formatter(move |mark, range| {
            let text = value_axis::label(mark.value, (*range.start(), *range.end()));
            if kind == YKind::Percent {
                format!("{text}%")
            } else {
                text
            }
        })
        .x_grid_spacer(move |input| {
            time_axis::grid_marks(&zone, input.bounds, input.base_step_size)
        })
        .x_axis_formatter(move |mark, range| {
            time_axis::label_for_mark(&zone, mark.value, (*range.start(), *range.end()))
        })
        .label_formatter(move |hover| hover_text(&zone, kind, *hover));
    plot.show(ui, |plot_ui| {
        for s in series {
            add_series(plot_ui, s);
        }
    });
}

fn hover_text(
    zone: &chrono::Local,
    kind: YKind,
    hover: egui_plot::HoverPosition<'_>,
) -> Option<String> {
    use egui_plot::HoverPosition;
    let shown = |y: f64| match kind {
        YKind::Money => price_text(y),
        YKind::Percent => format!("{y:+.2}%"),
    };
    match hover {
        HoverPosition::NearDataPoint {
            plot_name,
            position,
            ..
        } if !plot_name.is_empty() => Some(format!(
            "{plot_name}\n{}\n{}",
            time_axis::hover_label(zone, position.x),
            shown(position.y)
        )),
        HoverPosition::Elsewhere { position } => Some(time_axis::hover_label(zone, position.x)),
        HoverPosition::NearDataPoint { .. } => None,
    }
}

fn add_series(plot_ui: &mut egui_plot::PlotUi, s: Series) {
    if s.points.len() < 2 {
        // One observation (a quiet symbol, or the seed price): a marker, not a
        // line, and no invented history.
        plot_ui.points(
            Points::new(s.name, PlotPoints::from(s.points))
                .color(s.color)
                .radius(4.0),
        );
        return;
    }
    let floor = s.points.iter().map(|p| p[1]).fold(f64::INFINITY, f64::min);
    let mut line = Line::new(s.name, PlotPoints::from(s.points))
        .color(s.color)
        .width(if s.dashed { 1.2 } else { 2.0 });
    if s.dashed {
        line = line.style(egui_plot::LineStyle::dashed_dense());
    }
    if s.fill && floor.is_finite() {
        line = line.fill(floor as f32).fill_alpha(0.14);
    }
    plot_ui.line(line);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn snapshot_with(prices: &[(&str, Vec<Decimal>)]) -> SessionSnapshot {
        let price_history: HashMap<String, Vec<(DateTime<Utc>, Decimal)>> = prices
            .iter()
            .map(|(symbol, values)| {
                let points = values
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (at(i as i64), *p))
                    .collect();
                (symbol.to_string(), points)
            })
            .collect();
        SessionSnapshot {
            identity: None,
            evaluation: None,
            equity_history: Vec::new(),
            recent_orders: Vec::new(),
            price_history,
            equity_priced: true,
            positions: HashMap::new(),
            balances: HashMap::new(),
            equity: Decimal::ZERO,
            realized_pnl: HashMap::new(),
            margin_status: HashMap::new(),
            params: HashMap::new(),
            paused: false,
            open_orders: Vec::new(),
            timestamp: at(0),
        }
    }

    #[test]
    fn a_single_symbol_session_offers_equity_and_that_symbol_but_not_all() {
        let s = snapshot_with(&[("BTCUSDT", vec![dec!(1)])]);
        assert_eq!(
            options(&s),
            vec![ChartView::Equity, ChartView::Symbol("BTCUSDT".into())]
        );
    }

    #[test]
    fn several_symbols_are_offered_alphabetically_with_all_last() {
        let s = snapshot_with(&[
            ("ETHUSDT", vec![dec!(1)]),
            ("BNBUSDT", vec![dec!(1)]),
            ("BTCUSDT", vec![dec!(1)]),
        ]);
        let labels: Vec<_> = options(&s).iter().map(ChartView::label).collect();
        assert_eq!(labels, ["Equity", "BNBUSDT", "BTCUSDT", "ETHUSDT", "All"]);
    }

    #[test]
    fn a_symbol_with_no_prices_yet_is_not_offered() {
        let s = snapshot_with(&[("BTCUSDT", vec![dec!(1)]), ("BNBUSDT", vec![])]);
        assert_eq!(priced_symbols(&s), ["BTCUSDT"]);
        assert!(!options(&s).contains(&ChartView::All));
    }

    #[test]
    fn nothing_priced_offers_only_equity() {
        let s = snapshot_with(&[]);
        assert_eq!(options(&s), vec![ChartView::Equity]);
    }

    #[test]
    fn a_selection_that_is_no_longer_on_offer_falls_back_to_equity() {
        let one = snapshot_with(&[("BTCUSDT", vec![dec!(1)])]);
        assert_eq!(resolve(&ChartView::All, &one), ChartView::Equity);
        assert_eq!(
            resolve(&ChartView::Symbol("ETHUSDT".into()), &one),
            ChartView::Equity
        );
        let kept = ChartView::Symbol("BTCUSDT".into());
        assert_eq!(resolve(&kept, &one), kept);
        assert_eq!(resolve(&ChartView::Equity, &one), ChartView::Equity);
    }

    #[test]
    fn percent_change_starts_at_zero_and_scales_each_symbol_by_its_own_first_price() {
        let btc = snapshot_with(&[("BTCUSDT", vec![dec!(65000), dec!(65650), dec!(64350)])]);
        let bnb = snapshot_with(&[("BNBUSDT", vec![dec!(590), dec!(595.9), dec!(584.1)])]);
        let pct = |s: &SessionSnapshot, k: &str| -> Vec<f64> {
            percent_change_points(&s.price_history[k])
                .iter()
                .map(|p| p[1])
                .collect()
        };
        let (a, b) = (pct(&btc, "BTCUSDT"), pct(&bnb, "BNBUSDT"));
        assert_eq!(a[0], 0.0);
        // +1% and -1% either way, whatever the price level.
        for (x, y) in [(a[1], 1.0), (a[2], -1.0), (b[1], 1.0), (b[2], -1.0)] {
            assert!((x - y).abs() < 1e-9, "{x} vs {y}");
        }
    }

    #[test]
    fn percent_change_has_nothing_to_compare_against_a_non_positive_first_price() {
        let s = snapshot_with(&[("X", vec![dec!(0), dec!(5)])]);
        assert!(percent_change_points(&s.price_history["X"]).is_empty());
        assert!(percent_change_points(&[]).is_empty());
    }

    #[test]
    fn price_points_carry_unix_seconds_and_the_price() {
        let s = snapshot_with(&[("BTCUSDT", vec![dec!(65000.5), dec!(65001)])]);
        let points = price_points(&s.price_history["BTCUSDT"]);
        assert_eq!(points[0], [1_700_000_000.0, 65000.5]);
        assert_eq!(points[1], [1_700_000_001.0, 65001.0]);
    }

    #[test]
    fn prices_show_the_places_their_size_needs() {
        assert_eq!(price_text(65000.123), "65000.12");
        assert_eq!(price_text(591.034), "591.03");
        assert_eq!(price_text(2.5), "2.5000");
        assert_eq!(price_text(0.081234567), "0.081235");
    }

    /// Lines drawn on a card must stand out from it: 3:1 is WCAG's floor for
    /// graphics, checked on both themes and for every series colour.
    #[test]
    fn every_series_colour_is_visible_on_the_card_in_both_themes() {
        for theme in [egui::Theme::Light, egui::Theme::Dark] {
            let palette = Palette::for_theme(theme);
            for (i, color) in palette.series.iter().enumerate() {
                let ratio = crate::theme::contrast_ratio(*color, palette.card_fill);
                assert!(ratio >= 3.0, "{theme:?} series {i}: {ratio:.2}");
            }
        }
    }

    #[test]
    fn series_colours_are_distinct_so_lines_can_be_told_apart() {
        for theme in [egui::Theme::Light, egui::Theme::Dark] {
            let palette = Palette::for_theme(theme);
            let mut colors = palette.series.to_vec();
            colors.sort_by_key(|c| (c.r(), c.g(), c.b()));
            colors.dedup();
            assert_eq!(colors.len(), palette.series.len());
        }
    }

    #[test]
    fn a_symbol_keeps_its_colour_between_its_own_chart_and_all() {
        let s = snapshot_with(&[("ETHUSDT", vec![dec!(1)]), ("BTCUSDT", vec![dec!(1)])]);
        let palette = Palette::for_theme(egui::Theme::Light);
        let own = symbol_color(&palette, &s, "ETHUSDT");
        let (all, _) = series_for(&ChartView::All, &s, &palette);
        let in_all = all.iter().find(|x| x.name == "ETHUSDT").map(|x| x.color);
        assert_eq!(in_all, Some(own));
    }
}
