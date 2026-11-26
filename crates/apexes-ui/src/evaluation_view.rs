//! Evaluation and environment display for the operator console.
//!
//! Everything that is a *decision* lives here as plain data so it is
//! unit-testable without a window: which environment banner to show, how
//! close a limit is to being hit, how a fraction is formatted. The egui
//! drawing in [`show_evaluation`] only renders those decisions.

use crate::number_format;
use crate::theme::{Palette, palette_of};
use crate::time_axis;
use apexes_backtest::{EvaluationSummary, LimitUsage, SessionIdentity};
use chrono::{DateTime, Utc};
use egui::Color32;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Utilisation at which a limit turns amber / red.
pub const LIMIT_WARN: Decimal = Decimal::from_parts(7, 0, 0, false, 1);
pub const LIMIT_DANGER: Decimal = Decimal::from_parts(9, 0, 0, false, 1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Ok,
    Warn,
    Danger,
}

impl Severity {
    pub fn color(self, palette: &Palette) -> Color32 {
        match self {
            Self::Ok => palette.good,
            Self::Warn => palette.warn,
            Self::Danger => palette.danger,
        }
    }
}

pub fn limit_severity(fraction: Decimal) -> Severity {
    if fraction >= LIMIT_DANGER {
        Severity::Danger
    } else if fraction >= LIMIT_WARN {
        Severity::Warn
    } else {
        Severity::Ok
    }
}

/// What the environment banner says and how loudly. Built from the
/// engine's own reported identity, never from anything the launcher chose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentBanner {
    pub text: String,
    pub kind: EnvironmentKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvironmentKind {
    /// Live on real funds.
    RealFunds,
    /// Live on the exchange's test network (virtual funds).
    Testnet,
    /// Paper or backtest: no exchange involved.
    Simulated,
    /// The engine did not say. Treated as untrustworthy.
    Unknown,
}

impl EnvironmentKind {
    pub fn color(self, palette: &Palette) -> Color32 {
        match self {
            Self::RealFunds => palette.danger,
            Self::Testnet => palette.info,
            Self::Simulated => palette.muted,
            Self::Unknown => palette.warn,
        }
    }
}

pub fn environment_banner(identity: Option<&SessionIdentity>) -> EnvironmentBanner {
    let Some(identity) = identity else {
        return EnvironmentBanner {
            text:
                "ENVIRONMENT UNKNOWN: the engine did not report its network. Do not assume testnet."
                    .to_string(),
            kind: EnvironmentKind::Unknown,
        };
    };
    match (identity.mode.as_str(), identity.network.as_deref()) {
        ("live", Some("mainnet")) => EnvironmentBanner {
            text: format!(
                "MAINNET - REAL FUNDS   |   {}   |   {}",
                identity.session_id, identity.strategy
            ),
            kind: EnvironmentKind::RealFunds,
        },
        ("live", Some("testnet")) => EnvironmentBanner {
            text: format!(
                "TESTNET (virtual funds)   |   {}   |   {}",
                identity.session_id, identity.strategy
            ),
            kind: EnvironmentKind::Testnet,
        },
        ("live", other) => EnvironmentBanner {
            text: format!(
                "LIVE SESSION ON AN UNRECOGNISED NETWORK ({}) - treat as real funds",
                other.unwrap_or("none")
            ),
            kind: EnvironmentKind::RealFunds,
        },
        (mode, _) => EnvironmentBanner {
            text: format!(
                "{} (simulated, no exchange)   |   {}   |   {}",
                mode.to_uppercase(),
                identity.session_id,
                identity.strategy
            ),
            kind: EnvironmentKind::Simulated,
        },
    }
}

/// `0.1234` -> `"12.34%"`.
pub fn percent(fraction: Decimal) -> String {
    format!(
        "{}%",
        (fraction * Decimal::ONE_HUNDRED).round_dp(2).normalize()
    )
}

/// A signed amount with an explicit `+`, to two decimals.
pub fn signed(value: Decimal) -> String {
    let rounded = value.round_dp(2).normalize();
    if rounded > Decimal::ZERO {
        format!("+{rounded}")
    } else {
        rounded.to_string()
    }
}

/// Points for the equity line: Unix seconds on x (shown as local time by
/// `crate::time_axis`), equity on y.
pub fn plot_points(history: &[(DateTime<Utc>, Decimal)]) -> Vec<[f64; 2]> {
    history
        .iter()
        .map(|(at, equity)| [time_axis::to_x(*at), equity.to_f64().unwrap_or(0.0)])
        .collect()
}

/// Running peak at each point, so drawdown can be drawn as the gap
/// between the peak line and the equity line.
pub fn peak_points(history: &[(DateTime<Utc>, Decimal)]) -> Vec<[f64; 2]> {
    let mut peak = Decimal::MIN;
    history
        .iter()
        .map(|(at, equity)| {
            if *equity > peak {
                peak = *equity;
            }
            [time_axis::to_x(*at), peak.to_f64().unwrap_or(0.0)]
        })
        .collect()
}

pub fn banner(ui: &mut egui::Ui, banner: &EnvironmentBanner) {
    let palette = palette_of(ui.ctx());
    crate::theme::banner(ui, &palette, banner.kind.color(&palette), &banner.text);
}

/// A thin full-width track with a coloured fill of `fraction` of it. Even a
/// 0% limit shows its empty track, and a tiny fraction is still visible.
pub fn limit_bar(ui: &mut egui::Ui, fraction: f32, fill: Color32, track: Color32) {
    let height = 8.0;
    let (rect, _) = ui.allocate_exact_size(
        egui::vec2(ui.available_width(), height),
        egui::Sense::hover(),
    );
    ui.painter().rect_filled(rect, 4.0, track);
    let filled = bar_fill_width(rect.width(), fraction, height);
    if filled > 0.0 {
        let mut bar = rect;
        bar.set_width(filled);
        ui.painter().rect_filled(bar, 4.0, fill);
    }
}

/// How wide the filled part is: proportional, never beyond the track, and at
/// least as wide as the bar is tall once there is anything to show (so 1% is a
/// visible nub, not nothing). Zero stays zero.
pub fn bar_fill_width(track: f32, fraction: f32, min_visible: f32) -> f32 {
    if !fraction.is_finite() || fraction <= 0.0 || track <= 0.0 {
        return 0.0;
    }
    (track * fraction.min(1.0)).max(min_visible.min(track))
}

fn limit_row(ui: &mut egui::Ui, usage: &LimitUsage) {
    let palette = palette_of(ui.ctx());
    let fraction = usage.fraction();
    let severity = limit_severity(fraction);
    let width = ui.available_width();
    crate::widgets::pair_row(
        ui,
        &palette,
        width,
        &usage.name,
        &format!(
            "{} / {}  ({})",
            number_format::amount(usage.used),
            number_format::amount(usage.limit),
            percent(fraction)
        ),
        crate::widgets::LabelStyle::Normal,
    );
    // A bar as wide as its card, so every limit lines up with the next. Drawn
    // by hand: `ProgressBar` rounds a short bar into a dot and ignores the
    // width of a card it is placed in.
    limit_bar(
        ui,
        fraction.to_f32().unwrap_or(0.0),
        severity.color(&palette),
        palette.track,
    );
}

/// The PnL rows.
///
/// An account that started flat has one PnL, shown as before ("Total PnL",
/// "Realized PnL", "Unrealized PnL"). When the account held assets before the
/// session started, the market's effect on them is shown on its own line and
/// the strategy's result on another, so a price move on a balance the strategy
/// never traded is not read as the strategy losing money.
pub fn pnl_rows(e: &EvaluationSummary) -> Vec<(&'static str, String)> {
    if has_starting_holdings(e) {
        vec![
            ("Account PnL", signed(e.total_pnl)),
            ("  from holdings", signed(e.holdings_pnl)),
            ("Strategy PnL", signed(e.strategy_pnl)),
            ("  Realized", signed(e.realized_pnl)),
            ("  Unrealized", signed(e.unrealized_pnl)),
        ]
    } else {
        vec![
            ("Total PnL", signed(e.total_pnl)),
            ("Realized PnL", signed(e.realized_pnl)),
            ("Unrealized PnL", signed(e.unrealized_pnl)),
        ]
    }
}

fn has_starting_holdings(e: &EvaluationSummary) -> bool {
    e.holdings_value_at_start > Decimal::ZERO
}

/// The drawdown that judges the strategy: its own when the account started
/// with holdings (the account's would be the market's swing), else the
/// account's, which is the same thing.
pub fn drawdown_now(e: &EvaluationSummary) -> Decimal {
    if has_starting_holdings(e) {
        e.strategy_current_drawdown
    } else {
        e.current_drawdown
    }
}

pub fn drawdown_max(e: &EvaluationSummary) -> Decimal {
    if has_starting_holdings(e) {
        e.strategy_max_drawdown
    } else {
        e.max_drawdown
    }
}

/// One key figure at the top of the console.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tile {
    pub label: &'static str,
    pub value: String,
    /// The sign that colours the value, when it has one.
    pub sign: Option<crate::widgets::Sign>,
    /// A small line under the value.
    pub note: Option<String>,
}

/// The figures an operator looks at first, in the order they are read:
/// what the account is worth, what the strategy has done, how deep it has
/// been, and how exposed it is. Everything else stays in the details card.
///
/// When the account held assets before the session, the account's own PnL is
/// shown as a figure of its own next to the strategy's, so neither is mistaken
/// for the other.
/// The equity tile. While a held asset has no price the ledger counts it at
/// 1.0, so the number would be wrong by thousands: say so instead of showing it.
fn equity_tile(equity: Decimal, priced: bool, e: Option<&EvaluationSummary>) -> Tile {
    if !priced {
        return Tile {
            label: "Equity",
            value: PRICING.to_string(),
            sign: None,
            note: Some("waiting for a price of a held asset".to_string()),
        };
    }
    Tile {
        label: "Equity",
        value: number_format::money(equity),
        sign: None,
        note: e.map(|e| format!("started at {}", number_format::money(e.start_equity))),
    }
}

/// Shown in place of a number that cannot be computed yet.
pub const PRICING: &str = "pricing…";

pub fn key_tiles(equity: Decimal, priced: bool, e: Option<&EvaluationSummary>) -> Vec<Tile> {
    use crate::widgets::sign_of;
    let mut tiles = vec![equity_tile(equity, priced, e)];
    let Some(e) = e else { return tiles };
    let (pnl_label, pnl, pnl_note) = if has_starting_holdings(e) {
        (
            "Strategy PnL",
            e.strategy_pnl,
            Some(format!("account {}", signed(e.total_pnl))),
        )
    } else {
        ("PnL", e.total_pnl, None)
    };
    tiles.push(Tile {
        label: pnl_label,
        value: signed(pnl),
        sign: Some(sign_of(pnl)),
        note: pnl_note,
    });
    if has_starting_holdings(e) {
        tiles.push(Tile {
            label: "From holdings",
            value: signed(e.holdings_pnl),
            sign: Some(sign_of(e.holdings_pnl)),
            note: Some("market move on held assets".to_string()),
        });
    }
    tiles.push(Tile {
        label: "Drawdown",
        value: percent(drawdown_now(e)),
        sign: None,
        note: Some(format!("max {}", percent(drawdown_max(e)))),
    });
    tiles.push(Tile {
        label: "Exposure",
        value: percent(e.gross_exposure_fraction),
        sign: None,
        note: Some(format!("in market {}", percent(e.time_in_market_fraction))),
    });
    tiles.push(Tile {
        label: "Trades",
        value: e.trade_count.to_string(),
        sign: None,
        note: Some(format!("turnover {}", number_format::money(e.turnover))),
    });
    tiles
}

/// The rows of the details card, in two groups that are drawn as two blocks:
/// where the money stands, and how the strategy has behaved. A group is never
/// split across blocks (a lone "Drawdown (now)" stranded under the PnL rows
/// was the old layout's flaw).
pub fn detail_groups(e: &EvaluationSummary) -> [Vec<(&'static str, String)>; 2] {
    let mut money = vec![("Start equity", number_format::money(e.start_equity))];
    money.extend(pnl_rows(e));
    money.push(("Fees paid", number_format::fees(&e.fees_paid)));
    let behaviour = vec![
        ("Drawdown (now)", percent(drawdown_now(e))),
        ("Drawdown (max)", percent(drawdown_max(e))),
        ("Gross exposure", percent(e.gross_exposure_fraction)),
        ("Time in market", percent(e.time_in_market_fraction)),
        ("Trades", e.trade_count.to_string()),
        ("Turnover", number_format::money(e.turnover)),
    ];
    [money, behaviour]
}

/// All the detail rows in reading order.
pub fn detail_rows(e: &EvaluationSummary) -> Vec<(&'static str, String)> {
    detail_groups(e).into_iter().flatten().collect()
}

/// The details card: every figure, label left and value right-aligned so the
/// digits line up down the column.
pub fn details_card(ui: &mut egui::Ui, e: &EvaluationSummary) {
    use crate::widgets::{LabelStyle, pair_row};
    let palette = palette_of(ui.ctx());
    crate::widgets::card(ui, &palette, |ui| {
        crate::widgets::section_title(ui, "Details");
        let groups = detail_groups(e);
        let inner = ui.available_width();
        // The two groups side by side when the card is wide, stacked when not.
        let side_by_side = inner >= DETAILS_TWO_COLUMN_FROM;
        let gap = 28.0;
        let block_width = if side_by_side {
            (inner - gap) / 2.0
        } else {
            inner
        };
        let draw = |ui: &mut egui::Ui, rows: &[(&'static str, String)]| {
            ui.spacing_mut().item_spacing.y = 0.0;
            for (name, value) in rows {
                let (label, style) = match name.strip_prefix(' ') {
                    Some(sub) => (sub.trim_start(), LabelStyle::Sub),
                    None => (*name, LabelStyle::Normal),
                };
                pair_row(ui, &palette, block_width, label, value, style);
            }
        };
        if side_by_side {
            ui.horizontal_top(|ui| {
                ui.spacing_mut().item_spacing.x = gap;
                for rows in &groups {
                    ui.vertical(|ui| draw(ui, rows));
                }
            });
        } else {
            for (index, rows) in groups.iter().enumerate() {
                if index > 0 {
                    ui.add_space(10.0);
                }
                ui.vertical(|ui| draw(ui, rows));
            }
        }
    });
}

/// Card width from which the details are laid out in two blocks.
pub const DETAILS_TWO_COLUMN_FROM: f32 = 520.0;

/// The risk-limits card: each limit as a bar with its numbers.
pub fn limits_card(ui: &mut egui::Ui, usages: &[LimitUsage]) {
    let palette = palette_of(ui.ctx());
    crate::widgets::card(ui, &palette, |ui| {
        crate::widgets::section_title(ui, "Risk limits");
        if usages.is_empty() {
            ui.label(crate::widgets::caption(&palette, "no limits configured"));
            return;
        }
        for usage in usages {
            limit_row(ui, usage);
            ui.add_space(4.0);
        }
    });
}

/// Height of the equity chart. Taller than the old 180: a chart is the one
/// thing on this screen read by shape, and it was the smallest part of it.
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn identity(mode: &str, network: Option<&str>) -> SessionIdentity {
        SessionIdentity {
            session_id: "s-1".to_string(),
            network: network.map(str::to_string),
            mode: mode.to_string(),
            strategy: "demo.drift".to_string(),
        }
    }

    #[test]
    fn mainnet_live_is_the_loud_real_funds_banner() {
        let b = environment_banner(Some(&identity("live", Some("mainnet"))));
        assert_eq!(b.kind, EnvironmentKind::RealFunds);
        assert!(
            b.text.contains("MAINNET") && b.text.contains("REAL FUNDS"),
            "{}",
            b.text
        );
    }

    #[test]
    fn testnet_live_says_virtual_funds_and_is_not_red() {
        let b = environment_banner(Some(&identity("live", Some("testnet"))));
        assert_eq!(b.kind, EnvironmentKind::Testnet);
        assert!(
            b.text.contains("TESTNET") && b.text.contains("virtual"),
            "{}",
            b.text
        );
        for theme in [egui::Theme::Light, egui::Theme::Dark] {
            let p = Palette::for_theme(theme);
            assert_ne!(
                b.kind.color(&p),
                EnvironmentKind::RealFunds.color(&p),
                "{theme:?}"
            );
        }
    }

    #[test]
    fn an_engine_that_reports_no_identity_is_never_shown_as_testnet() {
        let b = environment_banner(None);
        assert_eq!(b.kind, EnvironmentKind::Unknown);
        assert!(b.text.contains("UNKNOWN"));
        assert!(!b.text.contains("TESTNET"));
    }

    #[test]
    fn a_live_session_on_an_unrecognised_network_fails_safe_to_real_funds() {
        let b = environment_banner(Some(&identity("live", Some("staging"))));
        assert_eq!(b.kind, EnvironmentKind::RealFunds);
        let b = environment_banner(Some(&identity("live", None)));
        assert_eq!(b.kind, EnvironmentKind::RealFunds);
    }

    #[test]
    fn paper_and_backtest_are_marked_simulated() {
        for mode in ["paper", "backtest"] {
            let b = environment_banner(Some(&identity(mode, None)));
            assert_eq!(b.kind, EnvironmentKind::Simulated);
            assert!(b.text.to_lowercase().contains("simulated"));
        }
    }

    #[test]
    fn limit_severity_thresholds() {
        assert_eq!(limit_severity(dec!(0)), Severity::Ok);
        assert_eq!(limit_severity(dec!(0.69)), Severity::Ok);
        assert_eq!(limit_severity(dec!(0.7)), Severity::Warn);
        assert_eq!(limit_severity(dec!(0.89)), Severity::Warn);
        assert_eq!(limit_severity(dec!(0.9)), Severity::Danger);
        assert_eq!(limit_severity(dec!(1.5)), Severity::Danger);
    }

    #[test]
    fn formatting_helpers() {
        assert_eq!(percent(dec!(0.12345)), "12.34%");
        assert_eq!(percent(dec!(0.05)), "5%", "trailing zeros are dropped");
        assert_eq!(percent(dec!(0.2)), "20%");
        assert_eq!(percent(dec!(0)), "0%");
        assert_eq!(signed(dec!(5.678)), "+5.68");
        assert_eq!(signed(dec!(-5.678)), "-5.68");
        assert_eq!(signed(dec!(0)), "0");
    }

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn summary_with(holdings_value: Decimal) -> EvaluationSummary {
        EvaluationSummary {
            start_equity: dec!(74350.26),
            total_pnl: dec!(-271.99),
            realized_pnl: dec!(-0.07),
            strategy_pnl: dec!(-0.08),
            holdings_pnl: dec!(-271.91),
            holdings_value_at_start: holdings_value,
            unrealized_pnl: dec!(-0.01),
            current_drawdown: dec!(0.0092),
            max_drawdown: dec!(0.0116),
            strategy_current_drawdown: dec!(0.000001),
            strategy_max_drawdown: dec!(0.000002),
            gross_exposure_fraction: dec!(0.87),
            fees_paid: std::collections::HashMap::new(),
            trade_count: 2,
            turnover: dec!(90.45),
            time_in_market_fraction: dec!(0.0136),
            limit_usage: Vec::new(),
        }
    }

    /// The screenshot: the loss was the market's, and the panel now says so.
    #[test]
    fn with_starting_holdings_the_market_and_the_strategy_are_separate_lines() {
        let rows = pnl_rows(&summary_with(dec!(64000)));
        let get = |name: &str| {
            rows.iter()
                .find(|(n, _)| n.trim() == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get("Account PnL").as_deref(), Some("-271.99"));
        assert_eq!(get("from holdings").as_deref(), Some("-271.91"));
        assert_eq!(get("Strategy PnL").as_deref(), Some("-0.08"));
        assert_eq!(get("Realized").as_deref(), Some("-0.07"));
    }

    #[test]
    fn a_flat_start_keeps_the_original_labels() {
        let rows = pnl_rows(&summary_with(dec!(0)));
        let names: Vec<_> = rows.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, ["Total PnL", "Realized PnL", "Unrealized PnL"]);
    }

    #[test]
    fn the_drawdown_shown_is_the_strategys_when_there_were_holdings() {
        let with = summary_with(dec!(64000));
        assert_eq!(drawdown_max(&with), dec!(0.000002));
        assert_eq!(drawdown_now(&with), dec!(0.000001));
        let flat = summary_with(dec!(0));
        assert_eq!(drawdown_max(&flat), dec!(0.0116));
        assert_eq!(drawdown_now(&flat), dec!(0.0092));
    }

    #[test]
    fn a_limit_bar_is_proportional_bounded_and_a_small_value_is_still_visible() {
        assert_eq!(bar_fill_width(200.0, 0.5, 8.0), 100.0);
        assert_eq!(bar_fill_width(200.0, 1.0, 8.0), 200.0);
        assert_eq!(
            bar_fill_width(200.0, 3.0, 8.0),
            200.0,
            "over the limit stops at the track"
        );
        assert_eq!(
            bar_fill_width(200.0, 0.01, 8.0),
            8.0,
            "1% is a visible nub, not nothing"
        );
        assert_eq!(bar_fill_width(200.0, 0.0, 8.0), 0.0, "zero stays zero");
        assert_eq!(bar_fill_width(200.0, -0.2, 8.0), 0.0);
        assert_eq!(bar_fill_width(200.0, f32::NAN, 8.0), 0.0);
        assert_eq!(bar_fill_width(0.0, 0.5, 8.0), 0.0, "no track, no bar");
        assert_eq!(
            bar_fill_width(5.0, 0.5, 8.0),
            5.0,
            "never wider than a tiny track"
        );
    }

    #[test]
    fn the_key_figures_for_a_flat_start_are_equity_pnl_drawdown_exposure_trades() {
        let tiles = key_tiles(dec!(74078.28), true, Some(&summary_with(dec!(0))));
        let labels: Vec<_> = tiles.iter().map(|t| t.label).collect();
        assert_eq!(labels, ["Equity", "PnL", "Drawdown", "Exposure", "Trades"]);
        assert_eq!(tiles[0].value, "74078.28");
        assert_eq!(tiles[1].value, "-271.99");
        assert_eq!(tiles[1].sign, Some(crate::widgets::Sign::Loss));
    }

    /// The screenshot's case: the headline PnL is the strategy's, and the
    /// market's move has a tile of its own, so the two cannot be confused.
    #[test]
    fn with_holdings_the_headline_pnl_is_the_strategys_and_the_markets_move_is_beside_it() {
        let tiles = key_tiles(dec!(74078.28), true, Some(&summary_with(dec!(64000))));
        let labels: Vec<_> = tiles.iter().map(|t| t.label).collect();
        assert_eq!(
            labels,
            [
                "Equity",
                "Strategy PnL",
                "From holdings",
                "Drawdown",
                "Exposure",
                "Trades"
            ]
        );
        let strategy = &tiles[1];
        assert_eq!(strategy.value, "-0.08");
        assert_eq!(strategy.note.as_deref(), Some("account -271.99"));
        let holdings = &tiles[2];
        assert_eq!(holdings.value, "-271.91");
        assert_eq!(holdings.sign, Some(crate::widgets::Sign::Loss));
    }

    #[test]
    fn the_drawdown_tile_is_the_strategys_when_there_were_holdings() {
        let with = key_tiles(dec!(1), true, Some(&summary_with(dec!(64000))));
        let dd = with.iter().find(|t| t.label == "Drawdown").unwrap();
        assert_eq!(dd.value, percent(dec!(0.000001)));
        let flat = key_tiles(dec!(1), true, Some(&summary_with(dec!(0))));
        let dd = flat.iter().find(|t| t.label == "Drawdown").unwrap();
        assert_eq!(dd.value, percent(dec!(0.0092)));
    }

    #[test]
    fn before_any_evaluation_only_equity_is_shown_and_nothing_is_invented() {
        let tiles = key_tiles(dec!(10000), true, None);
        assert_eq!(tiles.len(), 1);
        assert_eq!(tiles[0].label, "Equity");
        assert!(tiles[0].note.is_none());
    }

    /// A group is never split: the money rows stay together and so do the
    /// behaviour rows, so a block break cannot strand one row on its own.
    #[test]
    fn the_details_are_two_whole_groups_money_and_behaviour() {
        let [money, behaviour] = detail_groups(&summary_with(dec!(64000)));
        let names = |rows: &[(&'static str, String)]| -> Vec<String> {
            rows.iter().map(|(n, _)| n.trim().to_string()).collect()
        };
        assert!(names(&money).contains(&"Strategy PnL".to_string()));
        assert!(names(&money).contains(&"Fees paid".to_string()));
        assert_eq!(names(&behaviour)[0], "Drawdown (now)");
        assert!(names(&behaviour).contains(&"Turnover".to_string()));
        assert!(
            !names(&money).iter().any(|n| n.starts_with("Drawdown")),
            "drawdown rows are not stranded with the money rows"
        );
        assert!(
            behaviour.len() >= 5 && money.len() >= 5,
            "neither block is a stub"
        );
    }

    #[test]
    fn the_details_card_keeps_every_number_the_tiles_summarise() {
        let rows = detail_rows(&summary_with(dec!(64000)));
        let names: Vec<_> = rows.iter().map(|(n, _)| n.trim()).collect();
        for expected in [
            "Start equity",
            "Account PnL",
            "from holdings",
            "Strategy PnL",
            "Realized",
            "Unrealized",
            "Drawdown (now)",
            "Drawdown (max)",
            "Gross exposure",
            "Time in market",
            "Trades",
            "Turnover",
            "Fees paid",
        ] {
            assert!(
                names.contains(&expected),
                "{expected} missing from {names:?}"
            );
        }
    }

    #[test]
    fn plot_points_are_unix_seconds_so_the_axis_can_show_clock_time() {
        let history = vec![(at(0), dec!(100)), (at(10), dec!(110)), (at(25), dec!(90))];
        assert_eq!(
            plot_points(&history),
            vec![
                [1_700_000_000.0, 100.0],
                [1_700_000_010.0, 110.0],
                [1_700_000_025.0, 90.0]
            ]
        );
        assert!(plot_points(&[]).is_empty());
    }

    #[test]
    fn the_peak_line_shares_the_equity_line_x_values() {
        let history = vec![(at(0), dec!(100)), (at(7), dec!(120))];
        let xs = |p: Vec<[f64; 2]>| p.iter().map(|v| v[0]).collect::<Vec<_>>();
        assert_eq!(xs(plot_points(&history)), xs(peak_points(&history)));
    }

    #[test]
    fn the_peak_line_never_falls() {
        let history = vec![
            (at(0), dec!(100)),
            (at(1), dec!(120)),
            (at(2), dec!(90)),
            (at(3), dec!(130)),
        ];
        let peaks: Vec<f64> = peak_points(&history).iter().map(|p| p[1]).collect();
        assert_eq!(peaks, vec![100.0, 120.0, 120.0, 130.0]);
    }
}
