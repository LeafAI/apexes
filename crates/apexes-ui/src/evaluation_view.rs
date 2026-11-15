//! Evaluation and environment display for the operator console.
//!
//! Everything that is a *decision* lives here as plain data so it is
//! unit-testable without a window: which environment banner to show, how
//! close a limit is to being hit, how a fraction is formatted. The egui
//! drawing in [`show_evaluation`] only renders those decisions.

use crate::theme::{Palette, palette_of};
use apexes_backtest::{EvaluationSummary, LimitUsage, SessionIdentity, SessionSnapshot};
use chrono::{DateTime, Utc};
use egui::Color32;
use egui_plot::{Line, Plot, PlotPoints};
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

/// Points for the equity line: seconds since the first point on x.
pub fn plot_points(history: &[(DateTime<Utc>, Decimal)]) -> Vec<[f64; 2]> {
    let Some((start, _)) = history.first() else {
        return Vec::new();
    };
    history
        .iter()
        .map(|(at, equity)| {
            [
                (*at - *start).num_milliseconds() as f64 / 1000.0,
                equity.to_f64().unwrap_or(0.0),
            ]
        })
        .collect()
}

/// Running peak at each point, so drawdown can be drawn as the gap
/// between the peak line and the equity line.
pub fn peak_points(history: &[(DateTime<Utc>, Decimal)]) -> Vec<[f64; 2]> {
    let Some((start, _)) = history.first() else {
        return Vec::new();
    };
    let mut peak = Decimal::MIN;
    history
        .iter()
        .map(|(at, equity)| {
            if *equity > peak {
                peak = *equity;
            }
            [
                (*at - *start).num_milliseconds() as f64 / 1000.0,
                peak.to_f64().unwrap_or(0.0),
            ]
        })
        .collect()
}

pub fn banner(ui: &mut egui::Ui, banner: &EnvironmentBanner) {
    let palette = palette_of(ui.ctx());
    crate::theme::banner(ui, &palette, banner.kind.color(&palette), &banner.text);
}

fn limit_row(ui: &mut egui::Ui, usage: &LimitUsage) {
    let palette = palette_of(ui.ctx());
    let fraction = usage.fraction();
    let severity = limit_severity(fraction);
    ui.horizontal(|ui| {
        ui.label(&usage.name);
        let bar = egui::ProgressBar::new(fraction.to_f32().unwrap_or(0.0).clamp(0.0, 1.0))
            .fill(severity.color(&palette))
            .desired_width(160.0);
        ui.add(bar);
        ui.label(format!(
            "{} / {}  ({})",
            usage.used,
            usage.limit,
            percent(fraction)
        ));
    });
}

fn metrics_grid(ui: &mut egui::Ui, e: &EvaluationSummary) {
    egui::Grid::new("evaluation_metrics")
        .num_columns(2)
        .spacing([24.0, 4.0])
        .show(ui, |ui| {
            let mut row = |name: &str, value: String| {
                ui.label(name);
                ui.label(value);
                ui.end_row();
            };
            row("Start equity", e.start_equity.round_dp(2).to_string());
            row("Total PnL", signed(e.total_pnl));
            row("Realized PnL", signed(e.realized_pnl));
            row("Unrealized PnL", signed(e.unrealized_pnl));
            row("Drawdown (now)", percent(e.current_drawdown));
            row("Drawdown (max)", percent(e.max_drawdown));
            row("Gross exposure", percent(e.gross_exposure_fraction));
            row("Time in market", percent(e.time_in_market_fraction));
            row("Trades", e.trade_count.to_string());
            row("Turnover", e.turnover.round_dp(2).to_string());
            let mut fees: Vec<_> = e.fees_paid.iter().collect();
            fees.sort_by(|a, b| a.0.cmp(b.0));
            let fees_text = if fees.is_empty() {
                "-".to_string()
            } else {
                fees.iter()
                    .map(|(currency, amount)| {
                        let label = if currency.is_empty() {
                            "?"
                        } else {
                            currency.as_str()
                        };
                        format!("{} {label}", amount.round_dp(8))
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            row("Fees paid", fees_text);
        });
}

/// Equity timeline with the running peak, and the metrics and limit
/// utilisation underneath.
pub fn show_evaluation(ui: &mut egui::Ui, snapshot: &SessionSnapshot) {
    let palette = palette_of(ui.ctx());
    ui.heading("Evaluation");
    if snapshot.equity_history.len() < 2 {
        ui.weak("collecting equity history...");
    } else {
        let equity = plot_points(&snapshot.equity_history);
        let peak = peak_points(&snapshot.equity_history);
        Plot::new("equity_timeline")
            .height(180.0)
            .allow_scroll(false)
            .x_axis_label("seconds since first point")
            .y_axis_label("equity")
            .show(ui, |plot_ui| {
                plot_ui.line(
                    Line::new("peak", PlotPoints::from(peak))
                        .color(palette.plot_peak)
                        .style(egui_plot::LineStyle::dashed_dense()),
                );
                plot_ui.line(Line::new("equity", PlotPoints::from(equity)).color(palette.info));
            });
    }

    let Some(evaluation) = &snapshot.evaluation else {
        ui.weak("no evaluation data from this engine yet");
        return;
    };
    ui.add_space(6.0);
    metrics_grid(ui, evaluation);
    if !evaluation.limit_usage.is_empty() {
        ui.add_space(6.0);
        ui.strong("Risk limits");
        for usage in &evaluation.limit_usage {
            limit_row(ui, usage);
        }
    }
}

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

    #[test]
    fn plot_points_are_seconds_since_the_first_point() {
        let history = vec![(at(0), dec!(100)), (at(10), dec!(110)), (at(25), dec!(90))];
        assert_eq!(
            plot_points(&history),
            vec![[0.0, 100.0], [10.0, 110.0], [25.0, 90.0]]
        );
        assert!(plot_points(&[]).is_empty());
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
