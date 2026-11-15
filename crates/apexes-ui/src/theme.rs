//! Light/dark theme support.
//!
//! One place decides every colour the GUI uses, so that a colour is always
//! chosen for the theme it is drawn on. Before this, each view carried its own
//! `const RED/GREEN/AMBER/BLUE` tuned for a dark background, which are hard to
//! read on a light one.
//!
//! Two rules shape the palette:
//! - **Status colours are never the only signal.** Every status also has words
//!   ("TESTNET", "HALTED", "running"); colour only reinforces them.
//! - **The environment banner stays loud in both themes.** Its fill is a
//!   saturated colour with white text, which reads on a light or a dark
//!   window, and `MAINNET - REAL FUNDS` is the most saturated of them all.
//!
//! The user's choice (`System`, `Light` or `Dark`) is saved with the other UI
//! preferences (`crate::preferences`) so it survives a restart; `System`
//! follows the operating system's setting.

use egui::Color32;
use serde::{Deserialize, Serialize};

/// What the user chose. `System` follows the OS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemeChoice {
    #[default]
    System,
    Light,
    Dark,
}

impl ThemeChoice {
    pub const ALL: [ThemeChoice; 3] = [Self::System, Self::Light, Self::Dark];

    pub fn label(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Light => "Light",
            Self::Dark => "Dark",
        }
    }

    /// The next choice when cycling with the keyboard: System, Light, Dark.
    pub fn next(self) -> Self {
        match self {
            Self::System => Self::Light,
            Self::Light => Self::Dark,
            Self::Dark => Self::System,
        }
    }

    pub fn preference(self) -> egui::ThemePreference {
        match self {
            Self::System => egui::ThemePreference::System,
            Self::Light => egui::ThemePreference::Light,
            Self::Dark => egui::ThemePreference::Dark,
        }
    }

    /// The theme actually shown, given what the OS reports (which can be
    /// unknown, in which case dark, egui's own default).
    pub fn resolve(self, system: Option<egui::Theme>) -> egui::Theme {
        match self {
            Self::Light => egui::Theme::Light,
            Self::Dark => egui::Theme::Dark,
            Self::System => system.unwrap_or(egui::Theme::Dark),
        }
    }
}

/// Every colour the views use, for one resolved theme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Palette {
    /// Good: connected, running, trading.
    pub good: Color32,
    /// Take care: stale, paused, near a limit, testnet notice text.
    pub warn: Color32,
    /// Bad or dangerous: halted, failed, mainnet.
    pub danger: Color32,
    /// Neutral information: testnet, plot line.
    pub info: Color32,
    /// De-emphasised: simulated environments, history lines.
    pub muted: Color32,
    /// Text on a saturated banner fill. Always white: the fills below are
    /// dark enough in both themes for white to read.
    pub on_banner: Color32,
    /// The dashed running-peak line behind the equity plot.
    pub plot_peak: Color32,
}

impl Palette {
    pub fn for_theme(theme: egui::Theme) -> Self {
        match theme {
            // Brighter tones so small text stays readable on a dark window.
            egui::Theme::Dark => Self {
                good: Color32::from_rgb(70, 190, 100),
                warn: Color32::from_rgb(235, 165, 40),
                danger: Color32::from_rgb(225, 70, 70),
                info: Color32::from_rgb(90, 150, 230),
                muted: Color32::from_gray(150),
                on_banner: Color32::WHITE,
                plot_peak: Color32::from_gray(150),
            },
            // Deeper tones: the dark-theme ones wash out on white.
            egui::Theme::Light => Self {
                good: Color32::from_rgb(25, 130, 55),
                warn: Color32::from_rgb(175, 105, 0),
                danger: Color32::from_rgb(190, 30, 30),
                info: Color32::from_rgb(35, 85, 175),
                muted: Color32::from_gray(95),
                on_banner: Color32::WHITE,
                plot_peak: Color32::from_gray(120),
            },
        }
    }

    /// Fill for an environment/halt banner, always dark enough for white text.
    pub fn banner_fill(&self, color: Color32) -> Color32 {
        // The theme's own colours are already tuned; on a dark window the
        // brighter tones need pulling down so white text keeps its contrast.
        let darken = |c: u8| (u16::from(c) * 70 / 100) as u8;
        if contrast_ratio(self.on_banner, color) >= MIN_BANNER_CONTRAST {
            color
        } else {
            Color32::from_rgb(darken(color.r()), darken(color.g()), darken(color.b()))
        }
    }
}

/// WCAG's threshold for large text; banners are bold and large.
pub const MIN_BANNER_CONTRAST: f64 = 3.0;

fn channel_luminance(c: u8) -> f64 {
    let v = f64::from(c) / 255.0;
    if v <= 0.03928 {
        v / 12.92
    } else {
        ((v + 0.055) / 1.055).powf(2.4)
    }
}

fn relative_luminance(c: Color32) -> f64 {
    0.2126 * channel_luminance(c.r())
        + 0.7152 * channel_luminance(c.g())
        + 0.0722 * channel_luminance(c.b())
}

/// WCAG contrast ratio between two colours (1.0 to 21.0).
pub fn contrast_ratio(a: Color32, b: Color32) -> f64 {
    let (la, lb) = (relative_luminance(a), relative_luminance(b));
    let (light, dark) = if la >= lb { (la, lb) } else { (lb, la) };
    (light + 0.05) / (dark + 0.05)
}

/// The window background egui uses for a theme, so contrast checks and the
/// palette stay anchored to what is really drawn.
pub fn window_background(theme: egui::Theme) -> Color32 {
    match theme {
        egui::Theme::Dark => egui::Visuals::dark().panel_fill,
        egui::Theme::Light => egui::Visuals::light().panel_fill,
    }
}

/// Applies `choice` to the egui context. Cheap and idempotent, so it is safe
/// to call whenever the choice changes.
pub fn apply(ctx: &egui::Context, choice: ThemeChoice) {
    ctx.set_theme(choice.preference());
}

/// The palette for what `ctx` is showing right now.
pub fn palette_of(ctx: &egui::Context) -> Palette {
    Palette::for_theme(ctx.theme())
}

/// A full-width status banner: a saturated fill with white text. Every view
/// uses this one helper, so a banner reads the same on either theme.
pub fn banner(ui: &mut egui::Ui, palette: &Palette, color: Color32, text: &str) {
    egui::Frame::new()
        .fill(palette.banner_fill(color))
        .inner_margin(6.0)
        .show(ui, |ui| {
            ui.label(egui::RichText::new(text).strong().color(palette.on_banner));
        });
}

/// A small filled circle in `color`, drawn rather than typed: the `\u{25CF}`
/// glyph is missing from egui's bundled fonts and shows as an empty box.
pub fn status_dot(ui: &mut egui::Ui, color: Color32) {
    let size = ui.text_style_height(&egui::TextStyle::Body) * 0.7;
    let (rect, _) = ui.allocate_exact_size(egui::vec2(size, size), egui::Sense::hover());
    ui.painter().circle_filled(rect.center(), size / 2.0, color);
}

/// The window's own background. eframe gives `ui()` a surface with none and
/// clears to a fixed near-black whatever the theme, which makes light mode
/// unreadable; every app clears with this and draws on a [`page`].
pub fn clear_color(visuals: &egui::Visuals) -> [f32; 4] {
    visuals.panel_fill.to_normalized_gamma_f32()
}

/// Draws `add_contents` on the theme's panel background, filling the window.
pub fn page<R>(ui: &mut egui::Ui, add_contents: impl FnOnce(&mut egui::Ui) -> R) -> R {
    egui::CentralPanel::default().show(ui, add_contents).inner
}

/// A compact selector (System / Light / Dark) for a top bar. Returns the new
/// choice when the user changed it.
pub fn selector(ui: &mut egui::Ui, current: ThemeChoice) -> Option<ThemeChoice> {
    let mut chosen = current;
    egui::ComboBox::from_id_salt("theme_selector")
        .selected_text(format!("Theme: {}", current.label()))
        .show_ui(ui, |ui| {
            for choice in ThemeChoice::ALL {
                ui.selectable_value(&mut chosen, choice, choice.label());
            }
        });
    (chosen != current).then_some(chosen)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn choices_cycle_through_all_three_and_back() {
        let mut c = ThemeChoice::System;
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(c);
            c = c.next();
        }
        assert_eq!(
            seen,
            vec![ThemeChoice::System, ThemeChoice::Light, ThemeChoice::Dark]
        );
        assert_eq!(c, ThemeChoice::System);
    }

    #[test]
    fn system_follows_the_os_and_falls_back_to_dark_when_unknown() {
        assert_eq!(
            ThemeChoice::System.resolve(Some(egui::Theme::Light)),
            egui::Theme::Light
        );
        assert_eq!(
            ThemeChoice::System.resolve(Some(egui::Theme::Dark)),
            egui::Theme::Dark
        );
        assert_eq!(ThemeChoice::System.resolve(None), egui::Theme::Dark);
    }

    #[test]
    fn an_explicit_choice_ignores_the_os() {
        assert_eq!(
            ThemeChoice::Light.resolve(Some(egui::Theme::Dark)),
            egui::Theme::Light
        );
        assert_eq!(
            ThemeChoice::Dark.resolve(Some(egui::Theme::Light)),
            egui::Theme::Dark
        );
        assert_eq!(ThemeChoice::Light.resolve(None), egui::Theme::Light);
    }

    #[test]
    fn the_default_is_system() {
        assert_eq!(ThemeChoice::default(), ThemeChoice::System);
    }

    /// Every status colour must be readable as text on its own theme's window.
    #[test]
    fn status_text_colours_are_readable_on_their_own_background() {
        for theme in [egui::Theme::Light, egui::Theme::Dark] {
            let palette = Palette::for_theme(theme);
            let background = window_background(theme);
            for (name, color) in [
                ("good", palette.good),
                ("warn", palette.warn),
                ("danger", palette.danger),
                ("info", palette.info),
                ("muted", palette.muted),
            ] {
                let ratio = contrast_ratio(color, background);
                assert!(
                    ratio >= 3.0,
                    "{theme:?} {name} {color:?} on {background:?}: contrast {ratio:.2} is below 3.0"
                );
            }
        }
    }

    /// The banners carry the safety message (testnet vs REAL FUNDS): white
    /// text must stay legible on every fill in both themes.
    #[test]
    fn banner_text_is_legible_on_every_banner_fill_in_both_themes() {
        for theme in [egui::Theme::Light, egui::Theme::Dark] {
            let palette = Palette::for_theme(theme);
            for (name, color) in [
                ("good", palette.good),
                ("warn", palette.warn),
                ("danger", palette.danger),
                ("info", palette.info),
                ("muted", palette.muted),
            ] {
                let fill = palette.banner_fill(color);
                let ratio = contrast_ratio(palette.on_banner, fill);
                assert!(
                    ratio >= MIN_BANNER_CONTRAST,
                    "{theme:?} {name} banner fill {fill:?}: white text contrast {ratio:.2}"
                );
            }
        }
    }

    #[test]
    fn mainnet_stays_visibly_different_from_testnet_in_both_themes() {
        for theme in [egui::Theme::Light, egui::Theme::Dark] {
            let p = Palette::for_theme(theme);
            assert_ne!(p.danger, p.info, "{theme:?}");
            // Red channel dominates for danger, blue for info: distinguishable
            // even without relying on the exact shade.
            assert!(p.danger.r() > p.danger.b(), "{theme:?}");
            assert!(p.info.b() > p.info.r(), "{theme:?}");
        }
    }

    #[test]
    fn contrast_ratio_matches_known_wcag_values() {
        assert!((contrast_ratio(Color32::WHITE, Color32::BLACK) - 21.0).abs() < 0.01);
        assert!((contrast_ratio(Color32::WHITE, Color32::WHITE) - 1.0).abs() < 0.001);
        // Symmetric.
        let a = Color32::from_rgb(10, 120, 200);
        let b = Color32::from_rgb(240, 240, 20);
        assert!((contrast_ratio(a, b) - contrast_ratio(b, a)).abs() < 1e-9);
    }
}
