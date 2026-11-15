//! What every Apexes window shares: the theme (applied at startup, switched
//! from a selector, saved when it changes), the window options, and the
//! theme shortcut. One implementation, so the dashboard, the console and the
//! launcher cannot drift apart.

use std::path::PathBuf;

use egui::{Key, KeyboardShortcut, Modifiers};

use crate::preferences::UiPreferences;
use crate::tabs::TabCommand;
use crate::theme::{self, ThemeChoice};

/// Cmd+Shift+L (Ctrl+Shift+L elsewhere) cycles System -> Light -> Dark.
pub const CYCLE_THEME: KeyboardShortcut =
    KeyboardShortcut::new(Modifiers::COMMAND.plus(Modifiers::SHIFT), Key::L);

const DIGIT_KEYS: [Key; 9] = [
    Key::Num1,
    Key::Num2,
    Key::Num3,
    Key::Num4,
    Key::Num5,
    Key::Num6,
    Key::Num7,
    Key::Num8,
    Key::Num9,
];

/// Reads the tab shortcuts for this frame (consuming the keys so a text field
/// does not also see them): Cmd/Ctrl+1..9 pick a tab (1 is the launcher),
/// Cmd/Ctrl+[ and ] step left and right, Cmd/Ctrl+W closes the console in front.
pub fn tab_command(ctx: &egui::Context) -> Option<TabCommand> {
    ctx.input_mut(|input| {
        let cmd = Modifiers::COMMAND;
        if let Some(index) = DIGIT_KEYS.iter().position(|k| input.consume_key(cmd, *k)) {
            return Some(TabCommand::Focus(index));
        }
        if input.consume_key(cmd, Key::OpenBracket) {
            return Some(TabCommand::Step(-1));
        }
        if input.consume_key(cmd, Key::CloseBracket) {
            return Some(TabCommand::Step(1));
        }
        input
            .consume_key(cmd, Key::W)
            .then_some(TabCommand::CloseActive)
    })
}

/// The smallest size at which the console's tables and banners stay usable.
pub const MIN_WINDOW_SIZE: [f32; 2] = [760.0, 520.0];
const DEFAULT_WINDOW_SIZE: [f32; 2] = [1180.0, 780.0];

/// Window options shared by the apps. Size and position are remembered
/// between runs (`persist_window`, backed by eframe's `persistence`
/// feature); the minimum size stops the window shrinking to something
/// unreadable.
pub fn native_options() -> eframe::NativeOptions {
    eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size(DEFAULT_WINDOW_SIZE)
            .with_min_inner_size(MIN_WINDOW_SIZE),
        persist_window: true,
        ..Default::default()
    }
}

/// The theme state of one window.
#[derive(Debug, Clone)]
pub struct Chrome {
    choice: ThemeChoice,
    path: Option<PathBuf>,
    applied: Option<ThemeChoice>,
}

impl Chrome {
    /// Reads the saved preference from `path` (`None`: nothing is saved, for
    /// tests).
    pub fn load(path: Option<PathBuf>) -> Self {
        let choice = path
            .as_deref()
            .map_or_else(ThemeChoice::default, |p| UiPreferences::load(p).theme);
        Self {
            choice,
            path,
            applied: None,
        }
    }

    /// `~/.apexes/ui.json` for the current user, or no persistence when
    /// `HOME` is unknown.
    pub fn from_home() -> Self {
        Self::load(
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| UiPreferences::default_path(&home)),
        )
    }

    pub fn choice(&self) -> ThemeChoice {
        self.choice
    }

    /// Call once per frame: applies the theme if it changed (and the first
    /// time), handles the shortcut. Returns true when the choice changed.
    pub fn update(&mut self, ctx: &egui::Context) -> bool {
        let pressed = ctx.input_mut(|i| i.consume_shortcut(&CYCLE_THEME));
        let changed = pressed && self.set(self.choice.next());
        if self.applied != Some(self.choice) {
            theme::apply(ctx, self.choice);
            self.applied = Some(self.choice);
        }
        changed
    }

    /// Draws the Theme selector; saves and applies a change at once.
    pub fn selector(&mut self, ui: &mut egui::Ui) {
        if let Some(chosen) = theme::selector(ui, self.choice) {
            self.set(chosen);
            self.update(ui.ctx());
        }
    }

    /// Sets and saves the choice. Returns whether it changed. A save failure
    /// is logged, not fatal: the theme still applies for this run.
    pub fn set(&mut self, choice: ThemeChoice) -> bool {
        if choice == self.choice {
            return false;
        }
        self.choice = choice;
        if let Some(path) = &self.path
            && let Err(error) = (UiPreferences { theme: choice }).save(path)
        {
            tracing::warn!(%error, path = %path.display(), "could not save the theme preference");
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir()
            .join(format!(
                "apexes-chrome-{name}-{}",
                uuid::Uuid::new_v4().simple()
            ))
            .join("ui.json")
    }

    #[test]
    fn a_change_is_saved_and_a_new_window_starts_with_it() {
        let path = temp_path("persist");
        let mut chrome = Chrome::load(Some(path.clone()));
        assert_eq!(chrome.choice(), ThemeChoice::System);
        assert!(chrome.set(ThemeChoice::Light));
        assert!(!chrome.set(ThemeChoice::Light), "no change, no write");
        assert_eq!(
            Chrome::load(Some(path.clone())).choice(),
            ThemeChoice::Light
        );
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn an_unwritable_location_still_lets_the_theme_change() {
        let mut chrome = Chrome::load(Some(PathBuf::from("/proc/definitely/not/writable/ui.json")));
        assert!(chrome.set(ThemeChoice::Dark));
        assert_eq!(chrome.choice(), ThemeChoice::Dark);
    }

    #[test]
    fn without_a_path_nothing_is_written() {
        let mut chrome = Chrome::load(None);
        assert!(chrome.set(ThemeChoice::Dark));
    }

    #[test]
    fn update_applies_the_choice_to_the_context() {
        let ctx = egui::Context::default();
        let mut chrome = Chrome::load(None);
        chrome.set(ThemeChoice::Light);
        chrome.update(&ctx);
        assert_eq!(ctx.theme(), egui::Theme::Light);
        chrome.set(ThemeChoice::Dark);
        chrome.update(&ctx);
        assert_eq!(ctx.theme(), egui::Theme::Dark);
    }

    #[test]
    fn the_window_has_a_usable_minimum_and_remembers_its_place() {
        let options = native_options();
        assert!(options.persist_window);
        let min = options.viewport.min_inner_size.expect("min size");
        assert_eq!([min.x, min.y], MIN_WINDOW_SIZE);
        let size = options.viewport.inner_size.expect("default size");
        assert!(size.x >= MIN_WINDOW_SIZE[0] && size.y >= MIN_WINDOW_SIZE[1]);
    }
}
