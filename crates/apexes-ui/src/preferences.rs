//! The user's UI preferences, kept in `~/.apexes/ui.json`.
//!
//! Only what the app itself decides lives here (the theme). The window's size
//! and position are persisted by `eframe`, which also clamps a saved position
//! to the monitors that are connected *now*, so a window is never restored
//! off-screen after a monitor is unplugged.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::theme::ThemeChoice;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiPreferences {
    #[serde(default)]
    pub theme: ThemeChoice,
}

impl UiPreferences {
    /// `~/.apexes/ui.json`, next to the files the supervisor keeps.
    pub fn default_path(home: &Path) -> PathBuf {
        home.join(".apexes").join("ui.json")
    }

    /// Loads preferences; a missing or unreadable file means the defaults. A
    /// corrupt file must never stop the app from starting.
    pub fn load(path: &Path) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default()
    }

    /// Saves preferences. Written to a temporary file and renamed, so a crash
    /// mid-write cannot leave a half-written file behind.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let text = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        let temp = path.with_extension("json.tmp");
        std::fs::write(&temp, text)?;
        std::fs::rename(&temp, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir()
            .join(format!(
                "apexes-prefs-{name}-{}",
                uuid::Uuid::new_v4().simple()
            ))
            .join("ui.json")
    }

    fn cleanup(path: &Path) {
        if let Some(dir) = path.parent() {
            std::fs::remove_dir_all(dir).ok();
        }
    }

    #[test]
    fn the_default_is_to_follow_the_system() {
        assert_eq!(UiPreferences::default().theme, ThemeChoice::System);
    }

    #[test]
    fn preferences_round_trip_and_survive_a_restart() {
        let path = temp_path("roundtrip");
        for theme in ThemeChoice::ALL {
            UiPreferences { theme }.save(&path).unwrap();
            assert_eq!(UiPreferences::load(&path).theme, theme);
        }
        cleanup(&path);
    }

    #[test]
    fn a_missing_or_corrupt_file_means_defaults_and_never_an_error() {
        let path = temp_path("corrupt");
        assert_eq!(UiPreferences::load(&path), UiPreferences::default());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        for bad in [
            "",
            "not json",
            r#"{"theme":"purple"}"#,
            "[1,2,3]",
            r#"{"theme":7}"#,
        ] {
            std::fs::write(&path, bad).unwrap();
            assert_eq!(
                UiPreferences::load(&path),
                UiPreferences::default(),
                "{bad:?}"
            );
        }
        cleanup(&path);
    }

    #[test]
    fn unknown_fields_from_a_newer_version_are_ignored() {
        let path = temp_path("future");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, r#"{"theme":"dark","future":true}"#).unwrap();
        assert_eq!(UiPreferences::load(&path).theme, ThemeChoice::Dark);
        cleanup(&path);
    }

    #[test]
    fn saving_leaves_no_temp_file_behind() {
        let path = temp_path("atomic");
        UiPreferences::default().save(&path).unwrap();
        let leftovers = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
            .count();
        assert_eq!(leftovers, 0);
        cleanup(&path);
    }

    #[test]
    fn the_file_lives_in_the_apexes_directory() {
        assert_eq!(
            UiPreferences::default_path(Path::new("/home/x")),
            PathBuf::from("/home/x/.apexes/ui.json")
        );
    }
}
