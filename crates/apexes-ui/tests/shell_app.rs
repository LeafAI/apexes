//! Headless tests of the operator window: several consoles at once, friendly
//! switching, and nothing lost on the way.

use apexes_core::binance_network::BinanceNetwork;
use apexes_supervisor::{SessionId, SessionInfo, Supervisor};
use apexes_ui::chrome::Chrome;
use apexes_ui::shell_app::ShellApp;
use apexes_ui::tabs::View;
use apexes_ui::theme::ThemeChoice;
use egui::{Key, Modifiers};
use egui_kittest::Harness;
use egui_kittest::kittest::Queryable;

fn info(id: &str) -> SessionInfo {
    SessionInfo {
        id: SessionId::parse(id).unwrap(),
        pid: 1,
        port: 1,
        network: Some(BinanceNetwork::Testnet),
        alive: true,
        // Nothing listens here: the console just reports it is not connected.
        control_url: "ws://127.0.0.1:1/session/ws".to_string(),
        token: Some("t".repeat(32)),
        log_path: "/tmp/x.log".into(),
    }
}

struct Fixture {
    harness: Harness<'static, ShellApp>,
    _runtime: tokio::runtime::Runtime,
    home: std::path::PathBuf,
}

impl Fixture {
    fn new(name: &str, choice: ThemeChoice) -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let home = std::env::temp_dir().join(format!(
            "apexes-shell-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&home).unwrap();
        let prefs = apexes_ui::preferences::UiPreferences::default_path(&home);
        let mut chrome = Chrome::load(Some(prefs));
        chrome.set(choice);
        let handle = runtime.handle().clone();
        let supervisor = Supervisor::new(&home);
        let harness = Harness::builder()
            .with_size(egui::vec2(1000.0, 700.0))
            .build_eframe(move |_cc| ShellApp::with_chrome(supervisor, handle, chrome));
        Self {
            harness,
            _runtime: runtime,
            home,
        }
    }

    fn app(&mut self) -> &mut ShellApp {
        self.harness.state_mut()
    }

    fn open(&mut self, id: &str) {
        self.app().open_console(&info(id));
        self.harness.step();
    }

    fn press(&mut self, modifiers: Modifiers, key: Key) {
        self.harness.key_press_modifiers(modifiers, key);
        self.harness.step();
        self.harness.step();
    }

    fn active(&mut self) -> View {
        self.app().tabs().active().clone()
    }

    fn ids(&mut self) -> Vec<String> {
        self.app().tabs().ids().to_vec()
    }

    fn shows(&self, text: &str) -> bool {
        self.harness
            .query_all_by_label_contains(text)
            .next()
            .is_some()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.home).ok();
    }
}

fn console(id: &str) -> View {
    View::Console(id.to_string())
}

#[test]
fn it_starts_on_the_launcher_with_no_console_tabs() {
    let mut f = Fixture::new("start", ThemeChoice::Dark);
    f.harness.step();
    assert_eq!(f.active(), View::Launcher);
    assert!(f.ids().is_empty());
    assert!(f.shows("Launcher"));
}

#[test]
fn opening_two_sessions_keeps_both_and_shows_the_newest() {
    let mut f = Fixture::new("two", ThemeChoice::Dark);
    f.open("alpha");
    f.open("beta");
    assert_eq!(
        f.ids(),
        ["alpha", "beta"],
        "the first console is not replaced"
    );
    assert_eq!(f.active(), console("beta"));
    assert!(f.shows("alpha"), "both tabs are in the bar");
    assert!(f.shows("beta"));
}

#[test]
fn opening_a_session_that_is_already_open_just_brings_it_forward() {
    let mut f = Fixture::new("reopen", ThemeChoice::Dark);
    f.open("alpha");
    f.open("beta");
    f.open("alpha");
    assert_eq!(f.ids(), ["alpha", "beta"]);
    assert_eq!(f.active(), console("alpha"));
}

#[test]
fn keyboard_shortcuts_switch_step_and_close_tabs() {
    let mut f = Fixture::new("keys", ThemeChoice::Dark);
    f.open("alpha");
    f.open("beta");

    f.press(Modifiers::COMMAND, Key::Num1);
    assert_eq!(f.active(), View::Launcher, "Cmd+1 is the launcher");
    f.press(Modifiers::COMMAND, Key::Num2);
    assert_eq!(f.active(), console("alpha"), "Cmd+2 is the first console");
    f.press(Modifiers::COMMAND, Key::CloseBracket);
    assert_eq!(f.active(), console("beta"), "Cmd+] steps right");
    f.press(Modifiers::COMMAND, Key::CloseBracket);
    assert_eq!(f.active(), View::Launcher, "and wraps to the launcher");
    f.press(Modifiers::COMMAND, Key::OpenBracket);
    assert_eq!(f.active(), console("beta"), "Cmd+[ steps left, wrapping");

    f.press(Modifiers::COMMAND, Key::W);
    assert_eq!(f.ids(), ["alpha"], "Cmd+W closed the console in front");
    assert_eq!(
        f.active(),
        console("alpha"),
        "focus moves to its left neighbour"
    );
    f.press(Modifiers::COMMAND, Key::W);
    assert_eq!(f.active(), View::Launcher);
    f.press(Modifiers::COMMAND, Key::W);
    assert_eq!(
        f.active(),
        View::Launcher,
        "the launcher itself cannot be closed"
    );
}

#[test]
fn switching_tabs_does_not_close_or_reset_a_console() {
    let mut f = Fixture::new("keep", ThemeChoice::Dark);
    f.open("alpha");
    f.open("beta");
    for _ in 0..3 {
        f.press(Modifiers::COMMAND, Key::Num1);
        f.press(Modifiers::COMMAND, Key::Num3);
    }
    assert_eq!(f.ids(), ["alpha", "beta"]);
}

#[test]
fn a_shortcut_to_a_tab_that_does_not_exist_does_nothing() {
    let mut f = Fixture::new("nine", ThemeChoice::Dark);
    f.open("alpha");
    f.press(Modifiers::COMMAND, Key::Num9);
    assert_eq!(f.active(), console("alpha"));
}

#[test]
fn a_console_that_cannot_reach_its_engine_says_so_in_its_tab() {
    let mut f = Fixture::new("offline", ThemeChoice::Dark);
    f.open("alpha");
    f.press(Modifiers::COMMAND, Key::Num1);
    assert!(
        f.shows("(offline)"),
        "a background tab with a lost connection is flagged"
    );
}

#[test]
fn the_theme_choice_is_saved_and_applied_from_the_shortcut() {
    let mut f = Fixture::new("theme", ThemeChoice::Light);
    f.harness.step();
    assert_eq!(f.harness.ctx.theme(), egui::Theme::Light);
    f.press(Modifiers::COMMAND | Modifiers::SHIFT, Key::L);
    assert_eq!(f.harness.ctx.theme(), egui::Theme::Dark);
    let saved = apexes_ui::preferences::UiPreferences::load(
        &apexes_ui::preferences::UiPreferences::default_path(&f.home),
    );
    assert_eq!(saved.theme, ThemeChoice::Dark, "survives a restart");
}

/// `APEXES_SHOTS=/tmp/shots cargo test -p apexes-ui --test shell_app -- renders --ignored`
#[test]
#[ignore = "writes PNG files for manual inspection"]
fn renders_the_window_in_both_themes() {
    let dir = std::env::var("APEXES_SHOTS").unwrap_or_else(|_| "/tmp/shots".to_string());
    std::fs::create_dir_all(&dir).unwrap();
    for (name, choice) in [("light", ThemeChoice::Light), ("dark", ThemeChoice::Dark)] {
        let mut f = Fixture::new(name, choice);
        f.open("testnet-eth-1");
        f.open("testnet-btc-2");
        f.press(Modifiers::COMMAND, Key::Num1);
        f.harness.step();
        f.harness
            .render()
            .unwrap()
            .save(format!("{dir}/shell-launcher-{name}.png"))
            .unwrap();
    }
}
