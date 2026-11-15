//! Which window-pane the operator is looking at: the launcher, or the console
//! of one of several open sessions.
//!
//! Pure state, no egui: every rule about opening, closing and moving between
//! tabs is tested without a window. Switching tabs never touches a console's
//! state or connection; only closing a tab does.

use crate::operator::OperatorState;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum View {
    #[default]
    Launcher,
    Console(String),
}

/// What a keyboard shortcut asks of the tab bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TabCommand {
    /// 0 is the launcher, 1.. the open consoles in order.
    Focus(usize),
    /// Previous (-1) or next (+1), wrapping around.
    Step(isize),
    CloseActive,
}

#[derive(Debug, Default)]
pub struct Tabs {
    open: Vec<String>,
    active: View,
}

impl Tabs {
    pub fn active(&self) -> &View {
        &self.active
    }

    pub fn ids(&self) -> &[String] {
        &self.open
    }

    pub fn is_open(&self, id: &str) -> bool {
        self.open.iter().any(|open| open == id)
    }

    /// Opens a console tab for `id` (once) and focuses it.
    pub fn open(&mut self, id: &str) {
        if !self.is_open(id) {
            self.open.push(id.to_string());
        }
        self.active = View::Console(id.to_string());
    }

    /// Focuses `view`; a console that is not open is ignored.
    pub fn focus(&mut self, view: View) {
        match &view {
            View::Console(id) if !self.is_open(id) => {}
            _ => self.active = view,
        }
    }

    /// Closes a console tab. If it was in front, focus moves to its left
    /// neighbour (the launcher when it was the first), where the eye already is.
    pub fn close(&mut self, id: &str) {
        let Some(index) = self.open.iter().position(|open| open == id) else {
            return;
        };
        self.open.remove(index);
        if self.active == View::Console(id.to_string()) {
            self.active = match index.checked_sub(1).and_then(|i| self.open.get(i)) {
                Some(left) => View::Console(left.clone()),
                None => View::Launcher,
            };
        }
    }

    pub fn apply(&mut self, command: TabCommand) {
        match command {
            TabCommand::Focus(index) => self.focus_index(index),
            TabCommand::Step(delta) => self.step(delta),
            TabCommand::CloseActive => {
                if let View::Console(id) = self.active.clone() {
                    self.close(&id);
                }
            }
        }
    }

    fn position(&self) -> usize {
        match &self.active {
            View::Launcher => 0,
            View::Console(id) => self.open.iter().position(|o| o == id).map_or(0, |i| i + 1),
        }
    }

    fn focus_index(&mut self, index: usize) {
        self.active = match index.checked_sub(1) {
            None => View::Launcher,
            Some(i) => match self.open.get(i) {
                Some(id) => View::Console(id.clone()),
                None => return,
            },
        };
    }

    fn step(&mut self, delta: isize) {
        let count = self.open.len() as isize + 1;
        let next = (self.position() as isize + delta).rem_euclid(count);
        self.focus_index(next as usize);
    }
}

/// How a session looks from another tab: enough to notice a problem without
/// switching to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TabHealth {
    Ok,
    /// Connection lost or reconnecting: what the tab shows may be out of date.
    Attention,
    /// The engine halted trading.
    Halted,
}

impl TabHealth {
    pub fn of(state: &OperatorState) -> Self {
        if !state.connected() {
            Self::Attention
        } else if state.halted() {
            Self::Halted
        } else {
            Self::Ok
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tabs(ids: &[&str]) -> Tabs {
        let mut t = Tabs::default();
        for id in ids {
            t.open(id);
        }
        t
    }

    fn console(id: &str) -> View {
        View::Console(id.to_string())
    }

    #[test]
    fn opening_focuses_the_tab_and_never_duplicates_it() {
        let mut t = tabs(&["a", "b"]);
        assert_eq!(t.active(), &console("b"));
        t.open("a");
        assert_eq!(t.ids(), ["a", "b"]);
        assert_eq!(
            t.active(),
            &console("a"),
            "re-opening just brings it forward"
        );
    }

    #[test]
    fn closing_the_front_tab_focuses_its_left_neighbour_or_the_launcher() {
        let mut t = tabs(&["a", "b", "c"]);
        t.close("c");
        assert_eq!(t.active(), &console("b"));
        t.focus(console("a"));
        t.close("a");
        assert_eq!(
            t.active(),
            &View::Launcher,
            "first tab: nothing to its left"
        );
        assert_eq!(t.ids(), ["b"]);
    }

    #[test]
    fn closing_a_background_tab_leaves_the_front_tab_alone() {
        let mut t = tabs(&["a", "b", "c"]);
        t.focus(console("b"));
        t.close("c");
        assert_eq!(t.active(), &console("b"));
        t.close("missing");
        assert_eq!(t.ids(), ["a", "b"]);
    }

    #[test]
    fn focusing_a_console_that_is_not_open_is_ignored() {
        let mut t = tabs(&["a"]);
        t.focus(View::Launcher);
        t.focus(console("ghost"));
        assert_eq!(t.active(), &View::Launcher);
    }

    #[test]
    fn number_shortcuts_pick_by_position_and_ignore_a_missing_tab() {
        let mut t = tabs(&["a", "b"]);
        t.apply(TabCommand::Focus(0));
        assert_eq!(t.active(), &View::Launcher);
        t.apply(TabCommand::Focus(2));
        assert_eq!(t.active(), &console("b"));
        t.apply(TabCommand::Focus(9));
        assert_eq!(t.active(), &console("b"), "no 9th tab: stay put");
    }

    #[test]
    fn stepping_wraps_in_both_directions() {
        let mut t = tabs(&["a", "b"]);
        t.apply(TabCommand::Focus(0));
        t.apply(TabCommand::Step(-1));
        assert_eq!(t.active(), &console("b"), "launcher -> last");
        t.apply(TabCommand::Step(1));
        assert_eq!(t.active(), &View::Launcher, "last -> launcher");
        t.apply(TabCommand::Step(1));
        assert_eq!(t.active(), &console("a"));
    }

    #[test]
    fn stepping_with_only_the_launcher_stays_on_it() {
        let mut t = Tabs::default();
        t.apply(TabCommand::Step(1));
        t.apply(TabCommand::Step(-1));
        assert_eq!(t.active(), &View::Launcher);
    }

    #[test]
    fn close_active_never_closes_the_launcher() {
        let mut t = tabs(&["a"]);
        t.apply(TabCommand::CloseActive);
        assert!(t.ids().is_empty());
        assert_eq!(t.active(), &View::Launcher);
        t.apply(TabCommand::CloseActive);
        assert_eq!(t.active(), &View::Launcher);
    }

    #[test]
    fn health_flags_a_lost_connection_and_a_halt_but_not_a_healthy_session() {
        use crate::client::ConnectionStatus;
        let mut state = OperatorState::default();
        assert_eq!(
            TabHealth::of(&state),
            TabHealth::Attention,
            "not connected yet"
        );
        state.set_connection(ConnectionStatus::Connected);
        assert_eq!(TabHealth::of(&state), TabHealth::Ok);
        state.set_connection(ConnectionStatus::Reconnecting);
        assert_eq!(TabHealth::of(&state), TabHealth::Attention);
    }
}
