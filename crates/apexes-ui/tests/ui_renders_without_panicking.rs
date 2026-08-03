//! Drives one `egui_kittest` headless render pass over
//! `apexes_ui::ui::DashboardApp` with synthetic state (a few
//! populated rows plus one still-empty row), asserting no panic --
//! this is the automatable half of this project's two-part UI
//! testing strategy for `egui` code (the other half is the plain
//! `app_state`/`client` unit tests, which need no rendering at all).
//! Visual correctness itself is not automated by this test.

use apexes_core::{MarketEvent, TradeEvent, Venue};
use apexes_ui::app_state::AppState;
use apexes_ui::client::ConnectionStatus;
use apexes_ui::ui::DashboardApp;
use egui_kittest::Harness;
use rust_decimal_macros::dec;

#[test]
fn dashboard_app_renders_one_frame_without_panicking() {
    let symbols = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "BNBUSDT".to_string(),
    ];
    let mut app_state = AppState::new(&symbols);
    app_state.apply_event(&MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: chrono::Utc::now(),
    }));
    // ETHUSDT/BNBUSDT stay at their default (empty) RowState -- the
    // render pass must handle both populated and still-empty rows
    // without panicking.

    let mut harness = Harness::builder()
        .build_eframe(|_cc| DashboardApp::new_for_test(app_state, ConnectionStatus::Connected));

    // `Harness::run` refuses UIs that keep requesting repaints (this
    // app calls `request_repaint_after` every frame by design, so
    // `run` would abort with "exceeded max_steps") -- `step` renders
    // exactly one frame, which is all this smoke test needs.
    harness.step();
}
