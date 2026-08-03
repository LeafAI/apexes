//! Mirrors `candlestick_renders_without_panicking.rs` (Task 4) for
//! `TickLineChart` -- see that file's own doc comment for the shared
//! rationale and the known `wgpu`-backend environment dependency.

use apexes_chart::geometry::TickPoint;
use apexes_chart::tick_line::TickLineChart;
use apexes_chart::view_state::ViewState;
use chrono::{TimeZone, Utc};
use egui_kittest::Harness;
use rust_decimal_macros::dec;

fn t(epoch_secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
}

fn sample_ticks() -> Vec<TickPoint> {
    vec![
        TickPoint {
            timestamp: t(0),
            bid: dec!(64990),
            ask: dec!(65010),
        },
        TickPoint {
            timestamp: t(30),
            bid: dec!(65000),
            ask: dec!(65020),
        },
        TickPoint {
            timestamp: t(60),
            bid: dec!(64980),
            ask: dec!(65005),
        },
    ]
}

struct TestApp {
    chart: TickLineChart,
    ticks: Vec<TickPoint>,
    view: ViewState,
}

impl eframe::App for TestApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.chart.show(
            ui,
            &self.ticks,
            &mut self.view,
            egui::Vec2::new(400.0, 200.0),
        );
    }
}

#[test]
fn tick_line_chart_renders_one_frame_without_panicking() {
    let mut harness = Harness::builder().wgpu().build_eframe(|cc| {
        let render_state = cc
            .wgpu_render_state
            .as_ref()
            .expect("wgpu render state should be present when the wgpu harness feature is used");
        let chart = TickLineChart::new(&render_state.device, render_state.target_format);
        TestApp {
            chart,
            ticks: sample_ticks(),
            view: ViewState::new_following_live(),
        }
    });

    harness.run();
}
