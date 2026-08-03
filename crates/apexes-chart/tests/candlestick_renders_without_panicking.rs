//! Drives one `egui_kittest` headless render pass (using its real
//! `wgpu`-feature `WgpuTestRenderer`, which prefers a software
//! rasterizer where available) over `CandlestickChart::show` with
//! synthetic `Bar` data, asserting no panic. Known environment
//! dependency: requires a working `wgpu` backend at test-run time
//! (Metal on the current macOS dev environment; would need Mesa
//! llvmpipe on a GPU-less Linux CI machine) -- see the design spec
//! §7 for the full rationale.

use apexes_chart::candlestick::CandlestickChart;
use apexes_chart::view_state::ViewState;
use apexes_core::{Bar, BarInterval, Venue};
use chrono::{TimeZone, Utc};
use egui_kittest::Harness;
use rust_decimal_macros::dec;

fn t(epoch_secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
}

fn sample_bars() -> Vec<Bar> {
    vec![
        Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(65000),
            high: dec!(65200),
            low: dec!(64900),
            close: dec!(65100),
            volume: dec!(10),
            interval: BarInterval::OneMinute,
            open_time: t(0),
            close_time: t(60),
        },
        Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(65100),
            high: dec!(65150),
            low: dec!(64800),
            close: dec!(64850),
            volume: dec!(8),
            interval: BarInterval::OneMinute,
            open_time: t(60),
            close_time: t(120),
        },
    ]
}

struct TestApp {
    chart: CandlestickChart,
    bars: Vec<Bar>,
    view: ViewState,
}

impl eframe::App for TestApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.chart.show(
            ui,
            &self.bars,
            &mut self.view,
            egui::Vec2::new(400.0, 200.0),
        );
    }
}

#[test]
fn candlestick_chart_renders_one_frame_without_panicking() {
    let mut harness = Harness::builder().wgpu().build_eframe(|cc| {
        let render_state = cc
            .wgpu_render_state
            .as_ref()
            .expect("wgpu render state should be present when the wgpu harness feature is used");
        let chart = CandlestickChart::new(&render_state.device, render_state.target_format);
        TestApp {
            chart,
            bars: sample_bars(),
            view: ViewState::new_following_live(),
        }
    });

    harness.run();
}
