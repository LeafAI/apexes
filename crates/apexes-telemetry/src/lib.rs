//! Process-wide telemetry for Apexes binaries: Prometheus metrics and
//! `tracing` initialisation.
//!
//! Metrics live in one global [`Metrics`] (see [`metrics()`]) so any crate
//! can record without plumbing a handle through every constructor; the
//! engine exposes [`render()`] on `/metrics`. All recording methods are
//! cheap atomic operations and never fail.

use std::sync::OnceLock;
use std::time::Duration;

use prometheus::{
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder,
};

/// Outcome label values for `apexes_orders_submitted_total`.
pub mod order_result {
    pub const ACCEPTED: &str = "accepted";
    pub const REJECTED: &str = "rejected";
    pub const UNCERTAIN: &str = "uncertain";
    pub const RISK_REJECTED: &str = "risk_rejected";
    pub const HALTED: &str = "halted";
}

pub struct Metrics {
    registry: Registry,
    pub orders_submitted: IntCounterVec,
    pub order_submit_latency: Histogram,
    pub fills: IntCounter,
    pub open_orders: IntGauge,
    pub reconcile_runs: IntCounterVec,
    /// Orders whose status changed or that produced fills during a
    /// reconcile: i.e. state the push stream missed ("drift").
    pub reconcile_changes: IntCounter,
    pub stream_connected: GaugeVec,
    pub stream_reconnects: IntCounterVec,
    pub market_data_idle_seconds: Gauge,
    pub kill_switch_engaged: IntGauge,
    pub flatten_orders: IntCounter,
    pub unflattened_positions: IntGauge,
    pub rest_requests: IntCounterVec,
    pub rest_rate_limited: IntCounter,
    pub equity: Gauge,
    pub realized_pnl: Gauge,
    pub open_positions: IntGauge,
}

impl Metrics {
    fn new() -> Self {
        let registry = Registry::new_custom(Some("apexes".to_string()), None)
            .expect("static registry config is valid");
        macro_rules! reg {
            ($m:expr) => {{
                let m = $m;
                registry
                    .register(Box::new(m.clone()))
                    .expect("metric names are unique");
                m
            }};
        }
        Self {
            orders_submitted: reg!(
                IntCounterVec::new(
                    Opts::new("orders_submitted_total", "Order submissions by outcome"),
                    &["result"]
                )
                .unwrap()
            ),
            order_submit_latency: reg!(
                Histogram::with_opts(
                    HistogramOpts::new(
                        "order_submit_latency_seconds",
                        "Gateway round-trip for order submission"
                    )
                    .buckets(vec![
                        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0
                    ])
                )
                .unwrap()
            ),
            fills: reg!(IntCounter::new("fills_total", "Fills recorded").unwrap()),
            open_orders: reg!(IntGauge::new("open_orders", "Locally tracked open orders").unwrap()),
            reconcile_runs: reg!(
                IntCounterVec::new(
                    Opts::new("reconcile_runs_total", "Order reconcile passes by result"),
                    &["result"]
                )
                .unwrap()
            ),
            reconcile_changes: reg!(
                IntCounter::new(
                    "reconcile_changes_total",
                    "Orders changed by reconcile (state missed by the push stream)"
                )
                .unwrap()
            ),
            stream_connected: reg!(
                GaugeVec::new(
                    Opts::new("stream_connected", "1 if the user-data stream is connected"),
                    &["family"]
                )
                .unwrap()
            ),
            stream_reconnects: reg!(
                IntCounterVec::new(
                    Opts::new(
                        "stream_reconnects_total",
                        "User-data stream (re)connections"
                    ),
                    &["family"]
                )
                .unwrap()
            ),
            market_data_idle_seconds: reg!(
                Gauge::new(
                    "market_data_idle_seconds",
                    "Seconds since the last market event (live)"
                )
                .unwrap()
            ),
            kill_switch_engaged: reg!(
                IntGauge::new("kill_switch_engaged", "1 while the kill switch is engaged").unwrap()
            ),
            flatten_orders: reg!(
                IntCounter::new(
                    "flatten_orders_total",
                    "Closing orders submitted by a kill-switch flatten"
                )
                .unwrap()
            ),
            unflattened_positions: reg!(
                IntGauge::new(
                    "unflattened_positions",
                    "Open positions seen by the last flatten round"
                )
                .unwrap()
            ),
            rest_requests: reg!(
                IntCounterVec::new(
                    Opts::new(
                        "rest_requests_total",
                        "Exchange REST requests by status class"
                    ),
                    &["class"]
                )
                .unwrap()
            ),
            rest_rate_limited: reg!(
                IntCounter::new("rest_rate_limited_total", "429/418 responses received").unwrap()
            ),
            equity: reg!(Gauge::new("equity", "Session equity (quote currency)").unwrap()),
            realized_pnl: reg!(Gauge::new("realized_pnl", "Session realized PnL").unwrap()),
            open_positions: reg!(IntGauge::new("open_positions", "Open positions").unwrap()),
            registry,
        }
    }

    pub fn observe_submit_latency(&self, d: Duration) {
        self.order_submit_latency.observe(d.as_secs_f64());
    }

    pub fn count_rest_status(&self, status: u16) {
        let class = match status {
            200..=299 => "2xx",
            300..=399 => "3xx",
            400..=499 => "4xx",
            500..=599 => "5xx",
            _ => "other",
        };
        self.rest_requests.with_label_values(&[class]).inc();
        if status == 429 || status == 418 {
            self.rest_rate_limited.inc();
        }
    }

    /// Prometheus text exposition of every registered metric.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("text encoding cannot fail");
        String::from_utf8(buf).expect("prometheus text format is UTF-8")
    }
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// The process-wide metrics.
pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::new)
}

/// Prometheus text for `/metrics`.
pub fn render() -> String {
    metrics().render()
}

/// Initialises `tracing` for a binary: filter from `RUST_LOG` (default
/// `info`), JSON lines when `APEXES_LOG_FORMAT=json`, else human-readable.
/// Safe to call more than once (later calls are no-ops).
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    let result = if std::env::var("APEXES_LOG_FORMAT").is_ok_and(|v| v.eq_ignore_ascii_case("json"))
    {
        builder.json().try_init()
    } else {
        builder.try_init()
    };
    let _ = result;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_recorded_metrics_with_prefix_and_labels() {
        let m = metrics();
        m.orders_submitted
            .with_label_values(&[order_result::ACCEPTED])
            .inc();
        m.observe_submit_latency(Duration::from_millis(30));
        m.count_rest_status(429);
        m.stream_connected.with_label_values(&["spot"]).set(1.0);
        m.kill_switch_engaged.set(1);

        let text = render();
        assert!(text.contains("apexes_orders_submitted_total{result=\"accepted\"}"));
        assert!(text.contains("apexes_order_submit_latency_seconds_bucket"));
        assert!(text.contains("apexes_rest_requests_total{class=\"4xx\"}"));
        assert!(text.contains("apexes_rest_rate_limited_total"));
        assert!(text.contains("apexes_stream_connected{family=\"spot\"} 1"));
        assert!(text.contains("apexes_kill_switch_engaged 1"));
    }

    #[test]
    fn init_tracing_is_idempotent() {
        init_tracing();
        init_tracing();
    }
}
