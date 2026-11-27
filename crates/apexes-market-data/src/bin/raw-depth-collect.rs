//! F03.03: opt-in, bounded public Binance depth capture; never submits orders.
use apexes_market_data::{provider::BinanceExchangeKind, raw_depth_collector};
use apexes_storage::raw_depth_store::RawDepthStore;
use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::{
    future::Future,
    io::{self, Write},
    path::PathBuf,
    time::Duration,
};
use tokio::sync::oneshot;

const MAX_SECONDS: u64 = 3600;
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Market {
    Spot,
    #[value(name = "usd_m_perpetual")]
    UsdMPerpetual,
}

impl From<Market> for BinanceExchangeKind {
    fn from(market: Market) -> Self {
        match market {
            Market::Spot => Self::Spot,
            Market::UsdMPerpetual => Self::Perpetual,
        }
    }
}

fn symbol(value: &str) -> Result<String, String> {
    if value.is_empty()
        || value.len() > 32
        || !value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return Err("symbol must be 1–32 uppercase ASCII letters or digits".into());
    }
    Ok(value.into())
}

#[derive(Debug, Parser)]
#[command(
    about = "Capture public Binance depth to a dedicated ClickHouse store and durable journal. No trading or account access."
)]
struct Args {
    #[arg(long, value_parser = symbol)]
    symbol: String,
    #[arg(long, value_enum)]
    market: Market,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=MAX_SECONDS))]
    duration_seconds: u64,
    /// Existing directory for a new independent receipt journal.
    #[arg(long)]
    receipt_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StopReason {
    DurationElapsed,
    Interrupt,
    Terminate,
}

#[derive(Debug, Serialize)]
struct Completion {
    run_id: String,
    receipt_path: PathBuf,
    stop_reason: StopReason,
    upstream_completeness_verified: bool,
}

#[cfg(unix)]
struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn install() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn next(&mut self) -> StopReason {
        tokio::select! {
            _ = self.interrupt.recv() => StopReason::Interrupt,
            _ = self.terminate.recv() => StopReason::Terminate,
        }
    }
}

#[cfg(not(unix))]
struct Signals;

#[cfg(not(unix))]
impl Signals {
    fn install() -> io::Result<Self> {
        Ok(Self)
    }

    async fn next(&mut self) -> StopReason {
        let _ = tokio::signal::ctrl_c().await;
        StopReason::Interrupt
    }
}

/// Never abort the capture on the initial stop signal: let the consumer
/// close its input, drain accepted events and sync the terminal receipt.
/// If draining exceeds the bound, refuse a success report; the incomplete
/// journal remains diagnostic and must not be reused.
async fn run_bounded<C, S>(
    capture: C,
    stop: oneshot::Sender<()>,
    shutdown: S,
    drain_timeout: Duration,
) -> Result<(String, StopReason), String>
where
    C: Future<Output = Result<String, String>>,
    S: Future<Output = StopReason>,
{
    tokio::pin!(capture);
    let reason = tokio::select! {
        biased;
        result = &mut capture => {
            return match result {
                Ok(_) => Err("capture ended before a requested stop".into()),
                Err(error) => Err(error),
            };
        }
        reason = shutdown => reason,
    };
    stop.send(())
        .map_err(|_| "capture stop receiver closed before shutdown")?;
    let run = tokio::time::timeout(drain_timeout, capture)
        .await
        .map_err(|_| "capture drain timed out; inspect the journal, do not retry the run")??;
    Ok((run, reason))
}

async fn setup(args: &Args) -> Result<RawDepthStore, String> {
    let metadata =
        std::fs::metadata(&args.receipt_dir).map_err(|_| "receipt directory must already exist")?;
    if !metadata.is_dir() {
        return Err("receipt path is not a directory".into());
    }
    let url = std::env::var("APEXES_CAPTURE_CLICKHOUSE_URL")
        .map_err(|_| "APEXES_CAPTURE_CLICKHOUSE_URL must be explicitly configured")?;
    // Never print a connection URL or underlying setup error that could
    // contain user-supplied database credentials.
    tokio::time::timeout(SETUP_TIMEOUT, RawDepthStore::new(&url))
        .await
        .map_err(|_| "capture storage initialization timed out")?
        .map_err(|_| "capture storage initialization failed".into())
}

async fn execute(args: Args) -> Result<Completion, String> {
    let mut signals = Signals::install().map_err(|_| "could not install shutdown handlers")?;
    let store = tokio::select! {
        result = setup(&args) => result?,
        _ = signals.next() => return Err("capture interrupted during setup".into()),
    };
    let (stop_tx, stop_rx) = oneshot::channel();
    let shutdown = async {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(args.duration_seconds)) => StopReason::DurationElapsed,
            reason = signals.next() => reason,
        }
    };
    let (run_id, stop_reason) = run_bounded(
        raw_depth_collector::collect(
            &args.symbol,
            args.market.into(),
            &store,
            &args.receipt_dir,
            stop_rx,
        ),
        stop_tx,
        shutdown,
        DRAIN_TIMEOUT,
    )
    .await?;
    Ok(Completion {
        receipt_path: args.receipt_dir.join(format!("{run_id}.jsonl")),
        run_id,
        stop_reason,
        upstream_completeness_verified: false,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_ansi(false)
        .try_init()
        .map_err(|_| io::Error::other("could not initialize capture logging"))?;
    let result = execute(args).await.map_err(io::Error::other)?;
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &result)?;
    writeln!(stdout)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(extra: &[&str]) -> Result<Args, clap::Error> {
        Args::try_parse_from(std::iter::once("raw-depth-collect").chain(extra.iter().copied()))
    }

    #[test]
    fn requires_explicit_symbol_market_duration_and_receipt_directory() {
        let flags = [
            "--symbol",
            "BTCUSDT",
            "--market",
            "usd_m_perpetual",
            "--duration-seconds",
            "10",
            "--receipt-dir",
            "receipts",
        ];
        let args = parse(&flags).unwrap();
        assert!(matches!(args.market, Market::UsdMPerpetual));
        assert_eq!(args.duration_seconds, 10);
        for index in [0, 2, 4, 6] {
            let partial: Vec<_> = flags
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != index && *i != index + 1)
                .map(|(_, s)| *s)
                .collect();
            assert!(parse(&partial).is_err());
        }
    }

    #[test]
    fn rejects_invalid_symbol_market_and_duration_before_io() {
        for (symbol, market, duration) in [
            ("btcusdt", "spot", "1"),
            ("BTC/USDT", "spot", "1"),
            ("BTCUSDT", "testnet", "1"),
            ("BTCUSDT", "spot", "0"),
            ("BTCUSDT", "spot", "3601"),
        ] {
            assert!(
                parse(&[
                    "--symbol",
                    symbol,
                    "--market",
                    market,
                    "--duration-seconds",
                    duration,
                    "--receipt-dir",
                    "."
                ])
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn all_stop_causes_await_drain_before_success() {
        for reason in [
            StopReason::DurationElapsed,
            StopReason::Interrupt,
            StopReason::Terminate,
        ] {
            let (tx, rx) = oneshot::channel();
            let (id, observed) = run_bounded(
                async {
                    rx.await.map_err(|e| e.to_string())?;
                    tokio::task::yield_now().await;
                    Ok("run".into())
                },
                tx,
                std::future::ready(reason),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
            assert_eq!(id, "run");
            assert_eq!(observed, reason);
        }
    }

    #[tokio::test]
    async fn early_failure_and_drain_failure_cannot_report_success() {
        let (tx, _rx) = oneshot::channel();
        assert_eq!(
            run_bounded(
                std::future::ready(Err("write failed".into())),
                tx,
                std::future::pending(),
                Duration::from_secs(1)
            )
            .await
            .unwrap_err(),
            "write failed"
        );
        let (tx, rx) = oneshot::channel();
        let result = run_bounded(
            async {
                rx.await.unwrap();
                Err("ambiguous insert".into())
            },
            tx,
            std::future::ready(StopReason::Interrupt),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(result.unwrap_err(), "ambiguous insert");
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_drain_fails_closed() {
        let (tx, rx) = oneshot::channel();
        let result = run_bounded(
            async {
                rx.await.unwrap();
                std::future::pending().await
            },
            tx,
            std::future::ready(StopReason::Terminate),
            Duration::from_secs(1),
        )
        .await;
        assert!(result.unwrap_err().contains("drain timed out"));
    }
}
