//! Local, read-only historical export sealing and verification.
use apexes_backtest::historical_dataset::{DatasetDefinition, DatasetManifest, seal_dataset};
use apexes_core::MarketEvent;
use std::{
    io::{self, BufReader, Read, Write},
    path::Path,
};

fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Box<dyn std::error::Error>> {
    Ok(serde_json::from_reader(BufReader::new(
        std::fs::File::open(path)?,
    ))?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "qualify-collector") {
        if args.len() != 2 {
            return Err("usage: historical-manifest qualify-collector <journal.jsonl> (APEXES_CAPTURE_CLICKHOUSE_URL required)".into());
        }
        let url = std::env::var("APEXES_CAPTURE_CLICKHOUSE_URL")?;
        let mut raw = String::new();
        std::fs::File::open(Path::new(&args[1]))?
            .take(apexes_backtest::collector_receipts::MAX_JOURNAL_BYTES as u64 + 1)
            .read_to_string(&mut raw)?;
        let reader = apexes_storage::raw_depth_store::RawDepthReader::new(&url);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = runtime
            .block_on(apexes_backtest::collector_receipts::qualify_recorded_run(
                &raw, &reader,
            ))
            .map_err(io::Error::other)?;
        let mut out = io::stdout().lock();
        serde_json::to_writer_pretty(&mut out, &result)?;
        writeln!(out)?;
        return Ok(());
    }
    if args.len() != 3
        && !(args.len() == 4
            && (args[0] == "verify-coverage"
                || args[0] == "normalize"
                || args[0] == "verify-ranges"
                || args[0] == "build-ranges"))
    {
        return Err("usage: historical-manifest seal <definition.json> <events.json> | verify <manifest.json> <events.json> | verify-coverage <manifest.json> <events.json> <coverage.json> | normalize <definition.json> <events.json> <identity-policy.json> | verify-ranges <manifest.json> <events.json> <ranges.json> | build-ranges <manifest.json> <events.json> <contracts.json> | reconcile-collector <journal.jsonl> <batches.json> | qualify-collector <journal.jsonl>".into());
    }
    if args[0] == "reconcile-collector" && args.len() == 3 {
        let journal = std::fs::read_to_string(Path::new(&args[1]))?;
        let batches: Vec<apexes_storage::raw_depth_store::RawDepthBatch> =
            read(Path::new(&args[2]))?;
        let result = apexes_backtest::collector_receipts::reconcile(&journal, &batches)
            .map_err(io::Error::other)?;
        let mut out = io::stdout().lock();
        serde_json::to_writer_pretty(&mut out, &result)?;
        writeln!(out)?;
        return Ok(());
    }
    let events: Vec<MarketEvent> = read(Path::new(&args[2]))?;
    let mut out = io::stdout().lock();
    match args[0].to_str() {
        Some("seal") => {
            let definition: DatasetDefinition = read(Path::new(&args[1]))?;
            serde_json::to_writer_pretty(
                &mut out,
                &seal_dataset(definition, &events).map_err(io::Error::other)?,
            )?;
        }
        Some("verify") => {
            let manifest: DatasetManifest = read(Path::new(&args[1]))?;
            serde_json::to_writer_pretty(
                &mut out,
                &manifest.verify(&events).map_err(io::Error::other)?,
            )?;
        }
        Some("verify-coverage") if args.len() == 4 => {
            let manifest: DatasetManifest = read(Path::new(&args[1]))?;
            let coverage: apexes_backtest::source_coverage::SourceCoverage =
                read(Path::new(&args[3]))?;
            let verified = coverage
                .verify_dataset(&manifest, &events)
                .map_err(io::Error::other)?;
            serde_json::to_writer_pretty(&mut out, &verified)?;
        }
        Some("normalize") if args.len() == 4 => {
            let definition: DatasetDefinition = read(Path::new(&args[1]))?;
            let policy: apexes_backtest::source_identity::IdentityPolicy =
                read(Path::new(&args[3]))?;
            let result =
                apexes_backtest::source_identity::normalize_dataset(definition, policy, &events)
                    .map_err(io::Error::other)?;
            serde_json::to_writer_pretty(&mut out, &result)?;
        }
        Some("build-ranges") if args.len() == 4 => {
            let manifest: DatasetManifest = read(Path::new(&args[1]))?;
            let contracts: Vec<apexes_backtest::depth_ranges::NativeRangeContract> =
                read(Path::new(&args[3]))?;
            let evidence = apexes_backtest::depth_ranges::build_native_evidence(
                &manifest, &events, &contracts,
            )
            .map_err(io::Error::other)?;
            serde_json::to_writer_pretty(&mut out, &evidence)?;
        }
        Some("verify-ranges") if args.len() == 4 => {
            let manifest: DatasetManifest = read(Path::new(&args[1]))?;
            let evidence: apexes_backtest::depth_ranges::DepthRangeEvidence =
                read(Path::new(&args[3]))?;
            let verified = evidence
                .verify_dataset(&manifest, &events)
                .map_err(io::Error::other)?;
            serde_json::to_writer_pretty(&mut out, &verified)?;
        }
        _ => {
            return Err(
                "expected seal, verify, verify-coverage, normalize, verify-ranges, build-ranges or reconcile-collector"
                    .into(),
            );
        }
    }
    writeln!(out)?;
    Ok(())
}
