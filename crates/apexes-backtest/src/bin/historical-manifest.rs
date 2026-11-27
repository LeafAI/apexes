//! Local, read-only historical export sealing and verification.
use apexes_backtest::historical_dataset::{DatasetDefinition, DatasetManifest, seal_dataset};
use apexes_core::MarketEvent;
use std::{
    io::{self, BufReader, Write},
    path::Path,
};

fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Box<dyn std::error::Error>> {
    Ok(serde_json::from_reader(BufReader::new(
        std::fs::File::open(path)?,
    ))?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 3
        && !(args.len() == 4 && (args[0] == "verify-coverage" || args[0] == "normalize"))
    {
        return Err("usage: historical-manifest seal <definition.json> <events.json> | verify <manifest.json> <events.json> | verify-coverage <manifest.json> <events.json> <coverage.json> | normalize <definition.json> <events.json> <identity-policy.json>".into());
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
        _ => return Err("expected seal, verify, verify-coverage or normalize".into()),
    }
    writeln!(out)?;
    Ok(())
}
