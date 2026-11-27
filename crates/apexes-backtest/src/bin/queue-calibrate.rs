//! Offline JSON-in / JSON-out calibration. Never writes execution settings.
use apexes_backtest::queue_calibration::{QueueCalibrationInput, calibrate_queue};
use std::io::{self, BufReader, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let path = args
        .next()
        .ok_or("usage: queue-calibrate <observations.json>")?;
    if args.next().is_some() {
        return Err("usage: queue-calibrate <observations.json>".into());
    }
    let input: QueueCalibrationInput =
        serde_json::from_reader(BufReader::new(std::fs::File::open(path)?))?;
    let report = calibrate_queue(&input).map_err(io::Error::other)?;
    let mut out = io::stdout().lock();
    serde_json::to_writer_pretty(&mut out, &report)?;
    writeln!(out)?;
    Ok(())
}
