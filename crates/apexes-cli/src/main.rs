//! Apexes CLI entry point. This foundation-phase skeleton only proves
//! apexes-cli links against apexes-core and parses --version/--help;
//! real subcommands are added as subsystems land.

use clap::Parser;

#[derive(Parser)]
#[command(name = "apexes", version, about = "Apexes multi-market trading system")]
struct Cli;

fn main() {
    // apexes-core is depended upon (not yet used) to prove the workspace
    // links correctly end-to-end; referencing a type here keeps the
    // dependency from being flagged as unused once real subcommands land.
    let _ = std::any::type_name::<apexes_core::Instrument>();

    Cli::parse();
}
