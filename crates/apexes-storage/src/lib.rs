//! Apexes historical data storage: persists `apexes_core::MarketEvent`
//! (Trade, Tick, Depth, Bar) to ClickHouse and reads it back. ClickHouse
//! is the project's sole historical-data storage backend (see design spec
//! for the rationale on why this replaced an earlier local-Parquet plan).

pub mod conversion;
pub mod error;
pub mod reader;
pub mod schema;
pub mod writer;

pub use error::StorageError;
pub use reader::{EventType, StorageReader};
pub use writer::StorageWriter;
