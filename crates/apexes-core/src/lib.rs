//! Apexes core types: shared `Instrument`, `Order`, `Fill`, `MarketEvent`,
//! and `MarketDataProvider` used across backtest, paper, and live trading.
//!
//! This crate contains only data types and trait definitions — no gateway
//! implementations, no network I/O, no business logic beyond the
//! `OrderStatus` state machine.

pub mod account;
pub mod binance_network;
pub mod chain_data;
pub mod error;
pub mod fill;
pub mod instrument;
pub mod market_data;
pub mod order;
pub mod registry;
pub mod symbol_rules;

pub use account::{FundMovement, FundMovementStatus, WalletId};
pub use chain_data::{ChainBlockHeader, ChainDataProvider, DexPool, PendingTransactionHash};
pub use error::ApexesError;
pub use fill::{Fill, FillId};
pub use instrument::{AssetClass, Instrument, MarketType, Venue};
pub use market_data::{
    Bar, BarInterval, DepthSourceRange, DepthUpdate, MarkPriceUpdate, MarketDataProvider,
    MarketEvent, PriceLevel, Tick, TradeEvent,
};
pub use order::{MarginMode, Order, OrderId, OrderStatus, OrderType, Side};
pub use symbol_rules::{Conformed, RuleViolation, SymbolRules};
