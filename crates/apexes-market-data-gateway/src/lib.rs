//! Apexes market-data gateway: a standalone process that holds ONE real
//! upstream `apexes_core::MarketDataProvider` connection and broadcasts
//! every event to N connected WebSocket clients across process
//! boundaries, so multiple engine processes and `apexes-ui` don't each
//! need their own separate exchange connection. Covers `docs/FEATURES.md`
//! row `8.3`'s minimum-viable message bus scope -- see
//! `docs/superpowers/specs/2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md`
//! §3 for the full design and rationale.
//!
//! Deliberately NOT built on `apexes_message_bus::MarketEventBus`, which
//! is an in-process-only broadcast wrapper: this crate needs genuine
//! cross-process broadcast (via WebSocket, wired in a later task), a
//! different mechanism. `gateway.rs`'s internal forwarding loop mirrors
//! `MarketEventBus`'s own structural pattern closely (same
//! subscribe-once/broadcast/log-and-continue-on-lag conventions) without
//! depending on or wrapping that crate directly.
//!
//! This module (part 1 of 3 for the gateway crate) provides only the
//! subscriber-broadcast core: `Gateway::new`/`connect`/`subscribe`.
//! Persistence/replay (reusing `apexes-storage`) and the `main.rs`/CLI/
//! `axum` wiring that turns this into a runnable process are later tasks.

pub mod gateway;
pub mod protocol;

pub use gateway::Gateway;
