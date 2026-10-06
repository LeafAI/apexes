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
//! Part 1 of 3 for the gateway crate provided the subscriber-broadcast
//! core: `Gateway::new`/`connect`/`subscribe`. Part 2 (this addition)
//! adds `replay.rs` (historical replay via `apexes_storage::StorageReader`)
//! and `persistence.rs` (async, non-blocking persistence via
//! `apexes_storage::StorageWriter`), wired into `Gateway` via
//! `attach_persistence`/`flush_persistence_aged` and into `connect`'s
//! forwarding loop. Part 3 (this addition) adds `server.rs` (the
//! `axum` `GET /market` WebSocket endpoint, wiring replay + live
//! forwarding) and `main.rs` (CLI, CPU affinity, PID file, startup
//! sequence) -- see `main.rs`'s own doc comment for the PID file's
//! authoritative format.

pub mod gateway;
pub mod persistence;
pub mod protocol;
pub mod replay;
pub mod server;

pub use gateway::Gateway;
pub use persistence::PersistenceBuffer;
pub use replay::replay_since;
pub use server::{build_app, build_app_secured};
