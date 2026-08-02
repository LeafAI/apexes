//! Apexes in-process market event bus. Covers `docs/FEATURES.md` row
//! `1.7`'s in-process-broadcast-and-downsampling scope (genuine
//! inter-process/multi-machine message-bus support, e.g. NATS
//! JetStream, remains unimplemented and explicitly deferred -- see
//! `docs/superpowers/specs/2026-08-03-apexes-message-bus-design.md`
//! §1 for the full scope rationale).
//!
//! [`MarketEventBus`] deliberately does NOT implement
//! `apexes_core::MarketDataProvider` -- it is a distinct type with its
//! own `subscribe`/`subscribe_downsampled` methods returning real
//! `tokio::sync::broadcast::Receiver<MarketEvent>` values directly
//! (not `BoxStream`), so callers must explicitly construct and use a
//! `MarketEventBus` to opt into multi-subscriber broadcast semantics
//! rather than accidentally getting them through the existing trait.

pub mod bus;
pub mod test_support;

pub use bus::{MarketEventBus, receiver_into_stream};
