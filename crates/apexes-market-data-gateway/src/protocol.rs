//! Wire protocol for the gateway's `/market` WebSocket endpoint.
//!
//! Frames are `serde_json`-serialized [`apexes_core::MarketEvent`]
//! values directly (matching `apexes-ui`'s existing `/ws` 13a
//! convention) -- no new envelope type is needed here (unlike
//! `apexes-engine`'s `/session/ws`, which genuinely needs one to
//! distinguish control vs. snapshot messages; see the design spec's
//! §6 and the later `apexes-engine` task for that).
//!
//! [`Frame`] is a thin alias documenting this crate's own wire type at
//! the module boundary, distinct in intent (even though identical in
//! shape today) from any other crate's use of `MarketEvent` directly.

/// One `/market` WebSocket frame: a single `apexes_core::MarketEvent`,
/// `serde_json`-serialized with no additional envelope.
pub type Frame = apexes_core::MarketEvent;
