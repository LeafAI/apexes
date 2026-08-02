//! Test-only support code for this crate's own tests. `pub` (not
//! `#[cfg(test)]`-gated) so downstream crates that later want to test
//! against `MarketEventBus` can reuse `MockMarketDataProvider` too,
//! matching this project's existing `apexes-oms::MockExecutionGateway`
//! pattern (a real, reusable in-memory test double, not a throwaway
//! per-test mock).
//!
//! Deviation from the original plan draft: the plan's draft code
//! implemented `MarketDataProvider` for `Arc<MockMarketDataProvider>`
//! directly so a test could hold its own `Arc` handle to assert on
//! `subscribe_call_count()` while also handing a `Box<dyn
//! MarketDataProvider>` (wrapping a clone of that same `Arc`) to
//! `MarketEventBus`. That does not compile: `MarketDataProvider` is
//! defined in `apexes-core` and `Arc` is defined in `std`, so neither
//! type in `impl MarketDataProvider for Arc<MockMarketDataProvider>`
//! is local to this crate -- Rust's orphan rule (E0117) forbids it.
//! Instead, `MockMarketDataProvider` internally shares its state via
//! its own `Arc<MockState>` and exposes a cheap-to-clone
//! [`MockMarketDataProviderHandle`] (via `.handle()`) that a test can
//! keep after moving the `MockMarketDataProvider` itself into a
//! `Box<dyn MarketDataProvider>` -- same test capability
//! (`subscribe_call_count()`/`push_event()` from outside), no orphan-rule
//! violation.

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent};
use async_trait::async_trait;
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

struct MockState {
    subscribe_call_count: AtomicUsize,
    event_sender: Mutex<Option<mpsc::UnboundedSender<MarketEvent>>>,
}

fn push_event_on(state: &MockState, event: MarketEvent) {
    let guard = state
        .event_sender
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let sender = guard
        .as_ref()
        .expect("push_event called before subscribe() was ever called on this mock");
    sender
        .send(event)
        .expect("push_event: receiver side of the mock's channel was dropped");
}

/// An in-memory `MarketDataProvider` a test can push synthetic
/// `MarketEvent`s into via `push_event` -- `subscribe()` returns a
/// stream reading from an internal unbounded channel. Tracks how many
/// times `subscribe()` was actually called (`subscribe_call_count`),
/// which `MarketEventBus`'s lazy-connection/connection-reuse tests
/// assert on directly.
pub struct MockMarketDataProvider {
    state: Arc<MockState>,
}

impl Default for MockMarketDataProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MockMarketDataProvider {
    pub fn new() -> Self {
        Self {
            state: Arc::new(MockState {
                subscribe_call_count: AtomicUsize::new(0),
                event_sender: Mutex::new(None),
            }),
        }
    }

    /// A cheap-to-clone handle sharing this mock's internal state,
    /// for use by a test after the `MockMarketDataProvider` itself has
    /// been moved into a `Box<dyn MarketDataProvider>`.
    pub fn handle(&self) -> MockMarketDataProviderHandle {
        MockMarketDataProviderHandle {
            state: Arc::clone(&self.state),
        }
    }

    /// How many times `subscribe()` has actually been called on this
    /// provider -- `MarketEventBus`'s connection-reuse tests assert
    /// this stays at `1` across multiple `MarketEventBus::subscribe()`
    /// calls with the same instrument set.
    pub fn subscribe_call_count(&self) -> usize {
        self.state.subscribe_call_count.load(Ordering::SeqCst)
    }

    /// Pushes `event` into the stream returned by the most recent
    /// `subscribe()` call. Panics if `subscribe()` was never called
    /// (a test setup error) -- this is test-support code, not
    /// production code, so a panic here is an acceptable "fail fast
    /// on misuse" signal, not a violation of this project's
    /// no-bare-unwrap-in-production-code rule.
    pub fn push_event(&self, event: MarketEvent) {
        push_event_on(&self.state, event)
    }
}

/// A cheap-to-clone handle onto a [`MockMarketDataProvider`]'s shared
/// state, usable after the provider itself has been moved into a
/// `Box<dyn MarketDataProvider>` (e.g. handed to `MarketEventBus`).
#[derive(Clone)]
pub struct MockMarketDataProviderHandle {
    state: Arc<MockState>,
}

impl MockMarketDataProviderHandle {
    pub fn subscribe_call_count(&self) -> usize {
        self.state.subscribe_call_count.load(Ordering::SeqCst)
    }

    pub fn push_event(&self, event: MarketEvent) {
        push_event_on(&self.state, event)
    }
}

#[async_trait]
impl MarketDataProvider for MockMarketDataProvider {
    async fn subscribe(
        &self,
        _instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        use futures::StreamExt;
        self.state
            .subscribe_call_count
            .fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::unbounded_channel();
        *self
            .state
            .event_sender
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(tx);
        Ok(UnboundedReceiverStream::new(rx).boxed())
    }
}
