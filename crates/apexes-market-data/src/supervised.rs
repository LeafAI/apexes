//! Keeps a market-data stream alive through the failure `barter-data` cannot
//! see: a connection that has silently died.
//!
//! When the network drops without the OS delivering a reset (Wi-Fi roam, a
//! sleeping laptop, a NAT that forgets the flow), the socket stays
//! `ESTABLISHED` and simply goes quiet. `barter-data` reconnects only on an
//! error or a close, so it waits forever; the engine then saw no data for
//! 30 minutes and halted trading. Binance sends a ping every 20 s on a live
//! socket, so a stream that delivers nothing for [`IDLE_TIMEOUT`] is dead.
//!
//! [`run`] / [`run_isolated`] connect, forward items, and on idle (or end, or
//! failed connect) drop the stream and connect again with a capped
//! exponential backoff that starts over after a healthy connection.

use std::future::Future;
use std::time::Duration;

use futures::{Stream, StreamExt};

/// A stream that delivers nothing for this long is treated as dead. Right for
/// feeds that update every second or so (best bid/ask, depth, mark price).
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// For public trades, which on a quiet market (the testnet) can legitimately
/// be silent for minutes: longer, so a live socket is not churned.
pub const TRADES_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// Connecting (TCP, TLS, subscription) must finish within this.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// What the item handler wants next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Continue,
    /// The consumer is gone; stop for good.
    Stop,
}

#[derive(Debug, Clone, Copy)]
pub struct Timings {
    pub idle: Duration,
    pub connect: Duration,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
}

impl Timings {
    pub fn trades() -> Self {
        Self {
            idle: TRADES_IDLE_TIMEOUT,
            ..Self::default()
        }
    }
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            idle: IDLE_TIMEOUT,
            connect: CONNECT_TIMEOUT,
            backoff_initial: BACKOFF_INITIAL,
            backoff_max: BACKOFF_MAX,
        }
    }
}

/// How one connection attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The consumer is gone; stop for good.
    Stopped,
    /// Connect failed, the stream ended, or it went idle. `healthy` says the
    /// connection worked for a while first (it delivered something, or sat
    /// quiet for a whole idle window), as opposed to never working or being
    /// cut at once. Only an unhealthy one lengthens the backoff, so a quiet
    /// but live stream is recycled promptly instead of with growing gaps.
    Retry { healthy: bool },
}

/// Capped exponential backoff that starts over after a healthy connection.
#[derive(Debug)]
pub struct Backoff {
    timings: Timings,
    next: Duration,
}

impl Backoff {
    pub fn new(timings: Timings) -> Self {
        Self {
            next: timings.backoff_initial,
            timings,
        }
    }

    /// The delay before the next attempt, given how the last one went.
    pub fn after(&mut self, healthy: bool) -> Duration {
        if healthy {
            self.next = self.timings.backoff_initial;
        }
        let delay = self.next;
        self.next = (self.next * 2).min(self.timings.backoff_max);
        delay
    }
}

/// One attempt: connect, then forward until idle, end, or the consumer stops.
pub async fn attempt<C, Fut, S, T, H>(
    name: &str,
    timings: Timings,
    connect: &mut C,
    handle: &mut H,
) -> Outcome
where
    C: FnMut() -> Fut,
    Fut: Future<Output = Option<S>>,
    S: Stream<Item = T> + Unpin,
    H: FnMut(T) -> Flow,
{
    match tokio::time::timeout(timings.connect, connect()).await {
        Ok(Some(stream)) => pump(name, timings.idle, stream, handle).await,
        Ok(None) => {
            tracing::warn!(stream = name, "could not connect; retrying");
            Outcome::Retry { healthy: false }
        }
        Err(_) => {
            tracing::warn!(stream = name, "connect timed out; retrying");
            Outcome::Retry { healthy: false }
        }
    }
}

/// Attempts forever on the current runtime. For code that owns its own
/// sockets; see [`run_isolated`] for `barter-data`, which spawns tasks.
pub async fn run<C, Fut, S, T, H>(name: &str, timings: Timings, mut connect: C, mut handle: H)
where
    C: FnMut() -> Fut,
    Fut: Future<Output = Option<S>>,
    S: Stream<Item = T> + Unpin,
    H: FnMut(T) -> Flow,
{
    let mut backoff = Backoff::new(timings);
    loop {
        match attempt(name, timings, &mut connect, &mut handle).await {
            Outcome::Stopped => return,
            Outcome::Retry { healthy } => {
                tokio::time::sleep(backoff.after(healthy)).await;
            }
        }
    }
}

/// Like [`run`], but every attempt gets a fresh single-thread runtime that is
/// dropped when the attempt ends. `barter-data` spawns its own forwarding
/// task per stream and gives no handle to stop it, so dropping the stream
/// would leave a dead socket and its task behind on every recovery; dropping
/// the runtime ends them. Blocks the calling (dedicated) thread.
pub fn run_isolated<C, Fut, S, T, H>(name: &str, timings: Timings, mut connect: C, mut handle: H)
where
    C: FnMut() -> Fut,
    Fut: Future<Output = Option<S>>,
    S: Stream<Item = T> + Unpin,
    H: FnMut(T) -> Flow,
{
    let mut backoff = Backoff::new(timings);
    loop {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::error!(stream = name, %error, "cannot build a runtime; stopping");
                return;
            }
        };
        let outcome = runtime.block_on(attempt(name, timings, &mut connect, &mut handle));
        drop(runtime);
        match outcome {
            Outcome::Stopped => return,
            Outcome::Retry { healthy } => std::thread::sleep(backoff.after(healthy)),
        }
    }
}

/// Forwards items until the stream goes idle, ends, or the consumer stops.
async fn pump<S, T, H>(name: &str, idle: Duration, mut stream: S, handle: &mut H) -> Outcome
where
    S: Stream<Item = T> + Unpin,
    H: FnMut(T) -> Flow,
{
    let mut delivered = false;
    loop {
        match tokio::time::timeout(idle, stream.next()).await {
            Ok(Some(item)) => {
                delivered = true;
                if handle(item) == Flow::Stop {
                    return Outcome::Stopped;
                }
            }
            Ok(None) => {
                tracing::warn!(stream = name, "stream ended; reconnecting");
                return Outcome::Retry { healthy: delivered };
            }
            Err(_) => {
                tracing::warn!(
                    stream = name,
                    idle_secs = idle.as_secs_f64(),
                    "no data; the connection looks dead, reconnecting"
                );
                return Outcome::Retry { healthy: true };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    fn timings() -> Timings {
        Timings {
            idle: Duration::from_secs(10),
            connect: Duration::from_secs(5),
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(8),
        }
    }

    /// Items, then silence forever: a half-open connection.
    fn then_silent(items: Vec<u32>) -> impl Stream<Item = u32> + Unpin {
        stream::iter(items).chain(stream::pending())
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_connection_is_replaced_and_data_flows_again() {
        let connects = Rc::new(Cell::new(0));
        let seen = Rc::new(RefCell::new(Vec::new()));
        let task = run(
            "t",
            timings(),
            {
                let connects = connects.clone();
                move || {
                    let n = connects.get() + 1;
                    connects.set(n);
                    async move {
                        Some(then_silent(if n == 1 {
                            vec![1, 2]
                        } else {
                            vec![10, 11, 12]
                        }))
                    }
                }
            },
            {
                let seen = seen.clone();
                move |item| {
                    seen.borrow_mut().push(item);
                    if item == 12 {
                        Flow::Stop
                    } else {
                        Flow::Continue
                    }
                }
            },
        );
        task.await;
        assert_eq!(connects.get(), 2, "one reconnect after the silence");
        assert_eq!(*seen.borrow(), vec![1, 2, 10, 11, 12]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_idle_clock_runs_between_items_not_just_from_the_start() {
        let connects = Rc::new(Cell::new(0));
        let start = tokio::time::Instant::now();
        let stopped_at = Rc::new(Cell::new(None));
        let task = run(
            "t",
            timings(),
            {
                let connects = connects.clone();
                move || {
                    connects.set(connects.get() + 1);
                    // An item every 6 s keeps the connection alive for ever
                    // (idle limit is 10 s): it must never be recycled.
                    async {
                        Some(Box::pin(stream::unfold(0u32, |n| async move {
                            tokio::time::sleep(Duration::from_secs(6)).await;
                            Some((n, n + 1))
                        })))
                    }
                }
            },
            {
                let stopped_at = stopped_at.clone();
                move |n| {
                    if n >= 20 {
                        stopped_at.set(Some(start.elapsed()));
                        Flow::Stop
                    } else {
                        Flow::Continue
                    }
                }
            },
        );
        task.await;
        assert_eq!(connects.get(), 1, "a slow but live stream is left alone");
        assert!(stopped_at.get().unwrap() >= Duration::from_secs(120));
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_that_ends_is_reconnected() {
        let connects = Rc::new(Cell::new(0));
        let count = Rc::new(Cell::new(0));
        run(
            "t",
            timings(),
            {
                let connects = connects.clone();
                move || {
                    connects.set(connects.get() + 1);
                    async { Some(stream::iter(vec![1u32])) }
                }
            },
            {
                let count = count.clone();
                move |_| {
                    count.set(count.get() + 1);
                    if count.get() == 3 {
                        Flow::Stop
                    } else {
                        Flow::Continue
                    }
                }
            },
        )
        .await;
        assert_eq!(connects.get(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_connects_back_off_exponentially_up_to_the_cap() {
        let attempts = Rc::new(RefCell::new(Vec::<Duration>::new()));
        let start = tokio::time::Instant::now();
        run(
            "t",
            timings(),
            {
                let attempts = attempts.clone();
                move || {
                    attempts.borrow_mut().push(start.elapsed());
                    let n = attempts.borrow().len();
                    async move {
                        if n < 7 {
                            None
                        } else {
                            Some(stream::iter(vec![1u32]))
                        }
                    }
                }
            },
            |_| Flow::Stop,
        )
        .await;
        let gaps: Vec<u64> = attempts
            .borrow()
            .windows(2)
            .map(|w| (w[1] - w[0]).as_secs())
            .collect();
        assert_eq!(
            gaps,
            vec![1, 2, 4, 8, 8, 8],
            "doubles, then holds at the cap"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_backoff_resets_once_a_connection_has_delivered() {
        let attempts = Rc::new(RefCell::new(Vec::<Duration>::new()));
        let start = tokio::time::Instant::now();
        run(
            "t",
            timings(),
            {
                let attempts = attempts.clone();
                move || {
                    attempts.borrow_mut().push(start.elapsed());
                    let n = attempts.borrow().len();
                    async move {
                        match n {
                            1 | 2 => None,
                            3 => Some(stream::iter(vec![1u32])),
                            4 => Some(stream::iter(vec![1u32])),
                            _ => None,
                        }
                    }
                }
            },
            {
                let attempts = attempts.clone();
                move |_| {
                    if attempts.borrow().len() >= 4 {
                        Flow::Stop
                    } else {
                        Flow::Continue
                    }
                }
            },
        )
        .await;
        let gaps: Vec<u64> = attempts
            .borrow()
            .windows(2)
            .map(|w| (w[1] - w[0]).as_secs())
            .collect();
        // 1, 2 after the two failures; then the delivering connection resets
        // the delay to the initial 1 s rather than the 4 s it would be.
        assert_eq!(gaps, vec![1, 2, 1]);
    }

    /// A live feed that is merely quiet (the testnet's trades) must be
    /// recycled at a steady pace, not with ever-longer gaps.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_but_live_stream_is_recycled_without_growing_gaps() {
        let attempts = Rc::new(RefCell::new(Vec::<Duration>::new()));
        let start = tokio::time::Instant::now();
        let forever = run(
            "t",
            timings(),
            {
                let attempts = attempts.clone();
                move || {
                    attempts.borrow_mut().push(start.elapsed());
                    async { Some(stream::pending::<u32>()) }
                }
            },
            |_| Flow::Continue,
        );
        let _ = tokio::time::timeout(Duration::from_secs(100), forever).await;
        let gaps: Vec<u64> = attempts
            .borrow()
            .windows(2)
            .map(|w| (w[1] - w[0]).as_secs())
            .collect();
        assert!(gaps.len() >= 5, "{gaps:?}");
        assert!(
            gaps.iter().all(|g| *g == 11),
            "idle 10 s + a 1 s pause every time, never growing: {gaps:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_connect_that_hangs_is_abandoned_and_retried() {
        let connects = Rc::new(Cell::new(0));
        run(
            "t",
            timings(),
            {
                let connects = connects.clone();
                move || {
                    let n = connects.get() + 1;
                    connects.set(n);
                    async move {
                        if n == 1 {
                            std::future::pending::<()>().await;
                        }
                        Some(stream::iter(vec![1u32]))
                    }
                }
            },
            |_| Flow::Stop,
        )
        .await;
        assert_eq!(connects.get(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_consumer_that_has_gone_stops_it_without_reconnecting() {
        let connects = Rc::new(Cell::new(0));
        run(
            "t",
            timings(),
            {
                let connects = connects.clone();
                move || {
                    connects.set(connects.get() + 1);
                    async { Some(then_silent(vec![1])) }
                }
            },
            |_| Flow::Stop,
        )
        .await;
        assert_eq!(connects.get(), 1);
    }
}

#[cfg(test)]
mod isolated_tests {
    use super::*;
    use futures::stream;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn quick() -> Timings {
        Timings {
            idle: Duration::from_millis(60),
            connect: Duration::from_millis(500),
            backoff_initial: Duration::from_millis(5),
            backoff_max: Duration::from_millis(20),
        }
    }

    /// A task spawned during an attempt (as `barter-data` does) must not
    /// survive the attempt: that is what stops dead sockets piling up.
    #[test]
    fn tasks_spawned_by_an_attempt_end_with_it() {
        let alive = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let (a, n) = (alive.clone(), attempts.clone());
        let seen = Arc::new(AtomicUsize::new(0));
        let seen2 = seen.clone();
        run_isolated(
            "t",
            quick(),
            move || {
                let (a, n) = (a.clone(), n.clone());
                async move {
                    n.fetch_add(1, Ordering::SeqCst);
                    // Like barter's forwarder: holds a guard, never finishes.
                    struct Guard(Arc<AtomicUsize>);
                    impl Drop for Guard {
                        fn drop(&mut self) {
                            self.0.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                    a.fetch_add(1, Ordering::SeqCst);
                    let guard = Guard(a.clone());
                    tokio::spawn(async move {
                        let _guard = guard;
                        std::future::pending::<()>().await;
                    });
                    Some(stream::iter(vec![1u32]).chain(stream::pending()))
                }
            },
            move |_| {
                if seen2.fetch_add(1, Ordering::SeqCst) + 1 >= 3 {
                    Flow::Stop
                } else {
                    Flow::Continue
                }
            },
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "idle forced two reconnects"
        );
        assert_eq!(
            alive.load(Ordering::SeqCst),
            0,
            "no spawned task outlived its attempt"
        );
    }

    #[test]
    fn backoff_doubles_to_the_cap_and_resets_after_a_healthy_connection() {
        let mut b = Backoff::new(Timings {
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(8),
            ..Timings::default()
        });
        let secs: Vec<u64> = (0..6).map(|_| b.after(false).as_secs()).collect();
        assert_eq!(secs, vec![1, 2, 4, 8, 8, 8]);
        assert_eq!(b.after(true).as_secs(), 1);
        assert_eq!(b.after(false).as_secs(), 2);
    }
}
