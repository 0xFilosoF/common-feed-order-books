use {
    crate::model::{Exchange, FeedStatus, OrderBook, STALE_AFTER, SYMBOL},
    kanal::AsyncReceiver,
    serde::{Deserialize, Serialize},
    std::{
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        sync::{Notify, RwLock},
        time::Instant,
    },
    tokio_util::sync::CancellationToken,
};

pub enum FeedEvent {
    Status { status: FeedStatus, reason: String },
    Snapshot { book: Arc<OrderBook>, received: Instant },
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct FeedStats {
    pub snapshots: u64,
    pub invalid_frames: u64,
    pub reconnects: u64,
    pub delayed_intervals: u64,
    pub last_interval_ms: Option<u64>,
}

struct FeedRecord {
    status: FeedStatus,
    reason: String,
    status_since_ms: u64,
    last_error: Option<String>,
    book: Option<Arc<OrderBook>>,
    received: Option<Instant>,
    stats: FeedStats,
}

impl Default for FeedRecord {
    fn default() -> Self {
        Self {
            status: FeedStatus::Connecting,
            reason: "waiting for connection".into(),
            status_since_ms: unix_ms(),
            last_error: None,
            book: None,
            received: None,
            stats: FeedStats::default(),
        }
    }
}

#[derive(Default)]
struct State {
    revision: u64,
    feeds: [FeedRecord; 2],
}

#[derive(Clone, Default)]
pub struct Store {
    state: Arc<RwLock<State>>,
    changed: Arc<Notify>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeedView {
    pub exchange: Exchange,
    pub status: FeedStatus,
    pub reason: String,
    pub status_since_ms: u64,
    pub last_error: Option<String>,
    pub age_ms: Option<u64>,
    pub book: Option<Arc<OrderBook>>,
    pub stats: FeedStats,
    #[serde(skip)]
    pub age: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreSnapshot {
    pub symbol: &'static str,
    pub revision: u64,
    pub captured_at_ms: u64,
    pub feeds: [FeedView; 2],
}

impl Store {
    pub async fn apply(&self, exchange: Exchange, event: FeedEvent) {
        let mut state = self.state.write().await;
        let record = &mut state.feeds[exchange.index()];
        let previous_status = record.status;
        match event {
            FeedEvent::Status { status, reason } => {
                match status {
                    FeedStatus::InvalidFrame => record.stats.invalid_frames += 1,
                    FeedStatus::Reconnecting => record.stats.reconnects += 1,
                    // A new session must never inherit the old session's freshness.
                    FeedStatus::Connecting => record.received = None,
                    _ => {}
                }
                if matches!(status, FeedStatus::InvalidFrame | FeedStatus::Disconnected) {
                    record.last_error = Some(reason.clone());
                }
                record.status = status;
                record.reason = reason;
            }
            FeedEvent::Snapshot { book, received } => {
                if let Some(previous) = record.received {
                    let gap = received.saturating_duration_since(previous);
                    record.stats.last_interval_ms = Some(millis(gap));
                    if gap > STALE_AFTER {
                        record.stats.delayed_intervals += 1;
                    }
                }
                record.book = Some(book);
                record.received = Some(received);
                record.stats.snapshots += 1;
                record.status = FeedStatus::Live;
                record.reason = "validated partial snapshot".into();
            }
        }
        let transition = previous_status != record.status;
        if transition {
            record.status_since_ms = unix_ms();
        }
        let log = transition.then(|| (record.status, record.reason.clone()));
        state.revision += 1;
        drop(state);
        self.changed.notify_waiters();
        if let Some((status, reason)) = log {
            tracing::info!(?exchange, ?status, %reason, "feed state changed");
        }
    }

    pub async fn snapshot(&self) -> StoreSnapshot {
        let state = self.state.read().await;
        let now = Instant::now();
        let feeds = Exchange::ALL.map(|exchange| {
            let record = &state.feeds[exchange.index()];
            let age = record.received.map(|received| now.saturating_duration_since(received));
            let delayed = record.status == FeedStatus::Live && age.is_some_and(|age| age > STALE_AFTER);
            FeedView {
                exchange,
                status: if delayed { FeedStatus::Delayed } else { record.status },
                reason: if delayed {
                    format!("no valid snapshot for {}ms; deadline is 100ms + 10ms", millis(age.unwrap_or_default()))
                } else {
                    record.reason.clone()
                },
                status_since_ms: if delayed {
                    record.book.as_ref().map_or(record.status_since_ms, |book| book.received_at_ms.saturating_add(110))
                } else {
                    record.status_since_ms
                },
                last_error: record.last_error.clone(),
                age_ms: age.map(millis),
                age,
                book: record.book.clone(),
                stats: record.stats.clone(),
            }
        });
        StoreSnapshot { symbol: SYMBOL, revision: state.revision, captured_at_ms: unix_ms(), feeds }
    }

    /// Wait for a new revision, or for the reader's freshness/reporting deadline.
    pub async fn wait_for_change(&self, after: u64, wait: Duration) {
        let notified = self.changed.notified();
        tokio::pin!(notified);
        // Register before checking the revision to avoid a lost wakeup.
        notified.as_mut().enable();
        if self.state.read().await.revision != after {
            return;
        }
        let _ = tokio::time::timeout(wait, notified).await;
    }
}

pub async fn run_writer(
    exchange: Exchange,
    receiver: AsyncReceiver<FeedEvent>,
    store: Store,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    // Kanal ReceiveFuture must run to completion; selecting it against cancellation
    // can consume and lose a message. Shutdown drops the producer, closing this channel.
    while let Ok(event) = receiver.recv().await {
        if shutdown.is_cancelled() {
            break;
        }
        store.apply(exchange, event).await;
    }
    anyhow::ensure!(shutdown.is_cancelled(), "{exchange:?} storage channel closed unexpectedly");
    store.apply(exchange, FeedEvent::Status { status: FeedStatus::Stopped, reason: "service shutdown".into() }).await;
    Ok(())
}

pub fn unix_ms() -> u64 {
    millis(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default())
}

pub fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
