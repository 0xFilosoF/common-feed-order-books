use {
    crate::{
        config::RedisConfig,
        model::{Exchange, FeedStatus, Level, SYMBOL},
        signal::Signal,
        store::{FeedStats, FeedView, unix_ms},
    },
    anyhow::{Context, ensure},
    redis::aio::MultiplexedConnection,
    serde::{Deserialize, Serialize},
    std::time::Duration,
    tokio::time::timeout,
};

#[derive(Deserialize)]
struct StoredSnapshot {
    schema_version: u32,
    symbol: String,
    revision: u64,
    captured_at_ms: u64,
    feeds: [FeedView; 2],
    signal: Signal,
}

#[derive(Debug, Serialize)]
pub struct Observation {
    pub observed_at_ms: u64,
    pub captured_at_ms: u64,
    pub publication_age_ms: Option<u64>,
    pub symbol: String,
    pub revision: u64,
    pub feeds: [ObservedFeed; 2],
    // This decision belongs to captured_at_ms, never to the current poll time.
    pub signal_at_capture: Signal,
}

#[derive(Debug, Serialize)]
pub struct ObservedFeed {
    pub exchange: Exchange,
    pub status_at_capture: FeedStatus,
    pub reason_at_capture: String,
    pub age_at_capture_ms: Option<u64>,
    pub estimated_age_at_read_ms: Option<u64>,
    pub update_id: Option<u64>,
    pub best_bid: Option<Level>,
    pub best_ask: Option<Level>,
    pub bid_levels: usize,
    pub ask_levels: usize,
    pub stats: FeedStats,
}

pub struct SnapshotReader {
    client: redis::Client,
    config: RedisConfig,
    connection: Option<MultiplexedConnection>,
}

impl SnapshotReader {
    pub fn new(config: RedisConfig) -> anyhow::Result<Self> {
        Ok(Self { client: redis::Client::open(config.url.as_str())?, config, connection: None })
    }

    pub async fn poll(&mut self) -> anyhow::Result<Option<Observation>> {
        let result = timeout(Duration::from_millis(self.config.io_timeout_ms), async {
            if self.connection.is_none() {
                self.connection = Some(
                    self.client.get_multiplexed_async_connection().await.context("Redis observer connect failed")?,
                );
            }
            let connection = self.connection.as_mut().context("Redis observer connection missing")?;
            let payload: Option<Vec<u8>> = redis::cmd("GET")
                .arg(&self.config.key)
                .query_async(connection)
                .await
                .context("Redis observer GET failed")?;
            payload.map(|payload| decode(&payload, unix_ms())).transpose()
        })
        .await
        .context("Redis observer I/O timeout")
        .and_then(|result| result);
        if result.is_err() {
            self.connection = None;
        }
        result
    }
}

fn decode(payload: &[u8], observed_at_ms: u64) -> anyhow::Result<Observation> {
    let snapshot: StoredSnapshot = serde_json::from_slice(payload).context("invalid Redis snapshot JSON")?;
    ensure!(snapshot.schema_version == 1, "unsupported snapshot schema {}", snapshot.schema_version);
    ensure!(snapshot.symbol == SYMBOL, "unexpected snapshot symbol {}", snapshot.symbol);
    ensure!(snapshot.feeds.each_ref().map(|feed| feed.exchange) == Exchange::ALL, "unexpected snapshot feed order");
    // Wall-clock skew can put capture in the future; report unknown age in that case.
    let publication_age_ms = observed_at_ms.checked_sub(snapshot.captured_at_ms);
    let feeds = snapshot.feeds.map(|feed| ObservedFeed {
        exchange: feed.exchange,
        status_at_capture: feed.status,
        reason_at_capture: feed.reason,
        age_at_capture_ms: feed.age_ms,
        estimated_age_at_read_ms: feed.age_ms.zip(publication_age_ms).map(|(age, elapsed)| age.saturating_add(elapsed)),
        update_id: feed.book.as_ref().map(|book| book.update_id),
        best_bid: feed.book.as_ref().and_then(|book| book.bids.first().cloned()),
        best_ask: feed.book.as_ref().and_then(|book| book.asks.first().cloned()),
        bid_levels: feed.book.as_ref().map_or(0, |book| book.bids.len()),
        ask_levels: feed.book.as_ref().map_or(0, |book| book.asks.len()),
        stats: feed.stats,
    });
    Ok(Observation {
        observed_at_ms,
        captured_at_ms: snapshot.captured_at_ms,
        publication_age_ms,
        symbol: snapshot.symbol,
        revision: snapshot.revision,
        feeds,
        signal_at_capture: snapshot.signal,
    })
}

#[cfg(test)]
#[path = "../tests/unit/observer.rs"]
mod tests;
