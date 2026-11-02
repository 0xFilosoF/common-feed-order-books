use {
    crate::{
        config::{RedisConfig, SignalConfig},
        signal::{Signal, evaluate},
        store::{Store, StoreSnapshot},
    },
    anyhow::Context,
    redis::aio::MultiplexedConnection,
    serde::Serialize,
    std::time::Duration,
    tokio::time::{MissedTickBehavior, interval, timeout},
    tokio_util::sync::CancellationToken,
};

#[derive(Debug, Serialize)]
pub struct PublishedSnapshot {
    pub schema_version: u32,
    #[serde(flatten)]
    pub market: StoreSnapshot,
    pub signal: Signal,
}

impl PublishedSnapshot {
    pub fn new(market: StoreSnapshot, config: &SignalConfig) -> Self {
        let signal = evaluate(&market, config.threshold_bps);
        Self { schema_version: 1, market, signal }
    }
}

pub struct SnapshotSink {
    client: redis::Client,
    config: RedisConfig,
    connection: Option<MultiplexedConnection>,
}

impl SnapshotSink {
    pub fn new(config: RedisConfig) -> anyhow::Result<Self> {
        Ok(Self { client: redis::Client::open(config.url.as_str())?, config, connection: None })
    }

    pub async fn publish(&mut self, store: &Store, signal: &SignalConfig) -> anyhow::Result<()> {
        let result = timeout(Duration::from_millis(self.config.io_timeout_ms), async {
            if self.connection.is_none() {
                self.connection =
                    Some(self.client.get_multiplexed_async_connection().await.context("Redis connect failed")?);
            }
            // Capture AFTER connecting: a slow connection must not age the snapshot before serialization.
            let snapshot = PublishedSnapshot::new(store.snapshot().await, signal);
            let json = serde_json::to_vec(&snapshot).context("snapshot serialization failed")?;
            let connection = self.connection.as_mut().context("Redis connection missing")?;
            redis::cmd("SET")
                .arg(&self.config.key)
                .arg(json)
                .arg("PX")
                .arg(self.config.ttl_ms)
                .query_async::<()>(connection)
                .await
                .context("Redis snapshot SET failed")
        })
        .await
        .context("Redis I/O timeout")
        .and_then(|result| result);
        if result.is_err() {
            self.connection = None;
        }
        result
    }
}

pub async fn run(
    store: Store,
    redis: RedisConfig,
    signal: SignalConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut ticker = interval(Duration::from_millis(redis.snapshot_interval_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut sink = SnapshotSink::new(redis)?;
    let mut healthy = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticker.tick() => {}
        }
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            result = sink.publish(&store, &signal) => result,
        };
        match result {
            Ok(()) => {
                if healthy != Some(true) {
                    tracing::info!("Redis snapshot sink connected");
                }
                healthy = Some(true);
            }
            Err(error) => {
                if healthy != Some(false) {
                    tracing::warn!(reason = %format!("{error:#}"), "Redis snapshot sink unavailable; retrying, feeds continue");
                } else {
                    tracing::debug!(reason = %format!("{error:#}"), "Redis snapshot retry failed");
                }
                healthy = Some(false);
            }
        }
    }
}
