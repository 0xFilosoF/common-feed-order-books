use {
    crate::{
        config::FeedConfig,
        model::{Exchange, FeedStatus, parse_snapshot},
        store::{FeedEvent, unix_ms},
    },
    anyhow::{Context, bail, ensure},
    futures_util::{SinkExt, StreamExt},
    kanal::AsyncSender,
    std::{
        sync::Arc,
        time::{Duration, SystemTime},
    },
    tokio::time::{Instant, sleep, sleep_until, timeout},
    tokio_tungstenite::{
        connect_async_with_config,
        tungstenite::{
            Error as WebSocketError, Message,
            http::{StatusCode, header::RETRY_AFTER},
            protocol::WebSocketConfig,
        },
    },
    tokio_util::sync::CancellationToken,
};

pub async fn run(
    exchange: Exchange,
    config: FeedConfig,
    sender: AsyncSender<FeedEvent>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    run_at(exchange, exchange.url(), config, sender, shutdown).await
}

pub(crate) async fn run_at(
    exchange: Exchange,
    url: &str,
    config: FeedConfig,
    sender: AsyncSender<FeedEvent>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut failures = 0_u32;
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        status(&sender, &config, &shutdown, FeedStatus::Connecting, "opening public depth stream".into()).await?;
        tracing::info!(?exchange, url, "connecting feed");
        let started = Instant::now();
        let result = session(url, &config, &sender, &shutdown).await;
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let error = result.err().unwrap_or_else(|| anyhow::anyhow!("WebSocket session ended"));
        let reason = format!("{error:#}");
        tracing::warn!(?exchange, %reason, "feed disconnected");
        status(&sender, &config, &shutdown, FeedStatus::Disconnected, reason.clone()).await?;
        // Short-lived successful handshakes must not cause a rapid reconnect loop.
        if started.elapsed() >= Duration::from_secs(30) {
            failures = 0;
        }
        // Server cooldowns are a minimum, independent of the local backoff cap/jitter.
        let delay = backoff(&config, failures).max(server_retry_delay(&error, SystemTime::now()).unwrap_or_default());
        failures = failures.saturating_add(1);
        let reason = format!("attempt {failures}, retry in {}ms: {reason}", delay.as_millis());
        status(&sender, &config, &shutdown, FeedStatus::Reconnecting, reason).await?;
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = sleep(delay) => {}
        }
    }
}

pub(crate) fn backoff(config: &FeedConfig, failures: u32) -> Duration {
    let ceiling = config.reconnect_initial_ms.saturating_mul(1_u64 << failures.min(31)).min(config.reconnect_max_ms);
    Duration::from_millis(fastrand::u64((ceiling / 2).max(1)..=ceiling))
}

fn server_retry_delay(error: &anyhow::Error, now: SystemTime) -> Option<Duration> {
    let WebSocketError::Http(response) = error.downcast_ref::<WebSocketError>()? else {
        return None;
    };
    if let Some(delay) = response.headers().get(RETRY_AFTER).and_then(|value| value.to_str().ok()).and_then(|value| {
        let value = value.trim();
        if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
            // Saturate oversized delta-seconds instead of falling back to an early retry.
            let seconds = value
                .bytes()
                .fold(0_u64, |seconds, byte| seconds.saturating_mul(10).saturating_add(u64::from(byte - b'0')));
            Some(Duration::from_secs(seconds))
        } else {
            httpdate::parse_http_date(value).ok().map(|date| date.duration_since(now).unwrap_or_default())
        }
    }) {
        return Some(delay);
    }
    match response.status() {
        // A missing/malformed header must not turn rate limits or IP bans into fast retries.
        StatusCode::TOO_MANY_REQUESTS => Some(Duration::from_secs(60)),
        StatusCode::IM_A_TEAPOT => Some(Duration::from_secs(120)),
        _ => None,
    }
}

async fn session(
    url: &str,
    config: &FeedConfig,
    sender: &AsyncSender<FeedEvent>,
    shutdown: &CancellationToken,
) -> anyhow::Result<()> {
    let ws_config = WebSocketConfig::default().max_message_size(Some(64 * 1024)).max_frame_size(Some(64 * 1024));
    let (mut socket, _) = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Ok(()),
        connected = timeout(Duration::from_millis(config.connect_timeout_ms), connect_async_with_config(url, Some(ws_config), true)) => {
            connected.context("WebSocket connect timeout")?.context("WebSocket handshake failed")?
        }
    };
    status(
        sender,
        config,
        shutdown,
        FeedStatus::Synchronizing,
        "connected; waiting for first full partial snapshot".into(),
    )
    .await?;
    let idle_timeout = Duration::from_millis(config.idle_timeout_ms);
    let mut last_valid = Instant::now();
    let mut last_id = None;
    let mut bad_frames = 0_u32;
    loop {
        let message = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = sleep_until(last_valid + idle_timeout) => bail!("depth watchdog timeout: no valid snapshot for {}ms", config.idle_timeout_ms),
            message = socket.next() => message.context("WebSocket EOF without close frame")?.context("WebSocket receive failed")?,
        };
        let received = Instant::now();
        let received_at_ms = unix_ms();
        let bytes = match &message {
            Message::Text(text) => text.as_bytes(),
            Message::Binary(bytes) => bytes.as_ref(),
            Message::Ping(_) => {
                // Tungstenite has already queued a pong with the exact ping payload.
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Ok(()),
                    flushed = timeout(Duration::from_millis(config.channel_timeout_ms), socket.flush()) => {
                        flushed.context("WebSocket pong timeout")?.context("WebSocket pong failed")?;
                    }
                }
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(frame) => bail!("WebSocket closed by peer: {frame:?}"),
            Message::Frame(_) => continue,
        };
        let parsed = parse_snapshot(bytes, received_at_ms).and_then(|book| {
            ensure!(
                last_id.is_none_or(|id| book.update_id >= id),
                "snapshot update ID regressed: {:?} -> {}",
                last_id,
                book.update_id
            );
            Ok(book)
        });
        match parsed {
            Ok(book) => {
                last_id = Some(book.update_id);
                bad_frames = 0;
                last_valid = received;
                emit(sender, config, shutdown, FeedEvent::Snapshot { book: Arc::new(book), received }).await?;
            }
            Err(error) => {
                bad_frames += 1;
                let reason = format!("bad depth frame {bad_frames}/{}: {error:#}", config.max_bad_frames);
                status(sender, config, shutdown, FeedStatus::InvalidFrame, reason.clone()).await?;
                if bad_frames >= config.max_bad_frames {
                    bail!("resync required: {reason}");
                }
            }
        }
    }
}

async fn status(
    sender: &AsyncSender<FeedEvent>,
    config: &FeedConfig,
    shutdown: &CancellationToken,
    status: FeedStatus,
    reason: String,
) -> anyhow::Result<()> {
    emit(sender, config, shutdown, FeedEvent::Status { status, reason }).await
}

async fn emit(
    sender: &AsyncSender<FeedEvent>,
    config: &FeedConfig,
    shutdown: &CancellationToken,
    event: FeedEvent,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_millis(config.channel_timeout_ms);
    let mut event = Some(event);
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        if sender.try_send_option(&mut event).context("storage channel closed")? {
            return Ok(());
        }
        ensure!(Instant::now() < deadline, "storage channel backpressure timeout");
        // No cancelable Kanal futures: preserve ownership until the event is enqueued.
        // The normal hot path succeeds on the first try and never sleeps.
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = sleep_until((Instant::now() + Duration::from_millis(1)).min(deadline)) => {}
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/feed.rs"]
mod tests;
