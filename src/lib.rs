pub mod config;
pub mod feed;
pub mod model;
pub mod observer;
pub mod signal;
pub mod snapshot;
pub mod store;

use {
    anyhow::Context, config::AppConfig, model::Exchange, std::time::Duration, store::Store, tokio::task::JoinSet,
    tokio_util::sync::CancellationToken,
};

pub async fn run(config: AppConfig) -> anyhow::Result<()> {
    config.validate()?;
    let store = Store::default();
    let shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();
    for exchange in Exchange::ALL {
        let (sender, receiver) = kanal::bounded_async(config.feed.channel_capacity);
        tasks.spawn(store::run_writer(exchange, receiver, store.clone(), shutdown.clone()));
        tasks.spawn(feed::run(exchange, config.feed.clone(), sender, shutdown.clone()));
    }
    tasks.spawn(signal::run(store.clone(), config.signal.clone(), shutdown.clone()));
    tasks.spawn(snapshot::run(store.clone(), config.redis.clone(), config.signal.clone(), shutdown.clone()));
    tracing::info!(symbol = model::SYMBOL, threads = config.io.threads, "order book service started");
    let outcome = tokio::select! {
        received = wait_for_shutdown() => received,
        result = tasks.join_next() => match result {
            Some(Ok(Err(error))) => Err(error.context("service task failed")),
            Some(Err(error)) => Err(anyhow::anyhow!(error).context("service task panicked")),
            _ => Err(anyhow::anyhow!("service task stopped unexpectedly")),
        }
    };
    shutdown.cancel();
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::error!(%error, "task failed during shutdown"),
                Err(error) => tracing::error!(%error, "task panicked during shutdown"),
            }
        }
    };
    if tokio::time::timeout(Duration::from_secs(5), drain).await.is_err() {
        tasks.abort_all();
        tracing::warn!("shutdown deadline exceeded; remaining tasks aborted");
    }
    // Writers have stopped, so the final persisted signal cannot retain an actionable bias.
    for exchange in Exchange::ALL {
        store
            .apply(
                exchange,
                store::FeedEvent::Status { status: model::FeedStatus::Stopped, reason: "service shutdown".into() },
            )
            .await;
    }
    let mut sink = snapshot::SnapshotSink::new(config.redis)?;
    if let Err(error) = sink.publish(&store, &config.signal).await {
        tracing::warn!(%error, "could not publish final stopped snapshot; previous snapshot will expire");
    }
    tracing::info!("order book service stopped");
    outcome
}

pub async fn wait_for_shutdown() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .context("cannot install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("cannot install SIGINT handler")?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.context("cannot install shutdown handler")?;
    tracing::info!("shutdown signal received");
    Ok(())
}

#[cfg(test)]
mod tests;
