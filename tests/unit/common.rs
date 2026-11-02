use {
    crate::{
        config::{AppConfig, Settings},
        model::{Exchange, STREAM, parse_snapshot},
        store::{FeedEvent, Store, StoreSnapshot, unix_ms},
    },
    serde_json::json,
    std::{sync::Arc, time::Duration},
    tokio::time::{Instant, timeout},
};

pub(crate) fn config() -> AppConfig {
    let settings: Settings = ::config::Config::builder()
        .add_source(::config::File::from_str(include_str!("../../config.yaml"), ::config::FileFormat::Yaml))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();
    settings.app
}

pub(crate) fn frame(id: u64, bid: &str, ask: &str) -> String {
    json!({"stream": STREAM, "data": {"lastUpdateId": id, "bids": [[bid, "2"]], "asks": [[ask, "3"]]}}).to_string()
}

pub(crate) async fn put(store: &Store, exchange: Exchange, id: u64, bid: &str, ask: &str) {
    let book = Arc::new(parse_snapshot(frame(id, bid, ask).as_bytes(), unix_ms()).unwrap());
    store.apply(exchange, FeedEvent::Snapshot { book, received: Instant::now() }).await;
}

pub(crate) async fn wait_until(store: &Store, predicate: impl Fn(&StoreSnapshot) -> bool) -> StoreSnapshot {
    timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = store.snapshot().await;
            if predicate(&snapshot) {
                return snapshot;
            }
            store.wait_for_change(snapshot.revision, Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("feed condition timed out")
}
