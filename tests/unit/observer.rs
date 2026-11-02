use {
    super::decode,
    crate::{
        config::SignalConfig,
        model::{Exchange, parse_snapshot},
        signal::Decision,
        snapshot::PublishedSnapshot,
        store::{FeedEvent, Store},
    },
    rust_decimal::Decimal,
    std::sync::Arc,
    tokio::time::Instant,
};

async fn snapshot() -> Vec<u8> {
    let store = Store::default();
    for exchange in Exchange::ALL {
        let frame = if exchange == Exchange::Binance {
            br#"{"stream":"ethusdt@depth20@100ms","data":{"lastUpdateId":1,"bids":[["1999","1"]],"asks":[["2001","2"]]}}"#.as_slice()
        } else {
            br#"{"stream":"ethusdt@depth20@100ms","data":{"u":2,"bids":[["2001","3"]],"asks":[["2003","4"]]}}"#
                .as_slice()
        };
        let book = Arc::new(parse_snapshot(frame, 1000).unwrap());
        store.apply(exchange, FeedEvent::Snapshot { book, received: Instant::now() }).await;
    }
    let mut market = store.snapshot().await;
    market.captured_at_ms = 1000;
    for feed in &mut market.feeds {
        feed.age_ms = Some(40);
    }
    let signal = SignalConfig { threshold_bps: Decimal::from(2), report_interval_ms: 1000 };
    serde_json::to_vec(&PublishedSnapshot::new(market, &signal)).unwrap()
}

#[tokio::test]
async fn observer_reads_publisher_schema_and_preserves_capture_semantics() {
    let observation = decode(&snapshot().await, 1080).unwrap();
    assert_eq!(observation.publication_age_ms, Some(80));
    assert_eq!(observation.feeds[0].estimated_age_at_read_ms, Some(120));
    assert_eq!(observation.feeds[1].update_id, Some(2));
    assert_eq!(observation.feeds[1].best_ask.as_ref().unwrap().price, Decimal::from(2003));
    // The historical decision is retained under an explicit capture-only field.
    assert_eq!(observation.signal_at_capture.decision, Decision::LongBias);
}

#[tokio::test]
async fn observer_reports_unknown_age_when_capture_clock_is_ahead() {
    let observation = decode(&snapshot().await, 999).unwrap();
    assert_eq!(observation.publication_age_ms, None);
    assert_eq!(observation.feeds[0].estimated_age_at_read_ms, None);
}

#[tokio::test]
async fn observer_rejects_invalid_json_and_unknown_schema() {
    assert!(decode(b"broken", 1000).is_err());
    let mut payload: serde_json::Value = serde_json::from_slice(&snapshot().await).unwrap();
    payload["schema_version"] = 2.into();
    assert!(decode(&serde_json::to_vec(&payload).unwrap(), 1000).is_err());
}
