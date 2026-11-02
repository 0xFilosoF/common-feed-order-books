use {
    crate::{
        config::{AppConfig, Settings},
        feed,
        model::{Exchange, FeedStatus, STREAM, parse_snapshot},
        signal::{Decision, evaluate},
        store::{FeedEvent, Store, StoreSnapshot, run_writer, unix_ms},
    },
    futures_util::{SinkExt, StreamExt},
    rust_decimal::Decimal,
    serde_json::json,
    std::{str::FromStr, sync::Arc, time::Duration},
    tokio::{
        net::TcpListener,
        time::{Instant, sleep, timeout},
    },
    tokio_tungstenite::{accept_async, tungstenite::Message},
    tokio_util::sync::CancellationToken,
};

fn config() -> AppConfig {
    let settings: Settings = ::config::Config::builder()
        .add_source(::config::File::from_str(include_str!("../config.yaml"), ::config::FileFormat::Yaml))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();
    settings.app
}

fn frame(id: u64, bid: &str, ask: &str) -> String {
    json!({"stream": STREAM, "data": {"lastUpdateId": id, "bids": [[bid, "2"]], "asks": [[ask, "3"]]}}).to_string()
}

async fn put(store: &Store, exchange: Exchange, id: u64, bid: &str, ask: &str) {
    let book = Arc::new(parse_snapshot(frame(id, bid, ask).as_bytes(), unix_ms()).unwrap());
    store.apply(exchange, FeedEvent::Snapshot { book, received: Instant::now() }).await;
}

#[test]
fn parses_both_exchange_formats_and_sorts_levels() {
    let binance = parse_snapshot(frame(10, "2000.01", "2000.02").as_bytes(), 123).unwrap();
    assert_eq!(binance.bids[0].price, Decimal::from_str("2000.01").unwrap());
    assert_eq!(binance.received_at_ms, 123);
    let aster = json!({"stream": STREAM, "data": {
        "e": "depthUpdate", "E": 456, "T": 455, "s": "ETHUSDT", "U": 10, "u": 20, "pu": 9,
        "bids": [["1999", "1"], ["2000", "2"]], "asks": [["2002", "1"], ["2001", "2"]]
    }});
    let aster = parse_snapshot(aster.to_string().as_bytes(), 789).unwrap();
    assert_eq!(aster.update_id, 20);
    assert_eq!(aster.event_time_ms, Some(456));
    assert_eq!(aster.bids[0].price, Decimal::from(2000));
    assert_eq!(aster.asks[0].price, Decimal::from(2001));
}

#[test]
fn rejects_malformed_crossed_empty_duplicate_and_wrong_symbol_books() {
    for bad in ["oops".to_string(), frame(1, "2002", "2001"), frame(1, "NaN", "2001"), frame(1, "0", "2001")] {
        assert!(parse_snapshot(bad.as_bytes(), 0).is_err(), "accepted: {bad}");
    }
    let base: serde_json::Value = serde_json::from_str(&frame(1, "2000", "2001")).unwrap();
    for (field, value) in [
        ("bids", json!([])),
        ("bids", json!([["2000", "1"], ["2000", "2"]])),
        ("asks", json!([["2001", "0"]])),
        ("s", json!("BTCUSDT")),
        ("bids", json!([["2000", "-1"]])),
        ("bids", json!([["9999999999", "1"]])),
    ] {
        let mut bad = base.clone();
        bad["data"][field] = value;
        assert!(parse_snapshot(bad.to_string().as_bytes(), 0).is_err());
    }
    let mut bad = base;
    bad["stream"] = json!("ethusdt@depth@100ms");
    assert!(parse_snapshot(bad.to_string().as_bytes(), 0).is_err());
}

#[tokio::test(start_paused = true)]
async fn freshness_boundary_uses_receive_time_and_blocks_signal_without_new_events() {
    let store = Store::default();
    put(&store, Exchange::Binance, 1, "1999", "2001").await;
    put(&store, Exchange::Aster, 1, "2001", "2003").await;
    tokio::time::advance(Duration::from_millis(110)).await;
    assert_eq!(store.snapshot().await.feeds[0].status, FeedStatus::Live);
    tokio::time::advance(Duration::from_nanos(1)).await;
    let snapshot = store.snapshot().await;
    assert_eq!(snapshot.feeds[0].status, FeedStatus::Delayed);
    let signal = evaluate(&snapshot, Decimal::from(2));
    assert_eq!(signal.decision, Decision::Hold);
    assert!(signal.reason.contains("Delayed"));
    assert_eq!(signal.binance_mid, Some(Decimal::from(2000)));
}

#[tokio::test]
async fn signal_directions_threshold_and_independent_feed_failures() {
    let store = Store::default();
    assert_eq!(evaluate(&store.snapshot().await, Decimal::from(2)).decision, Decision::Hold);
    put(&store, Exchange::Binance, 1, "1999", "2001").await;
    for (bid, ask, expected) in [
        ("2001", "2003", Decision::LongBias),
        ("1997", "1999", Decision::ShortBias),
        ("1999.4", "2001.4", Decision::Hold), // exactly +2bps
        ("1998.6", "2000.6", Decision::Hold), // exactly -2bps
    ] {
        put(&store, Exchange::Aster, 1, bid, ask).await;
        assert_eq!(evaluate(&store.snapshot().await, Decimal::from(2)).decision, expected);
    }
    for status in [
        FeedStatus::Disconnected,
        FeedStatus::Reconnecting,
        FeedStatus::InvalidFrame,
        FeedStatus::Synchronizing,
        FeedStatus::Stopped,
    ] {
        store.apply(Exchange::Aster, FeedEvent::Status { status, reason: "test failure".into() }).await;
        put(&store, Exchange::Binance, 2, "1999", "2001").await;
        let snapshot = store.snapshot().await;
        assert_eq!(snapshot.feeds[0].status, FeedStatus::Live);
        assert_eq!(evaluate(&snapshot, Decimal::from(2)).decision, Decision::Hold);
    }
}

#[tokio::test]
async fn reader_keeps_immutable_snapshot_and_wait_does_not_lose_updates() {
    let store = Store::default();
    put(&store, Exchange::Binance, 1, "1999", "2001").await;
    let first = store.snapshot().await;
    put(&store, Exchange::Binance, 2, "2001", "2003").await;
    let started = Instant::now();
    store.wait_for_change(first.revision, Duration::from_secs(10)).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(first.feeds[0].book.as_ref().unwrap().update_id, 1);
    let second = store.snapshot().await;
    assert_eq!(second.feeds[0].book.as_ref().unwrap().update_id, 2);
    assert!(!Arc::ptr_eq(first.feeds[0].book.as_ref().unwrap(), second.feeds[0].book.as_ref().unwrap()));
    let waiter = tokio::spawn({
        let store = store.clone();
        async move { store.wait_for_change(second.revision, Duration::from_secs(10)).await }
    });
    tokio::task::yield_now().await;
    put(&store, Exchange::Aster, 1, "1999", "2001").await;
    timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn long_poll_times_out_on_silence_and_reconnect_invalidates_old_freshness() {
    let store = Store::default();
    put(&store, Exchange::Aster, 5, "1999", "2001").await;
    let revision = store.snapshot().await.revision;
    let started = Instant::now();
    store.wait_for_change(revision, Duration::from_millis(111)).await;
    assert_eq!(started.elapsed(), Duration::from_millis(111));
    assert_eq!(store.snapshot().await.feeds[1].status, FeedStatus::Delayed);
    store
        .apply(Exchange::Aster, FeedEvent::Status { status: FeedStatus::Connecting, reason: "reconnect".into() })
        .await;
    let snapshot = store.snapshot().await;
    assert_eq!(snapshot.feeds[1].status, FeedStatus::Connecting);
    assert_eq!(snapshot.feeds[1].age_ms, None);
    put(&store, Exchange::Aster, 1, "1999", "2001").await;
    assert_eq!(store.snapshot().await.feeds[1].status, FeedStatus::Live);
}

async fn wait_until(store: &Store, predicate: impl Fn(&StoreSnapshot) -> bool) -> StoreSnapshot {
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

#[tokio::test]
async fn websocket_recovers_from_bad_frame_close_and_new_session_sequence_reset() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/stream", listener.local_addr().unwrap());
    let (control, commands) = kanal::bounded_async::<u8>(8);
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(tcp).await.unwrap();
        ws.send(Message::Text(frame(50, "1999", "2001").into())).await.unwrap();
        commands.recv().await.unwrap();
        ws.send(Message::Text("broken JSON".into())).await.unwrap();
        commands.recv().await.unwrap();
        ws.send(Message::Text(frame(51, "2000", "2002").into())).await.unwrap();
        commands.recv().await.unwrap();
        ws.send(Message::Text(frame(49, "2000", "2002").into())).await.unwrap();
        commands.recv().await.unwrap();
        ws.close(None).await.unwrap();
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(tcp).await.unwrap();
        ws.send(Message::Text(frame(1, "2001", "2003").into())).await.unwrap();
        commands.recv().await.unwrap();
    });
    let store = Store::default();
    let shutdown = CancellationToken::new();
    let (sender, receiver) = kanal::bounded_async(32);
    let writer = tokio::spawn(run_writer(Exchange::Aster, receiver, store.clone(), shutdown.clone()));
    let mut cfg = config().feed;
    cfg.reconnect_initial_ms = 20;
    let producer = tokio::spawn({
        let stop = shutdown.clone();
        async move { feed::run_at(Exchange::Aster, &url, cfg, sender, stop).await }
    });
    wait_until(&store, |s| s.feeds[1].stats.snapshots == 1).await;
    control.send(1).await.unwrap();
    let bad = wait_until(&store, |s| s.feeds[1].status == FeedStatus::InvalidFrame).await;
    assert_eq!(bad.feeds[1].book.as_ref().unwrap().update_id, 50);
    control.send(2).await.unwrap();
    wait_until(&store, |s| s.feeds[1].stats.snapshots == 2).await;
    control.send(3).await.unwrap();
    let regressed = wait_until(&store, |s| s.feeds[1].stats.invalid_frames == 2).await;
    assert!(regressed.feeds[1].reason.contains("regressed"));
    control.send(4).await.unwrap();
    let recovered = wait_until(&store, |s| s.feeds[1].stats.snapshots == 3).await;
    assert_eq!(recovered.feeds[1].book.as_ref().unwrap().update_id, 1);
    assert_eq!(recovered.feeds[1].stats.reconnects, 1);
    assert!(recovered.feeds[1].last_error.as_ref().unwrap().contains("closed by peer"));
    shutdown.cancel();
    producer.await.unwrap().unwrap();
    writer.await.unwrap().unwrap();
    control.send(5).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn ping_pong_does_not_mask_depth_watchdog() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/stream", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(tcp).await.unwrap();
        ws.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
        let response = timeout(Duration::from_secs(1), ws.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(response, Message::Pong(vec![1, 2, 3].into()));
        // More pings prove the transport is alive while the depth stream is dead.
        loop {
            sleep(Duration::from_millis(20)).await;
            if ws.send(Message::Ping(vec![4].into())).await.is_err() {
                break;
            }
        }
    });
    let store = Store::default();
    let shutdown = CancellationToken::new();
    let (sender, receiver) = kanal::bounded_async(32);
    let writer = tokio::spawn(run_writer(Exchange::Binance, receiver, store.clone(), shutdown.clone()));
    let mut cfg = config().feed;
    cfg.idle_timeout_ms = 150;
    let producer = tokio::spawn({
        let stop = shutdown.clone();
        async move { feed::run_at(Exchange::Binance, &url, cfg, sender, stop).await }
    });
    let dead = wait_until(&store, |s| s.feeds[0].stats.reconnects > 0).await;
    assert!(dead.feeds[0].last_error.as_ref().unwrap().contains("watchdog timeout"));
    assert_eq!(dead.feeds[0].stats.snapshots, 0);
    shutdown.cancel();
    producer.await.unwrap().unwrap();
    writer.await.unwrap().unwrap();
    timeout(Duration::from_secs(1), server).await.unwrap().unwrap();
}

#[tokio::test]
async fn full_storage_channel_fails_explicitly_instead_of_hanging() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/stream", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(tcp).await.unwrap();
        let _ = ws.next().await;
    });
    let (sender, _receiver) = kanal::bounded_async(1);
    let error = timeout(
        Duration::from_secs(1),
        feed::run_at(Exchange::Binance, &url, config().feed, sender, CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(format!("{error:#}").contains("backpressure timeout"));
    timeout(Duration::from_secs(1), server).await.unwrap().unwrap();
}

#[tokio::test]
async fn repeated_bad_frames_force_resync_and_recover_without_blocking_other_book() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/stream", listener.local_addr().unwrap());
    let stop_server = CancellationToken::new();
    let server = tokio::spawn({
        let stop = stop_server.clone();
        async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            ws.send(Message::Text(frame(100, "1999", "2001").into())).await.unwrap();
            for _ in 0..3 {
                ws.send(Message::Text("bad".into())).await.unwrap();
            }
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            ws.send(Message::Text(frame(1, "2001", "2003").into())).await.unwrap();
            stop.cancelled().await;
        }
    });
    let store = Store::default();
    let shutdown = CancellationToken::new();
    let (sender, receiver) = kanal::bounded_async(32);
    let writer = tokio::spawn(run_writer(Exchange::Aster, receiver, store.clone(), shutdown.clone()));
    let mut cfg = config().feed;
    cfg.reconnect_initial_ms = 20;
    let producer = tokio::spawn({
        let stop = shutdown.clone();
        async move { feed::run_at(Exchange::Aster, &url, cfg, sender, stop).await }
    });
    let recovered = wait_until(&store, |s| s.feeds[1].stats.snapshots == 2).await;
    assert_eq!(recovered.feeds[1].stats.invalid_frames, 3);
    assert!(recovered.feeds[1].last_error.as_ref().unwrap().contains("resync required"));
    put(&store, Exchange::Binance, 200, "1999", "2001").await;
    assert_eq!(evaluate(&store.snapshot().await, Decimal::from(2)).decision, Decision::LongBias);
    shutdown.cancel();
    producer.await.unwrap().unwrap();
    writer.await.unwrap().unwrap();
    stop_server.cancel();
    server.await.unwrap();
}

#[tokio::test]
async fn extreme_price_ratio_returns_hold_without_decimal_panic() {
    let store = Store::default();
    put(&store, Exchange::Binance, 1, "0.0000000000000000000000000001", "0.0000000000000000000000000003").await;
    put(&store, Exchange::Aster, 1, "999999998", "1000000000").await;
    let signal = evaluate(&store.snapshot().await, Decimal::from(2));
    assert_eq!(signal.decision, Decision::Hold);
    assert_eq!(signal.relative_mid_bps, None);
    assert!(signal.reason.contains("overflow"));
}

#[test]
fn configuration_and_backoff_are_bounded() {
    let mut cfg = config();
    cfg.validate().unwrap();
    for attempt in [0, 1, 10, 31, u32::MAX] {
        let delay = feed::backoff(&cfg.feed, attempt);
        assert!(delay > Duration::ZERO && delay <= Duration::from_millis(cfg.feed.reconnect_max_ms));
    }
    cfg.feed.idle_timeout_ms = 110;
    assert!(cfg.validate().is_err());
    cfg = config();
    cfg.redis.ttl_ms = cfg.redis.snapshot_interval_ms;
    assert!(cfg.validate().is_err());
    cfg = config();
    cfg.io.threads = 0;
    assert!(cfg.validate().is_err());
}
