use {
    super::server_retry_delay,
    crate::{
        feed,
        model::{Exchange, FeedStatus},
        signal::{Decision, evaluate},
        store::{Store, run_writer},
        tests::common::{config, frame, put, wait_until},
    },
    futures_util::{SinkExt, StreamExt},
    rust_decimal::Decimal,
    std::time::{Duration, SystemTime, UNIX_EPOCH},
    tokio::{
        net::TcpListener,
        sync::oneshot,
        time::{Instant, sleep, timeout},
    },
    tokio_tungstenite::{
        accept_async, accept_hdr_async,
        tungstenite::{
            Error as WebSocketError, Message,
            handshake::server::{Callback, ErrorResponse, Request, Response},
            http::{StatusCode, header::RETRY_AFTER},
        },
    },
    tokio_util::sync::CancellationToken,
};

struct RejectHandshake {
    status: StatusCode,
    retry_after: &'static str,
}

impl Callback for RejectHandshake {
    fn on_request(self, _: &Request, _: Response) -> Result<Response, ErrorResponse> {
        Err(Response::builder().status(self.status).header(RETRY_AFTER, self.retry_after).body(None).unwrap())
    }
}

fn handshake_error(status: StatusCode, retry_after: Option<&str>) -> anyhow::Error {
    let mut response = tokio_tungstenite::tungstenite::http::Response::builder().status(status);
    if let Some(value) = retry_after {
        response = response.header(RETRY_AFTER, value);
    }
    anyhow::Error::new(WebSocketError::Http(Box::new(response.body(None).unwrap())))
        .context("WebSocket handshake failed")
}

#[test]
fn retry_after_supports_seconds_and_http_dates_through_error_context() {
    let now = UNIX_EPOCH + Duration::from_secs(1_793_625_000);
    let future = httpdate::fmt_http_date(now + Duration::from_secs(120));
    let past = httpdate::fmt_http_date(now - Duration::from_secs(120));
    for (value, expected) in [
        ("120", Duration::from_secs(120)),
        (" 120 ", Duration::from_secs(120)),
        ("0", Duration::ZERO),
        (future.as_str(), Duration::from_secs(120)),
        (past.as_str(), Duration::ZERO),
        ("184467440737095516160", Duration::from_secs(u64::MAX)),
    ] {
        let error = handshake_error(StatusCode::TOO_MANY_REQUESTS, Some(value));
        assert_eq!(server_retry_delay(&error, now), Some(expected), "Retry-After: {value}");
    }
    let error = handshake_error(StatusCode::SERVICE_UNAVAILABLE, Some("90"));
    assert_eq!(server_retry_delay(&error, now), Some(Duration::from_secs(90)));
}

#[test]
fn missing_or_invalid_retry_after_uses_rate_limit_cooldowns() {
    for (status, seconds) in [(StatusCode::TOO_MANY_REQUESTS, 60), (StatusCode::IM_A_TEAPOT, 120)] {
        for value in [None, Some(""), Some("invalid"), Some("-1"), Some("+1"), Some("1.5")] {
            let error = handshake_error(status, value);
            assert_eq!(server_retry_delay(&error, SystemTime::now()), Some(Duration::from_secs(seconds)));
        }
    }
    for status in [StatusCode::BAD_REQUEST, StatusCode::SERVICE_UNAVAILABLE] {
        assert_eq!(server_retry_delay(&handshake_error(status, None), SystemTime::now()), None);
    }
    assert_eq!(server_retry_delay(&anyhow::anyhow!("connection refused"), SystemTime::now()), None);
}

#[tokio::test]
async fn server_retry_after_exceeds_local_cap_and_recovers_both_feeds() {
    for (exchange, status) in
        [(Exchange::Binance, StatusCode::TOO_MANY_REQUESTS), (Exchange::Aster, StatusCode::IM_A_TEAPOT)]
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/stream", listener.local_addr().unwrap());
        let (reconnected, mut connection_time) = oneshot::channel();
        let shutdown = CancellationToken::new();
        let server = tokio::spawn({
            let stop = shutdown.clone();
            async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let started = Instant::now();
                let rejected = accept_hdr_async(tcp, RejectHandshake { status, retry_after: "1" }).await;
                assert!(matches!(rejected, Err(WebSocketError::Http(_))));
                let (tcp, _) = listener.accept().await.unwrap();
                reconnected.send(started.elapsed()).unwrap();
                let mut ws = accept_async(tcp).await.unwrap();
                ws.send(Message::Text(frame(1, "1999", "2001").into())).await.unwrap();
                stop.cancelled().await;
            }
        });
        let store = Store::default();
        let (sender, receiver) = kanal::bounded_async(32);
        let writer = tokio::spawn(run_writer(exchange, receiver, store.clone(), shutdown.clone()));
        let mut cfg = config().feed;
        cfg.reconnect_initial_ms = 10;
        cfg.reconnect_max_ms = 20;
        let producer = tokio::spawn({
            let stop = shutdown.clone();
            async move { feed::run_at(exchange, &url, cfg, sender, stop).await }
        });
        let index = exchange.index();
        let reconnecting = wait_until(&store, |s| s.feeds[index].status == FeedStatus::Reconnecting).await;
        assert!(reconnecting.feeds[index].reason.contains("retry in 1000ms"));
        assert!(reconnecting.feeds[index].last_error.as_ref().unwrap().contains(&status.as_u16().to_string()));
        assert!(timeout(Duration::from_millis(100), &mut connection_time).await.is_err());
        let elapsed = timeout(Duration::from_secs(2), connection_time).await.unwrap().unwrap();
        assert!(elapsed >= Duration::from_secs(1), "retried too early: {elapsed:?}");
        let recovered = wait_until(&store, |s| s.feeds[index].stats.snapshots == 1).await;
        assert_eq!(recovered.feeds[index].status, FeedStatus::Live);
        assert_eq!(recovered.feeds[index].stats.reconnects, 1);
        shutdown.cancel();
        producer.await.unwrap().unwrap();
        writer.await.unwrap().unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn shutdown_cancels_even_an_oversized_server_cooldown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/stream", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let rejected = accept_hdr_async(
            tcp,
            RejectHandshake { status: StatusCode::TOO_MANY_REQUESTS, retry_after: "184467440737095516160" },
        )
        .await;
        assert!(matches!(rejected, Err(WebSocketError::Http(_))));
    });
    let store = Store::default();
    let shutdown = CancellationToken::new();
    let (sender, receiver) = kanal::bounded_async(32);
    let writer = tokio::spawn(run_writer(Exchange::Binance, receiver, store.clone(), shutdown.clone()));
    let producer = tokio::spawn({
        let stop = shutdown.clone();
        async move { feed::run_at(Exchange::Binance, &url, config().feed, sender, stop).await }
    });
    wait_until(&store, |s| s.feeds[0].status == FeedStatus::Reconnecting).await;
    shutdown.cancel();
    timeout(Duration::from_secs(1), producer).await.unwrap().unwrap().unwrap();
    writer.await.unwrap().unwrap();
    server.await.unwrap();
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
