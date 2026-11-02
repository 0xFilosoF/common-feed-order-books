use {
    super::common::put,
    crate::{
        model::{Exchange, FeedStatus},
        signal::{Decision, evaluate},
        store::{FeedEvent, Store},
    },
    rust_decimal::Decimal,
    std::{sync::Arc, time::Duration},
    tokio::time::{Instant, timeout},
};

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
