use {
    super::common::put,
    crate::{
        model::{Exchange, FeedStatus},
        signal::{Decision, evaluate},
        store::{FeedEvent, Store},
    },
    rust_decimal::Decimal,
};

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
async fn extreme_price_ratio_returns_hold_without_decimal_panic() {
    let store = Store::default();
    put(&store, Exchange::Binance, 1, "0.0000000000000000000000000001", "0.0000000000000000000000000003").await;
    put(&store, Exchange::Aster, 1, "999999998", "1000000000").await;
    let signal = evaluate(&store.snapshot().await, Decimal::from(2));
    assert_eq!(signal.decision, Decision::Hold);
    assert_eq!(signal.relative_mid_bps, None);
    assert!(signal.reason.contains("overflow"));
}
