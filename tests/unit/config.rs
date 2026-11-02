use {super::common::config, crate::feed, std::time::Duration};

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
