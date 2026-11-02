use {
    super::common::frame,
    crate::model::{STREAM, parse_snapshot},
    rust_decimal::Decimal,
    serde_json::json,
    std::str::FromStr,
};

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
