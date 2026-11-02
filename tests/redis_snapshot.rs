use {
    feed_order_books::{
        config::{RedisConfig, SignalConfig},
        model::{Exchange, FeedStatus},
        observer::SnapshotReader,
        snapshot::SnapshotSink,
        store::{FeedEvent, Store},
    },
    redis::AsyncCommands,
    rust_decimal::Decimal,
    std::{
        net::TcpListener,
        process::{Child, Command, Stdio},
        time::Duration,
    },
    tokio::time::{sleep, timeout},
};

struct RedisProcess(Child);

impl RedisProcess {
    fn start(port: u16, directory: &std::path::Path) -> Self {
        Self(
            Command::new("redis-server")
                .args(["--bind", "127.0.0.1", "--port", &port.to_string(), "--save", "", "--appendonly", "no"])
                .arg("--dir")
                .arg(directory)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("install redis-server to run this integration test"),
        )
    }
}

impl Drop for RedisProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn connect(client: &redis::Client) -> redis::aio::MultiplexedConnection {
    timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(connection) = client.get_multiplexed_async_connection().await {
                return connection;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Redis did not become ready")
}

#[tokio::test]
#[ignore = "requires local redis-server executable; starts its own isolated Redis instance"]
async fn snapshots_expire_and_sink_recovers_after_redis_restart() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let server = RedisProcess::start(port, directory.path());
    let cfg = RedisConfig {
        url: format!("redis://127.0.0.1:{port}/0"),
        key: "test:ETHUSDT:snapshot".into(),
        snapshot_interval_ms: 25,
        ttl_ms: 200,
        io_timeout_ms: 100,
    };
    let client = redis::Client::open(cfg.url.as_str()).unwrap();
    let mut connection = connect(&client).await;
    let store = Store::default();
    let signal = SignalConfig { threshold_bps: Decimal::from(2), report_interval_ms: 1000 };
    let mut sink = SnapshotSink::new(cfg.clone()).unwrap();
    let mut observer = SnapshotReader::new(cfg.clone()).unwrap();
    assert!(observer.poll().await.unwrap().is_none());
    sink.publish(&store, &signal).await.unwrap();
    let observation = observer.poll().await.unwrap().unwrap();
    assert_eq!(observation.symbol, "ETHUSDT");
    assert_eq!(observation.feeds[0].exchange, Exchange::Binance);
    let output = Command::new(env!("CARGO_BIN_EXE_snapshot-watch"))
        .arg("--once")
        .env("BOOKS_CONFIG", concat!(env!("CARGO_MANIFEST_DIR"), "/config.yaml"))
        .env("BOOKS__APP__REDIS__URL", &cfg.url)
        .env("BOOKS__APP__REDIS__KEY", &cfg.key)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let output: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output["event"], "snapshot");
    assert_eq!(output["observation"]["signal_at_capture"]["decision"], "hold");
    let json: String = connection.get(&cfg.key).await.unwrap();
    let snapshot: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(snapshot["schema_version"], 1);
    assert_eq!(snapshot["symbol"], "ETHUSDT");
    assert_eq!(snapshot["signal"]["decision"], "hold");
    assert_eq!(snapshot["feeds"].as_array().unwrap().len(), 2);
    let keys: Vec<String> = connection.keys("*").await.unwrap();
    assert_eq!(keys, vec![cfg.key.clone()]);
    let ttl: i64 = connection.pttl(&cfg.key).await.unwrap();
    assert!(ttl > 0 && ttl <= 200);
    drop(server);
    assert!(sink.publish(&store, &signal).await.is_err());
    assert!(observer.poll().await.is_err());
    store
        .apply(
            Exchange::Aster,
            FeedEvent::Status { status: FeedStatus::Disconnected, reason: "test network loss".into() },
        )
        .await;
    let _restarted = RedisProcess::start(port, directory.path());
    let mut connection = connect(&client).await;
    sink.publish(&store, &signal).await.unwrap();
    let json: String = connection.get(&cfg.key).await.unwrap();
    let snapshot: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(snapshot["feeds"][1]["status"], "disconnected");
    assert_eq!(snapshot["feeds"][1]["reason"], "test network loss");
    assert_eq!(snapshot["signal"]["decision"], "hold");
    let recovered = observer.poll().await.unwrap().unwrap();
    assert_eq!(recovered.feeds[1].status_at_capture, FeedStatus::Disconnected);
    sleep(Duration::from_millis(250)).await;
    let expired: Option<String> = connection.get(&cfg.key).await.unwrap();
    assert!(expired.is_none());
    assert!(observer.poll().await.unwrap().is_none());
}
