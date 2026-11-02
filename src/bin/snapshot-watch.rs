use {
    anyhow::{Context, bail},
    feed_order_books::{config::Settings, observer::SnapshotReader, store::unix_ms, wait_for_shutdown},
    serde_json::json,
    std::{
        io::{self, Write},
        time::Duration,
    },
    tokio::time::{MissedTickBehavior, interval},
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let once = match args.as_slice() {
        [] => false,
        [arg] if arg == "--once" => true,
        [arg] if arg == "--help" => {
            println!("Usage: snapshot-watch [--once]\nPoll the configured Redis snapshot and print JSON observations.");
            return Ok(());
        }
        _ => bail!("Usage: snapshot-watch [--once]"),
    };
    let settings = Settings::load()?;
    let config = settings.app.redis;
    let mut reader = SnapshotReader::new(config.clone())?;
    let mut ticker = interval(Duration::from_millis(config.snapshot_interval_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let shutdown = wait_for_shutdown();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            result = &mut shutdown => return result,
            _ = ticker.tick() => {}
        }
        let result = tokio::select! {
            biased;
            result = &mut shutdown => return result,
            result = reader.poll() => result,
        };
        let output = match &result {
            Ok(Some(observation)) => json!({ "event": "snapshot", "observation": observation }),
            Ok(None) => json!({ "event": "no_snapshot", "observed_at_ms": unix_ms(), "key": config.key }),
            Err(error) => {
                json!({ "event": "observer_error", "observed_at_ms": unix_ms(), "reason": format!("{error:#}") })
            }
        };
        let mut stdout = io::stdout().lock();
        serde_json::to_writer(&mut stdout, &output).context("cannot write observation")?;
        writeln!(stdout).context("cannot write observation newline")?;
        drop(stdout);
        if once {
            return result.map(|_| ());
        }
    }
}
