use {
    anyhow::{Context, ensure},
    config::{Config, Environment, File},
    rust_decimal::Decimal,
    serde::Deserialize,
    std::{env, path::Path, time::Duration},
    tracing_subscriber::EnvFilter,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub app: AppConfig,
    pub logs: LogConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub io: ThreadsConfig,
    pub feed: FeedConfig,
    pub redis: RedisConfig,
    pub signal: SignalConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadsConfig {
    #[serde(default = "default_threads")]
    pub threads: usize,
}

fn default_threads() -> usize {
    std::thread::available_parallelism().map_or(2, |count| (count.get() / 2).max(2))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedConfig {
    pub channel_capacity: usize,
    pub channel_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub max_bad_frames: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    pub url: String,
    pub key: String,
    pub snapshot_interval_ms: u64,
    pub ttl_ms: u64,
    pub io_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalConfig {
    pub threshold_bps: Decimal,
    pub report_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    pub level: String,
    pub json: bool,
}

impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        // Process environment > .env.local > .env > config.yaml.
        for file in [".env.local", ".env"] {
            if Path::new(file).exists() {
                dotenvy::from_filename(file).with_context(|| format!("cannot load {file}"))?;
            }
        }
        let path = env::var("BOOKS_CONFIG").unwrap_or_else(|_| "config.yaml".into());
        let settings: Self = Config::builder()
            .add_source(File::from(Path::new(&path)))
            .add_source(Environment::with_prefix("BOOKS").prefix_separator("__").separator("__").try_parsing(true))
            .build()
            .with_context(|| format!("cannot read {path}"))?
            .try_deserialize()
            .context("invalid configuration")?;
        settings.app.validate()?;
        Ok(settings)
    }
}

impl AppConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.io.threads > 0, "app.io.threads must be positive");
        let f = &self.feed;
        ensure!(f.channel_capacity > 0, "feed.channel_capacity must be positive");
        ensure!(f.channel_timeout_ms > 0 && f.connect_timeout_ms > 0, "feed timeouts must be positive");
        ensure!(Duration::from_millis(f.idle_timeout_ms) > crate::model::STALE_AFTER, "idle timeout must exceed 110ms");
        ensure!(f.reconnect_initial_ms > 0 && f.reconnect_max_ms >= f.reconnect_initial_ms, "invalid reconnect range");
        ensure!(f.max_bad_frames > 0, "max_bad_frames must be positive");
        let r = &self.redis;
        redis::Client::open(r.url.as_str()).context("invalid redis.url")?;
        ensure!(!r.key.trim().is_empty(), "redis.key must not be empty");
        ensure!(r.snapshot_interval_ms > 0 && r.io_timeout_ms > 0, "Redis intervals must be positive");
        ensure!(r.ttl_ms / 2 > r.snapshot_interval_ms, "Redis TTL must exceed two snapshot intervals");
        ensure!(self.signal.threshold_bps > Decimal::ZERO, "signal.threshold_bps must be positive");
        ensure!(self.signal.report_interval_ms > 0, "signal.report_interval_ms must be positive");
        Ok(())
    }
}

impl LogConfig {
    pub fn init(&self) -> anyhow::Result<()> {
        let filter = EnvFilter::try_new(&self.level).context("invalid logs.level")?;
        if self.json {
            tracing_subscriber::fmt().with_env_filter(filter).json().try_init().map_err(|e| anyhow::anyhow!(e))
        } else {
            tracing_subscriber::fmt().with_env_filter(filter).try_init().map_err(|e| anyhow::anyhow!(e))
        }
    }
}
