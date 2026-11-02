use {
    anyhow::{Context, ensure},
    rust_decimal::Decimal,
    serde::{Deserialize, Serialize},
    std::time::Duration,
};

pub const SYMBOL: &str = "ETHUSDT";
pub const STREAM: &str = "ethusdt@depth20@100ms";
pub const DEPTH: usize = 20;
pub const UPDATE_INTERVAL: Duration = Duration::from_millis(100);
pub const STALE_AFTER: Duration = Duration::from_millis(110);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    Binance,
    Aster,
}

impl Exchange {
    pub const ALL: [Self; 2] = [Self::Binance, Self::Aster];

    pub const fn index(self) -> usize {
        match self {
            Self::Binance => 0,
            Self::Aster => 1,
        }
    }

    pub const fn url(self) -> &'static str {
        match self {
            Self::Binance => "wss://stream.binance.com:443/stream?streams=ethusdt@depth20@100ms",
            Self::Aster => "wss://sstream.asterdex.com/stream?streams=ethusdt@depth20@100ms",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Level {
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderBook {
    pub update_id: u64,
    pub event_time_ms: Option<u64>,
    pub received_at_ms: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl OrderBook {
    pub fn mid(&self) -> Decimal {
        (self.bids[0].price + self.asks[0].price) / Decimal::TWO
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedStatus {
    Connecting,
    Synchronizing,
    Live,
    Delayed,
    InvalidFrame,
    Disconnected,
    Reconnecting,
    Stopped,
}

#[derive(Deserialize)]
struct Envelope {
    stream: String,
    data: WireBook,
}

#[derive(Deserialize)]
struct WireBook {
    #[serde(rename = "lastUpdateId", alias = "u")]
    update_id: u64,
    #[serde(rename = "E")]
    event_time_ms: Option<u64>,
    #[serde(rename = "s", alias = "symbol")]
    symbol: Option<String>,
    #[serde(alias = "b")]
    bids: Vec<[String; 2]>,
    #[serde(alias = "a")]
    asks: Vec<[String; 2]>,
}

pub fn parse_snapshot(bytes: &[u8], received_at_ms: u64) -> anyhow::Result<OrderBook> {
    let envelope: Envelope = serde_json::from_slice(bytes).context("invalid partial-depth JSON")?;
    ensure!(envelope.stream == STREAM, "unexpected stream {}", envelope.stream);
    let wire = envelope.data;
    if let Some(symbol) = wire.symbol {
        ensure!(symbol == SYMBOL, "unexpected symbol {symbol}");
    }
    let bids = levels(wire.bids, true)?;
    let asks = levels(wire.asks, false)?;
    ensure!(bids[0].price < asks[0].price, "crossed or locked order book");
    Ok(OrderBook { update_id: wire.update_id, event_time_ms: wire.event_time_ms, received_at_ms, bids, asks })
}

fn levels(raw: Vec<[String; 2]>, descending: bool) -> anyhow::Result<Vec<Level>> {
    ensure!(!raw.is_empty() && raw.len() <= DEPTH, "snapshot side must contain 1..=20 levels");
    let mut levels = raw
        .into_iter()
        .map(|[price, quantity]| {
            let price: Decimal = price.parse().context("invalid level price")?;
            let quantity: Decimal = quantity.parse().context("invalid level quantity")?;
            // Bound arithmetic as well as input; much larger than any ETHUSDT market level.
            ensure!(price > Decimal::ZERO && price <= Decimal::from(1_000_000_000), "price outside supported range");
            ensure!(
                quantity > Decimal::ZERO && quantity <= Decimal::from(1_000_000_000),
                "quantity outside supported range"
            );
            Ok(Level { price, quantity })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    levels.sort_unstable_by(|a, b| if descending { b.price.cmp(&a.price) } else { a.price.cmp(&b.price) });
    ensure!(levels.windows(2).all(|pair| pair[0].price != pair[1].price), "duplicate price level");
    Ok(levels)
}
