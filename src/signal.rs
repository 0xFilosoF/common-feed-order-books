use {
    crate::{
        config::SignalConfig,
        model::{FeedStatus, STALE_AFTER},
        store::{Store, StoreSnapshot},
    },
    rust_decimal::Decimal,
    serde::{Deserialize, Serialize},
    std::time::Duration,
    tokio::time::Instant,
    tokio_util::sync::CancellationToken,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Hold,
    LongBias,
    ShortBias,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Signal {
    pub decision: Decision,
    pub reason: String,
    pub binance_mid: Option<Decimal>,
    pub aster_mid: Option<Decimal>,
    pub relative_mid_bps: Option<Decimal>,
}

pub fn evaluate(snapshot: &StoreSnapshot, threshold_bps: Decimal) -> Signal {
    let [binance, aster] = &snapshot.feeds;
    let binance_mid = binance.book.as_ref().map(|book| book.mid());
    let aster_mid = aster.book.as_ref().map(|book| book.mid());
    let relative_mid_bps = binance_mid
        .zip(aster_mid)
        .and_then(|(b, a)| a.checked_sub(b)?.checked_div(b)?.checked_mul(Decimal::from(10_000)));
    let (decision, reason) = if snapshot.feeds.iter().any(|feed| feed.status != FeedStatus::Live || feed.book.is_none())
    {
        (
            Decision::Hold,
            format!(
                "feed unavailable: Binance={:?} ({}); Aster={:?} ({})",
                binance.status, binance.reason, aster.status, aster.reason
            ),
        )
    } else if let Some(bps) = relative_mid_bps {
        let decision = if bps > threshold_bps {
            Decision::LongBias
        } else if bps < -threshold_bps {
            Decision::ShortBias
        } else {
            Decision::Hold
        };
        (decision, format!("Aster vs Binance mid={bps:.4}bps, threshold=±{threshold_bps}bps; bias refers to Binance"))
    } else {
        (Decision::Hold, "cannot compute relative mid: missing book or decimal overflow".into())
    };
    Signal { decision, reason, binance_mid, aster_mid, relative_mid_bps }
}

pub async fn run(store: Store, config: SignalConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    let report_interval = Duration::from_millis(config.report_interval_ms);
    let mut last_report = Instant::now();
    let mut previous = None;
    loop {
        let snapshot = store.snapshot().await;
        let signal = evaluate(&snapshot, config.threshold_bps);
        let statuses = snapshot.feeds.each_ref().map(|feed| feed.status);
        let signature = (signal.decision, statuses);
        if previous != Some(signature) || last_report.elapsed() >= report_interval {
            tracing::info!(
                decision = ?signal.decision, reason = %signal.reason,
                binance_mid = ?signal.binance_mid, aster_mid = ?signal.aster_mid,
                relative_mid_bps = ?signal.relative_mid_bps,
                binance_status = ?statuses[0], aster_status = ?statuses[1],
                binance_age_ms = ?snapshot.feeds[0].age_ms, aster_age_ms = ?snapshot.feeds[1].age_ms,
                revision = snapshot.revision, "signal"
            );
            previous = Some(signature);
            last_report = Instant::now();
        }
        // Silence must wake the reader even if no writer produces another event.
        let mut wait = report_interval.saturating_sub(last_report.elapsed());
        for feed in &snapshot.feeds {
            if feed.status == FeedStatus::Live
                && let Some(age) = feed.age
            {
                wait = wait.min(STALE_AFTER.saturating_sub(age) + Duration::from_nanos(1));
            }
        }
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = store.wait_for_change(snapshot.revision, wait) => {}
        }
    }
}
