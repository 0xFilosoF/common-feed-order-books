use feed_order_books::config::Settings;

fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("cannot install rustls crypto provider"))?;
    let settings = Settings::load()?;
    settings.logs.init()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(settings.app.io.threads)
        .thread_name("order-books")
        .enable_all()
        .build()?;
    runtime.block_on(feed_order_books::run(settings.app))
}
