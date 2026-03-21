mod bridge;
mod commands;
mod config;
mod homecore;
mod hue;
mod sync;
mod translator;

use anyhow::Result;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use bridge::Bridge;
use config::HuePluginConfig;

const MAX_ATTEMPTS: u32 = 3;
const RETRY_DELAY_SECS: u64 = 60;

#[tokio::main]
async fn main() {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/config.toml".to_string());

    let _log_guard = init_logging(&config_path);

    let cfg = match HuePluginConfig::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, path = %config_path, "Failed to load config");
            std::process::exit(1);
        }
    };

    for attempt in 1..=MAX_ATTEMPTS {
        info!(attempt, max = MAX_ATTEMPTS, "Starting hc-hue plugin");
        match try_start(&cfg).await {
            Ok(()) => return,
            Err(e) => {
                if attempt < MAX_ATTEMPTS {
                    error!(error = %e, attempt, "Startup failed; retrying in {RETRY_DELAY_SECS} s");
                    tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                } else {
                    error!(error = %e, "Startup failed after {MAX_ATTEMPTS} attempts; exiting");
                    std::process::exit(1);
                }
            }
        }
    }
}

fn init_logging(config_path: &str) -> tracing_appender::non_blocking::WorkerGuard {
    let log_dir = std::path::Path::new(config_path)
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("logs"))
        .unwrap_or_else(|| std::path::PathBuf::from("logs"));
    std::fs::create_dir_all(&log_dir).ok();

    let file_appender = tracing_appender::rolling::daily(&log_dir, "hc-hue.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let stderr_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "hc_hue=info".parse().unwrap());
    let file_filter = EnvFilter::new("debug");

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(stderr_filter);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_filter(file_filter);

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();

    guard
}

async fn try_start(cfg: &HuePluginConfig) -> Result<()> {
    let discovered = hue::discovery::discover_bridges(&cfg.hue).await?;
    let bridges = cfg.effective_bridges(&discovered);

    if bridges.is_empty() {
        error!(
            "No Hue bridges configured or discovered; set [[bridges]] in config/config.toml"
        );
        anyhow::bail!("no hue bridges available");
    }

    let hc_client = homecore::HomecoreClient::connect(&cfg.homecore).await?;
    let publisher = hc_client.publisher();
    let (hc_tx, hc_rx) = mpsc::channel::<(String, serde_json::Value)>(256);

    for bridge in &bridges {
        let bridge_device_id = bridge.device_id();
        publisher
            .register_device(
                &bridge_device_id,
                &format!("Hue Bridge {}", bridge.name),
                "bridge",
                None,
            )
            .await?;
        publisher.subscribe_commands(&bridge_device_id).await?;
        publisher.publish_availability(&bridge_device_id, true).await?;
    }

    publisher.publish_plugin_status("active").await?;

    info!(count = bridges.len(), "Hue bridges registered with HomeCore");

    tokio::spawn(hc_client.run(hc_tx));

    let bridge_runtime = Bridge::new(cfg.clone(), bridges, publisher);
    bridge_runtime.run(hc_rx).await
}
