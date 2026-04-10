mod bridge;
mod commands;
mod config;
mod hue;
mod logging;
mod sync;
mod translator;

use anyhow::Result;
use plugin_sdk_rs::{PluginClient, PluginConfig};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

use bridge::Bridge;
use config::HuePluginConfig;

const MAX_ATTEMPTS: u32 = 3;
const RETRY_DELAY_SECS: u64 = 60;

#[tokio::main]
async fn main() {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/config.toml".to_string());

    let (_log_guard, log_level_handle, mqtt_log_handle) = init_logging(&config_path);

    let cfg = match HuePluginConfig::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, path = %config_path, "Failed to load config");
            std::process::exit(1);
        }
    };

    for attempt in 1..=MAX_ATTEMPTS {
        info!(attempt, max = MAX_ATTEMPTS, "Starting hc-hue plugin");
        match try_start(
            &cfg,
            &config_path,
            log_level_handle.clone(),
            mqtt_log_handle.clone(),
        )
        .await
        {
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

fn init_logging(
    config_path: &str,
) -> (
    tracing_appender::non_blocking::WorkerGuard,
    hc_logging::LogLevelHandle,
    plugin_sdk_rs::mqtt_log_layer::MqttLogHandle,
) {
    #[derive(serde::Deserialize, Default)]
    struct Bootstrap {
        #[serde(default)]
        logging: logging::LoggingConfig,
    }
    let bootstrap: Bootstrap = std::fs::read_to_string(config_path)
        .ok()
        .and_then(|s| toml::from_str(&s).ok())
        .unwrap_or_default();
    logging::init_logging(config_path, "hc-hue", "hc_hue=info", &bootstrap.logging)
}

async fn try_start(
    cfg: &HuePluginConfig,
    config_path: &str,
    log_level_handle: hc_logging::LogLevelHandle,
    mqtt_log_handle: plugin_sdk_rs::mqtt_log_layer::MqttLogHandle,
) -> Result<()> {
    let discovered = hue::discovery::discover_bridges(&cfg.hue).await?;
    let bridges = cfg.effective_bridges(&discovered);

    if bridges.is_empty() {
        error!("No Hue bridges configured or discovered; set [[bridges]] in config/config.toml");
        anyhow::bail!("no hue bridges available");
    }

    let sdk_config = PluginConfig {
        broker_host: cfg.homecore.broker_host.clone(),
        broker_port: cfg.homecore.broker_port,
        plugin_id: cfg.homecore.plugin_id.clone(),
        password: cfg.homecore.password.clone(),
    };

    let client = PluginClient::connect(sdk_config).await?;
    mqtt_log_handle.connect(
        client.mqtt_client(),
        &cfg.homecore.plugin_id,
        &cfg.logging.log_forward_level,
    );
    let publisher = client.device_publisher();
    let (cmd_tx, cmd_rx) = mpsc::channel::<(String, serde_json::Value)>(256);

    // Enable management protocol (heartbeat + remote config/log commands).
    let mgmt = client
        .enable_management(
            60,
            Some(env!("CARGO_PKG_VERSION").to_string()),
            Some(config_path.to_string()),
            Some(log_level_handle),
        )
        .await?;

    // Start the SDK event loop FIRST so the MQTT eventloop is pumping while
    // we register devices.
    let cmd_tx_clone = cmd_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = client
            .run_managed(
                move |device_id, payload| {
                    let _ = cmd_tx_clone.try_send((device_id, payload));
                },
                mgmt,
            )
            .await
        {
            error!(error = %e, "SDK event loop exited with error");
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Register bridge devices via DevicePublisher (PluginClient is consumed).
    for bridge in &bridges {
        let bridge_device_id = bridge.device_id();
        if let Err(e) = publisher
            .register_device_full(
                &bridge_device_id,
                &format!("Hue Bridge {}", bridge.name),
                Some("bridge"),
                None,
                None,
            )
            .await
        {
            error!(device_id = %bridge_device_id, error = %e, "Failed to register bridge device");
        }
        if let Err(e) = publisher.subscribe_commands(&bridge_device_id).await {
            error!(device_id = %bridge_device_id, error = %e, "Failed to subscribe bridge commands");
        }
        if let Err(e) = publisher
            .publish_availability(&bridge_device_id, true)
            .await
        {
            error!(device_id = %bridge_device_id, error = %e, "Failed to publish bridge availability");
        }
    }

    if let Err(e) = publisher.publish_plugin_status("active").await {
        error!(error = %e, "Failed to publish plugin status");
    }

    info!(
        count = bridges.len(),
        "Hue bridges registered with HomeCore"
    );

    let bridge_runtime = Bridge::new(cfg.clone(), config_path.to_string(), bridges, publisher);
    bridge_runtime.run(cmd_rx).await
}
