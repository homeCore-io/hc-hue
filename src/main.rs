mod bridge;
mod commands;
mod config;
mod hue;
mod logging;
mod pairing;
mod sync;
mod translator;

use anyhow::Result;
use plugin_sdk_rs::{PluginClient, PluginConfig};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

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

    if !cfg.hue.compact_motion_facets {
        warn!(
            "compact_motion_facets = false: each Hue motion/temperature/light_level \
             service will register as a separate HomeCore device. Set this to true \
             (the default) to merge all sensors on one physical device into one \
             HomeCore device with multi-facet attributes."
        );
    }

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

    let client = PluginClient::connect(sdk_config)
        .await?
        // Cross-restart device tracking — the SDK mirrors every
        // register/unregister to disk, and `publisher.reconcile_devices`
        // can later prune anything that vanished from upstream while
        // the plugin was offline. File lives next to config.toml.
        .with_device_persistence(published_ids_path(config_path));
    mqtt_log_handle.connect(
        client.mqtt_client(),
        &cfg.homecore.plugin_id,
        &cfg.logging.log_forward_level,
    );
    let publisher = client.device_publisher();
    let (cmd_tx, cmd_rx) = mpsc::channel::<(String, serde_json::Value)>(256);

    // Manual-refresh channel — `refresh_devices` manifest action pings
    // this so the bridge runtime re-walks every API endpoint and
    // republishes lights / groups / scenes / sensors. Useful after
    // editing names or rooms in the Hue app.
    let (refresh_tx, refresh_rx) = mpsc::channel::<()>(8);
    // `discover_bridges` re-runs SSDP/mDNS on demand. The result is
    // returned synchronously through the management custom_handler — the
    // discovery wait is short (couple of seconds) so blocking is fine.
    let discovery_cfg_for_handler = cfg.hue.clone();
    // Streaming `pair_bridge` action publishes newly-paired bridges back
    // to the runtime over this channel; the bridge runtime adds them to
    // its apis vec and refreshes without restart.
    let (new_bridge_tx, new_bridge_rx) = mpsc::channel::<hue::models::BridgeTarget>(8);
    let pairing_handle = pairing::PairingHandle::new(
        config_path.to_string(),
        cfg.clone(),
        new_bridge_tx,
    );

    // Enable management protocol (heartbeat + remote config/log commands +
    // capability manifest).
    let mgmt = client
        .enable_management(
            60,
            Some(env!("CARGO_PKG_VERSION").to_string()),
            Some(config_path.to_string()),
            Some(log_level_handle),
        )
        .await?
        .with_capabilities(capabilities_manifest())
        .with_custom_handler(move |cmd| match cmd["action"].as_str()? {
            "refresh_devices" | "cleanup_stale_devices" => {
                // Both actions feed the same refresh-then-reconcile path
                // in Bridge::run; the manifest exposes them as two
                // separate entry points so the UI can frame them
                // distinctly (routine refresh vs. cleanup).
                let _ = refresh_tx.try_send(());
                Some(serde_json::json!({ "status": "ok" }))
            }
            _ => None,
        });
    // Layer the streaming pair_bridge action on top of the management +
    // capabilities handle.
    let mgmt = pairing::register_actions(mgmt, pairing_handle);
    // Streaming discover_bridges: SSDP + N-UPnP can stack past the 5s
    // sync RPC timeout depending on network conditions, so it lives
    // outside the sync custom_handler dispatch.
    let discovery_cfg_for_stream = discovery_cfg_for_handler.clone();
    let mgmt = mgmt.with_streaming_action(plugin_sdk_rs::StreamingAction::new(
        "discover_bridges",
        move |ctx, _params| {
            let cfg = discovery_cfg_for_stream.clone();
            async move { discover_bridges_streaming(ctx, cfg).await }
        },
    ));

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
    bridge_runtime.run(cmd_rx, refresh_rx, new_bridge_rx).await
}

/// Capability manifest for hc-hue. Plugin actions exposed to the admin
/// UI (Plugins → Hue) and to hc-mcp via `list_plugin_actions`.
fn capabilities_manifest() -> hc_types::Capabilities {
    use hc_types::{Action, Capabilities, Concurrency, RequiresRole};
    Capabilities {
        spec: "1".into(),
        plugin_id: String::new(), // SDK fills from configured plugin_id
        actions: vec![
            Action {
                id: "refresh_devices".into(),
                label: "Refresh devices".into(),
                description: Some(
                    "Re-walk every configured Hue bridge and republish all \
                     lights, groups, scenes, and sensors to homeCore. \
                     Also reconciles the published-device snapshot — \
                     anything that was registered last session but no \
                     longer exists on its bridge gets unregistered. \
                     Use this after renaming devices or moving them \
                     between rooms in the Hue app, or when something \
                     looks stale."
                        .into(),
                ),
                params: None,
                result: None,
                stream: false,
                cancelable: false,
                concurrency: Concurrency::default(),
                item_key: None,
                item_operations: None,
                requires_role: RequiresRole::User,
                timeout_ms: None,
            },
            Action {
                id: "cleanup_stale_devices".into(),
                label: "Clean up stale devices".into(),
                description: Some(
                    "Force a fresh sync from every configured bridge and \
                     unregister any homeCore devices that no longer exist \
                     on the bridge. Same effect as `refresh_devices` but \
                     framed for cleanup instead of routine refresh — use \
                     this to clear zombies left over from config edits, \
                     bridge replacements, or development churn. For a \
                     wipe-and-rebuild (re-register everything from \
                     scratch), use the core API: \
                     `DELETE /api/v1/plugins/plugin.hue/devices`."
                        .into(),
                ),
                params: None,
                result: None,
                stream: false,
                cancelable: false,
                concurrency: Concurrency::default(),
                item_key: None,
                item_operations: None,
                requires_role: RequiresRole::Admin,
                timeout_ms: None,
            },
            Action {
                id: "discover_bridges".into(),
                label: "Discover bridges".into(),
                description: Some(
                    "Run SSDP / mDNS / cloud discovery to find Hue bridges \
                     on the network. Streams each bridge as it's found \
                     (bridge_id, host, name) so you can decide whether to \
                     add any to config/config.toml."
                        .into(),
                ),
                params: None,
                result: Some(serde_json::json!({
                    "discovered": { "type": "array" },
                    "count": { "type": "integer" },
                })),
                stream: true,
                cancelable: false,
                concurrency: Concurrency::Single,
                item_key: Some("bridge_id".into()),
                item_operations: None,
                requires_role: RequiresRole::User,
                timeout_ms: Some(60_000),
            },
            Action {
                id: "pair_bridge".into(),
                label: "Pair Hue bridge".into(),
                description: Some(
                    "Pair a new Hue bridge by polling its link-button. \
                     Optionally pass `host` to target a specific IP; \
                     otherwise discovery picks the first unpaired bridge. \
                     The action waits for you to press the physical link \
                     button on the bridge — once pressed, the new app key \
                     is saved to config.toml and the bridge starts \
                     publishing immediately."
                        .into(),
                ),
                params: Some(serde_json::json!({
                    "host": {
                        "type": "string",
                        "description": "Optional bridge IP/hostname; auto-discovers if omitted",
                    },
                    "name": {
                        "type": "string",
                        "description": "Optional friendly name for the new bridge entry",
                    },
                })),
                result: Some(serde_json::json!({
                    "bridge_id": { "type": "string" },
                    "host": { "type": "string" },
                    "name": { "type": "string" },
                })),
                stream: true,
                cancelable: true,
                concurrency: Concurrency::Single,
                item_key: Some("bridge_id".into()),
                item_operations: Some(vec![hc_types::ItemOp::Add]),
                requires_role: RequiresRole::Admin,
                timeout_ms: Some(90_000),
            },
        ],
    }
}

/// Streaming `discover_bridges`. Wraps the same SSDP / N-UPnP routine
/// the runtime uses at startup, but emits each result as a stream
/// item as soon as it's identified — useful for slow networks where
/// discovery comfortably exceeds the 5s sync RPC budget.
///
/// Cancellation is currently a no-op: the underlying discovery has a
/// fixed timeout and returns when it returns. If we ever want to
/// short-circuit the SSDP wait we can split the discovery into a
/// loop that checks ctx.cancelled() between probes.
async fn discover_bridges_streaming(
    ctx: plugin_sdk_rs::StreamContext,
    cfg: config::HueConfig,
) -> anyhow::Result<()> {
    use serde_json::json;

    ctx.progress(
        Some(10),
        Some("starting"),
        Some("Running SSDP / N-UPnP discovery"),
    )
    .await?;

    let bridges = match hue::discovery::discover_bridges(&cfg).await {
        Ok(b) => b,
        Err(e) => return ctx.error(format!("discovery failed: {e}")).await,
    };

    ctx.progress(
        Some(80),
        Some("found"),
        Some(&format!("{} bridge(s) discovered", bridges.len())),
    )
    .await?;

    for b in &bridges {
        let _ = ctx
            .item_add(json!({
                "bridge_id": b.bridge_id,
                "host": b.host,
                "name": b.name,
            }))
            .await;
    }

    let entries: Vec<serde_json::Value> = bridges
        .iter()
        .map(|b| {
            json!({
                "bridge_id": b.bridge_id,
                "host": b.host,
                "name": b.name,
            })
        })
        .collect();
    ctx.complete(json!({
        "discovered": entries,
        "count": entries.len(),
    }))
    .await
}

/// Path for the cross-restart device-id snapshot, sibling to the
/// plugin's config.toml. The file is owned by the SDK's device
/// tracker — see `PluginClient::with_device_persistence`. Falls
/// back to the current directory if the config path has no parent.
fn published_ids_path(config_path: &str) -> std::path::PathBuf {
    std::path::Path::new(config_path)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(".published-device-ids.json")
}
