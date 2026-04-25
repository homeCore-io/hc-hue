//! Streaming `pair_bridge` action — drives the Hue link-button flow
//! end-to-end.
//!
//! Flow:
//!
//! 1. Resolve the target bridge. If `host` param is given, use it directly
//!    (and probe `/api/0/config` to learn `bridge_id`). Otherwise re-run
//!    discovery and pick the first bridge that isn't already paired.
//! 2. Emit a clear `progress("Press the link button on the bridge now")`
//!    event so the user knows what to do.
//! 3. Poll the Hue `POST /api` endpoint every 2 s. The bridge replies with
//!    `error: "link button not pressed"` until the button is pushed; once
//!    pushed, it returns a `username` (the app key).
//! 4. Persist the new bridge to `config/config.toml` via the existing
//!    `HuePluginConfig::save()` + `upsert_bridge_app_key()` path.
//! 5. Push the populated `BridgeTarget` into the runtime through the
//!    `new_bridge_tx` channel so it starts publishing without restart.
//! 6. Emit `item_add({bridge_id, host, name, status: "paired"})` and
//!    `complete({bridge_id, host, app_key})`.
//!
//! Cancel / timeout:
//! - The drawer's Cancel button → `is_canceled()` short-circuits the poll.
//! - 90 s budget (manifest `timeout_ms`) — Hue's link button itself times
//!   out at ~30 s, so we'll typically conclude well before the deadline.

use anyhow::{anyhow, Context, Result};
use plugin_sdk_rs::{ManagementHandle, StreamContext, StreamingAction};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::config::HuePluginConfig;
use crate::hue::api::HueApiClient;
use crate::hue::discovery;
use crate::hue::models::{BridgeTarget, DiscoveredBridge};

const POLL_INTERVAL_SECS: u64 = 2;

/// Cloneable handle bridging the streaming `pair_bridge` action with
/// long-lived plugin state (config file path, runtime channel, discovery
/// settings). One Mutex coordinates concurrent config writes — pairing
/// and any other future config mutator must serialise through it.
#[derive(Clone)]
pub struct PairingHandle {
    config_path: String,
    plugin_cfg: Arc<Mutex<HuePluginConfig>>,
    new_bridge_tx: mpsc::Sender<BridgeTarget>,
}

impl PairingHandle {
    pub fn new(
        config_path: String,
        plugin_cfg: HuePluginConfig,
        new_bridge_tx: mpsc::Sender<BridgeTarget>,
    ) -> Self {
        Self {
            config_path,
            plugin_cfg: Arc::new(Mutex::new(plugin_cfg)),
            new_bridge_tx,
        }
    }
}

/// Register the `pair_bridge` streaming action on a `ManagementHandle`.
pub fn register_actions(mgmt: ManagementHandle, handle: PairingHandle) -> ManagementHandle {
    mgmt.with_streaming_action(StreamingAction::new(
        "pair_bridge",
        move |ctx, params| {
            let h = handle.clone();
            async move { pair_bridge(ctx, params, h).await }
        },
    ))
}

async fn pair_bridge(ctx: StreamContext, params: Value, handle: PairingHandle) -> Result<()> {
    ctx.progress(Some(0), Some("starting"), Some("Resolving target bridge"))
        .await?;

    let host_param = params
        .get("host")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let name_override = params
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let target = match resolve_target(&handle, host_param, name_override).await {
        Ok(t) => t,
        Err(e) => return ctx.error(format!("could not resolve target bridge: {e}")).await,
    };
    ctx.progress(
        Some(20),
        Some("ready"),
        Some(&format!(
            "Press the link button on the Hue bridge at {} now",
            target.host
        )),
    )
    .await?;

    // Build a probe API client (no app_key yet) and poll until the button
    // is pressed or we time out / get cancelled.
    let api = HueApiClient::new(target.clone());
    let app_key = match poll_for_app_key(&ctx, &api).await? {
        Some(k) => k,
        None => return Ok(()), // canceled — terminal already emitted
    };

    ctx.progress(
        Some(80),
        Some("configured"),
        Some("Bridge paired; persisting to config and starting up"),
    )
    .await?;

    // Persist app_key to config.toml. Failure here is a hard error — the
    // user's session will work for the rest of this run, but a restart
    // would lose the pairing, which is worse than failing loudly now.
    let mut paired_target = target.clone();
    paired_target.app_key = Some(app_key.clone());
    if let Err(e) = persist_app_key(&handle, &paired_target, &app_key).await {
        return ctx
            .error(format!("paired bridge but failed to save config: {e}"))
            .await;
    }

    // Hand the populated target to the runtime so it starts publishing
    // immediately. Best-effort — if the runtime channel is full or
    // closed we still consider the pairing successful (the next plugin
    // restart will pick up the saved config).
    if let Err(e) = handle.new_bridge_tx.send(paired_target.clone()).await {
        warn!(error = %e, "failed to forward paired bridge to runtime; restart will pick it up");
    }

    let _ = ctx
        .item_add(json!({
            "bridge_id": paired_target.bridge_id,
            "host": paired_target.host,
            "name": paired_target.name,
            "status": "paired",
        }))
        .await;

    ctx.complete(json!({
        "bridge_id": paired_target.bridge_id,
        "host": paired_target.host,
        "name": paired_target.name,
        // Don't emit the raw app_key in the terminal payload — it's a
        // long-lived credential. Saved to config.toml; that's enough.
    }))
    .await
}

/// Walk the params (or run discovery) to settle on a single
/// `BridgeTarget` to attempt pairing against.
async fn resolve_target(
    handle: &PairingHandle,
    host_param: Option<String>,
    name_override: Option<String>,
) -> Result<BridgeTarget> {
    if let Some(host) = host_param {
        // User specified a host — probe it for bridge_id so we can match
        // existing config entries on next save.
        let probe_target = BridgeTarget {
            name: name_override.clone().unwrap_or_else(|| host.clone()),
            bridge_id: String::new(),
            host: host.clone(),
            app_key: None,
            verify_tls: true,
            allow_self_signed: true,
        };
        let probe = HueApiClient::new(probe_target);
        let summary = probe
            .fetch_bridge_summary()
            .await
            .with_context(|| format!("probing {host} for bridge config"))?;
        let bridge_id = summary
            .get("bridge_config")
            .and_then(|c| c.get("bridgeid"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let detected_name = summary
            .get("bridge_config")
            .and_then(|c| c.get("name"))
            .and_then(Value::as_str)
            .map(str::to_string);
        return Ok(BridgeTarget {
            name: name_override
                .or(detected_name)
                .unwrap_or_else(|| host.clone()),
            bridge_id,
            host,
            app_key: None,
            verify_tls: true,
            allow_self_signed: true,
        });
    }

    // No host given — discover and pick a bridge that isn't already
    // listed in config (no point re-pairing one we already have keys for).
    let cfg_snapshot = handle.plugin_cfg.lock().await.clone();
    let discovered = discovery::discover_bridges(&cfg_snapshot.hue)
        .await
        .context("discovery failed")?;
    let candidate = discovered
        .into_iter()
        .find(|d| !is_already_configured(&cfg_snapshot, d))
        .ok_or_else(|| anyhow!("no unpaired bridges discovered on the network"))?;

    Ok(BridgeTarget {
        name: name_override.unwrap_or(candidate.name.clone()),
        bridge_id: candidate.bridge_id.clone(),
        host: candidate.host.clone(),
        app_key: None,
        verify_tls: true,
        allow_self_signed: true,
    })
}

fn is_already_configured(cfg: &HuePluginConfig, d: &DiscoveredBridge) -> bool {
    cfg.bridges.iter().any(|b| {
        (!b.bridge_id.is_empty() && b.bridge_id == d.bridge_id)
            || (!b.host.is_empty() && b.host == d.host)
    })
}

/// Poll `pair_bridge` every `POLL_INTERVAL_SECS` until the user presses
/// the link button or the action is cancelled. `None` return means
/// cancelled — the caller has already emitted the terminal stage.
async fn poll_for_app_key(
    ctx: &StreamContext,
    api: &HueApiClient,
) -> Result<Option<String>> {
    let mut tick = 0u32;
    loop {
        if ctx.is_canceled() {
            ctx.canceled().await?;
            return Ok(None);
        }
        match api.pair_bridge("homecore#desktop").await {
            Ok(app_key) => return Ok(Some(app_key)),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("link button not pressed") {
                    tick = tick.saturating_add(1);
                    // Pulse a friendly progress every poll so the user
                    // knows we're alive. Cap percent at 70 — we save
                    // 80%/100% for after the success.
                    let pct = (20 + (tick * 5)).min(70) as u8;
                    ctx.progress(
                        Some(pct),
                        Some("waiting"),
                        Some("Still waiting — press the link button on the bridge"),
                    )
                    .await?;
                } else {
                    info!(error = %msg, "Hue pairing returned non-recoverable error");
                    return Err(e);
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
    }
}

/// Lock + load + mutate + write. Ensures concurrent pairings on
/// different bridges don't race the toml file.
async fn persist_app_key(
    handle: &PairingHandle,
    target: &BridgeTarget,
    app_key: &str,
) -> Result<()> {
    let mut guard = handle.plugin_cfg.lock().await;
    // Reload from disk in case the user edited config.toml between
    // plugin start and now. This is best-effort — if the file's
    // unreadable, we still have the in-memory copy.
    if let Ok(fresh) = HuePluginConfig::load(&handle.config_path) {
        *guard = fresh;
    }
    guard.upsert_bridge_app_key(target, app_key);
    guard.save(&handle.config_path)
}
