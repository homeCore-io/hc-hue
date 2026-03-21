use anyhow::Result;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::commands::{parse_homecore_command, PluginCommand};
use crate::config::HuePluginConfig;
use crate::homecore::HomecorePublisher;
use crate::hue::api::{EventstreamSignal, HueApiClient};
use crate::hue::models::{AccessoryCommand, BridgeTarget, LightCommand};
use crate::hue::registry::{HueRegistry, RegisteredLight};
use crate::sync;
use crate::translator;

pub struct Bridge {
    cfg: HuePluginConfig,
    publisher: HomecorePublisher,
    apis: Vec<HueApiClient>,
    registry: HueRegistry,
    command_started_at: Option<Instant>,
    eventstream_refresh_signal_total: u64,
    eventstream_incremental_applied_total: u64,
    eventstream_incremental_applied_by_bridge: HashMap<String, u64>,
    eventstream_fallback_refresh_total: u64,
    eventstream_fallback_refresh_by_bridge: HashMap<String, u64>,
    eventstream_refresh_signal_recent: u64,
    eventstream_incremental_applied_recent: u64,
    eventstream_incremental_applied_recent_by_bridge: HashMap<String, u64>,
    eventstream_fallback_refresh_recent: u64,
    eventstream_fallback_refresh_recent_by_bridge: HashMap<String, u64>,
}

impl Bridge {
    pub fn new(cfg: HuePluginConfig, bridges: Vec<BridgeTarget>, publisher: HomecorePublisher) -> Self {
        let apis = bridges.into_iter().map(HueApiClient::new).collect();
        Self {
            cfg,
            publisher,
            apis,
            registry: HueRegistry::default(),
            command_started_at: None,
            eventstream_refresh_signal_total: 0,
            eventstream_incremental_applied_total: 0,
            eventstream_incremental_applied_by_bridge: HashMap::new(),
            eventstream_fallback_refresh_total: 0,
            eventstream_fallback_refresh_by_bridge: HashMap::new(),
            eventstream_refresh_signal_recent: 0,
            eventstream_incremental_applied_recent: 0,
            eventstream_incremental_applied_recent_by_bridge: HashMap::new(),
            eventstream_fallback_refresh_recent: 0,
            eventstream_fallback_refresh_recent_by_bridge: HashMap::new(),
        }
    }

    pub async fn run(mut self, mut hc_rx: mpsc::Receiver<(String, Value)>) -> Result<()> {
        info!(bridges = self.apis.len(), "Hue bridge runtime started");

        let (event_tx, mut event_rx) = mpsc::channel::<EventstreamSignal>(128);

        if self.cfg.hue.eventstream_enabled {
            for api in &self.apis {
                if !api.has_app_key() {
                    continue;
                }
                let api = api.clone();
                let tx = event_tx.clone();
                let reconnect_secs = self.cfg.hue.eventstream_reconnect_secs;
                tokio::spawn(async move {
                    let _ = api.run_eventstream(tx, reconnect_secs).await;
                });
            }
        }

        for api in &self.apis {
            sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await?;
        }

        let mut resync_tick = tokio::time::interval(Duration::from_secs(
            self.cfg.hue.resync_interval_secs.max(5),
        ));
        let mut heartbeat_tick = tokio::time::interval(Duration::from_secs(
            self.cfg.hue.heartbeat_secs.max(5),
        ));

        loop {
            tokio::select! {
                msg = hc_rx.recv() => {
                    let Some((device_id, payload)) = msg else {
                        info!("HomeCore command channel closed; stopping hc-hue runtime");
                        break;
                    };
                    if let Err(e) = self.handle_homecore_command(&device_id, payload).await {
                        warn!(device_id, error = %e, "Command handling failed");
                    }
                }
                signal_msg = event_rx.recv() => {
                    let Some(signal) = signal_msg else {
                        continue;
                    };
                    match signal {
                        EventstreamSignal::Refresh { bridge_id } => {
                            self.eventstream_refresh_signal_total += 1;
                            self.eventstream_refresh_signal_recent += 1;
                            if let Some(api) = self.apis.iter().find(|a| a.target().bridge_id == bridge_id) {
                                sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await?;
                            }
                        }
                        EventstreamSignal::Data { bridge_id, payload } => {
                            if let Some(api) = self
                                .apis
                                .iter()
                                .find(|a| a.target().bridge_id == bridge_id)
                                .cloned()
                            {
                                let applied = sync::apply_eventstream_update(
                                    &self.publisher,
                                    &self.registry,
                                    &bridge_id,
                                    &payload,
                                ).await?;
                                if applied {
                                    self.record_eventstream_incremental_applied(&bridge_id);
                                } else {
                                    self.record_eventstream_fallback_refresh(&bridge_id);
                                    sync::refresh_bridge_state(&self.publisher, &mut self.registry, &api).await?;
                                }
                            }
                        }
                    }
                }
                _ = resync_tick.tick() => {
                    for api in &self.apis {
                        sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await?;
                    }
                }
                _ = heartbeat_tick.tick() => {
                    self.publisher.publish_plugin_status("active").await?;
                    let fallback_ratio = Self::fallback_ratio_pct(
                        self.eventstream_incremental_applied_total,
                        self.eventstream_fallback_refresh_total,
                    );
                    let recent_fallback_ratio = Self::fallback_ratio_pct(
                        self.eventstream_incremental_applied_recent,
                        self.eventstream_fallback_refresh_recent,
                    );
                    info!(
                        bridges_total = self.registry.bridge_count(),
                        bridges_online = self.registry.online_count(),
                        lights_total = self.registry.light_count(),
                        groups_total = self.registry.group_count(),
                        scenes_total = self.registry.scene_count(),
                        aux_total = self.registry.aux_count(),
                        eventstream_enabled = self.cfg.hue.eventstream_enabled,
                        eventstream_refresh_signal_total = self.eventstream_refresh_signal_total,
                        eventstream_incremental_applied_total = self.eventstream_incremental_applied_total,
                        eventstream_incremental_applied_bridges = self.eventstream_incremental_applied_by_bridge.len(),
                        eventstream_fallback_refresh_total = self.eventstream_fallback_refresh_total,
                        eventstream_fallback_ratio_pct = fallback_ratio,
                        eventstream_fallback_refresh_bridges = self.eventstream_fallback_refresh_by_bridge.len(),
                        eventstream_refresh_signal_recent = self.eventstream_refresh_signal_recent,
                        eventstream_incremental_applied_recent = self.eventstream_incremental_applied_recent,
                        eventstream_incremental_applied_recent_bridges = self.eventstream_incremental_applied_recent_by_bridge.len(),
                        eventstream_fallback_refresh_recent = self.eventstream_fallback_refresh_recent,
                        eventstream_fallback_refresh_recent_bridges = self.eventstream_fallback_refresh_recent_by_bridge.len(),
                        eventstream_fallback_ratio_recent_pct = recent_fallback_ratio,
                        state_bytes = self.registry.total_summary_bytes(),
                        "Hue plugin heartbeat"
                    );

                    let metrics = json!({
                        "plugin_id": self.publisher.plugin_id(),
                        "eventstream_enabled": self.cfg.hue.eventstream_enabled,
                        "eventstream_refresh_signal_total": self.eventstream_refresh_signal_total,
                        "eventstream_incremental_applied_total": self.eventstream_incremental_applied_total,
                        "eventstream_incremental_applied_by_bridge": self.eventstream_incremental_applied_by_bridge,
                        "eventstream_fallback_refresh_total": self.eventstream_fallback_refresh_total,
                        "eventstream_fallback_ratio_pct": fallback_ratio,
                        "eventstream_fallback_refresh_by_bridge": self.eventstream_fallback_refresh_by_bridge,
                        "eventstream_refresh_signal_recent": self.eventstream_refresh_signal_recent,
                        "eventstream_incremental_applied_recent": self.eventstream_incremental_applied_recent,
                        "eventstream_incremental_applied_recent_by_bridge": self.eventstream_incremental_applied_recent_by_bridge,
                        "eventstream_fallback_refresh_recent": self.eventstream_fallback_refresh_recent,
                        "eventstream_fallback_refresh_recent_by_bridge": self.eventstream_fallback_refresh_recent_by_bridge,
                        "eventstream_fallback_ratio_recent_pct": recent_fallback_ratio,
                    });
                    if let Err(e) = self.publisher.publish_event("plugin_metrics", &metrics).await {
                        warn!(error = %e, "Failed to publish plugin_metrics event");
                    }

                    self.reset_recent_eventstream_metrics();
                }
            }
        }

        self.publisher.publish_plugin_status("offline").await?;
        Ok(())
    }

    async fn handle_homecore_command(&mut self, device_id: &str, payload: Value) -> Result<()> {
        let cmd = parse_homecore_command(payload);
        self.command_started_at = Some(Instant::now());
        if let Some(api) = self.apis.iter().find(|api| api.target().device_id() == device_id) {
            match cmd {
                PluginCommand::Refresh => {
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "refresh", true, None).await;
                }
                PluginCommand::SetAvailability(online) => {
                    if let Err(e) = self.publisher.publish_availability(device_id, online).await {
                        self.observe_command_result(device_id, "set_availability", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "set_availability", true, None).await;
                }
                PluginCommand::Raw(raw) => {
                    if let Err(e) = api.execute_raw_command(&raw).await {
                        self.observe_command_result(device_id, "raw", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "raw", true, None).await;
                }
                PluginCommand::SetLightState(_) => {
                    warn!(device_id, "Ignoring light-state command sent to bridge device");
                    self.observe_command_result(device_id, "set_light_state", false, Some("unsupported target type")).await;
                }
                PluginCommand::ActivateScene { .. } => {
                    warn!(device_id, "Ignoring scene activation command sent to bridge device");
                    self.observe_command_result(device_id, "activate_scene", false, Some("unsupported target type")).await;
                }
                PluginCommand::SetAccessoryState(_) => {
                    warn!(device_id, "Ignoring accessory-state command sent to bridge device");
                    self.observe_command_result(device_id, "set_accessory_state", false, Some("unsupported target type")).await;
                }
                PluginCommand::SetEntertainmentActive { .. } => {
                    warn!(device_id, "Ignoring entertainment command sent to bridge device");
                    self.observe_command_result(device_id, "set_entertainment_active", false, Some("unsupported target type")).await;
                }
            }

            return Ok(());
        }

        if let Some(binding) = self.registry.get_light_binding(device_id) {
            let Some(api) = self
                .apis
                .iter()
                .find(|api| api.target().bridge_id == binding.bridge_id)
            else {
                warn!(device_id, bridge_id = %binding.bridge_id, "No Hue API client for light's bridge");
                return Ok(());
            };

            match cmd {
                PluginCommand::SetLightState(light_cmd) => {
                    if let Err(e) = Self::validate_light_command(binding, &light_cmd) {
                        self.observe_command_result(device_id, "set_light_state", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    if let Err(e) = api.execute_light_command(&binding.light_rid, &light_cmd).await {
                        self.observe_command_result(device_id, "set_light_state", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "set_light_state", true, None).await;
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh_after_command", false, Some(&e.to_string())).await;
                    }
                }
                PluginCommand::Refresh => {
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "refresh", true, None).await;
                }
                PluginCommand::Raw(raw) => {
                    if let Err(e) = api.execute_raw_command(&raw).await {
                        self.observe_command_result(device_id, "raw", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "raw", true, None).await;
                }
                PluginCommand::SetAvailability(online) => {
                    if let Err(e) = self.publisher.publish_availability(device_id, online).await {
                        self.observe_command_result(device_id, "set_availability", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "set_availability", true, None).await;
                }
                PluginCommand::ActivateScene { scene_id } => {
                    if let Some(scene_rid) = scene_id {
                        if let Err(e) = api.activate_scene(&scene_rid).await {
                            self.observe_command_result(device_id, "activate_scene", false, Some(&e.to_string())).await;
                            return Ok(());
                        }
                        self.observe_command_result(device_id, "activate_scene", true, None).await;
                        self.emit_scene_activated_event(device_id, &scene_rid, "light_device_command").await;
                        if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                            self.observe_command_result(device_id, "refresh_after_command", false, Some(&e.to_string())).await;
                        }
                    } else {
                        warn!(device_id, "activate_scene requires scene_id when sent to light device");
                        self.observe_command_result(device_id, "activate_scene", false, Some("missing scene_id")).await;
                    }
                }
                PluginCommand::SetAccessoryState(_) => {
                    warn!(device_id, "Ignoring accessory-state command sent to light device");
                    self.observe_command_result(device_id, "set_accessory_state", false, Some("unsupported target type")).await;
                }
                PluginCommand::SetEntertainmentActive { .. } => {
                    warn!(device_id, "Ignoring entertainment command sent to light device");
                    self.observe_command_result(device_id, "set_entertainment_active", false, Some("unsupported target type")).await;
                }
            }

            return Ok(());
        }

        if let Some(binding) = self.registry.get_group_binding(device_id) {
            let Some(api) = self
                .apis
                .iter()
                .find(|api| api.target().bridge_id == binding.bridge_id)
            else {
                warn!(device_id, bridge_id = %binding.bridge_id, "No Hue API client for grouped-light bridge");
                return Ok(());
            };

            match cmd {
                PluginCommand::SetLightState(group_cmd) => {
                    if let Err(e) = Self::validate_grouped_light_command(&group_cmd) {
                        self.observe_command_result(device_id, "set_group_state", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    if let Err(e) = api.execute_grouped_light_command(&binding.group_rid, &group_cmd).await {
                        self.observe_command_result(device_id, "set_group_state", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "set_group_state", true, None).await;
                    self.emit_group_action_event(device_id, &binding.group_rid, "group_device_command").await;
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh_after_command", false, Some(&e.to_string())).await;
                    }
                }
                PluginCommand::Refresh => {
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "refresh", true, None).await;
                }
                PluginCommand::SetAvailability(online) => {
                    if let Err(e) = self.publisher.publish_availability(device_id, online).await {
                        self.observe_command_result(device_id, "set_availability", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "set_availability", true, None).await;
                }
                PluginCommand::Raw(raw) => {
                    if let Err(e) = api.execute_raw_command(&raw).await {
                        self.observe_command_result(device_id, "raw", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "raw", true, None).await;
                }
                PluginCommand::ActivateScene { scene_id } => {
                    if let Some(scene_rid) = scene_id {
                        if let Err(e) = api.activate_scene(&scene_rid).await {
                            self.observe_command_result(device_id, "activate_scene", false, Some(&e.to_string())).await;
                            return Ok(());
                        }
                        self.observe_command_result(device_id, "activate_scene", true, None).await;
                        self.emit_scene_activated_event(device_id, &scene_rid, "group_device_command").await;
                        if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                            self.observe_command_result(device_id, "refresh_after_command", false, Some(&e.to_string())).await;
                        }
                    } else {
                        warn!(device_id, "activate_scene requires scene_id when sent to grouped-light device");
                        self.observe_command_result(device_id, "activate_scene", false, Some("missing scene_id")).await;
                    }
                }
                PluginCommand::SetAccessoryState(_) => {
                    warn!(device_id, "Ignoring accessory-state command sent to grouped-light device");
                    self.observe_command_result(device_id, "set_accessory_state", false, Some("unsupported target type")).await;
                }
                PluginCommand::SetEntertainmentActive { .. } => {
                    warn!(device_id, "Ignoring entertainment command sent to grouped-light device");
                    self.observe_command_result(device_id, "set_entertainment_active", false, Some("unsupported target type")).await;
                }
            }

            return Ok(());
        }

        if let Some(binding) = self.registry.get_scene_binding(device_id) {
            let Some(api) = self
                .apis
                .iter()
                .find(|api| api.target().bridge_id == binding.bridge_id)
            else {
                warn!(device_id, bridge_id = %binding.bridge_id, "No Hue API client for scene bridge");
                return Ok(());
            };

            match cmd {
                PluginCommand::ActivateScene { scene_id } => {
                    let target_scene = scene_id.unwrap_or_else(|| binding.scene_rid.clone());
                    if let Err(e) = api.activate_scene(&target_scene).await {
                        self.observe_command_result(device_id, "activate_scene", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "activate_scene", true, None).await;
                    self.emit_scene_activated_event(device_id, &target_scene, "scene_device_command").await;
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh_after_command", false, Some(&e.to_string())).await;
                    }
                }
                PluginCommand::Refresh => {
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "refresh", true, None).await;
                }
                PluginCommand::Raw(raw) => {
                    if let Err(e) = api.execute_raw_command(&raw).await {
                        self.observe_command_result(device_id, "raw", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "raw", true, None).await;
                }
                PluginCommand::SetAvailability(online) => {
                    if let Err(e) = self.publisher.publish_availability(device_id, online).await {
                        self.observe_command_result(device_id, "set_availability", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "set_availability", true, None).await;
                }
                PluginCommand::SetLightState(_) => {
                    warn!(device_id, "Ignoring light-state command sent to scene device");
                    self.observe_command_result(device_id, "set_light_state", false, Some("unsupported target type")).await;
                }
                PluginCommand::SetAccessoryState(_) => {
                    warn!(device_id, "Ignoring accessory-state command sent to scene device");
                    self.observe_command_result(device_id, "set_accessory_state", false, Some("unsupported target type")).await;
                }
                PluginCommand::SetEntertainmentActive { .. } => {
                    warn!(device_id, "Ignoring entertainment command sent to scene device");
                    self.observe_command_result(device_id, "set_entertainment_active", false, Some("unsupported target type")).await;
                }
            }

            return Ok(());
        }

        if let Some(binding) = self.registry.get_aux_binding(device_id) {
            let Some(api) = self
                .apis
                .iter()
                .find(|api| api.target().bridge_id == binding.bridge_id)
            else {
                warn!(device_id, bridge_id = %binding.bridge_id, "No Hue API client for auxiliary resource bridge");
                return Ok(());
            };

            match cmd {
                PluginCommand::SetAccessoryState(accessory_cmd) => {
                    if let Err(e) = Self::validate_accessory_command(&binding.resource_type, &accessory_cmd) {
                        self.observe_command_result(device_id, "set_accessory_state", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    if let Err(e) = api
                        .execute_accessory_command(
                            &binding.resource_type,
                            &binding.resource_rid,
                            &accessory_cmd,
                        )
                        .await
                    {
                        self.observe_command_result(device_id, "set_accessory_state", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "set_accessory_state", true, None).await;
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh_after_command", false, Some(&e.to_string())).await;
                    }
                }
                PluginCommand::Refresh => {
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "refresh", true, None).await;
                }
                PluginCommand::Raw(raw) => {
                    if let Err(e) = api.execute_raw_command(&raw).await {
                        self.observe_command_result(device_id, "raw", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "raw", true, None).await;
                }
                PluginCommand::SetAvailability(online) => {
                    if let Err(e) = self.publisher.publish_availability(device_id, online).await {
                        self.observe_command_result(device_id, "set_availability", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self.observe_command_result(device_id, "set_availability", true, None).await;
                }
                PluginCommand::SetLightState(_) => {
                    warn!(device_id, "Ignoring light-state command sent to auxiliary device");
                    self.observe_command_result(device_id, "set_light_state", false, Some("unsupported target type")).await;
                }
                PluginCommand::ActivateScene { .. } => {
                    warn!(device_id, "Ignoring scene activation command sent to auxiliary device");
                    self.observe_command_result(device_id, "activate_scene", false, Some("unsupported target type")).await;
                }
                PluginCommand::SetEntertainmentActive { config_id, active } => {
                    if let Err(e) = Self::validate_entertainment_target(&binding.resource_type) {
                        let target_config = config_id
                            .as_deref()
                            .unwrap_or(&binding.resource_rid)
                            .to_string();
                        self
                            .publish_entertainment_command_context(
                                device_id,
                                &target_config,
                                active,
                                false,
                                Some(&e.to_string()),
                            )
                            .await;
                        self.observe_command_result(device_id, "set_entertainment_active", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    let target_config = config_id.unwrap_or_else(|| binding.resource_rid.clone());
                    if let Err(e) = api.execute_entertainment_command(&target_config, active).await {
                        self
                            .publish_entertainment_command_context(
                                device_id,
                                &target_config,
                                active,
                                false,
                                Some(&e.to_string()),
                            )
                            .await;
                        self.observe_command_result(device_id, "set_entertainment_active", false, Some(&e.to_string())).await;
                        return Ok(());
                    }
                    self
                        .publish_entertainment_command_context(
                            device_id,
                            &target_config,
                            active,
                            true,
                            None,
                        )
                        .await;
                    self.observe_command_result(device_id, "set_entertainment_active", true, None).await;
                    self
                        .emit_entertainment_action_event(
                            device_id,
                            &target_config,
                            active,
                            "entertainment_device_command",
                        )
                        .await;
                    if let Err(e) = sync::refresh_bridge_state(&self.publisher, &mut self.registry, api).await {
                        self.observe_command_result(device_id, "refresh_after_command", false, Some(&e.to_string())).await;
                    }
                }
            }

            return Ok(());
        }

        warn!(device_id, "Command for unknown Hue device");
        self.observe_command_result(device_id, "unknown", false, Some("unknown device_id")).await;

        Ok(())
    }

    fn validate_accessory_command(resource_type: &str, command: &AccessoryCommand) -> Result<()> {
        if command.enabled.is_none() && command.motion_sensitivity.is_none() {
            anyhow::bail!("empty accessory command");
        }

        if command.motion_sensitivity.is_some() && resource_type != "motion" {
            anyhow::bail!(
                "motion_sensitivity is only supported for motion resources (target type: {resource_type})"
            );
        }

        if command.enabled.is_some()
            && !matches!(resource_type, "motion" | "temperature" | "light_level" | "contact")
        {
            anyhow::bail!(
                "enabled is not supported for accessory resource type: {resource_type}"
            );
        }

        Ok(())
    }

    fn validate_light_command(binding: &RegisteredLight, command: &LightCommand) -> Result<()> {
        if command.on.is_none()
            && command.brightness_pct.is_none()
            && command.color_temp_mirek.is_none()
            && command.color_xy.is_none()
            && command.effect.is_none()
            && command.dynamic_speed.is_none()
            && command.gradient_points.is_none()
            && command.identify.is_none()
        {
            anyhow::bail!("empty light command");
        }

        if command.gradient_points.is_some() && !binding.supports_gradient {
            anyhow::bail!("gradient_points not supported for this light");
        }

        if command.identify == Some(true) && !binding.supports_identify {
            anyhow::bail!("identify not supported for this light");
        }

        if let Some(effect) = &command.effect {
            if !binding.effect_values.is_empty() && !binding.effect_values.iter().any(|v| v == effect) {
                anyhow::bail!("effect '{effect}' not supported for this light");
            }
        }

        Ok(())
    }

    fn validate_grouped_light_command(command: &LightCommand) -> Result<()> {
        if command.on.is_none() && command.brightness_pct.is_none() {
            anyhow::bail!("no writable fields for grouped_light command");
        }

        if command.color_temp_mirek.is_some()
            || command.color_xy.is_some()
            || command.effect.is_some()
            || command.dynamic_speed.is_some()
            || command.gradient_points.is_some()
            || command.identify.is_some()
        {
            anyhow::bail!("grouped_light does not support advanced light fields");
        }

        Ok(())
    }

    fn validate_entertainment_target(resource_type: &str) -> Result<()> {
        if resource_type != "entertainment_configuration" {
            anyhow::bail!(
                "entertainment command is only supported for entertainment_configuration resources (target type: {resource_type})"
            );
        }
        Ok(())
    }

    fn record_eventstream_fallback_refresh(&mut self, bridge_id: &str) {
        self.eventstream_fallback_refresh_total += 1;
        self.eventstream_fallback_refresh_recent += 1;
        *self
            .eventstream_fallback_refresh_by_bridge
            .entry(bridge_id.to_string())
            .or_insert(0) += 1;
        *self
            .eventstream_fallback_refresh_recent_by_bridge
            .entry(bridge_id.to_string())
            .or_insert(0) += 1;
    }

    fn record_eventstream_incremental_applied(&mut self, bridge_id: &str) {
        self.eventstream_incremental_applied_total += 1;
        self.eventstream_incremental_applied_recent += 1;
        *self
            .eventstream_incremental_applied_by_bridge
            .entry(bridge_id.to_string())
            .or_insert(0) += 1;
        *self
            .eventstream_incremental_applied_recent_by_bridge
            .entry(bridge_id.to_string())
            .or_insert(0) += 1;
    }

    fn reset_recent_eventstream_metrics(&mut self) {
        self.eventstream_refresh_signal_recent = 0;
        self.eventstream_incremental_applied_recent = 0;
        self.eventstream_fallback_refresh_recent = 0;
        self.eventstream_incremental_applied_recent_by_bridge.clear();
        self.eventstream_fallback_refresh_recent_by_bridge.clear();
    }

    fn fallback_ratio_pct(incremental_applied_total: u64, fallback_refresh_total: u64) -> f64 {
        let base = incremental_applied_total + fallback_refresh_total;
        if base == 0 {
            0.0
        } else {
            (fallback_refresh_total as f64 / base as f64) * 100.0
        }
    }

    async fn observe_command_result(
        &self,
        device_id: &str,
        operation: &str,
        success: bool,
        error: Option<&str>,
    ) {
        let error_code = Self::classify_command_error(error);
        let latency_ms = self
            .command_started_at
            .as_ref()
            .map(|t| t.elapsed().as_millis())
            .unwrap_or(0);
        let retry_count = 0;
        let patch = translator::command_result_patch_with_metrics(
            operation,
            success,
            error,
            error_code,
            latency_ms,
            retry_count,
        );
        if let Err(e) = self.publisher.publish_state_partial(device_id, &patch).await {
            warn!(device_id, operation, error = %e, "Failed to publish command result state patch");
        }

        let payload = translator::command_result_event(
            self.publisher.plugin_id(),
            device_id,
            operation,
            success,
            error,
            error_code,
            latency_ms,
            retry_count,
        );
        if let Err(e) = self.publisher.publish_event("plugin_command_result", &payload).await {
            warn!(device_id, operation, error = %e, "Failed to publish command result event");
        }
    }

    fn classify_command_error(error: Option<&str>) -> Option<&'static str> {
        let err = error?.to_ascii_lowercase();

        if err.contains("unknown device_id") {
            return Some("unknown_device");
        }
        if err.contains("unsupported target type") {
            return Some("unsupported_target_type");
        }
        if err.contains("missing scene_id") {
            return Some("missing_required_field");
        }
        if err.contains("empty accessory command") {
            return Some("empty_command");
        }
        if err.contains("empty light command") {
            return Some("empty_command");
        }
        if err.contains("only supported for") || err.contains("not supported for") {
            return Some("unsupported_field_for_resource");
        }
        if err.contains("no writable fields") {
            return Some("no_writable_fields");
        }
        if err.contains("failed") || err.contains("request") {
            return Some("execution_failed");
        }

        Some("command_error")
    }

    async fn emit_scene_activated_event(&self, device_id: &str, scene_id: &str, source: &str) {
        let payload = translator::scene_activated_event(
            self.publisher.plugin_id(),
            device_id,
            scene_id,
            source,
        );
        if let Err(e) = self.publisher.publish_event("scene_activated", &payload).await {
            warn!(device_id, scene_id, error = %e, "Failed to publish scene_activated event");
        }
    }

    async fn emit_group_action_event(&self, device_id: &str, group_id: &str, source: &str) {
        let payload = translator::group_action_event(
            self.publisher.plugin_id(),
            device_id,
            group_id,
            source,
        );
        if let Err(e) = self.publisher.publish_event("group_action_applied", &payload).await {
            warn!(device_id, group_id, error = %e, "Failed to publish group_action_applied event");
        }
    }

    async fn emit_entertainment_action_event(
        &self,
        device_id: &str,
        config_id: &str,
        active: bool,
        source: &str,
    ) {
        let payload = translator::entertainment_action_event(
            self.publisher.plugin_id(),
            device_id,
            config_id,
            active,
            source,
        );
        if let Err(e) = self
            .publisher
            .publish_event("entertainment_action_applied", &payload)
            .await
        {
            warn!(device_id, config_id, error = %e, "Failed to publish entertainment_action_applied event");
        }
    }

    async fn publish_entertainment_command_context(
        &self,
        device_id: &str,
        config_id: &str,
        active: bool,
        success: bool,
        error: Option<&str>,
    ) {
        let action = if active { "start" } else { "stop" };
        let mut patch = json!({
            "last_entertainment_command_config_id": config_id,
            "last_entertainment_command_action": action,
            "last_entertainment_command_active": active,
            "last_entertainment_command_success": success,
        });
        if let Some(err) = error {
            patch["last_entertainment_command_error"] = json!(err);
        } else {
            patch["last_entertainment_command_error"] = Value::Null;
        }

        if let Err(e) = self.publisher.publish_state_partial(device_id, &patch).await {
            warn!(device_id, config_id, error = %e, "Failed to publish entertainment command context patch");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_motion_sensitivity_only_for_motion() {
        let cmd = AccessoryCommand {
            enabled: None,
            motion_sensitivity: Some(1),
        };

        assert!(Bridge::validate_accessory_command("motion", &cmd).is_ok());
        assert!(Bridge::validate_accessory_command("temperature", &cmd).is_err());
    }

    #[test]
    fn validates_enabled_resource_support() {
        let cmd = AccessoryCommand {
            enabled: Some(true),
            motion_sensitivity: None,
        };

        assert!(Bridge::validate_accessory_command("motion", &cmd).is_ok());
        assert!(Bridge::validate_accessory_command("light_level", &cmd).is_ok());
        assert!(Bridge::validate_accessory_command("device_power", &cmd).is_err());
    }

    #[test]
    fn rejects_empty_accessory_command() {
        let cmd = AccessoryCommand {
            enabled: None,
            motion_sensitivity: None,
        };
        assert!(Bridge::validate_accessory_command("motion", &cmd).is_err());
    }

    #[test]
    fn classifies_common_command_errors() {
        assert_eq!(
            Bridge::classify_command_error(Some("unsupported target type")),
            Some("unsupported_target_type")
        );
        assert_eq!(
            Bridge::classify_command_error(Some("unknown device_id")),
            Some("unknown_device")
        );
        assert_eq!(
            Bridge::classify_command_error(Some("motion_sensitivity is only supported for motion resources")),
            Some("unsupported_field_for_resource")
        );
        assert_eq!(
            Bridge::classify_command_error(Some("empty light command")),
            Some("empty_command")
        );
        assert_eq!(Bridge::classify_command_error(None), None);
    }

    #[test]
    fn fallback_ratio_pct_handles_edge_and_normal_cases() {
        assert_eq!(Bridge::fallback_ratio_pct(0, 0), 0.0);
        assert_eq!(Bridge::fallback_ratio_pct(10, 0), 0.0);
        assert_eq!(Bridge::fallback_ratio_pct(0, 5), 100.0);
        assert_eq!(Bridge::fallback_ratio_pct(3, 1), 25.0);
    }

    #[test]
    fn validates_entertainment_target_type() {
        assert!(Bridge::validate_entertainment_target("entertainment_configuration").is_ok());
        assert!(Bridge::validate_entertainment_target("motion").is_err());
    }

    #[test]
    fn validates_light_command_non_empty() {
        let binding = RegisteredLight {
            bridge_id: "b1".to_string(),
            light_rid: "l1".to_string(),
            supports_gradient: true,
            supports_identify: true,
            effect_values: vec!["candle".to_string()],
        };

        let empty = LightCommand::default();
        assert!(Bridge::validate_light_command(&binding, &empty).is_err());

        let with_effect = LightCommand {
            effect: Some("candle".to_string()),
            ..Default::default()
        };
        assert!(Bridge::validate_light_command(&binding, &with_effect).is_ok());

        let with_identify = LightCommand {
            identify: Some(true),
            ..Default::default()
        };
        assert!(Bridge::validate_light_command(&binding, &with_identify).is_ok());
    }

    #[test]
    fn validates_light_command_against_supported_capabilities() {
        let binding = RegisteredLight {
            bridge_id: "b1".to_string(),
            light_rid: "l1".to_string(),
            supports_gradient: false,
            supports_identify: false,
            effect_values: vec!["prism".to_string()],
        };

        let gradient = LightCommand {
            gradient_points: Some(vec![(0.1, 0.2)]),
            ..Default::default()
        };
        assert!(Bridge::validate_light_command(&binding, &gradient).is_err());

        let identify = LightCommand {
            identify: Some(true),
            ..Default::default()
        };
        assert!(Bridge::validate_light_command(&binding, &identify).is_err());

        let invalid_effect = LightCommand {
            effect: Some("candle".to_string()),
            ..Default::default()
        };
        assert!(Bridge::validate_light_command(&binding, &invalid_effect).is_err());
    }

    #[test]
    fn validates_grouped_light_command_supported_fields_only() {
        let ok = LightCommand {
            on: Some(true),
            ..Default::default()
        };
        assert!(Bridge::validate_grouped_light_command(&ok).is_ok());

        let unsupported = LightCommand {
            effect: Some("candle".to_string()),
            ..Default::default()
        };
        assert!(Bridge::validate_grouped_light_command(&unsupported).is_err());
    }
}
