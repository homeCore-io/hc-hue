use anyhow::Result;
use serde_json::json;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

use crate::config::{HueDisplayConfig, IlluminanceDisplay, TemperatureUnit};
use crate::homecore::{AttributeKind, AttributeSchema, DeviceSchema, HomecorePublisher};
use crate::hue::api::HueApiClient;
use crate::hue::models::{BridgeSnapshot, HueAuxDevice};
use crate::hue::registry::HueRegistry;
use crate::translator;

pub async fn refresh_bridge_state(
    publisher: &HomecorePublisher,
    registry: &mut HueRegistry,
    api: &HueApiClient,
    display_cfg: &HueDisplayConfig,
    compact_motion_facets: bool,
) -> Result<()> {
    let target = api.target();
    let device_id = target.device_id();

    match api.fetch_bridge_summary().await {
        Ok(summary) => {
            let state = translator::bridge_state(target, true, summary.clone());
            publisher.publish_state(&device_id, &state).await?;
            publisher.publish_availability(&device_id, true).await?;
            registry.upsert_bridge(
                device_id.clone(),
                BridgeSnapshot {
                    online: true,
                    summary,
                },
            );
            info!(device_id, "Hue bridge state refreshed");

            let lights = api.fetch_lights().await?;
            let light_owner_to_device = build_light_owner_map(&lights);
            for light in lights {
                if registry.ensure_light(&light) {
                    publisher
                        .register_device_with_capabilities(
                            &light.device_id,
                            &light.name,
                            "light",
                            None,
                            Some(translator::light_capabilities(&light)),
                        )
                        .await?;
                    publisher.subscribe_commands(&light.device_id).await?;
                    // Publish capability schema for the UI.
                    let light_schema = build_light_schema();
                    publisher.publish_device_schema(&light.device_id, &light_schema).await.ok();
                    info!(device_id = %light.device_id, name = %light.name, "Registered Hue light device");
                }

                publisher.publish_availability(&light.device_id, true).await?;
                publisher
                    .publish_state(&light.device_id, &translator::light_state(&light))
                    .await?;
            }

            let groups = api.fetch_grouped_lights().await?;
            for group in groups {
                if registry.ensure_group(&group) {
                    publisher
                        .register_device_with_capabilities(
                            &group.device_id,
                            &group.name,
                            "group",
                            group.area.as_deref(),
                            Some(translator::grouped_light_capabilities()),
                        )
                        .await?;
                    publisher.subscribe_commands(&group.device_id).await?;
                    // Publish capability schema for the UI.
                    let group_schema = build_group_schema();
                    publisher.publish_device_schema(&group.device_id, &group_schema).await.ok();
                    info!(device_id = %group.device_id, name = %group.name, "Registered Hue grouped-light device");
                }

                publisher.publish_availability(&group.device_id, true).await?;
                publisher
                    .publish_state(&group.device_id, &translator::grouped_light_state(&group))
                    .await?;
            }

            let scenes = api.fetch_scenes().await?;
            for scene in scenes {
                if registry.ensure_scene(&scene) {
                    publisher
                        .register_device_with_capabilities(
                            &scene.device_id,
                            &scene.name,
                            "scene",
                            scene.area.as_deref(),
                            Some(translator::scene_capabilities()),
                        )
                        .await?;
                    publisher.subscribe_commands(&scene.device_id).await?;
                    info!(device_id = %scene.device_id, name = %scene.name, "Registered Hue scene device");
                }

                publisher.publish_availability(&scene.device_id, true).await?;
                publisher
                    .publish_state(&scene.device_id, &translator::scene_state(&scene))
                    .await?;
            }

            let aux_devices = api.fetch_aux_devices().await?;
            let motion_owner_to_device = build_motion_owner_map(&aux_devices);
            let mut registered_publish_ids: HashSet<String> = HashSet::new();
            let mut full_state_published_ids: HashSet<String> = HashSet::new();
            for aux in aux_devices {
                let publish_device_id = if compact_motion_facets {
                    compact_publish_device_id(&aux, &motion_owner_to_device, &light_owner_to_device)
                } else {
                    aux.device_id.clone()
                };

                if registry.ensure_aux(&aux, &publish_device_id)
                    && registered_publish_ids.insert(publish_device_id.clone())
                    && !registry.is_primary_device_id(&publish_device_id)
                {
                    publisher
                        .register_device_with_capabilities(
                            &publish_device_id,
                            &aux.name,
                            aux_device_type(&aux.resource_type),
                            None,
                            Some(translator::aux_capabilities(&aux)),
                        )
                        .await?;
                    info!(device_id = %publish_device_id, name = %aux.name, kind = %aux.resource_type, "Registered Hue auxiliary device");
                }

                publisher.publish_availability(&publish_device_id, aux_is_available(&aux)).await?;
                let mut state = translator::aux_state(&aux);
                apply_display_preferences(&mut state, &aux.resource_type, display_cfg);

                let compacted = compact_motion_facets && publish_device_id != aux.device_id;
                if compacted {
                    strip_aux_metadata(&mut state);
                    publisher
                        .publish_state_partial(&publish_device_id, &state)
                        .await?;
                } else if full_state_published_ids.insert(publish_device_id.clone()) {
                    publisher.publish_state(&publish_device_id, &state).await?;
                } else {
                    strip_aux_metadata(&mut state);
                    publisher
                        .publish_state_partial(&publish_device_id, &state)
                        .await?;
                }
            }
        }
        Err(e) => {
            publisher.publish_availability(&device_id, false).await?;
            warn!(device_id, error = %e, "Failed to refresh Hue bridge state");
        }
    }

    Ok(())
}

fn make_attr(kind: AttributeKind, writable: bool, display_name: &str, unit: Option<&str>, min: Option<f64>, max: Option<f64>, step: Option<f64>) -> AttributeSchema {
    AttributeSchema {
        kind,
        writable,
        display_name: Some(display_name.to_string()),
        unit: unit.map(str::to_string),
        min,
        max,
        step,
        options: None,
    }
}

fn build_light_schema() -> DeviceSchema {
    let mut attrs = HashMap::new();
    attrs.insert("on".into(), make_attr(AttributeKind::Bool, true, "Power", None, None, None, None));
    attrs.insert("brightness_pct".into(), make_attr(AttributeKind::Integer, true, "Brightness", Some("%"), Some(1.0), Some(100.0), Some(1.0)));
    attrs.insert("color_temp".into(), make_attr(AttributeKind::ColorTemp, true, "Colour Temperature", Some("K"), Some(2000.0), Some(6535.0), Some(100.0)));
    attrs.insert("color_xy".into(), AttributeSchema {
        kind: AttributeKind::ColorXy,
        writable: true,
        display_name: Some("Colour".into()),
        unit: None, min: None, max: None, step: None, options: None,
    });
    DeviceSchema { attributes: attrs }
}

fn build_group_schema() -> DeviceSchema {
    let mut attrs = HashMap::new();
    attrs.insert("on".into(), make_attr(AttributeKind::Bool, true, "Power", None, None, None, None));
    attrs.insert("brightness_pct".into(), make_attr(AttributeKind::Integer, true, "Brightness", Some("%"), Some(1.0), Some(100.0), Some(1.0)));
    DeviceSchema { attributes: attrs }
}

fn aux_device_type(resource_type: &str) -> &str {
    match resource_type {
        "entertainment_configuration" => "entertainment",
        _ => "sensor",
    }
}

/// Determines the availability to publish for an auxiliary device.
///
/// Motion, contact, temperature, and light_level resources expose an `enabled`
/// field that reflects whether the sensor is active and reachable over Zigbee.
/// When `enabled = false` the sensor is either disabled by the user or has lost
/// connectivity (dead battery, out of range). All other resource types do not
/// expose connectivity state and are published as available unconditionally.
fn aux_is_available(aux: &HueAuxDevice) -> bool {
    match aux.resource_type.as_str() {
        "motion" | "temperature" | "light_level" | "contact" => aux
            .attributes
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        _ => true,
    }
}

pub async fn apply_eventstream_update(
    publisher: &HomecorePublisher,
    registry: &HueRegistry,
    bridge_id: &str,
    payload: &Value,
    display_cfg: &HueDisplayConfig,
    _compact_motion_facets: bool,
) -> Result<bool> {
    let mut applied_any = false;

    if let Some(events) = payload.as_array() {
        for event in events {
            applied_any |= apply_event_item(publisher, registry, bridge_id, event, display_cfg).await?;
        }
        return Ok(applied_any);
    }

    if payload.is_object() {
        applied_any |= apply_event_item(publisher, registry, bridge_id, payload, display_cfg).await?;
    }

    Ok(applied_any)
}

async fn apply_event_item(
    publisher: &HomecorePublisher,
    registry: &HueRegistry,
    bridge_id: &str,
    event: &Value,
    display_cfg: &HueDisplayConfig,
) -> Result<bool> {
    // `applied` means "the event type was recognized and dispatched to a known device".
    // It does NOT require that a non-empty state patch was produced — a keep-alive ping
    // or a connectivity notification with no new data still counts as handled.
    // Returning false triggers an expensive fallback full-refresh; only do that for
    // truly unrecognized event types.
    let mut applied = false;
    let Some(data_items) = event.get("data").and_then(|v| v.as_array()) else {
        return Ok(false);
    };

    for item in data_items {
        let Some(resource_type) = item.get("type").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(rid) = item.get("id").and_then(|v| v.as_str()) else {
            continue;
        };

        match resource_type {
            "light" => {
                if let Some(device_id) = registry.find_light_device_id(bridge_id, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    if let Some(on) = item
                        .get("on")
                        .and_then(|o| o.get("on"))
                        .and_then(|v| v.as_bool())
                    {
                        patch.insert("on".to_string(), json!(on));
                    }
                    if let Some(brightness) = item
                        .get("dimming")
                        .and_then(|d| d.get("brightness"))
                        .and_then(|v| v.as_f64())
                    {
                        patch.insert("brightness_pct".to_string(), json!(brightness));
                    }
                    if let Some(mirek) = item
                        .get("color_temperature")
                        .and_then(|ct| ct.get("mirek"))
                        .and_then(|v| v.as_u64())
                    {
                        patch.insert("color_temp_mirek".to_string(), json!(mirek));
                    }
                    if let Some((x, y)) = item
                        .get("color")
                        .and_then(|c| c.get("xy"))
                        .and_then(|xy| {
                            let x = xy.get("x")?.as_f64()?;
                            let y = xy.get("y")?.as_f64()?;
                            Some((x, y))
                        })
                    {
                        patch.insert("color_xy".to_string(), json!({ "x": x, "y": y }));
                    }
                    if let Some(effect) = item
                        .get("effects")
                        .and_then(|e| e.get("effect"))
                        .and_then(|v| v.as_str())
                    {
                        patch.insert("effect".to_string(), json!(effect));
                    }
                    if let Some(speed) = item
                        .get("dynamics")
                        .and_then(|d| d.get("speed"))
                        .and_then(|v| v.as_f64())
                    {
                        patch.insert("dynamic_speed".to_string(), json!(speed));
                    }
                    if let Some(status) = item
                        .get("dynamics")
                        .and_then(|d| d.get("status"))
                        .and_then(|v| v.as_str())
                    {
                        patch.insert("dynamic_status".to_string(), json!(status));
                    }
                    if let Some(points) = item
                        .get("gradient")
                        .and_then(|g| g.get("points"))
                        .and_then(|v| v.as_array())
                    {
                        let gradient_points = points
                            .iter()
                            .filter_map(|point| {
                                let xy = point.get("color")?.get("xy")?;
                                let x = xy.get("x")?.as_f64()?;
                                let y = xy.get("y")?.as_f64()?;
                                Some(json!({ "x": x, "y": y }))
                            })
                            .collect::<Vec<_>>();
                        if !gradient_points.is_empty() {
                            patch.insert("gradient_points".to_string(), Value::Array(gradient_points));
                        }
                    }

                    if !patch.is_empty() {
                        publisher
                            .publish_state_partial(&device_id, &Value::Object(patch))
                            .await?;
                        applied = true;
                    }
                }
            }
            "grouped_light" => {
                if let Some(device_id) = registry.find_group_device_id(bridge_id, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    if let Some(on) = item
                        .get("on")
                        .and_then(|o| o.get("on"))
                        .and_then(|v| v.as_bool())
                    {
                        patch.insert("on".to_string(), json!(on));
                    }
                    if let Some(brightness) = item
                        .get("dimming")
                        .and_then(|d| d.get("brightness"))
                        .and_then(|v| v.as_f64())
                    {
                        patch.insert("brightness_pct".to_string(), json!(brightness));
                    }

                    if !patch.is_empty() {
                        publisher
                            .publish_state_partial(&device_id, &Value::Object(patch))
                            .await?;
                        applied = true;
                    }
                }
            }
            "scene" => {
                if let Some(device_id) = registry.find_scene_device_id(bridge_id, rid) {
                    applied = true;
                    if let Some(active) = item
                        .get("status")
                        .and_then(|s| s.get("active"))
                        .and_then(|v| v.as_bool())
                    {
                        publisher
                            .publish_state_partial(&device_id, &json!({ "active": active }))
                            .await?;
                        applied = true;
                    }
                }
            }
            "motion" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = item.get("motion").and_then(|m| m.get("motion")).and_then(|v| v.as_bool()) {
                        patch.insert("motion".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("motion_valid").and_then(|v| v.as_bool()) {
                        patch.insert("motion_valid".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("enabled").and_then(|v| v.as_bool()) {
                        patch.insert("enabled".to_string(), json!(v));
                    }
                    if let Some(v) = item
                        .get("sensitivity")
                        .and_then(|s| s.get("sensitivity"))
                        .and_then(|v| v.as_u64())
                    {
                        patch.insert("motion_sensitivity".to_string(), json!(v));
                    }
                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }
                }
            }
            "temperature" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = extract_temperature_c(item) {
                        patch.insert("temperature_c".to_string(), json!(v));
                        patch.insert("temperature_f".to_string(), json!((v * 9.0 / 5.0) + 32.0));
                        patch.insert("temperature_unit".to_string(), json!("C"));
                    }
                    if let Some(v) = extract_temperature_valid(item) {
                        patch.insert("temperature_valid".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("enabled").and_then(|v| v.as_bool()) {
                        patch.insert("enabled".to_string(), json!(v));
                    }
                    apply_display_preferences_to_patch(&mut patch, resource_type, display_cfg);
                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }
                }
            }
            "light_level" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = extract_light_level_raw(item) {
                        patch.insert("illuminance_raw".to_string(), json!(v));
                        if let Some(lux) = light_level_to_lux(v) {
                            patch.insert("illuminance_lux".to_string(), json!(lux));
                        }
                        patch.insert("illuminance_unit".to_string(), json!("lux"));
                    }
                    if let Some(v) = extract_light_level_valid(item) {
                        patch.insert("illuminance_valid".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("enabled").and_then(|v| v.as_bool()) {
                        patch.insert("enabled".to_string(), json!(v));
                    }
                    apply_display_preferences_to_patch(&mut patch, resource_type, display_cfg);
                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }
                }
            }
            "contact" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = item.get("contact_report").and_then(|c| c.get("state")).and_then(|v| v.as_str()) {
                        patch.insert("contact_state".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("tampered").and_then(|v| v.as_bool()) {
                        patch.insert("tampered".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("enabled").and_then(|v| v.as_bool()) {
                        patch.insert("enabled".to_string(), json!(v));
                    }
                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }
                }
            }
            "device_power" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = item
                        .get("battery_level")
                        .and_then(|v| v.as_f64())
                        .or_else(|| {
                            item.get("power_state")
                                .and_then(|p| p.get("battery_level"))
                                .and_then(|v| v.as_f64())
                        })
                    {
                        patch.insert("battery_pct".to_string(), json!(v));
                    }
                    if let Some(v) = item
                        .get("battery_state")
                        .and_then(|v| v.as_str())
                        .or_else(|| {
                            item.get("power_state")
                                .and_then(|p| p.get("battery_state"))
                                .and_then(|v| v.as_str())
                        })
                    {
                        patch.insert("battery_state".to_string(), json!(v));
                    }
                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }
                }
            }
            "zigbee_connectivity" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = item.get("status").and_then(|v| v.as_str()) {
                        patch.insert("connectivity_status".to_string(), json!(v));
                    }

                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }
                }
            }
            "button" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    let mut button_event_value: Option<String> = None;

                    if let Some(v) = item
                        .get("button_report")
                        .and_then(|r| r.get("event"))
                        .and_then(|v| v.as_str())
                    {
                        patch.insert("button_event".to_string(), json!(v));
                        button_event_value = Some(v.to_string());
                    }
                    if let Some(v) = item
                        .get("button_report")
                        .and_then(|r| r.get("updated"))
                        .and_then(|v| v.as_str())
                    {
                        patch.insert("button_updated".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("repeat_interval").and_then(|v| v.as_u64()) {
                        patch.insert("button_repeat_interval_ms".to_string(), json!(v));
                    }

                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }

                    if let Some(event) = button_event_value {
                        let payload = translator::button_event(
                            publisher.plugin_id(),
                            &device_id,
                            bridge_id,
                            rid,
                            &event,
                        );
                        publisher.publish_event("device_button", &payload).await?;
                    }
                }
            }
            "relative_rotary" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    let action = item
                        .get("rotary_report")
                        .and_then(|r| r.get("action"))
                        .and_then(|v| v.as_str())
                        .map(ToString::to_string);
                    let direction = item
                        .get("rotary_report")
                        .and_then(|r| r.get("rotation"))
                        .and_then(|r| r.get("direction"))
                        .and_then(|v| v.as_str())
                        .map(ToString::to_string);
                    let steps = item
                        .get("rotary_report")
                        .and_then(|r| r.get("rotation"))
                        .and_then(|r| r.get("steps"))
                        .and_then(|v| v.as_i64());

                    if let Some(v) = action.as_deref() {
                        patch.insert("rotary_action".to_string(), json!(v));
                    }
                    if let Some(v) = direction.as_deref() {
                        patch.insert("rotary_direction".to_string(), json!(v));
                    }
                    if let Some(v) = steps {
                        patch.insert("rotary_steps".to_string(), json!(v));
                    }
                    if let Some(v) = item
                        .get("rotary_report")
                        .and_then(|r| r.get("updated"))
                        .and_then(|v| v.as_str())
                    {
                        patch.insert("rotary_updated".to_string(), json!(v));
                    }

                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }

                    if action.is_some() || direction.is_some() || steps.is_some() {
                        let payload = translator::rotary_event(
                            publisher.plugin_id(),
                            &device_id,
                            bridge_id,
                            rid,
                            action.as_deref(),
                            direction.as_deref(),
                            steps,
                        );
                        publisher.publish_event("device_rotary", &payload).await?;
                    }
                }
            }
            "entertainment_configuration" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    let mut active_value: Option<bool> = None;
                    let mut status_value: Option<String> = None;
                    let mut type_value: Option<String> = None;
                    if let Some(v) = item
                        .get("status")
                        .and_then(|s| s.get("active"))
                        .and_then(|v| v.as_bool())
                    {
                        active_value = Some(v);
                        patch.insert("entertainment_active".to_string(), json!(v));
                    }
                    if let Some(v) = item
                        .get("status")
                        .and_then(|s| s.get("status"))
                        .and_then(|v| v.as_str())
                    {
                        status_value = Some(v.to_string());
                        patch.insert("entertainment_status".to_string(), json!(v));
                    }
                    if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                        patch.insert("entertainment_name".to_string(), json!(name));
                    }
                    if let Some(owner) = item
                        .get("owner")
                        .and_then(|v| v.get("rid"))
                        .and_then(|v| v.as_str())
                    {
                        patch.insert("entertainment_owner".to_string(), json!(owner));
                    }
                    if let Some(channels) = item.get("channels").and_then(|v| v.as_array()) {
                        patch.insert("entertainment_channel_count".to_string(), json!(channels.len() as u32));
                    }
                    if let Some(segments) = item.get("segments").and_then(|v| v.as_array()) {
                        patch.insert("entertainment_segment_count".to_string(), json!(segments.len() as u32));
                    }
                    if let Some(proxy) = item.get("stream_proxy").and_then(|v| v.get("node")) {
                        patch.insert("entertainment_proxy_type".to_string(), json!(proxy));
                    }
                    if let Some(v) = item.get("configuration_type").and_then(|v| v.as_str()) {
                        type_value = Some(v.to_string());
                        patch.insert("entertainment_type".to_string(), json!(v));
                    }
                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        if active_value.is_some() || status_value.is_some() || type_value.is_some() {
                            let payload = translator::entertainment_status_changed_event(
                                publisher.plugin_id(),
                                &device_id,
                                rid,
                                active_value,
                                status_value.as_deref(),
                                type_value.as_deref(),
                                "eventstream_patch",
                            );
                            publisher
                                .publish_event("entertainment_status_changed", &payload)
                                .await?;
                        }
                        applied = true;
                    }
                }
            }
            "bridge_home" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    applied = true;
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = item
                        .get("children")
                        .and_then(|v| v.as_array())
                        .map(|a| a.len())
                    {
                        patch.insert("child_count".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("id_v1").and_then(|v| v.as_str()) {
                        patch.insert("id_v1".to_string(), json!(v));
                    }
                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }
                }
            }
            _ => {}
        }
    }

    Ok(applied)
}

fn light_level_to_lux(raw: f64) -> Option<f64> {
    if raw <= 0.0 {
        return None;
    }
    let lux = 10f64.powf((raw - 1.0) / 10000.0);
    if lux.is_finite() {
        Some(lux)
    } else {
        None
    }
}

fn extract_temperature_c(item: &Value) -> Option<f64> {
    let raw = item
        .get("temperature")
        .and_then(|v| v.as_f64())
        .or_else(|| {
            item.get("temperature")
                .and_then(|obj| obj.get("temperature"))
                .and_then(|v| v.as_f64())
        })?;

    let normalized = if raw.abs() > 120.0 { raw / 100.0 } else { raw };
    Some(normalized)
}

fn extract_temperature_valid(item: &Value) -> Option<bool> {
    item.get("temperature_valid")
        .and_then(|v| v.as_bool())
        .or_else(|| {
            item.get("temperature")
                .and_then(|obj| obj.get("temperature_valid"))
                .and_then(|v| v.as_bool())
        })
}

fn extract_light_level_raw(item: &Value) -> Option<f64> {
    item.get("light_level")
        .and_then(|v| v.as_f64())
        .or_else(|| {
            item.get("light")
                .and_then(|obj| obj.get("light_level"))
                .and_then(|v| v.as_f64())
        })
}

fn extract_light_level_valid(item: &Value) -> Option<bool> {
    item.get("light_level_valid")
        .and_then(|v| v.as_bool())
        .or_else(|| {
            item.get("light")
                .and_then(|obj| obj.get("light_level_valid"))
                .and_then(|v| v.as_bool())
        })
}

fn build_motion_owner_map(aux_devices: &[HueAuxDevice]) -> HashMap<String, String> {
    aux_devices
        .iter()
        .filter(|aux| aux.resource_type == "motion")
        .map(|aux| (aux.owner_rid.clone(), aux.device_id.clone()))
        .collect()
}

fn build_light_owner_map(lights: &[crate::hue::models::HueLight]) -> HashMap<String, String> {
    lights
        .iter()
        .map(|light| (light.owner_rid.clone(), light.device_id.clone()))
        .collect()
}

fn compact_publish_device_id(
    aux: &HueAuxDevice,
    motion_owner_to_device: &HashMap<String, String>,
    light_owner_to_device: &HashMap<String, String>,
) -> String {
    if matches!(
        aux.resource_type.as_str(),
        "temperature" | "light_level" | "device_power" | "zigbee_connectivity"
    ) {
        if let Some(motion_device_id) = motion_owner_to_device.get(&aux.owner_rid) {
            return motion_device_id.clone();
        }

        if let Some(light_device_id) = light_owner_to_device.get(&aux.owner_rid) {
            return light_device_id.clone();
        }
    }
    aux.device_id.clone()
}

fn strip_aux_metadata(state: &mut Value) {
    if let Some(obj) = state.as_object_mut() {
        obj.remove("kind");
        obj.remove("bridge_id");
        obj.remove("resource_type");
        obj.remove("resource_id");
    }
}

fn apply_display_preferences(state: &mut Value, resource_type: &str, display_cfg: &HueDisplayConfig) {
    let Some(obj) = state.as_object_mut() else {
        return;
    };
    apply_display_preferences_to_patch(obj, resource_type, display_cfg);
}

fn apply_display_preferences_to_patch(
    attrs: &mut serde_json::Map<String, Value>,
    resource_type: &str,
    display_cfg: &HueDisplayConfig,
) {
    match resource_type {
        "temperature" => {
            let temperature_c = attrs.get("temperature_c").and_then(Value::as_f64);
            if let Some(c) = temperature_c {
                let f = attrs
                    .get("temperature_f")
                    .and_then(Value::as_f64)
                    .unwrap_or((c * 9.0 / 5.0) + 32.0);
                attrs.insert("temperature_f".to_string(), json!(f));
                match display_cfg.temperature_unit {
                    TemperatureUnit::C => {
                        attrs.insert("temperature".to_string(), json!(c));
                        attrs.insert("temperature_unit".to_string(), json!("C"));
                    }
                    TemperatureUnit::F => {
                        attrs.insert("temperature".to_string(), json!(f));
                        attrs.insert("temperature_unit".to_string(), json!("F"));
                    }
                }
            }
        }
        "light_level" => {
            let raw = attrs.get("illuminance_raw").and_then(Value::as_f64);
            let lux = attrs
                .get("illuminance_lux")
                .and_then(Value::as_f64)
                .or_else(|| raw.and_then(light_level_to_lux));

            if let Some(v) = lux {
                attrs.insert("illuminance_lux".to_string(), json!(v));
            }

            match display_cfg.illuminance_display {
                IlluminanceDisplay::Lux => {
                    if let Some(v) = lux {
                        attrs.insert("illuminance".to_string(), json!(v));
                        attrs.insert("illuminance_unit".to_string(), json!("lux"));
                    }
                }
                IlluminanceDisplay::Raw => {
                    if let Some(v) = raw {
                        attrs.insert("illuminance".to_string(), json!(v));
                        attrs.insert("illuminance_unit".to_string(), json!("raw"));
                    }
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hue::models::HueAuxDevice;
    use crate::config::{HueDisplayConfig, IlluminanceDisplay, TemperatureUnit};
    use serde_json::json;

    #[test]
    fn maps_entertainment_aux_to_entertainment_device_type() {
        assert_eq!(aux_device_type("entertainment_configuration"), "entertainment");
        assert_eq!(aux_device_type("motion"), "sensor");
    }

    #[test]
    fn parses_nested_temperature_and_light_level_fields() {
        let temp_item = json!({
            "temperature": { "temperature": 2234.0, "temperature_valid": true }
        });
        assert_eq!(extract_temperature_c(&temp_item), Some(22.34));
        assert_eq!(extract_temperature_valid(&temp_item), Some(true));

        let light_item = json!({
            "light": { "light_level": 18000.0, "light_level_valid": false }
        });
        assert_eq!(extract_light_level_raw(&light_item), Some(18000.0));
        assert_eq!(extract_light_level_valid(&light_item), Some(false));
    }

    #[test]
    fn applies_display_preferences_for_temperature_and_illuminance() {
        let cfg_f_raw = HueDisplayConfig {
            temperature_unit: TemperatureUnit::F,
            illuminance_display: IlluminanceDisplay::Raw,
        };
        let mut attrs = serde_json::Map::new();
        attrs.insert("temperature_c".to_string(), json!(21.0));
        attrs.insert("illuminance_raw".to_string(), json!(15000.0));
        apply_display_preferences_to_patch(&mut attrs, "temperature", &cfg_f_raw);
        apply_display_preferences_to_patch(&mut attrs, "light_level", &cfg_f_raw);

        assert_eq!(attrs.get("temperature").and_then(Value::as_f64), Some(69.8));
        assert_eq!(attrs.get("temperature_unit").and_then(Value::as_str), Some("F"));
        assert_eq!(attrs.get("illuminance").and_then(Value::as_f64), Some(15000.0));
        assert_eq!(attrs.get("illuminance_unit").and_then(Value::as_str), Some("raw"));

        let cfg_c_lux = HueDisplayConfig {
            temperature_unit: TemperatureUnit::C,
            illuminance_display: IlluminanceDisplay::Lux,
        };
        apply_display_preferences_to_patch(&mut attrs, "temperature", &cfg_c_lux);
        apply_display_preferences_to_patch(&mut attrs, "light_level", &cfg_c_lux);

        assert_eq!(attrs.get("temperature").and_then(Value::as_f64), Some(21.0));
        assert_eq!(attrs.get("temperature_unit").and_then(Value::as_str), Some("C"));
        assert_eq!(attrs.get("illuminance_unit").and_then(Value::as_str), Some("lux"));
        assert!(attrs.get("illuminance").and_then(Value::as_f64).is_some());
    }

    #[test]
    fn compacts_temp_lux_battery_to_motion_device() {
        let motion = HueAuxDevice {
            bridge_id: "bridge-1".to_string(),
            owner_rid: "owner-1".to_string(),
            resource_type: "motion".to_string(),
            resource_id: "rid-motion".to_string(),
            device_id: "motion-dev".to_string(),
            name: "Sensor".to_string(),
            attributes: json!({}),
        };
        let temp = HueAuxDevice {
            bridge_id: "bridge-1".to_string(),
            owner_rid: "owner-1".to_string(),
            resource_type: "temperature".to_string(),
            resource_id: "rid-temp".to_string(),
            device_id: "temp-dev".to_string(),
            name: "Sensor".to_string(),
            attributes: json!({}),
        };

        let map = build_motion_owner_map(&[motion.clone(), temp.clone()]);
        let light_map: HashMap<String, String> = HashMap::new();
        assert_eq!(compact_publish_device_id(&temp, &map, &light_map), "motion-dev");
        assert_eq!(compact_publish_device_id(&motion, &map, &light_map), "motion-dev");
    }

    #[test]
    fn compacts_zigbee_connectivity_to_owner_light_when_no_motion_device() {
        let zigbee = HueAuxDevice {
            bridge_id: "bridge-1".to_string(),
            owner_rid: "owner-2".to_string(),
            resource_type: "zigbee_connectivity".to_string(),
            resource_id: "rid-zigbee".to_string(),
            device_id: "zigbee-dev".to_string(),
            name: "Light Owner".to_string(),
            attributes: json!({}),
        };

        let motion_map: HashMap<String, String> = HashMap::new();
        let mut light_map: HashMap<String, String> = HashMap::new();
        light_map.insert("owner-2".to_string(), "light-dev".to_string());

        assert_eq!(
            compact_publish_device_id(&zigbee, &motion_map, &light_map),
            "light-dev"
        );
    }
}
