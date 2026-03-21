use anyhow::Result;
use serde_json::json;
use serde_json::Value;
use tracing::{info, warn};

use crate::homecore::HomecorePublisher;
use crate::hue::api::HueApiClient;
use crate::hue::models::BridgeSnapshot;
use crate::hue::registry::HueRegistry;
use crate::translator;

pub async fn refresh_bridge_state(
    publisher: &HomecorePublisher,
    registry: &mut HueRegistry,
    api: &HueApiClient,
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
            for aux in aux_devices {
                if registry.ensure_aux(&aux) {
                    publisher
                        .register_device_with_capabilities(
                            &aux.device_id,
                            &aux.name,
                            "sensor",
                            None,
                            Some(translator::aux_capabilities(&aux)),
                        )
                        .await?;
                    info!(device_id = %aux.device_id, name = %aux.name, kind = %aux.resource_type, "Registered Hue auxiliary device");
                }

                publisher.publish_availability(&aux.device_id, true).await?;
                publisher
                    .publish_state(&aux.device_id, &translator::aux_state(&aux))
                    .await?;
            }
        }
        Err(e) => {
            publisher.publish_availability(&device_id, false).await?;
            warn!(device_id, error = %e, "Failed to refresh Hue bridge state");
        }
    }

    Ok(())
}

pub async fn apply_eventstream_update(
    publisher: &HomecorePublisher,
    registry: &HueRegistry,
    bridge_id: &str,
    payload: &Value,
) -> Result<bool> {
    let mut applied_any = false;

    if let Some(events) = payload.as_array() {
        for event in events {
            applied_any |= apply_event_item(publisher, registry, bridge_id, event).await?;
        }
        return Ok(applied_any);
    }

    if payload.is_object() {
        applied_any |= apply_event_item(publisher, registry, bridge_id, payload).await?;
    }

    Ok(applied_any)
}

async fn apply_event_item(
    publisher: &HomecorePublisher,
    registry: &HueRegistry,
    bridge_id: &str,
    event: &Value,
) -> Result<bool> {
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
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = item.get("temperature").and_then(|v| v.as_f64()) {
                        patch.insert("temperature_c".to_string(), json!(v));
                        patch.insert("temperature_f".to_string(), json!((v * 9.0 / 5.0) + 32.0));
                        patch.insert("temperature_unit".to_string(), json!("C"));
                    }
                    if let Some(v) = item.get("temperature_valid").and_then(|v| v.as_bool()) {
                        patch.insert("temperature_valid".to_string(), json!(v));
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
            "light_level" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = item.get("light_level").and_then(|v| v.as_f64()) {
                        patch.insert("illuminance_raw".to_string(), json!(v));
                        if let Some(lux) = light_level_to_lux(v) {
                            patch.insert("illuminance_lux".to_string(), json!(lux));
                        }
                        patch.insert("illuminance_unit".to_string(), json!("lux"));
                    }
                    if let Some(v) = item.get("light_level_valid").and_then(|v| v.as_bool()) {
                        patch.insert("illuminance_valid".to_string(), json!(v));
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
            "contact" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
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
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = item.get("battery_level").and_then(|v| v.as_f64()) {
                        patch.insert("battery_pct".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("battery_state").and_then(|v| v.as_str()) {
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
                    let mut patch = serde_json::Map::new();
                    if let Some(v) = item
                        .get("status")
                        .and_then(|s| s.get("active"))
                        .and_then(|v| v.as_bool())
                    {
                        patch.insert("entertainment_active".to_string(), json!(v));
                    }
                    if let Some(v) = item
                        .get("status")
                        .and_then(|s| s.get("status"))
                        .and_then(|v| v.as_str())
                    {
                        patch.insert("entertainment_status".to_string(), json!(v));
                    }
                    if let Some(v) = item.get("configuration_type").and_then(|v| v.as_str()) {
                        patch.insert("entertainment_type".to_string(), json!(v));
                    }
                    if !patch.is_empty() {
                        publisher.publish_state_partial(&device_id, &Value::Object(patch)).await?;
                        applied = true;
                    }
                }
            }
            "bridge_home" => {
                if let Some(device_id) = registry.find_aux_device_id(bridge_id, resource_type, rid) {
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
