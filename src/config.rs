use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::hue::models::{BridgeTarget, DiscoveredBridge};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HuePluginConfig {
    #[serde(default)]
    pub homecore: HomecoreConfig,
    #[serde(default)]
    pub hue: HueConfig,
    #[serde(default)]
    pub bridges: Vec<BridgeConfig>,
}

impl HuePluginConfig {
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config from {path}"))?;
        toml::from_str(&text).context("parsing config TOML")
    }

    /// Write the current config back to disk (overwrites the file).
    pub fn save(&self, path: &str) -> Result<()> {
        let text = toml::to_string_pretty(self).context("serializing config")?;
        std::fs::write(path, text)
            .with_context(|| format!("writing config to {path}"))
    }

    /// Add or update the app_key for the bridge identified by `target`.
    /// Matches on bridge_id first, then host.
    pub fn upsert_bridge_app_key(&mut self, target: &BridgeTarget, app_key: &str) {
        if let Some(entry) = self
            .bridges
            .iter_mut()
            .find(|b| b.bridge_id == target.bridge_id || b.host == target.host)
        {
            entry.app_key = app_key.to_string();
            // Ensure identifying fields are populated from the resolved target.
            if entry.bridge_id.is_empty() {
                entry.bridge_id = target.bridge_id.clone();
            }
            if entry.host.is_empty() {
                entry.host = target.host.clone();
            }
        } else {
            self.bridges.push(BridgeConfig {
                name:             target.name.clone(),
                bridge_id:        target.bridge_id.clone(),
                host:             target.host.clone(),
                app_key:          app_key.to_string(),
                verify_tls:       target.verify_tls,
                allow_self_signed: target.allow_self_signed,
            });
        }
    }

    pub fn effective_bridges(&self, discovered: &[DiscoveredBridge]) -> Vec<BridgeTarget> {
        if !self.bridges.is_empty() {
            let resolved = self
                .bridges
                .iter()
                .filter_map(|cfg| cfg.to_target(discovered))
                .collect::<Vec<_>>();

            // If explicit bridge entries were provided but none could be resolved,
            // fall back to discovered bridges so startup can proceed.
            if !resolved.is_empty() {
                return resolved;
            }
        }

        discovered
            .iter()
            .map(|d| BridgeTarget {
                name: d.name.clone(),
                bridge_id: d.bridge_id.clone(),
                host: d.host.clone(),
                app_key: None,
                verify_tls: true,
                allow_self_signed: true,
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HomecoreConfig {
    #[serde(default = "default_broker_host")]
    pub broker_host: String,
    #[serde(default = "default_broker_port")]
    pub broker_port: u16,
    #[serde(default = "default_plugin_id")]
    pub plugin_id: String,
    #[serde(default)]
    pub password: String,
}

impl Default for HomecoreConfig {
    fn default() -> Self {
        Self {
            broker_host: default_broker_host(),
            broker_port: default_broker_port(),
            plugin_id: default_plugin_id(),
            password: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HueConfig {
    #[serde(default = "default_true")]
    pub discovery_enabled: bool,
    #[serde(default = "default_true")]
    pub discovery_cloud_fallback: bool,
    #[serde(default = "default_discovery_timeout_secs")]
    pub discovery_timeout_secs: u64,
    #[serde(default = "default_true")]
    pub eventstream_enabled: bool,
    #[serde(default = "default_eventstream_reconnect_secs")]
    pub eventstream_reconnect_secs: u64,
    #[serde(default = "default_resync_interval_secs")]
    pub resync_interval_secs: u64,
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: u64,
    #[serde(default)]
    pub compact_motion_facets: bool,
    #[serde(default)]
    pub display: HueDisplayConfig,
}

impl Default for HueConfig {
    fn default() -> Self {
        Self {
            discovery_enabled: default_true(),
            discovery_cloud_fallback: default_true(),
            discovery_timeout_secs: default_discovery_timeout_secs(),
            eventstream_enabled: default_true(),
            eventstream_reconnect_secs: default_eventstream_reconnect_secs(),
            resync_interval_secs: default_resync_interval_secs(),
            heartbeat_secs: default_heartbeat_secs(),
            compact_motion_facets: false,
            display: HueDisplayConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HueDisplayConfig {
    #[serde(default)]
    pub temperature_unit: TemperatureUnit,
    #[serde(default)]
    pub illuminance_display: IlluminanceDisplay,
}

impl Default for HueDisplayConfig {
    fn default() -> Self {
        Self {
            temperature_unit: TemperatureUnit::default(),
            illuminance_display: IlluminanceDisplay::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TemperatureUnit {
    #[default]
    C,
    F,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IlluminanceDisplay {
    #[default]
    Lux,
    Raw,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeConfig {
    pub name: String,
    #[serde(default)]
    pub bridge_id: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub app_key: String,
    #[serde(default = "default_true")]
    pub verify_tls: bool,
    #[serde(default = "default_true")]
    pub allow_self_signed: bool,
}

impl BridgeConfig {
    fn to_target(&self, discovered: &[DiscoveredBridge]) -> Option<BridgeTarget> {
        let mut host = self.host.clone();
        let mut bridge_id = self.bridge_id.clone();

        if host.is_empty() || bridge_id.is_empty() {
            if let Some(found) = discovered.iter().find(|d| {
                (!self.bridge_id.is_empty() && d.bridge_id == self.bridge_id)
                    || (!self.host.is_empty() && d.host == self.host)
                    || d.name.eq_ignore_ascii_case(&self.name)
            }) {
                if host.is_empty() {
                    host = found.host.clone();
                }
                if bridge_id.is_empty() {
                    bridge_id = found.bridge_id.clone();
                }
            }
        }

        if host.is_empty() {
            return None;
        }

        if bridge_id.is_empty() {
            bridge_id = host.replace('.', "_");
        }

        Some(BridgeTarget {
            name: self.name.clone(),
            bridge_id,
            host,
            app_key: if self.app_key.trim().is_empty() {
                None
            } else {
                Some(self.app_key.clone())
            },
            verify_tls: self.verify_tls,
            allow_self_signed: self.allow_self_signed,
        })
    }
}

fn default_broker_host() -> String {
    "127.0.0.1".into()
}

fn default_broker_port() -> u16 {
    1883
}

fn default_plugin_id() -> String {
    "plugin.hue".into()
}

fn default_true() -> bool {
    true
}

fn default_resync_interval_secs() -> u64 {
    60
}

fn default_heartbeat_secs() -> u64 {
    30
}

fn default_discovery_timeout_secs() -> u64 {
    5
}

fn default_eventstream_reconnect_secs() -> u64 {
    3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn falls_back_to_discovered_when_configured_bridges_unresolved() {
        let cfg = HuePluginConfig {
            bridges: vec![BridgeConfig {
                name: "main".to_string(),
                bridge_id: String::new(),
                host: String::new(),
                app_key: String::new(),
                verify_tls: true,
                allow_self_signed: true,
            }],
            ..Default::default()
        };

        let discovered = vec![DiscoveredBridge {
            name: "Hue Bridge".to_string(),
            bridge_id: "bridge-1".to_string(),
            host: "10.0.0.10".to_string(),
        }];

        let effective = cfg.effective_bridges(&discovered);
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].host, "10.0.0.10");
        assert_eq!(effective[0].bridge_id, "bridge-1");
    }

    #[test]
    fn defaults_display_preferences() {
        let cfg = HuePluginConfig::default();
        assert_eq!(cfg.hue.display.temperature_unit, TemperatureUnit::C);
        assert_eq!(cfg.hue.display.illuminance_display, IlluminanceDisplay::Lux);
    }

    #[test]
    fn parses_display_preferences_from_toml() {
        let text = r#"
[hue]
[hue.display]
temperature_unit = "f"
illuminance_display = "raw"
"#;

        let cfg: HuePluginConfig = toml::from_str(text).expect("parse display config");
        assert_eq!(cfg.hue.display.temperature_unit, TemperatureUnit::F);
        assert_eq!(cfg.hue.display.illuminance_display, IlluminanceDisplay::Raw);
    }
}
