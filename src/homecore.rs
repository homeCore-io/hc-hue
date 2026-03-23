use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::HomecoreConfig;

// ── Inline device schema types (mirrors hc-types to avoid workspace dependency) ──

/// Data kind for a device attribute.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttributeKind {
    Bool,
    Integer,
    Float,
    #[serde(rename = "string")]
    Str,
    Enum,
    ColorXy,
    ColorRgb,
    ColorTemp,
    Json,
}

/// Describes a single attribute.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AttributeSchema {
    pub kind: AttributeKind,
    #[serde(default = "schema_default_true")]
    pub writable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<String>>,
}

fn schema_default_true() -> bool { true }

/// Full schema for one device.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DeviceSchema {
    pub attributes: HashMap<String, AttributeSchema>,
}

#[derive(Clone)]
pub struct HomecorePublisher {
    client: AsyncClient,
    plugin_id: String,
}

impl HomecorePublisher {
    pub fn plugin_id(&self) -> &str {
        &self.plugin_id
    }

    pub async fn publish_state(&self, device_id: &str, state: &Value) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/state");
        let payload = serde_json::to_vec(state)?;
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload)
            .await
            .context("publish_state failed")
    }

    pub async fn publish_state_partial(&self, device_id: &str, patch: &Value) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/state/partial");
        let payload = serde_json::to_vec(patch)?;
        self.client
            .publish(&topic, QoS::AtLeastOnce, false, payload)
            .await
            .context("publish_state_partial failed")
    }

    pub async fn publish_availability(&self, device_id: &str, online: bool) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/availability");
        let payload = if online { "online" } else { "offline" };
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload.as_bytes())
            .await
            .context("publish_availability failed")
    }

    pub async fn register_device(
        &self,
        device_id: &str,
        name: &str,
        device_type: &str,
        area: Option<&str>,
    ) -> Result<()> {
        self.register_device_with_capabilities(device_id, name, device_type, area, None)
            .await
    }

    pub async fn register_device_with_capabilities(
        &self,
        device_id: &str,
        name: &str,
        device_type: &str,
        area: Option<&str>,
        capabilities: Option<Value>,
    ) -> Result<()> {
        let topic = format!("homecore/plugins/{}/register", self.plugin_id);
        let mut payload = serde_json::json!({
            "device_id": device_id,
            "plugin_id": self.plugin_id,
            "name": name,
            "device_type": device_type,
        });

        if let Some(a) = area {
            payload["area"] = Value::String(a.to_string());
        }
        if let Some(c) = capabilities {
            payload["capabilities"] = c;
        }

        self.client
            .publish(&topic, QoS::AtLeastOnce, false, serde_json::to_vec(&payload)?)
            .await
            .context("register_device_with_capabilities failed")?;
        debug!(device_id, device_type, "Registered device with HomeCore");
        Ok(())
    }

    pub async fn subscribe_commands(&self, device_id: &str) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/cmd");
        self.client
            .subscribe(&topic, QoS::AtLeastOnce)
            .await
            .context("subscribe_commands failed")?;
        debug!(device_id, "Subscribed to commands");
        Ok(())
    }

    /// Publish a device capability schema (retained) to HomeCore.
    pub async fn publish_device_schema(
        &self,
        device_id: &str,
        schema: &DeviceSchema,
    ) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/schema");
        let payload = serde_json::to_vec(schema)
            .context("serialising device schema")?;
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload)
            .await
            .context("publish_device_schema failed")?;
        debug!(device_id, "Device schema published");
        Ok(())
    }

    pub async fn publish_plugin_status(&self, status: &str) -> Result<()> {
        let topic = format!("homecore/plugins/{}/status", self.plugin_id);
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, status.as_bytes())
            .await
            .context("publish_plugin_status failed")
    }

    pub async fn publish_event(&self, event_type: &str, payload: &Value) -> Result<()> {
        let topic = format!("homecore/events/{event_type}");
        self.client
            .publish(&topic, QoS::AtLeastOnce, false, serde_json::to_vec(payload)?)
            .await
            .context("publish_event failed")
    }
}

pub struct HomecoreClient {
    client: AsyncClient,
    eventloop: rumqttc::EventLoop,
    plugin_id: String,
}

impl HomecoreClient {
    pub async fn connect(cfg: &HomecoreConfig) -> Result<Self> {
        let mut opts = MqttOptions::new(&cfg.plugin_id, &cfg.broker_host, cfg.broker_port);
        opts.set_keep_alive(Duration::from_secs(30));
        opts.set_clean_session(true);
        if !cfg.password.is_empty() {
            opts.set_credentials(&cfg.plugin_id, &cfg.password);
        }

        let (client, eventloop) = AsyncClient::new(opts, 64);
        info!(
            host = %cfg.broker_host,
            port = cfg.broker_port,
            plugin_id = %cfg.plugin_id,
            "HomeCore MQTT client created"
        );
        Ok(Self {
            client,
            eventloop,
            plugin_id: cfg.plugin_id.clone(),
        })
    }

    pub fn publisher(&self) -> HomecorePublisher {
        HomecorePublisher {
            client: self.client.clone(),
            plugin_id: self.plugin_id.clone(),
        }
    }

    pub async fn run(mut self, tx: mpsc::Sender<(String, Value)>) -> Result<()> {
        info!("HomeCore MQTT event loop starting");
        loop {
            match self.eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!("Connected to HomeCore broker");
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let parts: Vec<&str> = p.topic.splitn(4, '/').collect();
                    if parts.len() == 4
                        && parts[0] == "homecore"
                        && parts[1] == "devices"
                        && parts[3] == "cmd"
                    {
                        let device_id = parts[2].to_string();
                        match serde_json::from_slice::<Value>(&p.payload) {
                            Ok(cmd) => {
                                if tx.send((device_id, cmd)).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Err(e) => warn!(topic = %p.topic, error = %e, "Non-JSON cmd payload"),
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    error!(error = %e, "HomeCore MQTT error; retrying in 2 s");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
}
