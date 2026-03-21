use anyhow::Result;
use reqwest::Client;
use reqwest::Url;
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::config::HueConfig;

use super::models::DiscoveredBridge;

pub async fn discover_bridges(cfg: &HueConfig) -> Result<Vec<DiscoveredBridge>> {
    if !cfg.discovery_enabled {
        info!("Hue discovery disabled in config");
        return Ok(Vec::new());
    }

    let mut results = Vec::new();
    let mut seen = HashSet::new();

    match discover_via_ssdp(cfg.discovery_timeout_secs).await {
        Ok(list) => {
            for bridge in list {
                let key = format!("{}@{}", bridge.bridge_id, bridge.host);
                if seen.insert(key) {
                    results.push(bridge);
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "Hue SSDP discovery failed");
        }
    }

    if cfg.discovery_cloud_fallback {
        let discovered = discover_via_nupnp(cfg.discovery_timeout_secs).await;
        match discovered {
            Ok(list) => {
                for bridge in list {
                    let key = format!("{}@{}", bridge.bridge_id, bridge.host);
                    if seen.insert(key) {
                        results.push(bridge);
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Hue N-UPnP discovery failed");
            }
        }
    }

    info!(count = results.len(), "Hue discovery complete");
    Ok(results)
}

#[derive(Debug, Deserialize)]
struct NupnpBridge {
    id: String,
    internalipaddress: String,
}

#[derive(Debug, Deserialize)]
struct BridgeConfig {
    #[serde(default)]
    bridgeid: String,
    #[serde(default)]
    name: String,
}

async fn discover_via_nupnp(timeout_secs: u64) -> Result<Vec<DiscoveredBridge>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs.max(1)))
        .build()?;

    let items = client
        .get("https://discovery.meethue.com")
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<NupnpBridge>>()
        .await?;

    let mut bridges = Vec::new();
    for item in items {
        let short = item.id.chars().take(8).collect::<String>();
        bridges.push(DiscoveredBridge {
            name: format!("hue-{short}"),
            bridge_id: item.id,
            host: item.internalipaddress,
        });
    }

    Ok(bridges)
}

async fn discover_via_ssdp(timeout_secs: u64) -> Result<Vec<DiscoveredBridge>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let msg = concat!(
        "M-SEARCH * HTTP/1.1\r\n",
        "HOST:239.255.255.250:1900\r\n",
        "MAN:\"ssdp:discover\"\r\n",
        "MX:2\r\n",
        "ST:upnp:rootdevice\r\n",
        "\r\n"
    );
    socket
        .send_to(msg.as_bytes(), "239.255.255.250:1900")
        .await?;

    let mut hosts = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(timeout_secs.max(1));
    let mut buf = vec![0u8; 2048];

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remain = deadline - now;

        match tokio::time::timeout(remain, socket.recv_from(&mut buf)).await {
            Ok(Ok((n, src))) => {
                let resp = String::from_utf8_lossy(&buf[..n]).to_string();
                if let Some(loc) = parse_ssdp_header(&resp, "location") {
                    if let Some(host) = host_from_location(&loc) {
                        hosts.insert(host);
                    }
                } else {
                    hosts.insert(src.ip().to_string());
                }
            }
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }

    enrich_hosts_with_bridge_config(hosts, timeout_secs).await
}

fn parse_ssdp_header(response: &str, key: &str) -> Option<String> {
    let wanted = key.to_ascii_lowercase();
    for line in response.lines() {
        let trimmed = line.trim();
        let mut parts = trimmed.splitn(2, ':');
        let Some(k) = parts.next() else { continue };
        let Some(v) = parts.next() else { continue };
        if k.trim().eq_ignore_ascii_case(&wanted) {
            return Some(v.trim().to_string());
        }
    }
    None
}

fn host_from_location(location: &str) -> Option<String> {
    let url = Url::parse(location).ok()?;
    let host = url.host_str()?;
    Some(host.to_string())
}

async fn enrich_hosts_with_bridge_config(
    hosts: HashSet<String>,
    timeout_secs: u64,
) -> Result<Vec<DiscoveredBridge>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs.max(1)))
        .build()?;

    let mut bridges = Vec::new();
    for host in hosts {
        let url = format!("http://{host}/api/config");
        let Ok(resp) = client.get(&url).send().await else {
            continue;
        };
        let Ok(resp) = resp.error_for_status() else {
            continue;
        };
        let Ok(cfg) = resp.json::<BridgeConfig>().await else {
            continue;
        };

        if cfg.bridgeid.is_empty() {
            continue;
        }

        let name = if cfg.name.is_empty() {
            let short = cfg.bridgeid.chars().take(8).collect::<String>();
            format!("hue-{short}")
        } else {
            cfg.name
        };

        bridges.push(DiscoveredBridge {
            name,
            bridge_id: cfg.bridgeid,
            host,
        });
    }

    Ok(bridges)
}
