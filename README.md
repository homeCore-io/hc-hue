# hc-hue

[![CI](https://github.com/homeCore-io/hc-hue/actions/workflows/ci.yml/badge.svg)](https://github.com/homeCore-io/hc-hue/actions/workflows/ci.yml) [![Release](https://github.com/homeCore-io/hc-hue/actions/workflows/release.yml/badge.svg)](https://github.com/homeCore-io/hc-hue/actions/workflows/release.yml) [![Dashboard](https://img.shields.io/badge/builds-dashboard-blue?style=flat-square)](https://homecore-io.github.io/ci-glance/)

Bridges Philips Hue devices into HomeCore via the CLIP v2 API with real-time eventstream updates.

## Supported device types

- Lights (dimmable, color temperature, full color)
- Switches and buttons
- Contact sensors
- Motion sensors (with optional compact facet merging)
- Temperature sensors
- Grouped lights (room/zone level, selectable)

## Setup

1. Copy `config/config.toml.example` to `config/config.toml`
2. Set `bridge_id`, `host`, and `app_key` (press the bridge link button, then use the Hue API to generate an app key)
3. Add a `[[plugins]]` entry in `homecore.toml`

## Configuration highlights

- `eventstream_enabled` — real-time state updates via Hue SSE (default: true)
- `resync_interval_secs` — periodic full refresh cadence
- `compact_motion_facets` — collapse sensor sub-devices onto the physical device
- `publish_grouped_lights` — expose room/zone grouped lights as HomeCore devices
- `publish_grouped_lights_for` — selectively publish specific rooms/zones (e.g. `["room:kitchen"]`)
- `temperature_unit` — `"c"` or `"f"` for published temperature values
