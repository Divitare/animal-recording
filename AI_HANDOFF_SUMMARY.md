# AI Handoff Summary

This file is a comprehensive handoff summary for this repository and for the project decisions made in the chat that led to the current state.

Its purpose is to let a new device, a new AI session, or a new collaborator quickly understand:

- what this project is
- what was decided architecturally
- what has already been implemented
- how the node and hub are meant to work
- how to install, operate, and troubleshoot them
- what ideas were discussed and what is still planned

## Project Goal

The long-term goal is a multi-node bird monitoring system.

Each `bird-node`:

- runs on a Raspberry Pi 5 with 4 GB RAM
- records from a USB microphone
- performs local BirdNET species detection
- saves only relevant bird detections and not endless full recordings
- collects health and runtime information
- uploads data to a central `bird-hub`

The `bird-hub`:

- receives data from multiple nodes
- stores events, clips, and health information
- provides a web UI and JSON API
- helps answer the main question:

What birds live in the area?

Location detail is useful too, but it is secondary to the species presence information.

## Main Architecture Decisions

## Hardware

- Standard node target: Raspberry Pi 5, 4 GB RAM
- External USB microphone
- No node web interface required
- GPS module is optional and was not purchased yet
- One day of local storage is enough for now, so storage pressure on the node is moderate

## Repository Structure

The repository was reorganized into:

- `v1/` = legacy codebase preserved as the original baseline
- `v2/` = active development area

Inside `v2/`:

- `v2/bird-node/`
- `v2/bird-hub/`

Active development should happen only in `v2/`.

## Install / Update Strategy

A single installer/update flow was created.

Root installer:

```bash
curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install.sh | sudo bash
```

Direct wrappers:

```bash
curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install-bird-node.sh | sudo bash
curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install-bird-hub.sh | sudo bash
```

Behavior:

- first install asks which variant to install:
  - `v1`
  - `v2 bird-node`
  - `v2 bird-hub`
- later runs auto-detect the installed variant and update that same variant

## Node Design Decisions

The node is a headless detection device.

It should:

- continuously record from USB audio
- analyze rolling `9 second` windows every `3 seconds`
- use BirdNET species recognition
- save only detected bird sounds
- store:
  - event id
  - UTC bird sound start time
  - UTC bird sound end time
  - species
  - confidence
  - clip/recording
- store health snapshots at least every `5 minutes`
- export:
  - node id
  - UTC time when available
  - species
  - confidence
  - clip
  - health snapshot
  - BirdNET/runtime version

## BirdNET Decisions

- BirdNET runs locally on the Raspberry Pi
- no external BirdNET cloud traffic is required
- the node can theoretically run BirdNET without internet access

Audio handling notes:

- we discussed 16 kHz compatibility carefully
- the node implementation was aligned to BirdNET requirements
- some microphones only work reliably at `48000 Hz`
- the node was updated to handle that case
- if the microphone captures at `16000 Hz`, the implementation can resample appropriately for BirdNET processing

In practice:

- some USB audio devices required `BIRD_MONITOR_SAMPLE_RATE=48000`
- BirdNET analysis logs showed live resampling behavior

## Health / Self-Check Decisions For The Node

The following health/self-check items were requested and implemented in the node design and runtime:

- microphone health
- clipping detection
- silence detection
- disk space
- CPU temperature
- uptime
- BirdNET health

Persistent statistics requested and implemented:

- hours recorded
- hours successfully analyzed
- microphone uptime
- detections per day

## Event Handling Decisions

We discussed how to handle long bird calls and overlapping bird sounds.

### Same Species Across Overlapping Windows

If the same bird call spans multiple overlapping `9 second` windows:

- it should become one logical event
- the recording should not be duplicated
- the event start/end should be extended across the continuous call

This was implemented.

### Different Species Overlapping

If one long bird call is happening and another bird makes a short call during that time:

- the long call and the short call should be stored as separate events
- overlapping different species should not be merged together

This was implemented.

### Clip Save Edge Cases

There was an issue where detections near the end of the rolling buffer could be recognized by BirdNET but the clip could not be saved because the needed audio had already rolled out of memory.

This was improved so that:

- clips are saved when possible
- a truncated clip can be saved instead of dropping the event entirely if only optional padding is unavailable

## Current Bird-Node Capabilities

At the current stage, `v2/bird-node`:

- runs headless with no web UI
- records continuously from a USB microphone
- analyzes rolling `9 second` windows every `3 seconds`
- saves only detected bird clips
- stores event metadata in SQLite
- writes a JSON status file
- records health snapshots
- tracks node statistics
- can export offline bundles
- can upload bundles to the hub
- can retry failed uploads
- can work with hub tokens
- can work with Cloudflare Access service tokens

## Important Bird-Node Runtime Paths

Typical installed node runtime paths:

- code: `/opt/bird-node/current`
- venv: `/opt/bird-node/.venv`
- data: `/var/lib/bird-node`
- logs: `/var/log/bird-node`
- env file: `/etc/bird-node.env`

## Important Bird-Node Commands

Service control:

```bash
sudo systemctl stop bird-node
sudo systemctl start bird-node
sudo systemctl restart bird-node
sudo journalctl -u bird-node -f
```

Manual sync:

```bash
sudo -u birdnode bash -lc 'cd /opt/bird-node/current && /opt/bird-node/.venv/bin/python -m bird_node sync-now'
```

Export events:

```bash
sudo -u birdnode bash -lc 'cd /opt/bird-node/current && /opt/bird-node/.venv/bin/python -m bird_node export-events --since-hours 24'
```

## Bird-Node Sync Behavior

The sync system evolved significantly.

### Early Behavior

Initially:

- bundles were limited to `25` detections and `12` health snapshots
- one sync cycle uploaded only one bundle
- backlog could remain until the next scheduled upload

### Improved Behavior

This was changed so that:

- a sync cycle keeps uploading bundle after bundle
- it drains the backlog that existed when that sync cycle started
- it does not run forever
- new detections created during that sync cycle are left for the next sync cycle

This prevents infinite sync loops while still keeping the hub reasonably up to date.

### Current Default Sync Settings

- regular upload interval: `1800` seconds (`30 minutes`)
- retry interval after failure: `300` seconds (`5 minutes`)
- max events per bundle: `25`
- max health snapshots per bundle: `12`

Meaning:

- every `30 minutes`, the node tries to upload everything that was pending at the start of that sync cycle
- failed uploads stop and retry later
- manual `sync-now` forces one immediate sync cycle

## Important Bird-Node Environment Variables

Core sync/network variables:

- `BIRD_MONITOR_HUB_URL`
- `BIRD_MONITOR_HUB_TOKEN`
- `BIRD_MONITOR_SYNC_INTERVAL_SECONDS`
- `BIRD_MONITOR_SYNC_RETRY_BASE_SECONDS`
- `BIRD_MONITOR_SYNC_MAX_EVENTS_PER_BUNDLE`
- `BIRD_MONITOR_SYNC_MAX_HEALTH_SNAPSHOTS_PER_BUNDLE`

Cloudflare Access service token support:

- `BIRD_MONITOR_CLOUDFLARE_ACCESS_CLIENT_ID`
- `BIRD_MONITOR_CLOUDFLARE_ACCESS_CLIENT_SECRET`

Useful audio/runtime variables:

- `BIRD_MONITOR_SAMPLE_RATE`
- `BIRD_MONITOR_DEVICE_NAME`
- `BIRD_MONITOR_DEVICE_INDEX`
- `BIRD_MONITOR_SPECIES_MIN_CONFIDENCE`

## Node Time / Clock Notes

Correct node time matters because detections are stored in UTC.

Useful checks:

```bash
timedatectl
date
date -u
```

Useful fixes:

```bash
sudo timedatectl set-timezone Europe/Berlin
sudo timedatectl set-ntp true
```

## Bird-Hub MVP Decisions

The first hub MVP was designed to include:

- API endpoints for node ingest and data access
- database tables for nodes, events, clips, health snapshots, and ingest batches
- a first web interface showing:
  - dashboard
  - nodes
  - events
  - event details

Later we expanded the node dashboard health visibility significantly.

## Current Bird-Hub Capabilities

At the current stage, `v2/bird-hub`:

- accepts node export bundles
- stores clips on disk
- stores metadata in SQLite
- deduplicates events by `event_id`
- stores node health snapshots
- stores ingest batch history
- exposes a JSON API
- provides a web UI
- supports node upload tokens
- supports manual bundle import
- has a Storage settings UI page for choosing file locations

## Important Bird-Hub Runtime Paths

Typical installed hub runtime paths:

- code: `/opt/bird-hub/current`
- venv: `/opt/bird-hub/.venv`
- data: `/var/lib/bird-hub`
- logs: `/var/log/bird-hub`
- env file: `/etc/bird-hub.env`

## Important Bird-Hub Commands

Install/update:

```bash
curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install-bird-hub.sh | sudo bash
```

Useful CLI commands:

```bash
python -m bird_hub status --pretty
python -m bird_hub create-node-token bird-node-01
python -m bird_hub ingest-bundle /path/to/export.zip
```

Useful service checks:

```bash
sudo systemctl status bird-hub -l --no-pager
sudo journalctl -u bird-hub -n 100 --no-pager
curl -i http://127.0.0.1:8080/api/v1/status
```

## Bird-Hub API Endpoints

Main implemented endpoints:

- `GET /api/v1/status`
- `POST /api/v1/ingest/bundle`
- `GET /api/v1/nodes`
- `GET /api/v1/nodes/<node_id>`
- `GET /api/v1/events`
- `GET /api/v1/events/<event_id>`
- `GET /api/v1/events/<event_id>/clip`
- `GET /api/v1/species/stats`
- `GET /api/v1/health-snapshots`
- `GET /api/v1/ingest/batches`

## Bird-Hub Nodes Dashboard Health Fields

We discussed what node health information is useful in the hub UI.

The requested health information was:

- last seen
- microphone health
- BirdNET health
- disk space
- CPU temperature
- sync status
- hours analyzed
- detections today

These were implemented on the hub nodes dashboard.

## Bird-Hub Storage Settings UI

One of the newest features is a web UI page for choosing where the hub saves files.

The hub UI now includes a `Storage` page where you can choose:

- database file path
- clip directory path
- upload bundle directory path

These choices are persisted in a runtime settings file:

- `hub_settings.json` inside the hub data directory

The UI can also move existing data into the new locations.

This is useful for:

- LXC mountpoints
- larger attached storage
- custom filesystem layouts

## Networking And Deployment Decisions

## Node To Hub Communication

Communication between node and hub is intended over WLAN or Ethernet.

We chose an offline-first upload design:

- node stores data locally
- node uploads to hub periodically
- if upload fails, node retries later
- after hub acknowledgment, node deletes uploaded local payload

## Remote Networks

When a node was moved to another network, a problem appeared:

- the node still pointed to `http://192.168.178.99:8080`
- this is a private LAN IP
- it is not reachable from a different remote network like the forest deployment

This was confirmed by:

- failed ping
- failed curl
- upload connection timeouts

Conclusion:

- private LAN IPs only work locally unless a VPN or special routing is used

## Cloudflare Tunnel / Access Discussion

To make the hub reachable from another network, a Cloudflare tunnel was introduced using:

- `birdnet.divitare.de`

### Observed Behavior

When Cloudflare Access rules were active, requests from the node got:

- `HTTP 302`
- redirect to Cloudflare login

That proved:

- the tunnel itself was reachable
- but browser-style Cloudflare Access login blocked API automation

### Temporary Success

When Cloudflare Access rules were disabled:

- `curl https://birdnet.divitare.de/api/v1/status` returned `HTTP 200`

That proved:

- node to hub communication through the Cloudflare tunnel worked

### Better Long-Term Solution

We then added support for Cloudflare Access service tokens.

The node can now send:

- `CF-Access-Client-Id`
- `CF-Access-Client-Secret`

This enables:

- Cloudflare Access rules to remain active
- the bird-node to bypass browser login using a service token

### Required Cloudflare Setup

Cloudflare Zero Trust should have:

1. a service token
2. an Access application for `birdnet.divitare.de`
3. a `Service Auth` policy allowing that service token

The node should then set:

```bash
BIRD_MONITOR_HUB_URL=https://birdnet.divitare.de
BIRD_MONITOR_HUB_TOKEN=your-bird-hub-token
BIRD_MONITOR_CLOUDFLARE_ACCESS_CLIENT_ID=your-cloudflare-access-client-id
BIRD_MONITOR_CLOUDFLARE_ACCESS_CLIENT_SECRET=your-cloudflare-access-client-secret
```

## Security Notes

Important security point:

- at one stage, hub status output showed:
  - `"allow_unauthenticated_ingest": true`

That is unsafe if the hub is reachable publicly.

Recommended secure setup:

- Cloudflare Access enabled
- bird-node uses Cloudflare Access service token headers
- hub ingest uses a hub Bearer token
- unauthenticated ingest disabled on the hub

The Cloudflare credentials and the hub Bearer token are separate layers:

- Cloudflare credentials let traffic pass Cloudflare Access
- hub Bearer token lets the hub accept the node upload

## Troubleshooting History / Lessons Learned

## Installer Issues

We hit multiple installer/update issues and addressed them over time:

- initial installer choices not working
- permission issues reading installer logs in `/tmp`
- BirdNET dependency verification failures
- Python/runtime compatibility problems
- service readiness verification issues

General lesson:

- check installer logs
- check `systemctl status`
- check `journalctl`
- verify the node writes `/var/lib/bird-node/status.json`

## Audio Device Issues

Observed failure modes:

- no input devices found
- invalid sample rate
- device busy

Important lessons:

- some microphones/adapters require `48000 Hz`
- `arecord` can fail with `Device or resource busy` if bird-node already owns the mic
- restart of `bird-node` can fix device detection after plugging/replugging USB audio

Useful commands:

```bash
arecord -l
sudo -u birdnode arecord -l
id birdnode
```

## Manual Transfer / SCP / Bundle Import Lessons

We manually moved export zips between node and hub using `scp`.

Important lesson:

- importing as `birdhub` from `/root/...` failed due to file permissions
- moving the zip to a readable location like `/tmp` fixed that

## Validation Commands That Were Useful

On node:

```bash
sudo systemctl status bird-node -l --no-pager
sudo journalctl -u bird-node -f
sudo cat /var/lib/bird-node/status.json
```

On hub:

```bash
sudo systemctl status bird-hub -l --no-pager
curl -i http://127.0.0.1:8080/api/v1/status
curl -i -X POST http://127.0.0.1:8080/api/v1/ingest/bundle
```

## Ideas Discussed

These ideas were explicitly discussed at some point:

- long bird calls should become one event instead of multiple duplicated recordings
- overlapping different species should remain separate
- location is nice to have, but not the top priority
- a Raspberry Pi can run BirdNET locally without internet
- one day of node storage is enough for now
- central hub dashboard should show useful health visibility
- node to hub communication should be robust against temporary outages
- Cloudflare tunnel can be used for remote nodes
- access should be protected while still allowing automated node uploads
- hub storage location should be user-selectable from the web UI

## Current Likely Next Steps

Depending on project priority, the next good steps are probably:

1. finish secure Cloudflare Access deployment with service-token-protected node uploads
2. disable unauthenticated ingest on the hub
3. verify the remote forest node can sync through Cloudflare with both tokens
4. continue improving species quality and false-positive handling
5. add more operational UX around node onboarding and storage management
6. potentially improve hub administration pages further

## Quick Start For A New AI Session

If starting a fresh AI session, the important context to provide is:

- this repo contains `v1` legacy and `v2` active code
- active development is in:
  - `v2/bird-node`
  - `v2/bird-hub`
- the system is a Raspberry Pi bird-node plus bird-hub architecture
- node sync is offline-first
- BirdNET runs locally
- node health tracking is implemented
- long same-species calls are merged into one event
- overlapping different species remain separate
- hub UI has node health fields and a Storage settings page
- Cloudflare tunnel is being used for remote node access
- Cloudflare Access service-token support was added to bird-node
- the next tasks should build on the current `v2` implementation, not re-open the `v1` design

## Suggested Prompt For A Future AI Session

You can paste something like this into a new AI session:

```text
Read AI_HANDOFF_SUMMARY.md first. This repository has a legacy v1 and an active v2 bird monitoring system. Active work is in v2/bird-node and v2/bird-hub. Bird-node runs on Raspberry Pi 5 with USB microphone, uses local BirdNET detection, stores only detected clips, health snapshots, and syncs offline-first to bird-hub. Bird-hub stores events/clips/health, has a web UI, node health dashboard, storage settings page, and supports token-based ingest. Cloudflare tunnel is used for remote hub access, and bird-node now supports Cloudflare Access service-token headers. Continue from the current v2 architecture and preserve existing behavior unless explicitly changing it.
```

## Final Note

This summary is intentionally long because it is meant to preserve not only code state but also project reasoning and operational lessons.

If this file becomes outdated, it should be updated whenever major architectural or deployment decisions change.
