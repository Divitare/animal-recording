# V2 Bird Node

This folder now contains the first real `bird-node` runtime.

Install or update `bird-node` directly with:

```bash
curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install-bird-node.sh | sudo bash
```

Current behavior:

- runs headless without a web interface
- records continuously from a USB microphone
- analyzes rolling `9 second` windows every `3 seconds` with BirdNET
- saves only detected bird clips instead of full continuous recordings
- stores each saved bird event with an `event_id`, UTC start/end timestamps, species, and confidence
- writes a local SQLite database and a JSON status file for health reporting
- reports node health and self-checks in `status.json`, including microphone health, clipping, silence, disk space, CPU temperature, uptime, and BirdNET health
- tracks persistent effort statistics such as hours recorded, hours successfully analyzed, microphone uptime, and detections per UTC day
- stores a persistent health snapshot at least every 5 minutes
- can export bird events, matching clips, and the nearest health snapshot as a zip archive with `python -m bird_node export-events`
- can queue and upload offline-first sync bundles to `bird-hub` over WLAN or Ethernet when `BIRD_MONITOR_HUB_URL` is configured
- tries regular hub uploads every 30 minutes and retries failed batches every 5 minutes by default
- deletes acknowledged local clips, detections, and uploaded health snapshots after the hub confirms receipt
- reports sync health in `status.json`

Example export command:

```bash
python -m bird_node export-events --since-hours 24
python -m bird_node sync-now
sudo -u birdnode bash -lc 'cd /opt/bird-node/current && /opt/bird-node/.venv/bin/python -m bird_node sync-now'
```

`sync-now` forces one immediate upload attempt. It does not wait for the next 30 minute schedule window.

Useful service commands:

```bash
sudo systemctl stop bird-node
sudo systemctl start bird-node
sudo systemctl restart bird-node
sudo journalctl -u bird-node -f
```

Main sync environment variables:

- `BIRD_MONITOR_HUB_URL`
- `BIRD_MONITOR_HUB_TOKEN`
- `BIRD_MONITOR_SYNC_INTERVAL_SECONDS`
- `BIRD_MONITOR_SYNC_RETRY_BASE_SECONDS`
- `BIRD_MONITOR_SYNC_MAX_EVENTS_PER_BUNDLE`
- `BIRD_MONITOR_SYNC_MAX_HEALTH_SNAPSHOTS_PER_BUNDLE`
