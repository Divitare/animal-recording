# V2 Bird Node

This folder now contains the first real `bird-node` runtime.

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

Example export command:

```bash
python -m bird_node export-events --since-hours 24
```
