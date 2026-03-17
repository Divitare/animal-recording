# V2 Bird Node

This folder now contains the first real `bird-node` runtime.

Current behavior:

- runs headless without a web interface
- records continuously from a USB microphone
- analyzes rolling `9 second` windows every `3 seconds` with BirdNET
- saves only detected bird clips instead of full continuous recordings
- stores each saved bird event with an `event_id`, UTC start/end timestamps, species, and confidence
- writes a local SQLite database and a JSON status file for health reporting
