# V2 Bird Hub

`bird-hub` is the central offline-first ingest and review service for the new multi-node system.

Current MVP features:

- accepts `bird-node` export zip bundles
- stores clip files on disk and metadata in SQLite
- deduplicates detections by `event_id`
- stores node health snapshots
- keeps ingest batch history for troubleshooting
- exposes JSON APIs for nodes, events, clips, health snapshots, species stats, and ingest batches
- provides a first web UI with:
  - dashboard
  - node list
  - node detail pages
  - event list
  - event detail pages with clip playback

Useful commands:

```bash
python -m bird_hub status --pretty
python -m bird_hub create-node-token bird-node-01
python -m bird_hub ingest-bundle /path/to/export.zip
```

For authenticated node uploads, create a token on the hub and place it into the node's
`BIRD_MONITOR_HUB_TOKEN` environment variable.

Main API endpoints:

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
