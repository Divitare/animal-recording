from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from .config import BirdNodeConfig
from .storage import BirdNodeStorage


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value[:-1] if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _select_nearest_snapshot(
    snapshots: list[dict[str, object]],
    *,
    event_started_at: str | None,
) -> dict[str, object] | None:
    event_started_dt = _parse_utc(event_started_at)
    if event_started_dt is None or not snapshots:
        return None

    return min(
        snapshots,
        key=lambda item: abs(
            (
                (_parse_utc(str(item.get("captured_at") or "")) or event_started_dt)
                - event_started_dt
            ).total_seconds()
        ),
    )


def _clip_archive_name(event_id: str, original_path: Path) -> str:
    suffix = original_path.suffix or ".wav"
    return f"clips/{event_id}{suffix}"


def export_events_archive(
    config: BirdNodeConfig,
    *,
    output_path: Path | None = None,
    since_hours: float = 24.0,
    since_utc: str | None = None,
    until_utc: str | None = None,
) -> Path:
    storage = BirdNodeStorage(config.database_path, config.status_file)
    storage.initialize()

    generated_at = _utc_now_iso()
    if since_utc is None:
        since_utc = (datetime.utcnow() - timedelta(hours=max(since_hours, 0.0))).isoformat() + "Z"
    if until_utc is None:
        until_utc = generated_at

    detections = storage.list_detections(since_utc=since_utc, until_utc=until_utc)
    snapshots = storage.list_health_snapshots()

    if output_path is None:
        export_dir = config.data_dir / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        output_path = export_dir / f"{config.node_id}-events-{generated_at.replace(':', '').replace('.', '')}.zip"
    else:
        output_path = output_path.resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)

    event_records: list[dict[str, object]] = []
    snapshot_records: list[dict[str, object]] = []
    snapshot_index_by_id: dict[int, dict[str, object]] = {}
    archive_files: list[tuple[Path, str]] = []

    for snapshot in snapshots:
        snapshot_record = {
            "snapshot_id": int(snapshot["id"]),
            "node_id": snapshot.get("node_id"),
            "captured_at_utc": snapshot.get("captured_at"),
            "time_source": snapshot.get("time_source"),
            "time_synchronized": bool(snapshot.get("time_synchronized")),
            "app_version": snapshot.get("app_commit") or config.app_commit,
            "runtime_backend": snapshot.get("runtime_backend"),
            "birdnet_version": snapshot.get("birdnet_version"),
            "snapshot": snapshot.get("payload") or {},
        }
        snapshot_index_by_id[int(snapshot["id"])] = snapshot_record
        snapshot_records.append(snapshot_record)

    for detection in detections:
        nearest_snapshot = _select_nearest_snapshot(
            snapshots,
            event_started_at=str(detection.get("started_at") or ""),
        )
        clip_original_path = Path(str(detection.get("clip_file_path") or ""))
        clip_exists = clip_original_path.exists()
        clip_archive_path = None
        if clip_exists:
            clip_archive_path = _clip_archive_name(str(detection["event_id"]), clip_original_path)
            archive_files.append((clip_original_path, clip_archive_path))

        snapshot_record = None
        if nearest_snapshot is not None:
            snapshot_record = snapshot_index_by_id.get(int(nearest_snapshot["id"]))

        event_records.append(
            {
                "record_id": int(detection["id"]),
                "node_id": detection["node_id"],
                "event_id": detection["event_id"],
                "time_source": "system",
                "utc_available": True,
                "event_start_utc": detection["started_at"],
                "event_end_utc": detection["ended_at"],
                "species": {
                    "common_name": detection["species_common_name"],
                    "scientific_name": detection.get("species_scientific_name"),
                },
                "confidence": float(detection["confidence"]),
                "clip": {
                    "archive_path": clip_archive_path,
                    "original_path": str(clip_original_path),
                    "exists": clip_exists,
                    "duration_seconds": float(detection["clip_duration_seconds"]),
                    "sample_rate": int(detection["sample_rate"]),
                    "channels": int(detection["channels"]),
                },
                "source_window": {
                    "started_at_utc": detection.get("source_window_started_at"),
                    "ended_at_utc": detection.get("source_window_ended_at"),
                },
                "analysis_duration_seconds": (
                    float(detection["analysis_duration_seconds"])
                    if detection.get("analysis_duration_seconds") is not None
                    else None
                ),
                "location": {
                    "name": detection.get("location_name"),
                    "latitude": detection.get("latitude"),
                    "longitude": detection.get("longitude"),
                },
                "app_version": (
                    snapshot_record.get("app_version")
                    if snapshot_record is not None
                    else config.app_commit
                ),
                "birdnet_runtime": {
                    "provider": "birdnet",
                    "runtime_backend": (
                        snapshot_record.get("runtime_backend")
                        if snapshot_record is not None
                        else None
                    ),
                    "birdnet_version": (
                        snapshot_record.get("birdnet_version")
                        if snapshot_record is not None
                        else None
                    ),
                },
                "health_snapshot_id": (
                    snapshot_record.get("snapshot_id")
                    if snapshot_record is not None
                    else None
                ),
                "health_snapshot_captured_at_utc": (
                    snapshot_record.get("captured_at_utc")
                    if snapshot_record is not None
                    else None
                ),
                "health_snapshot": (
                    snapshot_record.get("snapshot")
                    if snapshot_record is not None
                    else None
                ),
            }
        )

    archive_manifest = {
        "generated_at_utc": generated_at,
        "node_id": config.node_id,
        "app_version": config.app_commit,
        "window": {
            "since_utc": since_utc,
            "until_utc": until_utc,
        },
        "counts": {
            "events": len(event_records),
            "health_snapshots": len(snapshot_records),
            "clip_files": len(archive_files),
        },
        "events": event_records,
        "health_snapshots": snapshot_records,
    }

    with ZipFile(output_path, mode="w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("export.json", json.dumps(archive_manifest, indent=2, sort_keys=True) + "\n")
        for source_path, archive_name in archive_files:
            if source_path.exists():
                archive.write(source_path, archive_name)

    return output_path
