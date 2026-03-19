from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any
from zipfile import ZipFile

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from .config import BirdHubConfig
from .storage import BirdHubStorage, utc_now_iso


class IngestError(RuntimeError):
    """Raised when a node export bundle cannot be ingested."""


def _copy_upload_to_temp(config: BirdHubConfig, bundle: FileStorage) -> tuple[Path, str, str]:
    filename = secure_filename(bundle.filename or "bundle.zip") or "bundle.zip"
    temp_handle = tempfile.NamedTemporaryFile(
        delete=False,
        dir=config.upload_dir,
        prefix="ingest-",
        suffix=".zip",
    )
    sha256 = hashlib.sha256()
    with temp_handle as handle:
        while True:
            chunk = bundle.stream.read(1024 * 1024)
            if not chunk:
                break
            sha256.update(chunk)
            handle.write(chunk)
    return Path(temp_handle.name), filename, sha256.hexdigest()


def _load_manifest(archive: ZipFile) -> dict[str, Any]:
    try:
        with archive.open("export.json", "r") as handle:
            return json.load(handle)
    except KeyError as exc:
        raise IngestError("Bundle is missing export.json.") from exc
    except json.JSONDecodeError as exc:
        raise IngestError("Bundle export.json is not valid JSON.") from exc


def _clip_destination(
    config: BirdHubConfig,
    node_id: str,
    event_id: str,
    event_start_utc: str,
    archive_path: str | None,
) -> tuple[Path, str]:
    suffix = Path(archive_path or "").suffix or ".wav"
    day = (event_start_utc or "unknown-date")[:10].split("-")
    if len(day) == 3:
        relative = Path(node_id) / day[0] / day[1] / day[2] / f"{event_id}{suffix}"
    else:
        relative = Path(node_id) / f"{event_id}{suffix}"
    destination = (config.clip_dir / relative).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    return destination, relative.as_posix()


def _write_clip(archive: ZipFile, archive_path: str, destination: Path) -> tuple[int, str]:
    sha256 = hashlib.sha256()
    size_bytes = 0
    with archive.open(archive_path, "r") as source, destination.open("wb") as target:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            sha256.update(chunk)
            size_bytes += len(chunk)
            target.write(chunk)
    return size_bytes, sha256.hexdigest()


def ingest_bundle_file(
    config: BirdHubConfig,
    storage: BirdHubStorage,
    bundle: FileStorage,
    *,
    authorized_node_id: str | None = None,
) -> dict[str, Any]:
    temp_path, archive_filename, archive_sha256 = _copy_upload_to_temp(config, bundle)
    try:
        with ZipFile(temp_path, "r") as archive:
            manifest = _load_manifest(archive)
            node_id = str(manifest.get("node_id") or "").strip()
            if not node_id:
                raise IngestError("Bundle export.json is missing node_id.")
            if authorized_node_id and authorized_node_id != node_id:
                raise IngestError(f"Token is not allowed to upload data for node '{node_id}'.")

            received_at = utc_now_iso()
            batch_id = storage.create_ingest_batch(
                node_id=node_id,
                archive_filename=archive_filename,
                archive_sha256=archive_sha256,
                received_at=received_at,
            )

            processed_events = 0
            processed_snapshots = 0
            processed_clips = 0
            duplicate_events = 0
            snapshot_id_map: dict[int, int] = {}

            try:
                events = list(manifest.get("events") or [])
                snapshots = list(manifest.get("health_snapshots") or [])
                app_version = str(manifest.get("app_version") or "").strip() or None

                with storage._connect() as connection:
                    storage.upsert_node(
                        connection,
                        node_id=node_id,
                        display_name=node_id,
                        seen_at=received_at,
                        app_version=app_version,
                    )

                for snapshot in snapshots:
                    source_snapshot_id = snapshot.get("snapshot_id")
                    captured_at_utc = str(snapshot.get("captured_at_utc") or "").strip()
                    if not captured_at_utc:
                        continue
                    hub_snapshot_id = storage.upsert_health_snapshot(
                        ingest_batch_id=batch_id,
                        node_id=node_id,
                        source_snapshot_id=(int(source_snapshot_id) if source_snapshot_id is not None else None),
                        captured_at_utc=captured_at_utc,
                        time_source=str(snapshot.get("time_source") or "system"),
                        time_synchronized=bool(snapshot.get("time_synchronized")),
                        app_version=str(snapshot.get("app_version") or app_version or ""),
                        runtime_backend=str(snapshot.get("runtime_backend") or "") or None,
                        birdnet_version=str(snapshot.get("birdnet_version") or "") or None,
                        snapshot_payload=dict(snapshot.get("snapshot") or {}),
                    )
                    if source_snapshot_id is not None:
                        snapshot_id_map[int(source_snapshot_id)] = hub_snapshot_id
                    processed_snapshots += 1

                for event in events:
                    event_id = str(event.get("event_id") or "").strip()
                    if not event_id:
                        continue
                    if storage.event_exists(event_id):
                        duplicate_events += 1
                        continue

                    clip_payload = dict(event.get("clip") or {})
                    archive_path = str(clip_payload.get("archive_path") or "").strip() or None
                    clip_id = None
                    if archive_path:
                        try:
                            destination_path, relative_storage_path = _clip_destination(
                                config,
                                node_id,
                                event_id,
                                str(event.get("event_start_utc") or ""),
                                archive_path,
                            )
                            size_bytes, clip_sha256 = _write_clip(archive, archive_path, destination_path)
                            clip_id = storage.insert_clip(
                                ingest_batch_id=batch_id,
                                event_id=event_id,
                                node_id=node_id,
                                storage_path=relative_storage_path,
                                original_archive_path=archive_path,
                                duration_seconds=float(clip_payload.get("duration_seconds") or 0.0),
                                sample_rate=(int(clip_payload["sample_rate"]) if clip_payload.get("sample_rate") is not None else None),
                                channels=(int(clip_payload["channels"]) if clip_payload.get("channels") is not None else None),
                                size_bytes=size_bytes,
                                sha256=clip_sha256,
                            )
                            processed_clips += 1
                        except KeyError as exc:
                            raise IngestError(
                                f"Clip '{archive_path}' referenced by event '{event_id}' is missing from the bundle."
                            ) from exc

                    species = dict(event.get("species") or {})
                    health_snapshot_id = None
                    if event.get("health_snapshot_id") is not None:
                        health_snapshot_id = snapshot_id_map.get(int(event["health_snapshot_id"]))

                    runtime = dict(event.get("birdnet_runtime") or {})
                    location = dict(event.get("location") or {})
                    source_window = dict(event.get("source_window") or {})
                    storage.insert_event(
                        ingest_batch_id=batch_id,
                        node_id=node_id,
                        event_id=event_id,
                        species_common_name=str(species.get("common_name") or "Unknown bird"),
                        species_scientific_name=str(species.get("scientific_name") or "") or None,
                        confidence=float(event.get("confidence") or 0.0),
                        event_start_utc=str(event.get("event_start_utc") or ""),
                        event_end_utc=str(event.get("event_end_utc") or ""),
                        clip_id=clip_id,
                        health_snapshot_id=health_snapshot_id,
                        app_version=str(event.get("app_version") or app_version or "") or None,
                        runtime_backend=str(runtime.get("runtime_backend") or "") or None,
                        birdnet_version=str(runtime.get("birdnet_version") or "") or None,
                        time_source=str(event.get("time_source") or "system"),
                        utc_available=bool(event.get("utc_available", True)),
                        source_window_started_at=str(source_window.get("started_at_utc") or "") or None,
                        source_window_ended_at=str(source_window.get("ended_at_utc") or "") or None,
                        analysis_duration_seconds=(
                            float(event["analysis_duration_seconds"])
                            if event.get("analysis_duration_seconds") is not None
                            else None
                        ),
                        location_name=str(location.get("name") or "") or None,
                        latitude=(float(location["latitude"]) if location.get("latitude") not in (None, "") else None),
                        longitude=(float(location["longitude"]) if location.get("longitude") not in (None, "") else None),
                    )
                    processed_events += 1

                storage.finish_ingest_batch(
                    batch_id,
                    status="completed",
                    error_message=None,
                    processed_event_count=processed_events,
                    processed_snapshot_count=processed_snapshots,
                    processed_clip_count=processed_clips,
                )
            except Exception as exc:
                storage.finish_ingest_batch(
                    batch_id,
                    status="failed",
                    error_message=str(exc),
                    processed_event_count=processed_events,
                    processed_snapshot_count=processed_snapshots,
                    processed_clip_count=processed_clips,
                )
                raise

        return {
            "status": "ok",
            "node_id": node_id,
            "archive_filename": archive_filename,
            "archive_sha256": archive_sha256,
            "processed_event_count": processed_events,
            "processed_snapshot_count": processed_snapshots,
            "processed_clip_count": processed_clips,
            "duplicate_event_count": duplicate_events,
        }
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


def ingest_bundle_path(
    config: BirdHubConfig,
    storage: BirdHubStorage,
    bundle_path: Path,
    *,
    authorized_node_id: str | None = None,
) -> dict[str, Any]:
    bundle_path = bundle_path.resolve()
    with bundle_path.open("rb") as handle:
        bundle = FileStorage(stream=handle, filename=bundle_path.name, name="bundle")
        return ingest_bundle_file(config, storage, bundle, authorized_node_id=authorized_node_id)
