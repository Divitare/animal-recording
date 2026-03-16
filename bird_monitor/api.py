from __future__ import annotations

import csv
import json
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

from flask import Blueprint, Response, after_this_request, current_app, jsonify, request, send_file, stream_with_context, url_for

from .analytics import (
    SPECIES_EVENT_MERGE_GAP_SECONDS,
    build_species_events,
    build_species_statistics,
)
from .audio import input_setting_supported, list_input_devices, resolve_input_device
from .extensions import db
from .geocoding import GeocodingError, geocode_address
from .models import BirdDetection, RecorderSettings, Recording, RecordingSchedule
from .runtime_logging import clear_application_logs, get_birdnet_logger, get_recent_birdnet_logs
from .services import get_background_manager

api_bp = Blueprint("api", __name__)


def parse_client_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid datetime value '{value}'.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def serialize_detection(detection: BirdDetection) -> dict[str, object]:
    payload = detection.to_dict()
    recording_audio_available = Path(detection.recording.file_path).exists() if detection.recording else False
    payload["clip_url"] = (
        url_for("api.download_detection_clip", detection_id=detection.id)
        if detection.clip_file_path
        else None
    )
    payload["recording_audio_url"] = (
        url_for("api.download_recording_audio", recording_id=detection.recording_id)
        if recording_audio_available
        else None
    )
    return payload


def serialize_recording(recording: Recording) -> dict[str, object]:
    payload = recording.to_dict()
    audio_available = Path(recording.file_path).exists()
    payload["audio_available"] = audio_available
    payload["audio_url"] = url_for("api.download_recording_audio", recording_id=recording.id) if audio_available else None
    payload["detections"] = [
        serialize_detection(detection)
        for detection in recording.detections
        if detection.species_common_name
    ]
    return payload


def _json_error(message: str, status_code: int = 400):
    response = jsonify({"error": message})
    response.status_code = status_code
    return response


def _request_actor() -> str:
    forwarded_for = request.headers.get("X-Forwarded-For", "").strip()
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.remote_addr or "unknown"


def _audit_logger():
    return get_birdnet_logger()


def _service_snapshot(include_devices: bool) -> dict[str, object]:
    manager = get_background_manager()
    if manager is not None:
        return manager.get_status(include_devices=include_devices)

    return {
        "started": False,
        "is_recording": False,
        "manual_mode": False,
        "species_provider": "disabled",
        "species_available": None,
        "species_enabled": False,
        "species_error": None,
        "processing_stage": "idle",
        "processing_message": "Recorder background service is disabled.",
        "last_processing_summary": "No BirdNET analysis has run in this process.",
        "birdnet_runtime_details": None,
        "birdnet_log_file": current_app.config.get("BIRDNET_LOG_FILE"),
        "app_log_file": current_app.config.get("APP_LOG_FILE"),
        "birdnet_live_analysis_enabled": False,
        "birdnet_live_analysis_active": False,
        "birdnet_live_window_seconds": 9,
        "birdnet_live_interval_seconds": 9,
        "birdnet_live_pending_windows": 0,
        "birdnet_live_completed_windows": 0,
        "birdnet_live_last_window_started_at": None,
        "birdnet_live_last_window_ended_at": None,
        "live_detection_count": 0,
        "live_detected_species": [],
        "live_detections": [],
        "birdnet_last_analysis_target": None,
        "birdnet_last_analysis_started_at": None,
        "birdnet_last_analysis_finished_at": None,
        "birdnet_last_analysis_duration_seconds": None,
        "birdnet_last_analysis_scope": None,
        "birdnet_last_raw_detection_count": 0,
        "birdnet_last_merged_detection_count": 0,
        "birdnet_matches_after_recording": False,
        "waveform_samples": [],
        "available_devices": [] if include_devices else None,
    }


def _remove_file_if_present(path_value: str | None) -> bool:
    if not path_value:
        return False
    file_path = Path(path_value)
    if not file_path.exists():
        return False
    try:
        file_path.unlink()
    except OSError:
        return False
    return True


def _species_detection_query():
    return BirdDetection.query.filter(BirdDetection.species_common_name.is_not(None))


def _refresh_recording_detection_summary(recording_id: int) -> None:
    recording = Recording.query.get(recording_id)
    if recording is None:
        return

    detection_count = _species_detection_query().filter(BirdDetection.recording_id == recording_id).count()
    recording.bird_event_count = detection_count
    recording.has_bird_activity = detection_count > 0


def _collect_log_files() -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()

    for configured_path in (
        current_app.config.get("BIRDNET_LOG_FILE"),
        current_app.config.get("APP_LOG_FILE"),
    ):
        if not configured_path:
            continue
        base_path = Path(configured_path)
        for candidate in [base_path, *sorted(base_path.parent.glob(f"{base_path.name}.*"))]:
            if not candidate.is_file():
                continue
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(candidate)

    return candidates


@api_bp.get("/status")
def status():
    settings = RecorderSettings.get_or_create()
    return jsonify(
        {
            "app": {
                "commit": current_app.config.get("APP_COMMIT", "unknown"),
            },
            "service": _service_snapshot(include_devices=True),
            "settings": settings.to_dict(),
            "totals": {
                "recordings": Recording.query.count(),
                "detections": BirdDetection.query.filter(BirdDetection.species_common_name.is_not(None)).count(),
            },
        }
    )


@api_bp.get("/live")
def live_status():
    return jsonify(
        {
            "app": {
                "commit": current_app.config.get("APP_COMMIT", "unknown"),
            },
            "service": _service_snapshot(include_devices=False),
        }
    )


@api_bp.get("/live-stream")
def live_stream():
    manager = get_background_manager()

    @stream_with_context
    def generate():
        if manager is None:
            payload = {
                "app": {
                    "commit": current_app.config.get("APP_COMMIT", "unknown"),
                },
                "service": _service_snapshot(include_devices=False),
            }
            yield f"data: {json.dumps(payload)}\n\n"
            return

        last_revision = -1
        while True:
            next_revision = manager.wait_for_status_revision(last_revision, timeout=1.0)
            if next_revision == last_revision:
                yield ": keep-alive\n\n"
                continue

            last_revision = next_revision
            payload = {
                "app": {
                    "commit": current_app.config.get("APP_COMMIT", "unknown"),
                },
                "service": manager.get_status(include_devices=False),
            }
            yield f"data: {json.dumps(payload)}\n\n"

    response = Response(generate(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response


@api_bp.get("/birdnet/logs")
def birdnet_logs():
    try:
        limit = min(200, max(1, int(request.args.get("limit", "80"))))
    except ValueError:
        return _json_error("The BirdNET log limit must be a number.")

    service = _service_snapshot(include_devices=False)
    return jsonify(
        {
            "items": get_recent_birdnet_logs(limit),
            "log_file": current_app.config.get("BIRDNET_LOG_FILE"),
            "app_log_file": current_app.config.get("APP_LOG_FILE"),
            "runtime": service.get("birdnet_runtime_details"),
            "last_analysis_target": service.get("birdnet_last_analysis_target"),
            "last_analysis_started_at": service.get("birdnet_last_analysis_started_at"),
            "last_analysis_finished_at": service.get("birdnet_last_analysis_finished_at"),
            "last_analysis_duration_seconds": service.get("birdnet_last_analysis_duration_seconds"),
        }
    )


@api_bp.post("/birdnet/logs/clear")
def clear_birdnet_logs():
    cleared = clear_application_logs()
    return jsonify(
        {
            "ok": True,
            "message": "Bird monitor logs were cleared.",
            **cleared,
        }
    )


@api_bp.get("/birdnet/logs/download")
def download_birdnet_logs():
    log_files = _collect_log_files()
    if not log_files:
        return _json_error("No log files are available to download.", 404)

    exports_dir = Path(current_app.config["EXPORTS_DIR"])
    exports_dir.mkdir(parents=True, exist_ok=True)

    temp_file = tempfile.NamedTemporaryFile(
        prefix="bird-monitor-logs_",
        suffix=".zip",
        dir=exports_dir,
        delete=False,
    )
    temp_file.close()

    log_dir = Path(current_app.config["LOG_DIR"]).resolve()
    with zipfile.ZipFile(temp_file.name, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        for file_path in log_files:
            try:
                archive_name = str(file_path.resolve().relative_to(log_dir))
            except ValueError:
                archive_name = file_path.name
            archive.write(file_path, arcname=archive_name)

    @after_this_request
    def _cleanup(response):
        try:
            Path(temp_file.name).unlink(missing_ok=True)
        except OSError:
            pass
        return response

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return send_file(
        temp_file.name,
        as_attachment=True,
        download_name=f"bird-monitor-logs-{timestamp}.zip",
        mimetype="application/zip",
    )


@api_bp.get("/devices")
def devices():
    try:
        items = list_input_devices()
    except Exception as exc:
        return jsonify({"items": [], "error": str(exc)})
    return jsonify({"items": items})


@api_bp.get("/settings")
def get_settings():
    return jsonify(RecorderSettings.get_or_create().to_dict())


@api_bp.post("/geocode")
def geocode_location():
    payload = request.get_json(silent=True) or {}
    query = str(payload.get("query") or "").strip()
    if not query:
        return _json_error("An address or place name is required.")

    try:
        result = geocode_address(query)
    except GeocodingError as exc:
        return _json_error(str(exc))

    _audit_logger().info(
        "Geocode resolved for %s query=%s resolved_name=%s latitude=%s longitude=%s",
        _request_actor(),
        query,
        result.display_name,
        result.latitude,
        result.longitude,
    )
    return jsonify(result.to_dict())


@api_bp.put("/settings")
def update_settings():
    payload = request.get_json(silent=True) or {}
    settings = RecorderSettings.get_or_create()
    before = settings.to_dict()

    try:
        if "device_name" in payload:
            settings.device_name = (payload.get("device_name") or "").strip() or None

        if "device_index" in payload:
            device_index = payload.get("device_index")
            settings.device_index = None if device_index in ("", None) else int(device_index)

        if "sample_rate" in payload:
            settings.sample_rate = max(8000, int(payload["sample_rate"]))

        if "channels" in payload:
            settings.channels = max(1, min(2, int(payload["channels"])))

        if "segment_seconds" in payload:
            settings.segment_seconds = max(5, int(payload["segment_seconds"]))

        if "min_event_duration_seconds" in payload:
            settings.min_event_duration_seconds = max(0.05, float(payload["min_event_duration_seconds"]))

        if "location_name" in payload:
            settings.location_name = (payload.get("location_name") or "").strip() or None

        if "latitude" in payload:
            latitude = payload.get("latitude")
            settings.latitude = None if latitude in ("", None) else float(latitude)

        if "longitude" in payload:
            longitude = payload.get("longitude")
            settings.longitude = None if longitude in ("", None) else float(longitude)

        if "species_min_confidence" in payload:
            settings.species_min_confidence = min(0.99, max(0.05, float(payload["species_min_confidence"])))
    except (TypeError, ValueError) as exc:
        return _json_error(f"Invalid settings value: {exc}")

    should_auto_geocode = bool(payload.get("auto_geocode")) or (
        settings.location_name
        and ("latitude" not in payload or payload.get("latitude") in ("", None))
        and ("longitude" not in payload or payload.get("longitude") in ("", None))
    )
    if settings.location_name and should_auto_geocode:
        try:
            geocoded = geocode_address(settings.location_name)
        except GeocodingError as exc:
            return _json_error(str(exc))
        settings.location_name = geocoded.display_name
        settings.latitude = geocoded.latitude
        settings.longitude = geocoded.longitude

    if settings.latitude is not None and not (-90.0 <= settings.latitude <= 90.0):
        return _json_error("Latitude must be between -90 and 90.")

    if settings.longitude is not None and not (-180.0 <= settings.longitude <= 180.0):
        return _json_error("Longitude must be between -180 and 180.")

    if (settings.latitude is None) != (settings.longitude is None):
        return _json_error("Set both latitude and longitude, or leave both empty.")

    if "species_provider" in payload:
        provider = str(payload.get("species_provider") or "disabled").strip().casefold()
        if provider not in {"disabled", "birdnet"}:
            return _json_error("Unknown species analysis provider.")
        settings.species_provider = provider

    try:
        resolved_index, _ = resolve_input_device(settings.device_name, settings.device_index)
    except RuntimeError as exc:
        return _json_error(str(exc))

    if not input_setting_supported(resolved_index, settings.sample_rate, settings.channels):
        return _json_error(
            f"The selected microphone does not support {settings.sample_rate} Hz with {settings.channels} channel(s)."
        )

    db.session.commit()
    _audit_logger().info(
        "Settings updated by %s device_index=%s sample_rate=%s channels=%s segment_seconds=%s species_provider=%s confidence=%.2f location=%s latitude=%s longitude=%s",
        _request_actor(),
        settings.device_index,
        settings.sample_rate,
        settings.channels,
        settings.segment_seconds,
        settings.species_provider,
        settings.species_min_confidence,
        settings.location_name or "none",
        settings.latitude if settings.latitude is not None else "none",
        settings.longitude if settings.longitude is not None else "none",
    )
    _audit_logger().info(
        "Settings delta previous=%s updated=%s",
        before,
        settings.to_dict(),
    )
    return jsonify(settings.to_dict())


@api_bp.post("/manual-recording/start")
def start_manual_recording():
    manager = get_background_manager()
    if manager is None:
        return _json_error("The recorder service is disabled.", 503)
    _audit_logger().info("Manual recording start requested by %s", _request_actor())
    manager.request_manual_start()
    return jsonify({"service": manager.get_status(include_devices=False)})


@api_bp.post("/manual-recording/stop")
def stop_manual_recording():
    manager = get_background_manager()
    if manager is None:
        return _json_error("The recorder service is disabled.", 503)
    _audit_logger().info("Manual recording stop requested by %s", _request_actor())
    manager.request_manual_stop()
    return jsonify({"service": manager.get_status(include_devices=False)})


@api_bp.get("/schedules")
def list_schedules():
    items = RecordingSchedule.query.order_by(RecordingSchedule.name.asc()).all()
    return jsonify({"items": [item.to_dict() for item in items]})


def _upsert_schedule(schedule: RecordingSchedule, payload: dict[str, object]) -> RecordingSchedule:
    name = str(payload.get("name", schedule.name or "")).strip()
    if not name:
        raise ValueError("Schedule name is required.")

    days = payload.get("days_of_week")
    if not isinstance(days, list) or not days:
        raise ValueError("Select at least one weekday.")

    start_time = str(payload.get("start_time", "")).strip()
    end_time = str(payload.get("end_time", "")).strip()
    if len(start_time) != 5 or len(end_time) != 5:
        raise ValueError("Start and end times must use HH:MM format.")

    schedule.name = name
    schedule.set_days([int(item) for item in days])
    schedule.start_time = start_time
    schedule.end_time = end_time
    schedule.enabled = bool(payload.get("enabled", True))
    return schedule


@api_bp.post("/schedules")
def create_schedule():
    payload = request.get_json(silent=True) or {}
    schedule = RecordingSchedule()
    try:
        _upsert_schedule(schedule, payload)
    except ValueError as exc:
        return _json_error(str(exc))

    db.session.add(schedule)
    db.session.commit()
    _audit_logger().info("Schedule created by %s schedule=%s", _request_actor(), schedule.to_dict())
    return jsonify(schedule.to_dict()), 201


@api_bp.put("/schedules/<int:schedule_id>")
def update_schedule(schedule_id: int):
    schedule = RecordingSchedule.query.get_or_404(schedule_id)
    before = schedule.to_dict()
    payload = request.get_json(silent=True) or {}
    try:
        _upsert_schedule(schedule, payload)
    except ValueError as exc:
        return _json_error(str(exc))
    db.session.commit()
    _audit_logger().info(
        "Schedule updated by %s previous=%s updated=%s",
        _request_actor(),
        before,
        schedule.to_dict(),
    )
    return jsonify(schedule.to_dict())


@api_bp.delete("/schedules/<int:schedule_id>")
def delete_schedule(schedule_id: int):
    schedule = RecordingSchedule.query.get_or_404(schedule_id)
    payload = schedule.to_dict()
    db.session.delete(schedule)
    db.session.commit()
    _audit_logger().info("Schedule deleted by %s schedule=%s", _request_actor(), payload)
    return jsonify({"ok": True})


@api_bp.get("/recordings")
def list_recordings():
    try:
        end_at = parse_client_datetime(request.args.get("end")) or datetime.utcnow()
        start_at = parse_client_datetime(request.args.get("start")) or (end_at - timedelta(days=7))
    except ValueError as exc:
        return _json_error(str(exc))

    recordings = (
        Recording.query.filter(Recording.started_at < end_at, Recording.ended_at > start_at)
        .order_by(Recording.started_at.asc())
        .all()
    )
    detections = [
        detection
        for recording in recordings
        for detection in recording.detections
        if detection.species_common_name
    ]
    species_events = build_species_events(recordings)
    species_stats = build_species_statistics(species_events)
    return jsonify(
        {
            "range": {
                "start": start_at.replace(tzinfo=timezone.utc).isoformat(),
                "end": end_at.replace(tzinfo=timezone.utc).isoformat(),
            },
            "items": [serialize_recording(item) for item in recordings],
            "detections": [serialize_detection(item) for item in detections],
            "species_events": [event.to_dict() for event in species_events],
            "species_stats": species_stats,
            "species_event_merge_gap_seconds": SPECIES_EVENT_MERGE_GAP_SECONDS,
        }
    )


@api_bp.get("/recordings/<int:recording_id>/audio")
def download_recording_audio(recording_id: int):
    recording = Recording.query.get_or_404(recording_id)
    file_path = Path(recording.file_path)
    if not file_path.exists():
        return _json_error("Audio file is missing on disk.", 404)
    return send_file(file_path, as_attachment=True, download_name=file_path.name)


@api_bp.delete("/recordings/<int:recording_id>")
def delete_recording(recording_id: int):
    recording = Recording.query.get_or_404(recording_id)
    payload = serialize_recording(recording)
    clip_paths = [
        detection.clip_file_path
        for detection in recording.detections
        if detection.clip_file_path
    ]
    recording_file_path = recording.file_path

    db.session.delete(recording)
    db.session.commit()

    deleted_files = 0
    if _remove_file_if_present(recording_file_path):
        deleted_files += 1
    for clip_path in clip_paths:
        if _remove_file_if_present(clip_path):
            deleted_files += 1

    _audit_logger().info(
        "Recording deleted by %s recording=%s deleted_file_count=%s",
        _request_actor(),
        payload,
        deleted_files,
    )
    return jsonify({"ok": True, "deleted_file_count": deleted_files, "recording_id": recording_id})


@api_bp.delete("/detections/<int:detection_id>")
def delete_detection(detection_id: int):
    detection = BirdDetection.query.get_or_404(detection_id)
    payload = serialize_detection(detection)
    clip_file_path = detection.clip_file_path
    recording_id = detection.recording_id

    db.session.delete(detection)
    db.session.flush()
    _refresh_recording_detection_summary(recording_id)
    db.session.commit()

    deleted_files = 1 if _remove_file_if_present(clip_file_path) else 0
    _audit_logger().info(
        "Bird clip deleted by %s detection=%s deleted_file_count=%s",
        _request_actor(),
        payload,
        deleted_files,
    )
    return jsonify(
        {
            "ok": True,
            "detection_id": detection_id,
            "recording_id": recording_id,
            "deleted_file_count": deleted_files,
        }
    )


@api_bp.post("/detections/delete-range")
def delete_detections_in_range():
    payload = request.get_json(silent=True) or {}
    try:
        start_at = parse_client_datetime(payload.get("start"))
        end_at = parse_client_datetime(payload.get("end"))
    except ValueError as exc:
        return _json_error(str(exc))

    if start_at is None or end_at is None:
        return _json_error("Both start and end are required.")
    if end_at <= start_at:
        return _json_error("The end time must be after the start time.")

    detections = (
        _species_detection_query()
        .filter(BirdDetection.started_at < end_at, BirdDetection.ended_at > start_at)
        .order_by(BirdDetection.started_at.asc())
        .all()
    )
    if not detections:
        return jsonify(
            {
                "ok": True,
                "deleted_detection_count": 0,
                "deleted_file_count": 0,
                "affected_recording_count": 0,
            }
        )

    recording_ids = {detection.recording_id for detection in detections}
    clip_paths = [detection.clip_file_path for detection in detections if detection.clip_file_path]
    detection_ids = [detection.id for detection in detections]

    for detection in detections:
        db.session.delete(detection)
    db.session.flush()

    for recording_id in recording_ids:
        _refresh_recording_detection_summary(recording_id)
    db.session.commit()

    deleted_files = 0
    for clip_path in clip_paths:
        if _remove_file_if_present(clip_path):
            deleted_files += 1

    _audit_logger().info(
        "Bird clip range deleted by %s start=%s end=%s deleted_detection_count=%s affected_recording_count=%s deleted_file_count=%s detection_ids=%s",
        _request_actor(),
        start_at.replace(tzinfo=timezone.utc).isoformat(),
        end_at.replace(tzinfo=timezone.utc).isoformat(),
        len(detection_ids),
        len(recording_ids),
        deleted_files,
        detection_ids,
    )
    return jsonify(
        {
            "ok": True,
            "deleted_detection_count": len(detection_ids),
            "deleted_file_count": deleted_files,
            "affected_recording_count": len(recording_ids),
        }
    )


@api_bp.get("/detections/<int:detection_id>/clip")
def download_detection_clip(detection_id: int):
    detection = BirdDetection.query.get_or_404(detection_id)
    if not detection.clip_file_path:
        return _json_error("No separate clip was saved for this detection.", 404)

    file_path = Path(detection.clip_file_path)
    if not file_path.exists():
        return _json_error("Detection clip is missing on disk.", 404)

    return send_file(file_path, as_attachment=False, download_name=file_path.name)


@api_bp.get("/export")
def export_recordings():
    try:
        end_at = parse_client_datetime(request.args.get("end"))
        start_at = parse_client_datetime(request.args.get("start"))
    except ValueError as exc:
        return _json_error(str(exc))
    if start_at is None or end_at is None:
        return _json_error("Both start and end query parameters are required.")
    if end_at <= start_at:
        return _json_error("The end time must be after the start time.")

    recordings = (
        Recording.query.filter(Recording.started_at < end_at, Recording.ended_at > start_at)
        .order_by(Recording.started_at.asc())
        .all()
    )
    if not recordings:
        return _json_error("No recordings overlap the selected time span.", 404)

    exports_dir = Path(current_app.config["EXPORTS_DIR"])
    exports_dir.mkdir(parents=True, exist_ok=True)

    temp_file = tempfile.NamedTemporaryFile(
        prefix="bird-monitor-export_",
        suffix=".zip",
        dir=exports_dir,
        delete=False,
    )
    temp_file.close()

    manifest_buffer = StringIO()
    writer = csv.writer(manifest_buffer)
    writer.writerow(
        [
            "recording_id",
            "started_at_utc",
            "ended_at_utc",
            "file_name",
            "bird_event_count",
            "has_bird_activity",
        ]
    )

    recordings_root = Path(current_app.config["RECORDINGS_DIR"]).resolve()

    with zipfile.ZipFile(temp_file.name, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        for recording in recordings:
            file_path = Path(recording.file_path)
            if not file_path.exists():
                continue
            try:
                archive_name = str(file_path.resolve().relative_to(recordings_root))
            except ValueError:
                archive_name = file_path.name
            archive.write(file_path, arcname=archive_name)
            writer.writerow(
                [
                    recording.id,
                    recording.started_at.replace(tzinfo=timezone.utc).isoformat(),
                    recording.ended_at.replace(tzinfo=timezone.utc).isoformat(),
                    archive_name,
                    recording.bird_event_count,
                    recording.has_bird_activity,
                ]
            )

        archive.writestr("manifest.csv", manifest_buffer.getvalue())

    @after_this_request
    def _cleanup(response):
        try:
            Path(temp_file.name).unlink(missing_ok=True)
        except OSError:
            pass
        return response

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return send_file(
        temp_file.name,
        as_attachment=True,
        download_name=f"bird-recordings-{timestamp}.zip",
        mimetype="application/zip",
    )
