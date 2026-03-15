from __future__ import annotations

import csv
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

from flask import Blueprint, after_this_request, current_app, jsonify, request, send_file, url_for

from .analytics import (
    SPECIES_EVENT_MERGE_GAP_SECONDS,
    build_species_events,
    build_species_statistics,
)
from .audio import input_setting_supported, list_input_devices, resolve_input_device
from .extensions import db
from .geocoding import GeocodingError, geocode_address
from .models import BirdDetection, RecorderSettings, Recording, RecordingSchedule
from .runtime_logging import get_recent_birdnet_logs
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
    payload["clip_url"] = (
        url_for("api.download_detection_clip", detection_id=detection.id)
        if detection.clip_file_path
        else None
    )
    payload["recording_audio_url"] = url_for("api.download_recording_audio", recording_id=detection.recording_id)
    return payload


def serialize_recording(recording: Recording) -> dict[str, object]:
    payload = recording.to_dict()
    payload["audio_url"] = url_for("api.download_recording_audio", recording_id=recording.id)
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
        "birdnet_last_analysis_target": None,
        "birdnet_last_analysis_started_at": None,
        "birdnet_last_analysis_finished_at": None,
        "birdnet_last_analysis_duration_seconds": None,
        "birdnet_last_raw_detection_count": 0,
        "birdnet_last_merged_detection_count": 0,
        "birdnet_matches_after_recording": True,
        "waveform_samples": [],
        "available_devices": [] if include_devices else None,
    }


@api_bp.get("/status")
def status():
    settings = RecorderSettings.get_or_create()
    return jsonify(
        {
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
            "service": _service_snapshot(include_devices=False),
        }
    )


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

    return jsonify(result.to_dict())


@api_bp.put("/settings")
def update_settings():
    payload = request.get_json(silent=True) or {}
    settings = RecorderSettings.get_or_create()

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
    return jsonify(settings.to_dict())


@api_bp.post("/manual-recording/start")
def start_manual_recording():
    manager = get_background_manager()
    if manager is None:
        return _json_error("The recorder service is disabled.", 503)
    manager.request_manual_start()
    return jsonify({"service": manager.get_status(include_devices=False)})


@api_bp.post("/manual-recording/stop")
def stop_manual_recording():
    manager = get_background_manager()
    if manager is None:
        return _json_error("The recorder service is disabled.", 503)
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
    return jsonify(schedule.to_dict()), 201


@api_bp.put("/schedules/<int:schedule_id>")
def update_schedule(schedule_id: int):
    schedule = RecordingSchedule.query.get_or_404(schedule_id)
    payload = request.get_json(silent=True) or {}
    try:
        _upsert_schedule(schedule, payload)
    except ValueError as exc:
        return _json_error(str(exc))
    db.session.commit()
    return jsonify(schedule.to_dict())


@api_bp.delete("/schedules/<int:schedule_id>")
def delete_schedule(schedule_id: int):
    schedule = RecordingSchedule.query.get_or_404(schedule_id)
    db.session.delete(schedule)
    db.session.commit()
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
