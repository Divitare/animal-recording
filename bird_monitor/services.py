from __future__ import annotations

import atexit
import threading
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask

from .audio import list_input_devices, peak_amplitude, record_segment, save_capture
from .detection import detect_bird_activity
from .extensions import db
from .models import BirdDetection, RecorderSettings, Recording, RecordingSchedule, utc_iso
from .scheduler import get_active_windows
from .species import build_species_classifier, match_species_prediction


class RecordingManager:
    def __init__(self, app: Flask) -> None:
        self.app = app
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="bird-monitor-recorder", daemon=True)
        self._status_lock = threading.Lock()
        self._species_classifier = build_species_classifier()
        self._status: dict[str, object] = {
            "started": False,
            "is_recording": False,
            "last_error": None,
            "last_recording_at": None,
            "current_device_name": None,
            "active_schedule_names": [],
            "last_checked_at": None,
            "species_provider": getattr(self._species_classifier, "provider_name", "disabled"),
            "species_enabled": self._species_classifier.available(),
        }

    def start(self) -> None:
        if self._thread.is_alive():
            return
        self._thread.start()
        self._set_status(started=True)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=5)

    def get_status(self) -> dict[str, object]:
        with self._status_lock:
            data = dict(self._status)
        try:
            data["available_devices"] = list_input_devices()
        except Exception:
            data["available_devices"] = []
        return data

    def _set_status(self, **values: object) -> None:
        with self._status_lock:
            self._status.update(values)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            with self.app.app_context():
                settings = RecorderSettings.get_or_create()
                local_now = datetime.now().astimezone()
                schedules = RecordingSchedule.query.filter_by(enabled=True).all()
                active_windows = get_active_windows(schedules, local_now)

                if not active_windows:
                    self._set_status(
                        is_recording=False,
                        active_schedule_names=[],
                        last_checked_at=utc_iso(datetime.utcnow()),
                    )
                    self._stop_event.wait(5)
                    continue

                active_schedule_names = [window.schedule.name for window in active_windows]
                seconds_until_boundary = min(
                    max(1, int((window.ends_at - local_now).total_seconds()))
                    for window in active_windows
                )
                segment_seconds = max(1, min(settings.segment_seconds, seconds_until_boundary))
                started_at = datetime.utcnow()

                self._set_status(
                    is_recording=True,
                    active_schedule_names=active_schedule_names,
                    last_error=None,
                    last_checked_at=utc_iso(started_at),
                )

                try:
                    capture = record_segment(
                        duration_seconds=segment_seconds,
                        sample_rate=settings.sample_rate,
                        channels=settings.channels,
                        preferred_name=settings.device_name,
                        preferred_index=settings.device_index,
                    )
                    ended_at = datetime.utcnow()
                    file_path = self._build_recording_path(started_at)
                    save_capture(capture, file_path)

                    events = detect_bird_activity(
                        capture.samples,
                        capture.sample_rate,
                        min_event_duration_seconds=settings.min_event_duration_seconds,
                    )
                    species_predictions = []
                    if self._species_classifier.available():
                        try:
                            species_predictions = self._species_classifier.classify(file_path)
                        except Exception as exc:
                            self.app.logger.warning("Species detection failed for %s: %s", file_path, exc)

                    recording = Recording(
                        file_path=str(file_path),
                        started_at=started_at,
                        ended_at=ended_at,
                        duration_seconds=max((ended_at - started_at).total_seconds(), 0.0),
                        sample_rate=capture.sample_rate,
                        channels=capture.channels,
                        size_bytes=file_path.stat().st_size,
                        peak_amplitude=peak_amplitude(capture.samples),
                        device_name=capture.device_name,
                        has_bird_activity=bool(events),
                        bird_event_count=len(events),
                    )
                    db.session.add(recording)
                    db.session.flush()

                    for event in events:
                        species = match_species_prediction(event, species_predictions)
                        db.session.add(
                            BirdDetection(
                                recording_id=recording.id,
                                started_at=started_at + timedelta(seconds=event.start_offset_seconds),
                                ended_at=started_at + timedelta(seconds=event.end_offset_seconds),
                                confidence=event.confidence,
                                dominant_frequency_hz=event.dominant_frequency_hz,
                                species_common_name=species.common_name if species else None,
                                species_score=species.confidence if species else None,
                            )
                        )

                    db.session.commit()
                    self._set_status(
                        is_recording=False,
                        current_device_name=capture.device_name,
                        last_recording_at=utc_iso(ended_at),
                        last_error=None,
                    )
                except Exception as exc:
                    db.session.rollback()
                    self.app.logger.exception("Recording loop failed")
                    self._set_status(is_recording=False, last_error=str(exc))
                    self._stop_event.wait(5)
                    continue

            self._stop_event.wait(1)

    def _build_recording_path(self, started_at: datetime) -> Path:
        root = Path(self.app.config["RECORDINGS_DIR"])
        day_path = root / started_at.strftime("%Y") / started_at.strftime("%m") / started_at.strftime("%d")
        filename = f"recording_{started_at.strftime('%Y%m%dT%H%M%S_%f')}.wav"
        return day_path / filename


_manager_lock = threading.Lock()
_manager: RecordingManager | None = None


def start_background_services(app: Flask) -> RecordingManager | None:
    global _manager

    if app.config.get("DISABLE_BACKGROUND_RECORDER"):
        return None

    with _manager_lock:
        if _manager is None:
            _manager = RecordingManager(app)
            _manager.start()
            atexit.register(_manager.stop)
        app.extensions["recording_manager"] = _manager
        return _manager


def get_background_manager() -> RecordingManager | None:
    return _manager
