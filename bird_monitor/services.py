from __future__ import annotations

import atexit
import re
import threading
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from flask import Flask

from .audio import (
    extract_clip_samples,
    list_input_devices,
    peak_amplitude,
    record_segment,
    save_audio_samples,
    save_capture,
)
from .extensions import db
from .models import BirdDetection, RecorderSettings, Recording, RecordingSchedule, utc_iso
from .runtime_logging import get_birdnet_logger
from .scheduler import get_active_windows
from .species import SpeciesPrediction, build_species_classifier


class RecordingManager:
    def __init__(self, app: Flask) -> None:
        self.app = app
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="bird-monitor-recorder", daemon=True)
        self._status_lock = threading.Lock()
        self._manual_lock = threading.Lock()
        self._waveform_lock = threading.Lock()
        self._birdnet_logger = get_birdnet_logger()
        self._species_classifier = build_species_classifier()
        runtime_details = self._runtime_details()
        self._manual_mode = False
        self._manual_stop_requested = False
        self._waveform_samples: deque[float] = deque([0.0] * 120, maxlen=180)
        self._status: dict[str, object] = {
            "started": False,
            "is_recording": False,
            "manual_mode": False,
            "last_error": None,
            "last_recording_at": None,
            "current_device_name": None,
            "active_schedule_names": [],
            "last_checked_at": None,
            "segment_started_at": None,
            "activity_reason": "idle",
            "activity_message": "Waiting for the first schedule or a manual start.",
            "live_level": 0.0,
            "species_provider": "disabled",
            "species_available": self._species_classifier.available(),
            "species_enabled": False,
            "species_error": getattr(self._species_classifier, "failure_reason", None),
            "last_species_analysis_error": None,
            "birdnet_runtime_details": runtime_details,
            "birdnet_log_file": self.app.config.get("BIRDNET_LOG_FILE"),
            "app_log_file": self.app.config.get("APP_LOG_FILE"),
            "processing_stage": "idle",
            "processing_message": "Recorder is waiting. BirdNET analyzes finished segments after recording stops.",
            "last_processing_summary": "No BirdNET analysis has completed yet.",
            "last_detection_count": 0,
            "last_clip_count": 0,
            "last_detected_species": [],
            "birdnet_last_analysis_target": None,
            "birdnet_last_analysis_started_at": None,
            "birdnet_last_analysis_finished_at": None,
            "birdnet_last_analysis_duration_seconds": None,
            "birdnet_last_raw_detection_count": 0,
            "birdnet_last_merged_detection_count": 0,
            "birdnet_matches_after_recording": True,
        }
        self._birdnet_logger.info(
            "Recording manager started. BirdNET available=%s provider=%s backend=%s reason=%s",
            self._species_classifier.available(),
            runtime_details.get("provider", "birdnet"),
            runtime_details.get("runtime_backend", "unknown"),
            runtime_details.get("reason") or "none",
        )

    def start(self) -> None:
        if self._thread.is_alive():
            return
        self._thread.start()
        self._set_status(started=True)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=5)

    def request_manual_start(self) -> None:
        with self._manual_lock:
            self._manual_mode = True
            self._manual_stop_requested = False
        self._birdnet_logger.info("Manual recording start requested.")
        self._set_status(
            manual_mode=True,
            activity_reason="manual-armed",
            activity_message="Manual recording requested. The next segment will start immediately.",
            processing_stage="armed",
            processing_message="Manual recording is armed. BirdNET will analyze the segment after it ends.",
            last_error=None,
        )

    def request_manual_stop(self) -> None:
        with self._manual_lock:
            self._manual_mode = False
            self._manual_stop_requested = True
        self._birdnet_logger.info("Manual recording stop requested.")
        self._set_status(
            manual_mode=False,
            activity_message="Stopping manual recording...",
            processing_message="Stopping the current manual workflow...",
        )

    def get_status(self, include_devices: bool = True) -> dict[str, object]:
        with self._status_lock:
            data = dict(self._status)
        with self._waveform_lock:
            data["waveform_samples"] = list(self._waveform_samples)
        if include_devices:
            try:
                data["available_devices"] = list_input_devices()
            except Exception:
                data["available_devices"] = []
        return data

    def _set_status(self, **values: object) -> None:
        with self._status_lock:
            self._status.update(values)

    def _manual_requested(self) -> bool:
        with self._manual_lock:
            return self._manual_mode

    def _manual_stop_is_requested(self) -> bool:
        with self._manual_lock:
            return self._manual_stop_requested

    def _clear_manual_stop(self) -> None:
        with self._manual_lock:
            self._manual_stop_requested = False

    def _reset_waveform(self) -> None:
        with self._waveform_lock:
            self._waveform_samples.clear()
            self._waveform_samples.extend([0.0] * 120)
        self._set_status(live_level=0.0)

    def _append_waveform(self, chunk: np.ndarray) -> None:
        mono = chunk.astype(np.float32)
        if mono.ndim > 1:
            mono = np.mean(mono, axis=1)

        amplitudes = np.abs(mono)
        bucket_count = min(24, max(1, int(amplitudes.size / 256) or 1))
        buckets = np.array_split(amplitudes, bucket_count)
        values = [
            float(np.clip(np.mean(bucket) * 7.5, 0.0, 1.0))
            for bucket in buckets
            if bucket.size > 0
        ]

        if not values:
            return

        with self._waveform_lock:
            self._waveform_samples.extend(values)

        self._set_status(live_level=max(values))

    def _recording_should_stop(self, mode: str) -> bool:
        if self._stop_event.is_set():
            return True
        return mode == "manual" and self._manual_stop_is_requested()

    def _species_state(self, settings: RecorderSettings) -> tuple[str, bool, bool, str | None]:
        requested_provider = (settings.species_provider or "disabled").strip().casefold()
        species_available = self._species_classifier.available()
        species_enabled = requested_provider == "birdnet" and species_available
        species_error = None
        if requested_provider == "birdnet" and not species_available:
            species_error = getattr(self._species_classifier, "failure_reason", None) or "BirdNET is unavailable."
        return requested_provider, species_available, species_enabled, species_error

    def _runtime_details(self) -> dict[str, object]:
        details = dict(getattr(self._species_classifier, "runtime_details", {}) or {})
        details.setdefault("provider", "birdnet")
        details.setdefault("analysis_mode", "post-recording")
        details.setdefault("available", self._species_classifier.available())
        if not details.get("reason") and getattr(self._species_classifier, "failure_reason", None):
            details["reason"] = getattr(self._species_classifier, "failure_reason")
        return details

    def _analysis_details(self) -> dict[str, object]:
        return dict(getattr(self._species_classifier, "last_analysis_details", {}) or {})

    def _build_recording_summary(self, predictions: list[SpeciesPrediction]) -> tuple[str, list[str]]:
        if not predictions:
            return "BirdNET finished. No bird species were detected in the last segment.", []

        ordered_names: list[str] = []
        for prediction in sorted(predictions, key=lambda item: item.confidence, reverse=True):
            if prediction.common_name not in ordered_names:
                ordered_names.append(prediction.common_name)

        top_names = ", ".join(ordered_names[:4])
        summary = (
            f"BirdNET found {len(predictions)} detected bird occurrence(s) "
            f"across {len(ordered_names)} species: {top_names}."
        )
        return summary, ordered_names

    def _build_detection_clip_path(self, detected_at: datetime, common_name: str, detection_index: int) -> Path:
        root = Path(self.app.config["CLIPS_DIR"])
        day_path = root / detected_at.strftime("%Y") / detected_at.strftime("%m") / detected_at.strftime("%d")
        safe_name = re.sub(r"[^a-z0-9]+", "-", common_name.casefold()).strip("-") or "bird"
        filename = f"detection_{detected_at.strftime('%Y%m%dT%H%M%S_%f')}_{detection_index:02d}_{safe_name}.wav"
        return day_path / filename

    def _create_species_detections(
        self,
        *,
        capture_samples: np.ndarray,
        sample_rate: int,
        recording_started_at: datetime,
        predictions: list[SpeciesPrediction],
    ) -> tuple[list[BirdDetection], list[Path]]:
        detections: list[BirdDetection] = []
        clip_paths: list[Path] = []

        for index, prediction in enumerate(predictions, start=1):
            started_at = recording_started_at + timedelta(seconds=prediction.start_offset_seconds)
            ended_at = recording_started_at + timedelta(seconds=prediction.end_offset_seconds)
            clip_path = self._build_detection_clip_path(started_at, prediction.common_name, index)
            clip_samples = extract_clip_samples(
                capture_samples,
                sample_rate,
                prediction.start_offset_seconds,
                prediction.end_offset_seconds,
            )

            clip_file_path: str | None = None
            clip_duration_seconds: float | None = None
            if clip_samples.size > 0:
                save_audio_samples(clip_samples, sample_rate, clip_path)
                clip_paths.append(clip_path)
                clip_file_path = str(clip_path)
                clip_duration_seconds = float(clip_samples.shape[0] / max(sample_rate, 1))
                self._birdnet_logger.info(
                    "Saved BirdNET clip %s species=%s confidence=%.3f start=%.2fs end=%.2fs path=%s",
                    index,
                    prediction.common_name,
                    prediction.confidence,
                    prediction.start_offset_seconds,
                    prediction.end_offset_seconds,
                    clip_path,
                )
            else:
                self._birdnet_logger.warning(
                    "BirdNET clip %s for species=%s had no audio samples and was skipped.",
                    index,
                    prediction.common_name,
                )

            detections.append(
                BirdDetection(
                    recording_id=0,
                    started_at=started_at,
                    ended_at=ended_at,
                    confidence=prediction.confidence,
                    dominant_frequency_hz=0.0,
                    source="birdnet",
                    species_common_name=prediction.common_name,
                    species_scientific_name=prediction.scientific_name,
                    species_score=prediction.confidence,
                    clip_file_path=clip_file_path,
                    clip_duration_seconds=clip_duration_seconds,
                )
            )

        return detections, clip_paths

    def _run(self) -> None:
        while not self._stop_event.is_set():
            with self.app.app_context():
                settings = RecorderSettings.get_or_create()
                species_provider, species_available, species_enabled, species_error = self._species_state(settings)
                runtime_details = self._runtime_details()
                self._set_status(
                    species_provider=species_provider,
                    species_available=species_available,
                    species_enabled=species_enabled,
                    species_error=species_error or self.get_status(include_devices=False).get("last_species_analysis_error"),
                    birdnet_runtime_details=runtime_details,
                    birdnet_log_file=self.app.config.get("BIRDNET_LOG_FILE"),
                    app_log_file=self.app.config.get("APP_LOG_FILE"),
                )
                local_now = datetime.now().astimezone()
                schedules = RecordingSchedule.query.filter_by(enabled=True).all()
                active_windows = get_active_windows(schedules, local_now)
                manual_mode = self._manual_requested()

                if not manual_mode and not active_windows:
                    self._reset_waveform()
                    self._set_status(
                        is_recording=False,
                        manual_mode=False,
                        active_schedule_names=[],
                        activity_reason="idle",
                        activity_message="Idle. Waiting for a schedule or a manual start.",
                        processing_stage="idle",
                        processing_message="Recorder is waiting. BirdNET only runs after a finished recording segment is saved.",
                        segment_started_at=None,
                        last_checked_at=utc_iso(datetime.utcnow()),
                    )
                    if self._stop_event.wait(1):
                        break
                    continue

                active_schedule_names = [window.schedule.name for window in active_windows]
                capture_mode = "manual" if manual_mode else "schedule"
                if capture_mode == "manual":
                    segment_seconds = max(1, settings.segment_seconds)
                    activity_message = "Manual recording is active."
                else:
                    seconds_until_boundary = min(
                        max(1, int((window.ends_at - local_now).total_seconds()))
                        for window in active_windows
                    )
                    segment_seconds = max(1, min(settings.segment_seconds, seconds_until_boundary))
                    activity_message = f"Scheduled recording is active: {', '.join(active_schedule_names)}"

                started_at = datetime.utcnow()
                self._birdnet_logger.info(
                    "Preparing recording segment mode=%s segment_seconds=%s sample_rate=%s channels=%s preferred_device_index=%s preferred_device_name=%s active_schedules=%s species_enabled=%s",
                    capture_mode,
                    segment_seconds,
                    settings.sample_rate,
                    settings.channels,
                    settings.device_index if settings.device_index is not None else "auto",
                    settings.device_name or "auto",
                    active_schedule_names or ["none"],
                    species_enabled,
                )
                self._set_status(
                    is_recording=True,
                    manual_mode=manual_mode,
                    active_schedule_names=active_schedule_names,
                    activity_reason=capture_mode,
                    activity_message=activity_message,
                    processing_stage="recording",
                    processing_message="Recording audio now. BirdNET matching will start after this segment stops.",
                    segment_started_at=utc_iso(started_at),
                    last_error=None,
                    last_checked_at=utc_iso(started_at),
                )

                created_clip_paths: list[Path] = []
                try:
                    capture = record_segment(
                        duration_seconds=segment_seconds,
                        sample_rate=settings.sample_rate,
                        channels=settings.channels,
                        preferred_name=settings.device_name,
                        preferred_index=settings.device_index,
                        on_chunk=self._append_waveform,
                        should_stop=lambda: self._recording_should_stop(capture_mode),
                    )
                    ended_at = datetime.utcnow()
                    self._birdnet_logger.info(
                        "Recording segment finished sample_count=%s duration=%.2fs sample_rate=%s channels=%s device=%s peak_amplitude=%.6f manual_stop_requested=%s",
                        int(np.asarray(capture.samples).shape[0]),
                        max((ended_at - started_at).total_seconds(), 0.0),
                        capture.sample_rate,
                        capture.channels,
                        capture.device_name or "unknown",
                        peak_amplitude(capture.samples),
                        self._manual_stop_is_requested(),
                    )

                    if capture.samples.size == 0:
                        self._birdnet_logger.warning(
                            "Recording segment produced no audio samples mode=%s duration=%.2fs device=%s",
                            capture_mode,
                            max((ended_at - started_at).total_seconds(), 0.0),
                            capture.device_name or "unknown",
                        )
                        self._clear_manual_stop()
                        next_manual_mode = self._manual_requested()
                        next_reason = "manual-armed" if next_manual_mode else ("schedule" if active_schedule_names else "idle")
                        self._set_status(
                            is_recording=False,
                            manual_mode=next_manual_mode,
                            activity_reason=next_reason,
                            activity_message=(
                                "Manual recording is armed and waiting for the next segment."
                                if next_manual_mode
                                else (
                                    "Scheduled window remains active. The next segment will start shortly."
                                    if active_schedule_names
                                    else "Recording stopped before any audio was captured."
                                )
                            ),
                            processing_stage="idle",
                            processing_message="No audio was captured, so BirdNET had nothing to analyze.",
                            segment_started_at=None,
                            last_checked_at=utc_iso(ended_at),
                        )
                        if self._stop_event.wait(1):
                            break
                        continue

                    file_path = self._build_recording_path(started_at)
                    save_capture(capture, file_path)
                    self._birdnet_logger.info(
                        "Saved recording segment file=%s mode=%s duration=%.2fs sample_rate=%s channels=%s device=%s size_bytes=%s",
                        file_path,
                        capture_mode,
                        max((ended_at - started_at).total_seconds(), 0.0),
                        capture.sample_rate,
                        capture.channels,
                        capture.device_name or "unknown",
                        file_path.stat().st_size if file_path.exists() else "unknown",
                    )

                    species_predictions = []
                    last_processing_summary = "Species analysis is disabled for this recorder."
                    detected_species: list[str] = []
                    analysis_succeeded = False
                    if species_provider == "birdnet" and not species_enabled:
                        last_processing_summary = species_error or "BirdNET is unavailable, so no species analysis was run."
                        self._birdnet_logger.warning(
                            "Skipping BirdNET analysis for %s because the runtime is unavailable: %s",
                            file_path.name,
                            last_processing_summary,
                        )
                    if species_enabled:
                        analysis_started_at = datetime.utcnow()
                        self._set_status(
                            is_recording=False,
                            activity_reason="analyzing",
                            activity_message="BirdNET is analyzing the finished segment.",
                            processing_stage="analyzing",
                            processing_message="BirdNET matching is running now. It happens after recording stops, not in real time.",
                            segment_started_at=None,
                            birdnet_last_analysis_target=str(file_path),
                            birdnet_last_analysis_started_at=utc_iso(analysis_started_at),
                            birdnet_last_analysis_finished_at=None,
                            birdnet_last_analysis_duration_seconds=None,
                        )
                        self._birdnet_logger.info(
                            "Starting BirdNET post-recording analysis for %s location=%s latitude=%s longitude=%s min_confidence=%.2f",
                            file_path.name,
                            settings.location_name or "none",
                            settings.latitude if settings.latitude is not None else "none",
                            settings.longitude if settings.longitude is not None else "none",
                            settings.species_min_confidence,
                        )
                        try:
                            species_predictions = self._species_classifier.classify(
                                file_path,
                                latitude=settings.latitude,
                                longitude=settings.longitude,
                                recorded_at=started_at,
                                min_confidence=settings.species_min_confidence,
                            )
                            analysis_details = self._analysis_details()
                            analysis_succeeded = True
                            self._set_status(species_error=None, last_species_analysis_error=None)
                            last_processing_summary, detected_species = self._build_recording_summary(species_predictions)
                            self._set_status(
                                birdnet_last_analysis_finished_at=analysis_details.get("finished_at"),
                                birdnet_last_analysis_duration_seconds=analysis_details.get("duration_seconds"),
                                birdnet_last_raw_detection_count=analysis_details.get("raw_detection_count", 0),
                                birdnet_last_merged_detection_count=analysis_details.get("merged_detection_count", len(species_predictions)),
                            )
                        except Exception as exc:
                            self.app.logger.warning("Species detection failed for %s: %s", file_path, exc)
                            failure_message = f"BirdNET analysis failed: {exc}"
                            analysis_details = self._analysis_details()
                            last_processing_summary = failure_message
                            self._set_status(
                                species_error=failure_message,
                                last_species_analysis_error=failure_message,
                                birdnet_last_analysis_finished_at=analysis_details.get("finished_at"),
                                birdnet_last_analysis_duration_seconds=analysis_details.get("duration_seconds"),
                                birdnet_last_raw_detection_count=analysis_details.get("raw_detection_count", 0),
                                birdnet_last_merged_detection_count=analysis_details.get("merged_detection_count", 0),
                            )

                    timeline_detections: list[BirdDetection] = []
                    if species_predictions:
                        self._set_status(
                            processing_stage="extracting-clips",
                            processing_message=f"BirdNET found {len(species_predictions)} occurrence(s). Saving separate clip files now.",
                        )
                        self._birdnet_logger.info(
                            "BirdNET found %s merged detection(s) in %s. Extracting occurrence clips now.",
                            len(species_predictions),
                            file_path.name,
                        )
                        timeline_detections, created_clip_paths = self._create_species_detections(
                            capture_samples=capture.samples,
                            sample_rate=capture.sample_rate,
                            recording_started_at=started_at,
                            predictions=species_predictions,
                        )
                    elif species_enabled and analysis_succeeded:
                        self._birdnet_logger.info("BirdNET found no detections in %s, so no clips were created.", file_path.name)

                    self._set_status(
                        processing_stage="saving-results",
                        processing_message="Saving recording metadata, BirdNET detections, and clip references to the timeline.",
                    )
                    self._birdnet_logger.info(
                        "Persisting timeline entry file=%s duration=%.2fs detections=%s clips=%s has_bird_activity=%s",
                        file_path,
                        max((ended_at - started_at).total_seconds(), 0.0),
                        len(timeline_detections),
                        len(created_clip_paths),
                        bool(species_predictions),
                    )
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
                        has_bird_activity=bool(species_predictions),
                        bird_event_count=len(timeline_detections),
                    )
                    db.session.add(recording)
                    db.session.flush()

                    for detection in timeline_detections:
                        detection.recording_id = recording.id
                        db.session.add(detection)

                    db.session.commit()
                    self._birdnet_logger.info(
                        "Saved recording %s to the timeline with %s BirdNET detection(s) and %s clip file(s).",
                        recording.id,
                        len(timeline_detections),
                        len(created_clip_paths),
                    )
                    self._clear_manual_stop()

                    manual_still_requested = self._manual_requested()
                    next_reason = "manual-armed" if manual_still_requested else ("schedule" if active_schedule_names else "idle")
                    self._set_status(
                        is_recording=False,
                        manual_mode=manual_still_requested,
                        current_device_name=capture.device_name,
                        last_recording_at=utc_iso(ended_at),
                        activity_reason=next_reason,
                        activity_message=(
                            "Manual recording will continue with the next segment."
                            if manual_still_requested
                            else (
                                "Scheduled window is still active. The next segment starts soon."
                                if active_schedule_names
                                else "Waiting for the next schedule or manual start."
                            )
                        ),
                        processing_stage="idle",
                        processing_message="Recorder is waiting. BirdNET will run again after the next finished segment.",
                        last_processing_summary=last_processing_summary,
                        last_detection_count=len(timeline_detections),
                        last_clip_count=len(created_clip_paths),
                        last_detected_species=detected_species,
                        birdnet_last_merged_detection_count=len(timeline_detections),
                        segment_started_at=None,
                        last_error=None,
                    )
                except Exception as exc:
                    for clip_path in created_clip_paths:
                        try:
                            clip_path.unlink(missing_ok=True)
                            self._birdnet_logger.warning("Removed partially created BirdNET clip after failure path=%s", clip_path)
                        except OSError:
                            pass
                    db.session.rollback()
                    self.app.logger.exception("Recording loop failed")
                    self._birdnet_logger.exception("Background recorder loop failed.")
                    self._set_status(
                        is_recording=False,
                        manual_mode=self._manual_requested(),
                        activity_reason="idle",
                        activity_message="Recorder error. Check the message below.",
                        processing_stage="error",
                        processing_message="The background workflow stopped because of an error.",
                        segment_started_at=None,
                        last_error=str(exc),
                    )
                    if self._stop_event.wait(2):
                        break
                    continue

            if self._stop_event.wait(0.5):
                break

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
