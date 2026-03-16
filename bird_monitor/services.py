from __future__ import annotations

import atexit
import re
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from flask import Flask

from .audio import (
    extract_clip_to_file,
    list_input_devices,
    record_continuous_session,
)
from .extensions import db
from .models import BirdDetection, RecorderSettings, Recording, RecordingSchedule, utc_iso
from .runtime_logging import get_birdnet_logger
from .scheduler import get_active_windows
from .species import build_species_classifier

LIVE_BIRDNET_WINDOW_SECONDS = 9
MINIMUM_LIVE_ANALYSIS_SECONDS = 3
LIVE_DETECTION_PREVIEW_LIMIT = 5


@dataclass(frozen=True)
class SessionSpeciesDetection:
    started_at: datetime
    ended_at: datetime
    confidence: float
    species_common_name: str
    species_scientific_name: str | None


class AnalysisWindowAccumulator:
    def __init__(self, window_frames: int) -> None:
        self.window_frames = window_frames
        self._chunks: deque[np.ndarray] = deque()
        self._frame_count = 0

    def push(self, chunk: np.ndarray) -> list[np.ndarray]:
        prepared = np.asarray(chunk, dtype=np.float32).copy()
        if prepared.ndim == 1:
            prepared = prepared.reshape(-1, 1)

        self._chunks.append(prepared)
        self._frame_count += int(prepared.shape[0])

        windows: list[np.ndarray] = []
        while self._frame_count >= self.window_frames:
            windows.append(self._pop_frames(self.window_frames))
        return windows

    def flush_remainder(self, min_frames: int) -> np.ndarray | None:
        if self._frame_count < min_frames:
            return None
        return self._pop_frames(self._frame_count)

    def _pop_frames(self, frame_count: int) -> np.ndarray:
        remaining_frames = frame_count
        pieces: list[np.ndarray] = []

        while remaining_frames > 0 and self._chunks:
            chunk = self._chunks[0]
            chunk_frames = int(chunk.shape[0])
            if chunk_frames <= remaining_frames:
                pieces.append(chunk)
                self._chunks.popleft()
                remaining_frames -= chunk_frames
                continue

            pieces.append(chunk[:remaining_frames].copy())
            self._chunks[0] = chunk[remaining_frames:].copy()
            remaining_frames = 0

        consumed_frames = frame_count - remaining_frames
        self._frame_count = max(self._frame_count - consumed_frames, 0)

        if not pieces:
            return np.zeros((0, 1), dtype=np.float32)
        return np.concatenate(pieces, axis=0)


class RecordingManager:
    def __init__(self, app: Flask) -> None:
        self.app = app
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="bird-monitor-recorder", daemon=True)
        self._status_lock = threading.Lock()
        self._live_status_condition = threading.Condition()
        self._live_status_revision = 0
        self._manual_lock = threading.Lock()
        self._waveform_lock = threading.Lock()
        self._birdnet_logger = get_birdnet_logger()
        self._species_classifier = build_species_classifier()
        self._analysis_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="birdnet-live")
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
            "birdnet_live_window_seconds": LIVE_BIRDNET_WINDOW_SECONDS,
            "birdnet_live_interval_seconds": LIVE_BIRDNET_WINDOW_SECONDS,
            "birdnet_live_analysis_enabled": self._species_classifier.available(),
            "birdnet_live_analysis_active": False,
            "birdnet_live_pending_windows": 0,
            "birdnet_live_completed_windows": 0,
            "birdnet_live_last_window_started_at": None,
            "birdnet_live_last_window_ended_at": None,
            "live_detection_count": 0,
            "live_detected_species": [],
            "live_detections": [],
            "processing_stage": "idle",
            "processing_message": "Recorder is waiting. BirdNET checks each finished 9-second window while recording continues.",
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
            "birdnet_matches_after_recording": False,
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
        self._analysis_executor.shutdown(wait=False, cancel_futures=False)

    def request_manual_start(self) -> None:
        with self._manual_lock:
            self._manual_mode = True
            self._manual_stop_requested = False
        self._birdnet_logger.info("Manual recording start requested.")
        self._set_status(
            manual_mode=True,
            activity_reason="manual-armed",
            activity_message="Manual recording requested. Recording will continue until you press Stop.",
            processing_stage="armed",
            processing_message="Manual recording is armed. BirdNET will analyze each finished 9-second window while recording continues.",
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
            processing_message="Stopping the current recording and finishing any pending BirdNET windows...",
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
        with self._live_status_condition:
            self._live_status_revision += 1
            self._live_status_condition.notify_all()

    def current_status_revision(self) -> int:
        with self._live_status_condition:
            return self._live_status_revision

    def wait_for_status_revision(self, previous_revision: int, timeout: float = 1.0) -> int:
        with self._live_status_condition:
            if self._live_status_revision <= previous_revision:
                self._live_status_condition.wait(timeout=timeout)
            return self._live_status_revision

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
        details["analysis_mode"] = "continuous-live-9-second-windows"
        details.setdefault("available", self._species_classifier.available())
        if not details.get("reason") and getattr(self._species_classifier, "failure_reason", None):
            details["reason"] = getattr(self._species_classifier, "failure_reason")
        return details

    def _analysis_details(self) -> dict[str, object]:
        return dict(getattr(self._species_classifier, "last_analysis_details", {}) or {})

    def _build_recording_summary(self, detections: list[SessionSpeciesDetection]) -> tuple[str, list[str]]:
        if not detections:
            return "BirdNET finished. No bird species were detected in the last recording.", []

        ordered_names: list[str] = []
        for detection in sorted(detections, key=lambda item: item.confidence, reverse=True):
            if detection.species_common_name not in ordered_names:
                ordered_names.append(detection.species_common_name)

        top_names = ", ".join(ordered_names[:4])
        summary = (
            f"BirdNET found {len(detections)} detected bird occurrence(s) "
            f"across {len(ordered_names)} species: {top_names}."
        )
        return summary, ordered_names

    def _build_detection_clip_path(self, detected_at: datetime, common_name: str, detection_index: int) -> Path:
        root = Path(self.app.config["CLIPS_DIR"])
        day_path = root / detected_at.strftime("%Y") / detected_at.strftime("%m") / detected_at.strftime("%d")
        safe_name = re.sub(r"[^a-z0-9]+", "-", common_name.casefold()).strip("-") or "bird"
        filename = f"detection_{detected_at.strftime('%Y%m%dT%H%M%S_%f')}_{detection_index:02d}_{safe_name}.wav"
        return day_path / filename

    def _mix_to_mono(self, samples: np.ndarray) -> np.ndarray:
        prepared = np.asarray(samples, dtype=np.float32)
        if prepared.ndim == 2:
            prepared = np.mean(prepared, axis=1, dtype=np.float32)
        return np.ascontiguousarray(prepared.reshape(-1))

    def _unique_species_names(self, detections: list[SessionSpeciesDetection]) -> list[str]:
        ordered_names: list[str] = []
        for detection in sorted(detections, key=lambda item: item.confidence, reverse=True):
            if detection.species_common_name not in ordered_names:
                ordered_names.append(detection.species_common_name)
        return ordered_names

    def _top_live_window_species(self, detections: list[SessionSpeciesDetection]) -> list[SessionSpeciesDetection]:
        best_by_species: dict[tuple[str, str | None], SessionSpeciesDetection] = {}

        for detection in detections:
            key = (detection.species_common_name, detection.species_scientific_name)
            existing = best_by_species.get(key)
            if existing is None:
                best_by_species[key] = detection
                continue

            best_by_species[key] = SessionSpeciesDetection(
                started_at=min(existing.started_at, detection.started_at),
                ended_at=max(existing.ended_at, detection.ended_at),
                confidence=max(existing.confidence, detection.confidence),
                species_common_name=detection.species_common_name,
                species_scientific_name=detection.species_scientific_name or existing.species_scientific_name,
            )

        return sorted(
            best_by_species.values(),
            key=lambda item: (item.confidence, item.started_at),
            reverse=True,
        )[:LIVE_DETECTION_PREVIEW_LIMIT]

    def _live_detection_preview(self, detections: list[SessionSpeciesDetection]) -> list[dict[str, object]]:
        ordered = self._top_live_window_species(detections)
        return [
            {
                "started_at": utc_iso(item.started_at),
                "ended_at": utc_iso(item.ended_at),
                "species_common_name": item.species_common_name,
                "species_scientific_name": item.species_scientific_name,
                "confidence": item.confidence,
            }
            for item in ordered[:LIVE_DETECTION_PREVIEW_LIMIT]
        ]

    def _classify_live_window(
        self,
        *,
        samples: np.ndarray,
        sample_rate: int,
        window_started_at: datetime,
        latitude: float | None,
        longitude: float | None,
        min_confidence: float,
        capture_mode: str,
        window_index: int,
    ) -> tuple[list[SessionSpeciesDetection], dict[str, object]]:
        window_duration_seconds = float(np.asarray(samples).reshape(-1).shape[0] / max(sample_rate, 1))
        window_ended_at = window_started_at + timedelta(seconds=window_duration_seconds)
        self._birdnet_logger.info(
            "Submitting live BirdNET window index=%s mode=%s started_at=%s ended_at=%s duration=%.2fs sample_rate=%s min_confidence=%.2f",
            window_index,
            capture_mode,
            window_started_at.isoformat(),
            window_ended_at.isoformat(),
            window_duration_seconds,
            sample_rate,
            min_confidence,
        )
        predictions = self._species_classifier.classify_samples(
            samples,
            sample_rate=sample_rate,
            latitude=latitude,
            longitude=longitude,
            recorded_at=window_started_at,
            min_confidence=min_confidence,
            source_label=f"live-window-{window_index:03d}",
        )
        detections = [
            SessionSpeciesDetection(
                started_at=window_started_at + timedelta(seconds=prediction.start_offset_seconds),
                ended_at=window_started_at + timedelta(seconds=prediction.end_offset_seconds),
                confidence=prediction.confidence,
                species_common_name=prediction.common_name,
                species_scientific_name=prediction.scientific_name,
            )
            for prediction in predictions
        ]
        summary = {
            "window_index": window_index,
            "window_started_at": utc_iso(window_started_at),
            "window_ended_at": utc_iso(window_ended_at),
            "window_duration_seconds": window_duration_seconds,
            "detections": detections,
            "species": self._unique_species_names(detections),
        }
        return detections, summary

    def _create_species_detections(
        self,
        *,
        recording_file_path: Path,
        recording_started_at: datetime,
        detections: list[SessionSpeciesDetection],
    ) -> tuple[list[BirdDetection], list[Path]]:
        timeline_detections: list[BirdDetection] = []
        clip_paths: list[Path] = []

        for index, item in enumerate(detections, start=1):
            clip_path = self._build_detection_clip_path(item.started_at, item.species_common_name, index)
            clip_file_path: str | None = None
            clip_duration_seconds: float | None = None
            clip_duration_seconds = extract_clip_to_file(
                recording_file_path,
                clip_path,
                max((item.started_at - recording_started_at).total_seconds(), 0.0),
                max((item.ended_at - recording_started_at).total_seconds(), 0.0),
            )
            if clip_duration_seconds is not None:
                clip_paths.append(clip_path)
                clip_file_path = str(clip_path)
                self._birdnet_logger.info(
                    "Saved BirdNET clip %s species=%s confidence=%.3f start=%.2fs end=%.2fs path=%s",
                    index,
                    item.species_common_name,
                    item.confidence,
                    max((item.started_at - recording_started_at).total_seconds(), 0.0),
                    max((item.ended_at - recording_started_at).total_seconds(), 0.0),
                    clip_path,
                )
            else:
                self._birdnet_logger.warning(
                    "BirdNET clip %s for species=%s had no audio samples and was skipped.",
                    index,
                    item.species_common_name,
                )

            timeline_detections.append(
                BirdDetection(
                    recording_id=0,
                    started_at=item.started_at,
                    ended_at=item.ended_at,
                    confidence=item.confidence,
                    dominant_frequency_hz=0.0,
                    source="birdnet",
                    species_common_name=item.species_common_name,
                    species_scientific_name=item.species_scientific_name,
                    species_score=item.confidence,
                    clip_file_path=clip_file_path,
                    clip_duration_seconds=clip_duration_seconds,
                )
            )

        return timeline_detections, clip_paths

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
                    birdnet_live_analysis_enabled=species_enabled,
                    birdnet_live_window_seconds=LIVE_BIRDNET_WINDOW_SECONDS,
                    birdnet_live_interval_seconds=LIVE_BIRDNET_WINDOW_SECONDS,
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
                        processing_message="Recorder is waiting. BirdNET checks each finished 9-second window while recording continues.",
                        segment_started_at=None,
                        birdnet_live_analysis_active=False,
                        birdnet_live_pending_windows=0,
                        last_checked_at=utc_iso(datetime.utcnow()),
                    )
                    if self._stop_event.wait(1):
                        break
                    continue

                active_schedule_names = [window.schedule.name for window in active_windows]
                capture_mode = "manual" if manual_mode else "schedule"
                if capture_mode == "manual":
                    activity_message = "Manual recording is active. It will continue until you press Stop."
                else:
                    activity_message = (
                        f"Scheduled recording is active: {', '.join(active_schedule_names)}. "
                        "It will continue until the schedule window ends."
                    )

                started_at = datetime.utcnow()
                file_path = self._build_recording_path(started_at)
                live_window_frames = max(1, int(settings.sample_rate * LIVE_BIRDNET_WINDOW_SECONDS))
                minimum_live_frames = max(1, int(settings.sample_rate * MINIMUM_LIVE_ANALYSIS_SECONDS))
                live_window_accumulator = AnalysisWindowAccumulator(live_window_frames)
                session_lock = threading.Lock()
                session_detections: list[SessionSpeciesDetection] = []
                latest_live_window_detections: list[SessionSpeciesDetection] = []
                analysis_futures: set[Future] = set()
                pending_window_count = 0
                completed_window_count = 0
                successful_window_count = 0
                submitted_frames_total = 0
                submitted_window_count = 0
                live_failures: list[str] = []

                def session_should_stop() -> bool:
                    if self._stop_event.is_set():
                        return True
                    if capture_mode == "manual":
                        return self._manual_stop_is_requested()
                    return not bool(get_active_windows(schedules, datetime.now().astimezone()))

                def refresh_live_status(message: str | None = None) -> None:
                    with session_lock:
                        detections_snapshot = list(latest_live_window_detections)
                        pending_count = pending_window_count
                        completed_count = completed_window_count
                    preview = self._live_detection_preview(detections_snapshot)
                    self._set_status(
                        birdnet_live_analysis_active=pending_count > 0,
                        birdnet_live_pending_windows=pending_count,
                        birdnet_live_completed_windows=completed_count,
                        live_detection_count=len(detections_snapshot),
                        live_detected_species=self._unique_species_names(detections_snapshot),
                        live_detections=preview,
                        processing_message=message
                        or (
                            "Recording audio now. BirdNET is analyzing each finished 9-second window in parallel."
                            if pending_count > 0
                            else "Recording audio now. BirdNET is caught up with the finished 9-second windows."
                        ),
                    )

                def handle_live_window_completion(future: Future) -> None:
                    nonlocal pending_window_count, completed_window_count, successful_window_count, latest_live_window_detections
                    with session_lock:
                        analysis_futures.discard(future)
                    try:
                        detections, summary = future.result()
                    except Exception as exc:
                        analysis_details = self._analysis_details()
                        with session_lock:
                            pending_window_count = max(pending_window_count - 1, 0)
                            completed_window_count += 1
                            live_failures.append(str(exc))
                        self._set_status(
                            species_error=f"Live BirdNET window failed: {exc}",
                            last_species_analysis_error=f"Live BirdNET window failed: {exc}",
                            birdnet_last_analysis_target=analysis_details.get("file_path"),
                            birdnet_last_analysis_started_at=analysis_details.get("started_at"),
                            birdnet_last_analysis_finished_at=analysis_details.get("finished_at"),
                            birdnet_last_analysis_duration_seconds=analysis_details.get("duration_seconds"),
                            birdnet_last_raw_detection_count=analysis_details.get("raw_detection_count", 0),
                            birdnet_last_merged_detection_count=analysis_details.get("merged_detection_count", 0),
                        )
                        refresh_live_status(
                            f"Recording audio now. One live BirdNET window failed, but recording is still running: {exc}"
                        )
                        return

                    analysis_details = self._analysis_details()
                    with session_lock:
                        pending_window_count = max(pending_window_count - 1, 0)
                        completed_window_count += 1
                        successful_window_count += 1
                        session_detections.extend(detections)
                        session_detections.sort(key=lambda item: item.started_at)
                        latest_live_window_detections = self._top_live_window_species(detections)

                    self._birdnet_logger.info(
                        "Live BirdNET window completed index=%s started_at=%s ended_at=%s detections=%s species=%s",
                        summary["window_index"],
                        summary["window_started_at"],
                        summary["window_ended_at"],
                        len(summary["detections"]),
                        ", ".join(summary["species"]) if summary["species"] else "none",
                    )
                    self._set_status(
                        species_error=None,
                        last_species_analysis_error=None,
                        birdnet_last_analysis_target=analysis_details.get("file_path"),
                        birdnet_last_analysis_started_at=analysis_details.get("started_at"),
                        birdnet_last_analysis_finished_at=analysis_details.get("finished_at"),
                        birdnet_last_analysis_duration_seconds=analysis_details.get("duration_seconds"),
                        birdnet_last_raw_detection_count=analysis_details.get("raw_detection_count", 0),
                        birdnet_last_merged_detection_count=analysis_details.get("merged_detection_count", 0),
                        birdnet_live_last_window_started_at=summary["window_started_at"],
                        birdnet_live_last_window_ended_at=summary["window_ended_at"],
                    )
                    refresh_live_status(
                        "Recording audio now. BirdNET is analyzing each finished 9-second window in parallel."
                    )

                def submit_live_window(window_samples: np.ndarray, window_started_at: datetime) -> None:
                    nonlocal pending_window_count, submitted_window_count
                    mono_window = self._mix_to_mono(window_samples)
                    if mono_window.size < minimum_live_frames:
                        self._birdnet_logger.info(
                            "Skipping live BirdNET window shorter than %ss started_at=%s sample_count=%s",
                            MINIMUM_LIVE_ANALYSIS_SECONDS,
                            window_started_at.isoformat(),
                            int(mono_window.size),
                        )
                        return

                    submitted_window_count += 1
                    with session_lock:
                        pending_window_count += 1

                    future = self._analysis_executor.submit(
                        self._classify_live_window,
                        samples=mono_window,
                        sample_rate=settings.sample_rate,
                        window_started_at=window_started_at,
                        latitude=settings.latitude,
                        longitude=settings.longitude,
                        min_confidence=settings.species_min_confidence,
                        capture_mode=capture_mode,
                        window_index=submitted_window_count,
                    )
                    with session_lock:
                        analysis_futures.add(future)
                    future.add_done_callback(handle_live_window_completion)
                    refresh_live_status(
                        "Recording audio now. BirdNET is analyzing each finished 9-second window in parallel."
                    )

                def on_chunk(chunk: np.ndarray) -> None:
                    nonlocal submitted_frames_total
                    self._append_waveform(chunk)
                    if not species_enabled:
                        return

                    for live_window in live_window_accumulator.push(chunk):
                        window_started_at = started_at + timedelta(
                            seconds=float(submitted_frames_total / max(settings.sample_rate, 1))
                        )
                        submitted_frames_total += int(live_window.shape[0])
                        submit_live_window(live_window, window_started_at)

                self._birdnet_logger.info(
                    "Preparing continuous recording mode=%s sample_rate=%s channels=%s preferred_device_index=%s preferred_device_name=%s active_schedules=%s species_enabled=%s live_window_seconds=%s",
                    capture_mode,
                    settings.sample_rate,
                    settings.channels,
                    settings.device_index if settings.device_index is not None else "auto",
                    settings.device_name or "auto",
                    active_schedule_names or ["none"],
                    species_enabled,
                    LIVE_BIRDNET_WINDOW_SECONDS,
                )
                self._set_status(
                    is_recording=True,
                    manual_mode=manual_mode,
                    active_schedule_names=active_schedule_names,
                    activity_reason=capture_mode,
                    activity_message=activity_message,
                    processing_stage="recording",
                    processing_message="Recording audio now. BirdNET will analyze each finished 9-second window while the recording keeps going.",
                    segment_started_at=utc_iso(started_at),
                    last_error=None,
                    live_detection_count=0,
                    live_detected_species=[],
                    live_detections=[],
                    birdnet_live_analysis_active=False,
                    birdnet_live_pending_windows=0,
                    birdnet_live_completed_windows=0,
                    birdnet_live_last_window_started_at=None,
                    birdnet_live_last_window_ended_at=None,
                    last_checked_at=utc_iso(started_at),
                )

                created_clip_paths: list[Path] = []
                try:
                    session_capture = record_continuous_session(
                        target_path=file_path,
                        sample_rate=settings.sample_rate,
                        channels=settings.channels,
                        preferred_name=settings.device_name,
                        preferred_index=settings.device_index,
                        on_chunk=on_chunk,
                        should_stop=session_should_stop,
                    )
                    ended_at = datetime.utcnow()
                    self._birdnet_logger.info(
                        "Recording session finished frame_count=%s duration=%.2fs sample_rate=%s channels=%s device=%s peak_amplitude=%.6f manual_stop_requested=%s file=%s",
                        session_capture.frame_count,
                        session_capture.duration_seconds,
                        session_capture.sample_rate,
                        session_capture.channels,
                        session_capture.device_name or "unknown",
                        session_capture.peak_amplitude,
                        self._manual_stop_is_requested(),
                        file_path,
                    )

                    if session_capture.frame_count <= 0:
                        try:
                            file_path.unlink(missing_ok=True)
                        except OSError:
                            pass
                        self._birdnet_logger.warning(
                            "Recording session produced no audio samples mode=%s duration=%.2fs device=%s",
                            capture_mode,
                            session_capture.duration_seconds,
                            session_capture.device_name or "unknown",
                        )
                        self._clear_manual_stop()
                        next_manual_mode = self._manual_requested()
                        next_reason = "manual-armed" if next_manual_mode else ("schedule" if active_schedule_names else "idle")
                        self._set_status(
                            is_recording=False,
                            manual_mode=next_manual_mode,
                            activity_reason=next_reason,
                            activity_message=(
                                "Manual recording is armed and waiting to start."
                                if next_manual_mode
                                else (
                                    "Scheduled window remains active. Recording will restart shortly."
                                    if active_schedule_names
                                    else "Recording stopped before any audio was captured."
                                )
                            ),
                            processing_stage="idle",
                            processing_message="No audio was captured, so BirdNET had nothing to analyze.",
                            segment_started_at=None,
                            live_detection_count=0,
                            live_detected_species=[],
                            live_detections=[],
                            birdnet_live_analysis_active=False,
                            birdnet_live_pending_windows=0,
                            last_checked_at=utc_iso(ended_at),
                        )
                        if self._stop_event.wait(1):
                            break
                        continue

                    self._birdnet_logger.info(
                        "Saved continuous recording file=%s mode=%s duration=%.2fs sample_rate=%s channels=%s device=%s size_bytes=%s",
                        file_path,
                        capture_mode,
                        session_capture.duration_seconds,
                        session_capture.sample_rate,
                        session_capture.channels,
                        session_capture.device_name or "unknown",
                        file_path.stat().st_size if file_path.exists() else "unknown",
                    )

                    last_processing_summary = "Species analysis is disabled for this recorder."
                    detected_species: list[str] = []
                    if species_enabled:
                        remainder_window = live_window_accumulator.flush_remainder(minimum_live_frames)
                        if remainder_window is not None:
                            window_started_at = started_at + timedelta(
                                seconds=float(submitted_frames_total / max(settings.sample_rate, 1))
                            )
                            submitted_frames_total += int(remainder_window.shape[0])
                            self._birdnet_logger.info(
                                "Submitting final partial live BirdNET window after recording stop started_at=%s sample_count=%s",
                                window_started_at.isoformat(),
                                int(remainder_window.shape[0]),
                            )
                            submit_live_window(remainder_window, window_started_at)

                    with session_lock:
                        pending_futures = list(analysis_futures)
                    if pending_futures:
                        self._set_status(
                            is_recording=False,
                            activity_reason="analyzing",
                            activity_message="Recording stopped. BirdNET is finishing the last live windows.",
                            processing_stage="analyzing",
                            processing_message=(
                                f"Recording stopped. Waiting for {len(pending_futures)} live BirdNET window(s) to finish before saving the result."
                            ),
                            segment_started_at=None,
                        )
                        for future in pending_futures:
                            try:
                                future.result()
                            except Exception:
                                pass

                    live_detections = sorted(session_detections, key=lambda item: item.started_at)
                    if species_provider == "birdnet" and not species_enabled:
                        last_processing_summary = species_error or "BirdNET is unavailable, so no species analysis was run."
                        self._birdnet_logger.warning(
                            "Skipping BirdNET live analysis for %s because the runtime is unavailable: %s",
                            file_path.name,
                            last_processing_summary,
                        )
                    elif species_enabled and submitted_window_count == 0:
                        last_processing_summary = (
                            f"BirdNET was enabled, but the recording ended before a {MINIMUM_LIVE_ANALYSIS_SECONDS}-second analysis window was available."
                        )
                    elif species_enabled and successful_window_count == 0 and live_failures:
                        last_processing_summary = f"BirdNET live analysis failed: {live_failures[-1]}"
                    elif species_enabled:
                        self._set_status(species_error=None, last_species_analysis_error=None)
                        last_processing_summary, detected_species = self._build_recording_summary(live_detections)

                    if live_failures and successful_window_count > 0:
                        last_processing_summary = f"{last_processing_summary} Some live windows failed: {live_failures[-1]}"

                    timeline_detections: list[BirdDetection] = []
                    if live_detections:
                        self._set_status(
                            processing_stage="extracting-clips",
                            processing_message=f"BirdNET found {len(live_detections)} occurrence(s). Saving separate clip files now.",
                        )
                        self._birdnet_logger.info(
                            "BirdNET found %s merged live detection(s) in %s. Extracting occurrence clips now.",
                            len(live_detections),
                            file_path.name,
                        )
                        timeline_detections, created_clip_paths = self._create_species_detections(
                            recording_file_path=file_path,
                            recording_started_at=started_at,
                            detections=live_detections,
                        )
                    elif species_enabled and successful_window_count > 0:
                        self._birdnet_logger.info("BirdNET found no detections in %s, so no clips were created.", file_path.name)

                    discard_recording = species_enabled and successful_window_count > 0 and not live_detections and not live_failures
                    if discard_recording:
                        try:
                            file_path.unlink(missing_ok=True)
                        except OSError as exc:
                            self._birdnet_logger.warning(
                                "BirdNET found no birds in %s, but deleting the unneeded recording failed: %s",
                                file_path.name,
                                exc,
                            )
                        else:
                            self._birdnet_logger.info(
                                "BirdNET found no birds in %s after %s successful live window(s). Discarded the unneeded recording file.",
                                file_path.name,
                                successful_window_count,
                            )

                        self._clear_manual_stop()
                        manual_still_requested = self._manual_requested()
                        next_reason = "manual-armed" if manual_still_requested else ("schedule" if active_schedule_names else "idle")
                        discard_summary = (
                            "BirdNET checked the finished 9-second windows and found no birds, so the recording was discarded."
                        )
                        self._set_status(
                            is_recording=False,
                            manual_mode=manual_still_requested,
                            current_device_name=session_capture.device_name,
                            last_recording_at=utc_iso(ended_at),
                            activity_reason=next_reason,
                            activity_message=(
                                "Manual recording will continue until you press Stop."
                                if manual_still_requested
                                else (
                                    "Scheduled window is still active. Recording continues in the next session shortly."
                                    if active_schedule_names
                                    else "Waiting for the next schedule or manual start."
                                )
                            ),
                            processing_stage="idle",
                            processing_message="Recorder is waiting. The last 9-second BirdNET window found no birds, so nothing was saved.",
                            last_processing_summary=discard_summary,
                            last_detection_count=0,
                            last_clip_count=0,
                            last_detected_species=[],
                            live_detection_count=len(latest_live_window_detections),
                            live_detected_species=self._unique_species_names(latest_live_window_detections),
                            live_detections=self._live_detection_preview(latest_live_window_detections),
                            birdnet_live_analysis_active=False,
                            birdnet_live_pending_windows=0,
                            birdnet_live_completed_windows=completed_window_count,
                            birdnet_last_merged_detection_count=0,
                            segment_started_at=None,
                            last_error=None,
                        )
                        continue

                    self._set_status(
                        processing_stage="saving-results",
                        processing_message="Saving recording metadata, BirdNET detections, and clip references to the timeline.",
                    )
                    self._birdnet_logger.info(
                        "Persisting timeline entry file=%s duration=%.2fs detections=%s clips=%s has_bird_activity=%s",
                        file_path,
                        session_capture.duration_seconds,
                        len(timeline_detections),
                        len(created_clip_paths),
                        bool(live_detections),
                    )
                    recording = Recording(
                        file_path=str(file_path),
                        started_at=started_at,
                        ended_at=ended_at,
                        duration_seconds=session_capture.duration_seconds,
                        sample_rate=session_capture.sample_rate,
                        channels=session_capture.channels,
                        size_bytes=file_path.stat().st_size,
                        peak_amplitude=session_capture.peak_amplitude,
                        device_name=session_capture.device_name,
                        has_bird_activity=bool(live_detections),
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
                        current_device_name=session_capture.device_name,
                        last_recording_at=utc_iso(ended_at),
                        activity_reason=next_reason,
                        activity_message=(
                            "Manual recording will continue until you press Stop."
                            if manual_still_requested
                            else (
                                "Scheduled window is still active. Recording continues in the next session shortly."
                                if active_schedule_names
                                else "Waiting for the next schedule or manual start."
                            )
                        ),
                        processing_stage="idle",
                        processing_message="Recorder is waiting. BirdNET will analyze the next live 9-second windows when recording starts again.",
                        last_processing_summary=last_processing_summary,
                        last_detection_count=len(timeline_detections),
                        last_clip_count=len(created_clip_paths),
                        last_detected_species=detected_species,
                        live_detection_count=len(latest_live_window_detections),
                        live_detected_species=self._unique_species_names(latest_live_window_detections),
                        live_detections=self._live_detection_preview(latest_live_window_detections),
                        birdnet_live_analysis_active=False,
                        birdnet_live_pending_windows=0,
                        birdnet_live_completed_windows=completed_window_count,
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
                        birdnet_live_analysis_active=False,
                        birdnet_live_pending_windows=0,
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
