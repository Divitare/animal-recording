from __future__ import annotations

import json
import threading
import time
import uuid
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from .audio import peak_amplitude, save_audio_samples, stream_input_chunks
from .config import BirdNodeConfig
from .health import disk_usage_summary, read_cpu_temperature_celsius, root_mean_square
from .runtime_logging import get_application_logger, get_birdnet_logger
from .species import NullSpeciesClassifier, build_species_classifier
from .storage import BirdNodeStorage
from .sync import BirdNodeSyncManager

SESSION_DETECTION_MERGE_GAP_SECONDS = 1.5
MICROPHONE_STALE_AFTER_SECONDS = 10.0
BIRDNET_FAILURES_FOR_FAILING_HEALTH = 3
DEVICE_RETRY_DELAY_SECONDS = 5.0


def utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() + "Z"


def hours_from_seconds(value: float | int | None) -> float:
    return round(float(value or 0.0) / 3600.0, 4)


def empty_metric_totals() -> dict[str, float | int]:
    return {
        "recorded_seconds": 0.0,
        "analyzed_seconds": 0.0,
        "microphone_uptime_seconds": 0.0,
        "detection_count": 0,
        "birdnet_success_count": 0,
        "birdnet_failure_count": 0,
        "clipping_event_count": 0,
        "silence_event_count": 0,
        "overflow_event_count": 0,
    }


def is_retryable_audio_error(exc: Exception) -> bool:
    message = str(exc).casefold()
    return (
        "no input devices were found" in message
        or "configured microphone index" in message
        or "no input device matched microphone name filter" in message
    )


def split_interval_by_utc_day(started_at: datetime, ended_at: datetime) -> list[tuple[str, float]]:
    if ended_at <= started_at:
        return []

    buckets: list[tuple[str, float]] = []
    cursor = started_at
    while cursor.date() < ended_at.date():
        next_midnight = datetime.combine(cursor.date() + timedelta(days=1), datetime.min.time())
        buckets.append((cursor.date().isoformat(), max((next_midnight - cursor).total_seconds(), 0.0)))
        cursor = next_midnight

    buckets.append((cursor.date().isoformat(), max((ended_at - cursor).total_seconds(), 0.0)))
    return buckets


@dataclass(frozen=True)
class SessionSpeciesDetection:
    started_at: datetime
    ended_at: datetime
    confidence: float
    species_common_name: str
    species_scientific_name: str | None
    source_window_started_at: datetime
    source_window_ended_at: datetime


@dataclass(frozen=True)
class LiveAnalysisResult:
    detections: list[SessionSpeciesDetection]
    analysis_duration_seconds: float | None
    raw_detection_count: int
    merged_detection_count: int
    source_label: str


@dataclass
class PendingWindow:
    future: Future[LiveAnalysisResult]
    window_started_at: datetime
    window_ended_at: datetime


class AnalysisWindowAccumulator:
    def __init__(self, window_frames: int, step_frames: int) -> None:
        self.window_frames = window_frames
        self.step_frames = step_frames
        self._chunks: deque[np.ndarray] = deque()
        self._frame_count = 0
        self._buffer_start_frame = 0
        self._next_window_start_frame = 0

    def push(self, chunk: np.ndarray) -> list[tuple[np.ndarray, int]]:
        prepared = np.asarray(chunk, dtype=np.float32).copy()
        if prepared.ndim == 1:
            prepared = prepared.reshape(-1, 1)

        self._chunks.append(prepared)
        self._frame_count += int(prepared.shape[0])

        windows: list[tuple[np.ndarray, int]] = []
        while self._buffer_end_frame >= (self._next_window_start_frame + self.window_frames):
            relative_start = self._next_window_start_frame - self._buffer_start_frame
            windows.append((self._slice_frames(relative_start, self.window_frames), self._next_window_start_frame))
            self._next_window_start_frame += self.step_frames
            self._drop_frames_before(self._next_window_start_frame)
        return windows

    def flush_remainder(self, min_frames: int) -> tuple[np.ndarray, int] | None:
        remaining_frames = self._buffer_end_frame - self._next_window_start_frame
        if remaining_frames < min_frames:
            return None
        relative_start = self._next_window_start_frame - self._buffer_start_frame
        remainder = self._slice_frames(relative_start, remaining_frames)
        start_frame = self._next_window_start_frame
        self._next_window_start_frame = self._buffer_end_frame
        self._drop_frames_before(self._next_window_start_frame)
        return remainder, start_frame

    @property
    def _buffer_end_frame(self) -> int:
        return self._buffer_start_frame + self._frame_count

    def _slice_frames(self, relative_start_frame: int, frame_count: int) -> np.ndarray:
        remaining_frames = frame_count
        skip_frames = max(relative_start_frame, 0)
        pieces: list[np.ndarray] = []

        for chunk in self._chunks:
            if remaining_frames <= 0:
                break
            chunk_frames = int(chunk.shape[0])
            if skip_frames >= chunk_frames:
                skip_frames -= chunk_frames
                continue

            local_start = skip_frames
            available_frames = chunk_frames - local_start
            take_frames = min(available_frames, remaining_frames)
            pieces.append(chunk[local_start:local_start + take_frames].copy())
            remaining_frames -= take_frames
            skip_frames = 0

        if not pieces:
            return np.zeros((0, 1), dtype=np.float32)
        return np.concatenate(pieces, axis=0)

    def _drop_frames_before(self, target_start_frame: int) -> None:
        while self._chunks and self._buffer_start_frame < target_start_frame:
            chunk = self._chunks[0]
            chunk_frames = int(chunk.shape[0])
            drop_frames = min(target_start_frame - self._buffer_start_frame, chunk_frames)
            if drop_frames >= chunk_frames:
                self._chunks.popleft()
            else:
                self._chunks[0] = chunk[drop_frames:].copy()
            self._buffer_start_frame += drop_frames
            self._frame_count = max(self._frame_count - drop_frames, 0)


class RollingAudioBuffer:
    def __init__(self, max_frames: int) -> None:
        self.max_frames = max_frames
        self._chunks: deque[np.ndarray] = deque()
        self._buffer_start_frame = 0
        self._frame_count = 0
        self._lock = threading.Lock()

    def append(self, chunk: np.ndarray) -> int:
        prepared = np.asarray(chunk, dtype=np.float32).copy()
        if prepared.ndim == 1:
            prepared = prepared.reshape(-1, 1)

        with self._lock:
            start_frame = self._buffer_end_frame
            self._chunks.append(prepared)
            self._frame_count += int(prepared.shape[0])
            self._trim_if_needed()
            return start_frame

    def slice_frames(self, start_frame: int, end_frame: int) -> np.ndarray:
        if end_frame <= start_frame:
            return np.zeros((0, 1), dtype=np.float32)

        with self._lock:
            if start_frame < self._buffer_start_frame or end_frame > self._buffer_end_frame:
                return np.zeros((0, 1), dtype=np.float32)

            remaining_frames = end_frame - start_frame
            skip_frames = start_frame - self._buffer_start_frame
            pieces: list[np.ndarray] = []

            for chunk in self._chunks:
                if remaining_frames <= 0:
                    break
                chunk_frames = int(chunk.shape[0])
                if skip_frames >= chunk_frames:
                    skip_frames -= chunk_frames
                    continue

                local_start = skip_frames
                available_frames = chunk_frames - local_start
                take_frames = min(available_frames, remaining_frames)
                pieces.append(chunk[local_start:local_start + take_frames].copy())
                remaining_frames -= take_frames
                skip_frames = 0

            if not pieces:
                return np.zeros((0, 1), dtype=np.float32)
            return np.concatenate(pieces, axis=0)

    @property
    def _buffer_end_frame(self) -> int:
        return self._buffer_start_frame + self._frame_count

    def _trim_if_needed(self) -> None:
        target_start = max(0, self._buffer_end_frame - self.max_frames)
        while self._chunks and self._buffer_start_frame < target_start:
            chunk = self._chunks[0]
            chunk_frames = int(chunk.shape[0])
            drop_frames = min(target_start - self._buffer_start_frame, chunk_frames)
            if drop_frames >= chunk_frames:
                self._chunks.popleft()
            else:
                self._chunks[0] = chunk[drop_frames:].copy()
            self._buffer_start_frame += drop_frames
            self._frame_count = max(self._frame_count - drop_frames, 0)


class BirdNodeService:
    def __init__(self, config: BirdNodeConfig) -> None:
        self.config = config
        self.storage = BirdNodeStorage(config.database_path, config.status_file)
        self.logger = get_application_logger()
        self.birdnet_logger = get_birdnet_logger()
        self.classifier = NullSpeciesClassifier("BirdNET runtime is still starting.")
        self.stop_event = threading.Event()
        self.analysis_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bird-node-birdnet")
        self.pending_windows: list[PendingWindow] = []
        self.recent_saved_detections: list[SessionSpeciesDetection] = []
        self.service_started_at = datetime.utcnow()
        self.service_started_monotonic = time.monotonic()
        self.metrics_summary: dict[str, object] = {"totals": empty_metric_totals(), "daily": [], "updated_at": None}
        self.pending_total_metrics: dict[str, float | int] = empty_metric_totals()
        self.pending_daily_metrics: dict[str, dict[str, float | int]] = {}
        self.total_saved_detections = 0
        self.last_analysis_duration_seconds: float | None = None
        self.last_error: str | None = None
        self.current_device_name: str | None = None
        self.last_detection_species: list[str] = []
        self.last_detection_at: datetime | None = None
        self.last_event_id: str | None = None
        self.last_audio_chunk_at: datetime | None = None
        self.last_audio_peak_amplitude: float = 0.0
        self.last_audio_rms: float = 0.0
        self.current_silence_streak_seconds: float = 0.0
        self.current_clipping_streak_seconds: float = 0.0
        self.last_clipping_at: datetime | None = None
        self.last_overflow_at: datetime | None = None
        self.last_successful_analysis_at: datetime | None = None
        self.last_successful_analysis_coverage_end_at: datetime | None = None
        self.consecutive_birdnet_failures: int = 0
        self.last_health_snapshot_monotonic: float | None = None
        self.last_health_snapshot_id: int | None = None
        self.last_health_snapshot_at: datetime | None = None
        self.waiting_for_device_since: datetime | None = None
        self.last_status_write = 0.0
        self.sync_manager = BirdNodeSyncManager(config, self.storage, self.stop_event)

    def run_forever(self) -> None:
        self.storage.initialize()
        self.metrics_summary = self.storage.load_metrics_summary(max_days=self.config.status_history_days)
        self.total_saved_detections = int(
            ((self.metrics_summary.get("totals") or {}).get("detection_count") or 0)
        )
        self._write_status(recording=False, message="Starting bird-node. Initializing BirdNET runtime.")

        if self.config.disable_recorder:
            self.logger.warning("Recorder is disabled by configuration. Exiting without starting audio capture.")
            self._write_status(recording=False, message="Recorder disabled by configuration.")
            return

        self.classifier = build_species_classifier()

        if self.config.species_provider == "birdnet" and not self.classifier.available():
            self._write_status(recording=False, message="BirdNET initialization failed.")
            raise RuntimeError(getattr(self.classifier, "failure_reason", None) or "BirdNET is unavailable.")

        self.logger.info(
            "Starting bird-node node_id=%s sample_rate=%s channels=%s device_index=%s device_name=%s live_window=%ss live_step=%ss",
            self.config.node_id,
            self.config.sample_rate,
            self.config.channels,
            self.config.device_index if self.config.device_index is not None else "auto",
            self.config.device_name or "auto",
            self.config.live_window_seconds,
            self.config.live_step_seconds,
        )
        self.sync_manager.start()
        self._write_status(recording=False, message="Starting bird-node.")

        try:
            while not self.stop_event.is_set():
                try:
                    self._capture_loop()
                    break
                except Exception as exc:
                    self.last_error = str(exc)
                    if is_retryable_audio_error(exc):
                        if self.waiting_for_device_since is None:
                            self.waiting_for_device_since = datetime.utcnow()
                        self.current_device_name = None
                        self.logger.warning(
                            "Audio input is not available yet. Waiting %.1fs before retrying. error=%s",
                            DEVICE_RETRY_DELAY_SECONDS,
                            exc,
                        )
                        self._write_status(
                            recording=False,
                            message="Waiting for a usable microphone input device.",
                        )
                        if self.stop_event.wait(DEVICE_RETRY_DELAY_SECONDS):
                            break
                        continue
                    self.logger.exception("bird-node capture loop failed.")
                    raise
        finally:
            self.stop_event.set()
            self._drain_pending_windows(final_wait=True)
            self.sync_manager.stop()
            self.analysis_executor.shutdown(wait=False, cancel_futures=False)
            self._write_status(started=False, recording=False, message="bird-node stopped.")

    def stop(self) -> None:
        self.stop_event.set()

    def _capture_loop(self) -> None:
        stream_started_at = datetime.utcnow()
        rolling_buffer = RollingAudioBuffer(self.config.rolling_audio_buffer_seconds * self.config.sample_rate)
        accumulator = AnalysisWindowAccumulator(
            window_frames=self.config.live_window_seconds * self.config.sample_rate,
            step_frames=self.config.live_step_seconds * self.config.sample_rate,
        )
        minimum_live_frames = self.config.minimum_live_analysis_seconds * self.config.sample_rate
        submitted_window_count = 0

        for chunk in stream_input_chunks(
            sample_rate=self.config.sample_rate,
            channels=self.config.channels,
            preferred_name=self.config.device_name,
            preferred_index=self.config.device_index,
            should_stop=self.stop_event.is_set,
        ):
            if self.current_device_name is None:
                self.current_device_name = chunk.device_name
                self.waiting_for_device_since = None
                self.logger.info("Audio capture opened device=%s index=%s", chunk.device_name, chunk.device_index)

            chunk_start_frame = rolling_buffer.append(chunk.samples)
            chunk_end_frame = chunk_start_frame + int(chunk.samples.shape[0])
            chunk_started_at = stream_started_at + timedelta(
                seconds=float(chunk_start_frame / max(self.config.sample_rate, 1))
            )
            chunk_ended_at = stream_started_at + timedelta(
                seconds=float(chunk_end_frame / max(self.config.sample_rate, 1))
            )
            self._observe_audio_chunk(
                chunk.samples,
                chunk_started_at=chunk_started_at,
                chunk_ended_at=chunk_ended_at,
                overflowed=chunk.overflowed,
            )

            for window_samples, window_start_frame in accumulator.push(chunk.samples):
                window_started_at = stream_started_at + timedelta(
                    seconds=float(window_start_frame / max(self.config.sample_rate, 1))
                )
                window_ended_at = window_started_at + timedelta(
                    seconds=float(window_samples.shape[0] / max(self.config.sample_rate, 1))
                )
                self.pending_windows.append(
                    PendingWindow(
                        future=self.analysis_executor.submit(
                            self._analyze_window,
                            window_samples.copy(),
                            window_started_at,
                            window_ended_at,
                            submitted_window_count,
                        ),
                        window_started_at=window_started_at,
                        window_ended_at=window_ended_at,
                    )
                )
                submitted_window_count += 1

            self._drain_pending_windows(rolling_buffer=rolling_buffer, stream_started_at=stream_started_at)
            self._maybe_write_status(recording=True, message="Recording and analyzing live windows.")

        remainder = accumulator.flush_remainder(minimum_live_frames)
        if remainder is not None:
            remainder_samples, remainder_start_frame = remainder
            window_started_at = stream_started_at + timedelta(
                seconds=float(remainder_start_frame / max(self.config.sample_rate, 1))
            )
            window_ended_at = window_started_at + timedelta(
                seconds=float(remainder_samples.shape[0] / max(self.config.sample_rate, 1))
            )
            self.pending_windows.append(
                PendingWindow(
                    future=self.analysis_executor.submit(
                        self._analyze_window,
                        remainder_samples.copy(),
                        window_started_at,
                        window_ended_at,
                        submitted_window_count,
                    ),
                    window_started_at=window_started_at,
                    window_ended_at=window_ended_at,
                )
            )

        self._drain_pending_windows(rolling_buffer=rolling_buffer, stream_started_at=stream_started_at, final_wait=True)

    def _analyze_window(
        self,
        samples: np.ndarray,
        window_started_at: datetime,
        window_ended_at: datetime,
        window_index: int,
    ) -> LiveAnalysisResult:
        source_label = f"live-window-{window_index:06d}"
        predictions = self.classifier.classify_samples(
            samples,
            sample_rate=self.config.sample_rate,
            latitude=self.config.latitude,
            longitude=self.config.longitude,
            recorded_at=window_started_at,
            min_confidence=self.config.species_min_confidence,
            source_label=source_label,
        )
        detections = [
            SessionSpeciesDetection(
                started_at=window_started_at + timedelta(seconds=prediction.start_offset_seconds),
                ended_at=window_started_at + timedelta(seconds=prediction.end_offset_seconds),
                confidence=prediction.confidence,
                species_common_name=prediction.common_name,
                species_scientific_name=prediction.scientific_name,
                source_window_started_at=window_started_at,
                source_window_ended_at=window_ended_at,
            )
            for prediction in predictions
        ]
        details = dict(getattr(self.classifier, "last_analysis_details", {}) or {})
        return LiveAnalysisResult(
            detections=detections,
            analysis_duration_seconds=details.get("duration_seconds"),
            raw_detection_count=int(details.get("raw_detection_count") or 0),
            merged_detection_count=int(details.get("merged_detection_count") or len(detections)),
            source_label=source_label,
        )

    def _drain_pending_windows(
        self,
        *,
        rolling_buffer: RollingAudioBuffer | None = None,
        stream_started_at: datetime | None = None,
        final_wait: bool = False,
    ) -> None:
        while True:
            completed_any = False
            remaining: list[PendingWindow] = []

            for pending_window in self.pending_windows:
                if final_wait:
                    self._handle_pending_result(
                        pending_window,
                        rolling_buffer=rolling_buffer,
                        stream_started_at=stream_started_at,
                    )
                    completed_any = True
                    continue

                if pending_window.future.done():
                    self._handle_pending_result(
                        pending_window,
                        rolling_buffer=rolling_buffer,
                        stream_started_at=stream_started_at,
                    )
                    completed_any = True
                    continue

                remaining.append(pending_window)

            self.pending_windows = remaining
            if not final_wait or not completed_any:
                break

    def _handle_pending_result(
        self,
        pending_window: PendingWindow,
        *,
        rolling_buffer: RollingAudioBuffer | None,
        stream_started_at: datetime | None,
    ) -> None:
        try:
            result = pending_window.future.result()
        except Exception as exc:
            self.last_error = str(exc)
            self.consecutive_birdnet_failures += 1
            self._add_count_metric("birdnet_failure_count", pending_window.window_ended_at, 1)
            self.birdnet_logger.exception(
                "Live BirdNET window failed window_started_at=%s window_ended_at=%s",
                pending_window.window_started_at,
                pending_window.window_ended_at,
            )
            return

        self.consecutive_birdnet_failures = 0
        self.last_successful_analysis_at = pending_window.window_ended_at
        self._add_count_metric("birdnet_success_count", pending_window.window_ended_at, 1)
        self._record_successfully_analyzed_coverage(
            pending_window.window_started_at,
            pending_window.window_ended_at,
        )

        self._handle_analysis_result(
            result,
            rolling_buffer=rolling_buffer,
            stream_started_at=stream_started_at,
        )

    def _handle_analysis_result(
        self,
        result: LiveAnalysisResult,
        *,
        rolling_buffer: RollingAudioBuffer | None,
        stream_started_at: datetime | None,
    ) -> None:
        self.last_analysis_duration_seconds = result.analysis_duration_seconds
        if not result.detections or rolling_buffer is None or stream_started_at is None:
            return

        for detection in self._merge_session_detections(result.detections):
            if self._is_duplicate_detection(detection):
                continue
            event_id = f"evt-{uuid.uuid4().hex}"
            clip_path, clip_duration = self._save_detection_clip(
                detection,
                event_id=event_id,
                rolling_buffer=rolling_buffer,
                stream_started_at=stream_started_at,
            )
            if clip_path is None or clip_duration is None:
                continue

            self.total_saved_detections += 1
            self.last_detection_at = detection.ended_at
            self.last_event_id = event_id
            self.last_detection_species = [detection.species_common_name]
            self.recent_saved_detections.append(detection)
            self._prune_recent_saved_detections()
            record_id = self.storage.record_detection(
                {
                    "event_id": event_id,
                    "node_id": self.config.node_id,
                    "species_common_name": detection.species_common_name,
                    "species_scientific_name": detection.species_scientific_name,
                    "confidence": detection.confidence,
                    "started_at": utc_iso(detection.started_at),
                    "ended_at": utc_iso(detection.ended_at),
                    "clip_file_path": str(clip_path),
                    "clip_duration_seconds": clip_duration,
                    "sample_rate": self.config.sample_rate,
                    "channels": self.config.channels,
                    "source_window_started_at": utc_iso(detection.source_window_started_at),
                    "source_window_ended_at": utc_iso(detection.source_window_ended_at),
                    "analysis_duration_seconds": result.analysis_duration_seconds,
                    "location_name": self.config.location_name,
                    "latitude": self.config.latitude,
                    "longitude": self.config.longitude,
                    "created_at": utc_iso(datetime.utcnow()),
                }
            )
            self.metrics_summary = self.storage.load_metrics_summary(max_days=self.config.status_history_days)
            self.total_saved_detections = int(
                ((self.metrics_summary.get("totals") or {}).get("detection_count") or 0)
            )
            self.logger.info(
                "Saved bird detection record_id=%s event_id=%s species=%s confidence=%.3f started_at=%s ended_at=%s clip=%s",
                record_id,
                event_id,
                detection.species_common_name,
                detection.confidence,
                utc_iso(detection.started_at),
                utc_iso(detection.ended_at),
                clip_path,
            )

    def _observe_audio_chunk(
        self,
        samples: np.ndarray,
        *,
        chunk_started_at: datetime,
        chunk_ended_at: datetime,
        overflowed: bool,
    ) -> None:
        duration_seconds = max((chunk_ended_at - chunk_started_at).total_seconds(), 0.0)
        peak = peak_amplitude(samples)
        rms = root_mean_square(samples)

        self.last_audio_chunk_at = chunk_ended_at
        self.last_audio_peak_amplitude = peak
        self.last_audio_rms = rms

        self._add_duration_metric("recorded_seconds", chunk_started_at, chunk_ended_at)
        self._add_duration_metric("microphone_uptime_seconds", chunk_started_at, chunk_ended_at)

        if peak >= self.config.clipping_peak_threshold:
            self.current_clipping_streak_seconds += duration_seconds
            self.last_clipping_at = chunk_ended_at
            self._add_count_metric("clipping_event_count", chunk_ended_at, 1)
        else:
            self.current_clipping_streak_seconds = 0.0

        if rms <= self.config.silence_rms_threshold:
            self.current_silence_streak_seconds += duration_seconds
            self._add_count_metric("silence_event_count", chunk_ended_at, 1)
        else:
            self.current_silence_streak_seconds = 0.0

        if overflowed:
            self.last_overflow_at = chunk_ended_at
            self._add_count_metric("overflow_event_count", chunk_ended_at, 1)

    def _record_successfully_analyzed_coverage(self, started_at: datetime, ended_at: datetime) -> None:
        unique_started_at = started_at
        if self.last_successful_analysis_coverage_end_at is not None:
            unique_started_at = max(unique_started_at, self.last_successful_analysis_coverage_end_at)

        if ended_at > unique_started_at:
            self._add_duration_metric("analyzed_seconds", unique_started_at, ended_at)

        if self.last_successful_analysis_coverage_end_at is None or ended_at > self.last_successful_analysis_coverage_end_at:
            self.last_successful_analysis_coverage_end_at = ended_at

    def _merge_session_detections(
        self,
        detections: list[SessionSpeciesDetection],
        *,
        max_gap_seconds: float = SESSION_DETECTION_MERGE_GAP_SECONDS,
    ) -> list[SessionSpeciesDetection]:
        if not detections:
            return []

        ordered = sorted(
            detections,
            key=lambda item: (
                item.species_common_name,
                item.species_scientific_name or "",
                item.started_at,
                item.ended_at,
            ),
        )
        merged: list[SessionSpeciesDetection] = []

        for detection in ordered:
            if not merged:
                merged.append(detection)
                continue

            current = merged[-1]
            if (
                detection.species_common_name == current.species_common_name
                and detection.species_scientific_name == current.species_scientific_name
                and detection.started_at <= (current.ended_at + timedelta(seconds=max_gap_seconds))
            ):
                merged[-1] = SessionSpeciesDetection(
                    started_at=min(current.started_at, detection.started_at),
                    ended_at=max(current.ended_at, detection.ended_at),
                    confidence=max(current.confidence, detection.confidence),
                    species_common_name=current.species_common_name,
                    species_scientific_name=current.species_scientific_name,
                    source_window_started_at=min(current.source_window_started_at, detection.source_window_started_at),
                    source_window_ended_at=max(current.source_window_ended_at, detection.source_window_ended_at),
                )
                continue

            merged.append(detection)

        return sorted(merged, key=lambda item: item.started_at)

    def _is_duplicate_detection(self, candidate: SessionSpeciesDetection) -> bool:
        for existing in self.recent_saved_detections:
            if existing.species_common_name != candidate.species_common_name:
                continue
            if existing.species_scientific_name != candidate.species_scientific_name:
                continue
            if candidate.started_at <= (existing.ended_at + timedelta(seconds=SESSION_DETECTION_MERGE_GAP_SECONDS)):
                return True
        return False

    def _prune_recent_saved_detections(self) -> None:
        cutoff = datetime.utcnow() - timedelta(seconds=max(self.config.rolling_audio_buffer_seconds, 60))
        self.recent_saved_detections = [
            detection
            for detection in self.recent_saved_detections
            if detection.ended_at >= cutoff
        ]

    def _save_detection_clip(
        self,
        detection: SessionSpeciesDetection,
        *,
        event_id: str,
        rolling_buffer: RollingAudioBuffer,
        stream_started_at: datetime,
    ) -> tuple[Path | None, float | None]:
        start_frame = max(
            0,
            int(
                (
                    (detection.started_at - timedelta(seconds=self.config.detection_clip_padding_seconds)) - stream_started_at
                ).total_seconds()
                * self.config.sample_rate
            ),
        )
        end_frame = max(
            start_frame + 1,
            int(
                (
                    (detection.ended_at + timedelta(seconds=self.config.detection_clip_padding_seconds)) - stream_started_at
                ).total_seconds()
                * self.config.sample_rate
            ),
        )
        clip_samples = rolling_buffer.slice_frames(start_frame, end_frame)
        if clip_samples.size == 0:
            self.birdnet_logger.warning(
                "Could not save clip for %s because the required audio is no longer in the rolling buffer.",
                detection.species_common_name,
            )
            return None, None

        clip_path = self._build_clip_path(detection.started_at, detection.species_common_name, event_id)
        save_audio_samples(clip_samples, self.config.sample_rate, clip_path)
        clip_duration_seconds = float(np.asarray(clip_samples).shape[0] / max(self.config.sample_rate, 1))
        return clip_path, clip_duration_seconds

    def _build_clip_path(self, detected_at: datetime, common_name: str, event_id: str) -> Path:
        safe_name = "".join(character.lower() if character.isalnum() else "-" for character in common_name).strip("-")
        safe_name = "-".join(part for part in safe_name.split("-") if part) or "bird"
        day_path = self.config.clips_dir / detected_at.strftime("%Y") / detected_at.strftime("%m") / detected_at.strftime("%d")
        filename = f"detection_{detected_at.strftime('%Y%m%dT%H%M%S_%f')}_{event_id}_{safe_name}.wav"
        return day_path / filename

    def _add_duration_metric(self, metric_name: str, started_at: datetime, ended_at: datetime) -> None:
        for day_utc, seconds in split_interval_by_utc_day(started_at, ended_at):
            self.pending_total_metrics[metric_name] = float(self.pending_total_metrics.get(metric_name, 0.0) or 0.0) + seconds
            day_bucket = self._ensure_daily_metric_bucket(day_utc)
            day_bucket[metric_name] = float(day_bucket.get(metric_name, 0.0) or 0.0) + seconds

    def _add_count_metric(self, metric_name: str, occurred_at: datetime, amount: int = 1) -> None:
        if amount == 0:
            return
        self.pending_total_metrics[metric_name] = int(self.pending_total_metrics.get(metric_name, 0) or 0) + int(amount)
        day_bucket = self._ensure_daily_metric_bucket(occurred_at.date().isoformat())
        day_bucket[metric_name] = int(day_bucket.get(metric_name, 0) or 0) + int(amount)

    def _ensure_daily_metric_bucket(self, day_utc: str) -> dict[str, float | int]:
        bucket = self.pending_daily_metrics.get(day_utc)
        if bucket is None:
            bucket = empty_metric_totals()
            self.pending_daily_metrics[day_utc] = bucket
        return bucket

    def _flush_pending_metrics(self) -> None:
        if not self.pending_daily_metrics and not any(self.pending_total_metrics.get(key) for key in self.pending_total_metrics):
            return

        updated_at = utc_iso(datetime.utcnow()) or ""
        day_updates = [
            {"day_utc": day_utc, "updated_at": updated_at, **values}
            for day_utc, values in self.pending_daily_metrics.items()
        ]
        self.storage.persist_metric_deltas(
            totals=self.pending_total_metrics,
            day_updates=day_updates,
            updated_at=updated_at,
        )
        self.pending_total_metrics = empty_metric_totals()
        self.pending_daily_metrics = {}
        self.metrics_summary = self.storage.load_metrics_summary(max_days=self.config.status_history_days)
        self.total_saved_detections = int(
            ((self.metrics_summary.get("totals") or {}).get("detection_count") or 0)
        )

    def _build_microphone_health(self) -> dict[str, object]:
        now = datetime.utcnow()
        status = "starting"
        reasons: list[str] = []

        if self.current_device_name is None:
            status = "waiting-for-device"
            if self.waiting_for_device_since is not None:
                reasons.append("The node is waiting for a usable microphone input device.")
        elif self.last_audio_chunk_at is None:
            status = "starting"
        else:
            if (now - self.last_audio_chunk_at).total_seconds() > MICROPHONE_STALE_AFTER_SECONDS:
                status = "stalled"
                reasons.append("No recent audio chunks were received from the microphone.")
            elif self.current_clipping_streak_seconds >= self.config.live_step_seconds:
                status = "clipping"
                reasons.append("Recent microphone audio clipped at or above the configured peak threshold.")
            elif self.current_silence_streak_seconds >= self.config.silence_alert_seconds:
                status = "silent"
                reasons.append("The microphone has been near-silent for longer than the configured alert window.")
            elif self.last_overflow_at is not None and (now - self.last_overflow_at).total_seconds() <= MICROPHONE_STALE_AFTER_SECONDS:
                status = "overflowing"
                reasons.append("The audio input stream reported a recent overflow.")
            else:
                status = "healthy"

        totals = (self.metrics_summary.get("totals") or {})
        return {
            "status": status,
            "device_name": self.current_device_name,
            "waiting_for_device_since": utc_iso(self.waiting_for_device_since),
            "last_audio_chunk_at": utc_iso(self.last_audio_chunk_at),
            "last_peak_amplitude": round(self.last_audio_peak_amplitude, 6),
            "last_rms_amplitude": round(self.last_audio_rms, 6),
            "clipping_peak_threshold": self.config.clipping_peak_threshold,
            "silence_rms_threshold": self.config.silence_rms_threshold,
            "silence_alert_seconds": self.config.silence_alert_seconds,
            "current_silence_streak_seconds": round(self.current_silence_streak_seconds, 3),
            "current_clipping_streak_seconds": round(self.current_clipping_streak_seconds, 3),
            "last_clipping_at": utc_iso(self.last_clipping_at),
            "last_overflow_at": utc_iso(self.last_overflow_at),
            "clipping_event_count_total": int(totals.get("clipping_event_count") or 0),
            "silence_event_count_total": int(totals.get("silence_event_count") or 0),
            "overflow_event_count_total": int(totals.get("overflow_event_count") or 0),
            "reasons": reasons,
        }

    def _build_birdnet_health(self, runtime_details: dict[str, object]) -> dict[str, object]:
        totals = (self.metrics_summary.get("totals") or {})
        status = "healthy"
        reasons: list[str] = []

        if not self.classifier.available():
            status = "unavailable"
            reasons.append(getattr(self.classifier, "failure_reason", None) or "BirdNET is not available.")
        elif self.consecutive_birdnet_failures >= BIRDNET_FAILURES_FOR_FAILING_HEALTH:
            status = "failing"
            reasons.append("BirdNET analysis has failed repeatedly in the current session.")
        elif int(totals.get("birdnet_failure_count") or 0) > 0 and self.last_successful_analysis_at is None:
            status = "degraded"
            reasons.append("BirdNET has recorded failures and has not completed a successful analysis in this session yet.")

        return {
            "status": status,
            "available": self.classifier.available(),
            "provider": self.config.species_provider,
            "runtime_backend": runtime_details.get("runtime_backend"),
            "analysis_mode": runtime_details.get("analysis_mode"),
            "last_successful_analysis_at": utc_iso(self.last_successful_analysis_at),
            "last_analysis_duration_seconds": self.last_analysis_duration_seconds,
            "consecutive_failures": self.consecutive_birdnet_failures,
            "success_count_total": int(totals.get("birdnet_success_count") or 0),
            "failure_count_total": int(totals.get("birdnet_failure_count") or 0),
            "reasons": reasons,
        }

    def _build_statistics(self) -> dict[str, object]:
        totals = (self.metrics_summary.get("totals") or {})
        daily_rows = list(self.metrics_summary.get("daily") or [])
        return {
            "hours_recorded_total": hours_from_seconds(totals.get("recorded_seconds")),
            "hours_successfully_analyzed_total": hours_from_seconds(totals.get("analyzed_seconds")),
            "microphone_uptime_hours_total": hours_from_seconds(totals.get("microphone_uptime_seconds")),
            "detections_total": int(totals.get("detection_count") or 0),
            "detections_per_day": [
                {
                    "date_utc": row.get("date_utc"),
                    "detection_count": int(row.get("detection_count") or 0),
                    "hours_recorded": hours_from_seconds(row.get("recorded_seconds")),
                    "hours_successfully_analyzed": hours_from_seconds(row.get("analyzed_seconds")),
                    "microphone_uptime_hours": hours_from_seconds(row.get("microphone_uptime_seconds")),
                }
                for row in daily_rows
            ],
        }

    def _build_sync_status(self) -> dict[str, object]:
        try:
            return self.sync_manager.status_payload()
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            self.logger.exception("Failed to build hub sync status.")
            self.last_error = str(exc)
            return {
                "enabled": bool(self.config.hub_url),
                "hub_url": self.config.hub_url,
                "last_attempt_at": None,
                "last_successful_sync_at": None,
                "last_error": str(exc),
                "message": "Hub sync status is temporarily unavailable.",
                "current_batch_id": None,
                "queued_batch_count": None,
                "failed_batch_count": None,
                "synced_batch_count": None,
                "unsynced_detection_count": None,
                "unsynced_health_snapshot_count": None,
                "last_batch_status": None,
                "last_batch_created_at": None,
                "regular_upload_interval_seconds": self.config.sync_interval_seconds,
                "retry_interval_seconds": self.config.sync_retry_base_seconds,
                "next_regular_attempt_at": None,
            }

    def _build_time_status(self, now_utc: datetime) -> dict[str, object]:
        return {
            "current_utc": utc_iso(now_utc),
            "source": "system",
            "synchronized": False,
        }

    def _maybe_store_health_snapshot(self, status_payload: dict[str, object], *, force: bool = False) -> None:
        now_monotonic = time.monotonic()
        if not force and self.last_health_snapshot_monotonic is not None:
            elapsed = now_monotonic - self.last_health_snapshot_monotonic
            if elapsed < self.config.health_snapshot_interval_seconds:
                return

        runtime_details = dict((((status_payload.get("service") or {})).get("runtime_details")) or {})
        packages = dict((runtime_details.get("packages") or {}))
        snapshot_payload = {
            "captured_at": (((status_payload.get("time") or {})).get("current_utc")),
            "node_id": self.config.node_id,
            "time_source": (((status_payload.get("time") or {})).get("source")) or "system",
            "time_synchronized": bool((((status_payload.get("time") or {})).get("synchronized"))),
            "app_commit": self.config.app_commit,
            "runtime_backend": runtime_details.get("runtime_backend"),
            "birdnet_version": packages.get("birdnetlib"),
            "payload": status_payload,
        }
        try:
            snapshot_id = self.storage.record_health_snapshot(snapshot_payload)
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            self.logger.exception("Failed to store a bird-node health snapshot.")
            self.last_error = str(exc)
            return
        self.last_health_snapshot_monotonic = now_monotonic
        self.last_health_snapshot_id = snapshot_id
        current_utc = (((status_payload.get("time") or {})).get("current_utc"))
        self.last_health_snapshot_at = _parse_utc_or_none(current_utc)

    def _maybe_write_status(self, *, recording: bool, message: str) -> None:
        now = time.monotonic()
        if (now - self.last_status_write) < self.config.write_status_interval_seconds:
            return
        self._write_status(recording=recording, message=message)
        self.last_status_write = now

    def _write_status(self, *, started: bool = True, recording: bool, message: str) -> None:
        now_utc = datetime.utcnow()
        runtime_details = dict(getattr(self.classifier, "runtime_details", {}) or {})
        try:
            self._flush_pending_metrics()
            system_health = disk_usage_summary(
                self.config.data_dir,
                low_space_bytes=self.config.low_disk_free_bytes,
            )
            system_health.update(
                {
                    "cpu_temperature_celsius": read_cpu_temperature_celsius(),
                    "uptime_seconds": round(time.monotonic() - self.service_started_monotonic, 3),
                }
            )
            status_payload = {
                "app": {
                    "commit": self.config.app_commit,
                    "variant": "v2-bird-node",
                },
                "time": self._build_time_status(now_utc),
                "service": {
                    "started": started,
                    "recording": recording,
                    "node_id": self.config.node_id,
                    "current_device_name": self.current_device_name,
                    "species_provider": self.config.species_provider,
                    "species_available": self.classifier.available(),
                    "species_error": getattr(self.classifier, "failure_reason", None),
                    "last_error": self.last_error,
                    "last_analysis_duration_seconds": self.last_analysis_duration_seconds,
                    "last_detection_at": utc_iso(self.last_detection_at),
                    "last_event_id": self.last_event_id,
                    "last_detected_species": self.last_detection_species,
                    "last_health_snapshot_id": self.last_health_snapshot_id,
                    "last_health_snapshot_at": utc_iso(self.last_health_snapshot_at),
                    "pending_windows": len(self.pending_windows),
                    "saved_detection_count": self.total_saved_detections,
                    "status_file": str(self.config.status_file),
                    "database_path": str(self.config.database_path),
                    "log_dir": str(self.config.log_dir),
                    "message": message,
                    "updated_at": utc_iso(now_utc),
                    "runtime_details": runtime_details,
                    "sync": self._build_sync_status(),
                },
                "health": {
                    "microphone": self._build_microphone_health(),
                    "birdnet": self._build_birdnet_health(runtime_details),
                    "system": system_health,
                },
                "statistics": self._build_statistics(),
            }
            self._maybe_store_health_snapshot(
                status_payload,
                force=not started or self.last_health_snapshot_monotonic is None,
            )
            service_payload = status_payload["service"]
            if isinstance(service_payload, dict):
                service_payload["last_health_snapshot_id"] = self.last_health_snapshot_id
                service_payload["last_health_snapshot_at"] = utc_iso(self.last_health_snapshot_at)
            self.storage.write_status(status_payload)
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            self.logger.exception("Failed to write the full bird-node status payload; writing fallback status.")
            self.last_error = str(exc)
            self._write_fallback_status(
                started=started,
                recording=recording,
                message=message,
                now_utc=now_utc,
                runtime_details=runtime_details,
                failure=str(exc),
            )

    def _write_fallback_status(
        self,
        *,
        started: bool,
        recording: bool,
        message: str,
        now_utc: datetime,
        runtime_details: dict[str, object],
        failure: str,
    ) -> None:
        fallback_payload = {
            "app": {
                "commit": self.config.app_commit,
                "variant": "v2-bird-node",
            },
            "time": self._build_time_status(now_utc),
            "service": {
                "started": started,
                "recording": recording,
                "node_id": self.config.node_id,
                "current_device_name": self.current_device_name,
                "species_provider": self.config.species_provider,
                "species_available": self.classifier.available(),
                "species_error": getattr(self.classifier, "failure_reason", None),
                "last_error": failure,
                "last_analysis_duration_seconds": self.last_analysis_duration_seconds,
                "last_detection_at": utc_iso(self.last_detection_at),
                "last_event_id": self.last_event_id,
                "last_detected_species": self.last_detection_species,
                "last_health_snapshot_id": self.last_health_snapshot_id,
                "last_health_snapshot_at": utc_iso(self.last_health_snapshot_at),
                "pending_windows": len(self.pending_windows),
                "saved_detection_count": self.total_saved_detections,
                "status_file": str(self.config.status_file),
                "database_path": str(self.config.database_path),
                "log_dir": str(self.config.log_dir),
                "message": f"{message} (fallback status mode)",
                "updated_at": utc_iso(now_utc),
                "runtime_details": runtime_details,
                "sync": {
                    "enabled": bool(self.config.hub_url),
                    "hub_url": self.config.hub_url,
                    "last_error": failure,
                    "message": "Fallback sync status because the full status payload failed to build.",
                },
            },
            "health": {
                "microphone": {
                    "status": "unknown",
                    "device_name": self.current_device_name,
                    "reasons": ["The full bird-node status payload could not be built."],
                },
                "birdnet": {
                    "status": "healthy" if self.classifier.available() else "unavailable",
                    "available": self.classifier.available(),
                    "provider": self.config.species_provider,
                    "runtime_backend": runtime_details.get("runtime_backend"),
                    "analysis_mode": runtime_details.get("analysis_mode"),
                    "reasons": [failure],
                },
                "system": {
                    "status": "unknown",
                    "uptime_seconds": round(time.monotonic() - self.service_started_monotonic, 3),
                },
            },
            "statistics": {
                "hours_recorded_total": 0.0,
                "hours_successfully_analyzed_total": 0.0,
                "microphone_uptime_hours_total": 0.0,
                "detections_total": self.total_saved_detections,
                "detections_per_day": [],
            },
        }
        self.config.status_file.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.config.status_file.with_name(f".{self.config.status_file.name}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(fallback_payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temp_path.replace(self.config.status_file)


def _parse_utc_or_none(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value[:-1] if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None
