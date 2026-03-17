from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from .audio import save_audio_samples, stream_input_chunks
from .config import BirdNodeConfig
from .runtime_logging import get_application_logger, get_birdnet_logger
from .species import build_species_classifier
from .storage import BirdNodeStorage

SESSION_DETECTION_MERGE_GAP_SECONDS = 1.5


def utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() + "Z"


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
        self.classifier = build_species_classifier()
        self.stop_event = threading.Event()
        self.analysis_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bird-node-birdnet")
        self.pending_windows: list[PendingWindow] = []
        self.recent_saved_detections: list[SessionSpeciesDetection] = []
        self.total_saved_detections = 0
        self.last_analysis_duration_seconds: float | None = None
        self.last_error: str | None = None
        self.current_device_name: str | None = None
        self.last_detection_species: list[str] = []
        self.last_detection_at: datetime | None = None
        self.last_status_write = 0.0

    def run_forever(self) -> None:
        self.storage.initialize()

        if self.config.disable_recorder:
            self.logger.warning("Recorder is disabled by configuration. Exiting without starting audio capture.")
            self._write_status(recording=False, message="Recorder disabled by configuration.")
            return

        if self.config.species_provider == "birdnet" and not self.classifier.available():
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
        self._write_status(recording=False, message="Starting bird-node.")

        try:
            self._capture_loop()
        except Exception as exc:
            self.last_error = str(exc)
            self.logger.exception("bird-node capture loop failed.")
            raise
        finally:
            self._drain_pending_windows(final_wait=True)
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
                self.logger.info("Audio capture opened device=%s index=%s", chunk.device_name, chunk.device_index)

            rolling_buffer.append(chunk.samples)

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
            self.birdnet_logger.exception(
                "Live BirdNET window failed window_started_at=%s window_ended_at=%s",
                pending_window.window_started_at,
                pending_window.window_ended_at,
            )
            return

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
            clip_path, clip_duration = self._save_detection_clip(
                detection,
                rolling_buffer=rolling_buffer,
                stream_started_at=stream_started_at,
            )
            if clip_path is None or clip_duration is None:
                continue

            self.total_saved_detections += 1
            self.last_detection_at = detection.ended_at
            self.last_detection_species = [detection.species_common_name]
            self.recent_saved_detections.append(detection)
            self._prune_recent_saved_detections()
            record_id = self.storage.record_detection(
                {
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
            self.logger.info(
                "Saved bird detection id=%s species=%s confidence=%.3f clip=%s",
                record_id,
                detection.species_common_name,
                detection.confidence,
                clip_path,
            )

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

        clip_path = self._build_clip_path(detection.started_at, detection.species_common_name)
        save_audio_samples(clip_samples, self.config.sample_rate, clip_path)
        clip_duration_seconds = float(np.asarray(clip_samples).shape[0] / max(self.config.sample_rate, 1))
        return clip_path, clip_duration_seconds

    def _build_clip_path(self, detected_at: datetime, common_name: str) -> Path:
        safe_name = "".join(character.lower() if character.isalnum() else "-" for character in common_name).strip("-")
        safe_name = "-".join(part for part in safe_name.split("-") if part) or "bird"
        day_path = self.config.clips_dir / detected_at.strftime("%Y") / detected_at.strftime("%m") / detected_at.strftime("%d")
        filename = f"detection_{detected_at.strftime('%Y%m%dT%H%M%S_%f')}_{safe_name}.wav"
        return day_path / filename

    def _maybe_write_status(self, *, recording: bool, message: str) -> None:
        now = time.monotonic()
        if (now - self.last_status_write) < self.config.write_status_interval_seconds:
            return
        self._write_status(recording=recording, message=message)
        self.last_status_write = now

    def _write_status(self, *, started: bool = True, recording: bool, message: str) -> None:
        runtime_details = dict(getattr(self.classifier, "runtime_details", {}) or {})
        self.storage.write_status(
            {
                "app": {
                    "commit": self.config.app_commit,
                    "variant": "v2-bird-node",
                },
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
                    "last_detected_species": self.last_detection_species,
                    "pending_windows": len(self.pending_windows),
                    "saved_detection_count": self.total_saved_detections,
                    "status_file": str(self.config.status_file),
                    "database_path": str(self.config.database_path),
                    "log_dir": str(self.config.log_dir),
                    "message": message,
                    "updated_at": utc_iso(datetime.utcnow()),
                    "runtime_details": runtime_details,
                },
            }
        )
