from __future__ import annotations

import importlib.metadata
import os
import platform
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np

from .audio import describe_audio_file, load_audio_samples, rewrite_audio_file
from .detection import BirdActivityEvent
from .runtime_logging import get_birdnet_logger


@dataclass(frozen=True)
class SpeciesPrediction:
    start_offset_seconds: float
    end_offset_seconds: float
    common_name: str
    scientific_name: str | None
    confidence: float


class NullSpeciesClassifier:
    provider_name = "disabled"

    def __init__(self, reason: str | None = None, runtime_details: dict[str, object] | None = None) -> None:
        self.failure_reason = reason or "BirdNET runtime dependencies are unavailable."
        self.runtime_details = runtime_details or _collect_runtime_details(
            available=False,
            reason=self.failure_reason,
        )
        self.last_analysis_details = {
            "started_at": None,
            "finished_at": None,
            "duration_seconds": None,
            "file_path": None,
            "raw_detection_count": 0,
            "merged_detection_count": 0,
            "species": [],
            "error": self.failure_reason,
        }

    def available(self) -> bool:
        return False

    def classify(
        self,
        file_path: Path,
        *,
        latitude: float | None = None,
        longitude: float | None = None,
        recorded_at: datetime | None = None,
        min_confidence: float | None = None,
    ) -> list[SpeciesPrediction]:
        return []


class BirdNetSpeciesClassifier:
    provider_name = "birdnet"

    def __init__(self) -> None:
        self._logger = get_birdnet_logger()
        self._logger.info("Initializing BirdNET classifier runtime.")
        self.runtime_details = _collect_runtime_details(available=False, reason=None)
        self.last_analysis_details = {
            "started_at": None,
            "finished_at": None,
            "duration_seconds": None,
            "file_path": None,
            "raw_detection_count": 0,
            "merged_detection_count": 0,
            "species": [],
            "error": None,
        }
        from birdnetlib import Recording
        from birdnetlib.analyzer import Analyzer
        try:
            from birdnetlib import RecordingBuffer
        except Exception:
            RecordingBuffer = None

        self._recording_cls = Recording
        self._recording_buffer_cls = RecordingBuffer
        self._analyzer = Analyzer()
        self._min_confidence = float(os.getenv("BIRD_MONITOR_SPECIES_MIN_CONFIDENCE", "0.35"))
        self.runtime_details = _collect_runtime_details(
            available=True,
            reason=None,
            minimum_confidence=self._min_confidence,
        )
        packages = self.runtime_details.get("packages", {})
        self._logger.info(
            "BirdNET runtime ready. backend=%s birdnetlib=%s librosa=%s tensorflow=%s tflite-runtime=%s min_confidence=%.2f buffer_fallback=%s",
            self.runtime_details.get("runtime_backend", "unknown"),
            packages.get("birdnetlib") or "missing",
            packages.get("librosa") or "missing",
            packages.get("tensorflow") or "missing",
            packages.get("tflite-runtime") or "missing",
            self._min_confidence,
            self._recording_buffer_cls is not None,
        )

    def available(self) -> bool:
        return True

    def _analyze_detections(self, file_path: Path, kwargs: dict[str, object]) -> list[dict[str, object]]:
        recording = self._recording_cls(self._analyzer, str(file_path), **kwargs)
        recording.analyze()
        return list(getattr(recording, "detections", []))

    def _analyze_buffer_detections(
        self,
        samples: np.ndarray,
        sample_rate: int,
        kwargs: dict[str, object],
    ) -> list[dict[str, object]]:
        if self._recording_buffer_cls is None:
            raise RuntimeError("birdnetlib does not expose RecordingBuffer in this runtime.")

        attempts = [
            lambda: self._recording_buffer_cls(self._analyzer, samples, sample_rate=sample_rate, **kwargs),
            lambda: self._recording_buffer_cls(self._analyzer, samples, samplerate=sample_rate, **kwargs),
            lambda: self._recording_buffer_cls(self._analyzer, samples, sr=sample_rate, **kwargs),
            lambda: self._recording_buffer_cls(self._analyzer, samples, sample_rate, **kwargs),
        ]
        errors: list[str] = []
        for attempt in attempts:
            try:
                recording = attempt()
                recording.analyze()
                return list(getattr(recording, "detections", []))
            except TypeError as exc:
                errors.append(str(exc))

        raise RuntimeError(
            "BirdNET in-memory buffer fallback could not be constructed. "
            + " | ".join(errors[:3])
        )

    def classify(
        self,
        file_path: Path,
        *,
        latitude: float | None = None,
        longitude: float | None = None,
        recorded_at: datetime | None = None,
        min_confidence: float | None = None,
    ) -> list[SpeciesPrediction]:
        analysis_started_at = datetime.utcnow()
        effective_confidence = float(min_confidence if min_confidence is not None else self._min_confidence)
        file_size = file_path.stat().st_size if file_path.exists() else None
        self.last_analysis_details = {
            "started_at": analysis_started_at.isoformat() + "Z",
            "finished_at": None,
            "duration_seconds": None,
            "file_path": str(file_path),
            "raw_detection_count": 0,
            "merged_detection_count": 0,
            "species": [],
            "error": None,
        }
        kwargs: dict[str, object] = {
            "min_conf": effective_confidence,
        }
        if latitude is not None and longitude is not None:
            kwargs["lat"] = latitude
            kwargs["lon"] = longitude
        if recorded_at is not None:
            kwargs["date"] = recorded_at

        audio_details = _safe_describe_audio_file(file_path)
        if audio_details is not None:
            self._logger.info(
                "BirdNET input audio sample_rate=%s channels=%s frames=%s duration=%.2fs format=%s subtype=%s size_bytes=%s",
                audio_details["sample_rate"],
                audio_details["channels"],
                audio_details["frames"],
                audio_details["duration_seconds"],
                audio_details["format"],
                audio_details["subtype"],
                audio_details["size_bytes"],
            )
        self._logger.info(
            "BirdNET analysis started for %s size_bytes=%s min_confidence=%.2f latitude=%s longitude=%s recorded_at=%s",
            file_path,
            file_size if file_size is not None else "unknown",
            effective_confidence,
            latitude if latitude is not None else "none",
            longitude if longitude is not None else "none",
            recorded_at.isoformat() if recorded_at is not None else "none",
        )

        try:
            fallback_path: Path | None = None
            try:
                raw_detections = self._analyze_detections(file_path, kwargs)
            except Exception as exc:
                if not _is_audio_format_error(exc):
                    raise

                fallback_path = file_path.with_name(f"{file_path.stem}_birdnet_retry.wav")
                self._logger.warning(
                    "BirdNET could not read %s directly (%s). Rewriting a compatibility WAV and retrying once.",
                    file_path,
                    exc,
                )
                fallback_details = rewrite_audio_file(file_path, fallback_path)
                self._logger.info(
                    "BirdNET fallback WAV sample_rate=%s channels=%s frames=%s duration=%.2fs format=%s subtype=%s size_bytes=%s path=%s",
                    fallback_details["sample_rate"],
                    fallback_details["channels"],
                    fallback_details["frames"],
                    fallback_details["duration_seconds"],
                    fallback_details["format"],
                    fallback_details["subtype"],
                    fallback_details["size_bytes"],
                    fallback_details["path"],
                )
                try:
                    raw_detections = self._analyze_detections(fallback_path, kwargs)
                except Exception as fallback_exc:
                    if not _is_audio_format_error(fallback_exc):
                        raise

                    buffer_samples, buffer_sample_rate = load_audio_samples(fallback_path)
                    self._logger.warning(
                        "BirdNET could not read the fallback WAV directly either (%s). Trying in-memory buffer analysis with %s samples at %s Hz.",
                        fallback_exc,
                        int(buffer_samples.shape[0]),
                        buffer_sample_rate,
                    )
                    raw_detections = self._analyze_buffer_detections(
                        buffer_samples,
                        buffer_sample_rate,
                        kwargs,
                    )
            finally:
                if fallback_path is not None:
                    try:
                        fallback_path.unlink(missing_ok=True)
                    except OSError:
                        pass

            predictions: list[SpeciesPrediction] = []
            for item in raw_detections:
                common_name = item.get("common_name") or item.get("label")
                if not common_name:
                    continue
                predictions.append(
                    SpeciesPrediction(
                        start_offset_seconds=float(item.get("start_time", 0.0)),
                        end_offset_seconds=float(item.get("end_time", item.get("start_time", 0.0))),
                        common_name=str(common_name),
                        scientific_name=_clean_optional_text(item.get("scientific_name")),
                        confidence=float(item.get("confidence", 0.0)),
                    )
                )
            merged_predictions = merge_species_predictions(predictions)
            unique_species = []
            for prediction in merged_predictions:
                if prediction.common_name not in unique_species:
                    unique_species.append(prediction.common_name)

            finished_at = datetime.utcnow()
            duration_seconds = max((finished_at - analysis_started_at).total_seconds(), 0.0)
            self.last_analysis_details = {
                "started_at": analysis_started_at.isoformat() + "Z",
                "finished_at": finished_at.isoformat() + "Z",
                "duration_seconds": duration_seconds,
                "file_path": str(file_path),
                "raw_detection_count": len(raw_detections),
                "merged_detection_count": len(merged_predictions),
                "species": unique_species,
                "error": None,
            }

            self._logger.info(
                "BirdNET analysis finished for %s in %.2fs raw_detections=%s merged_detections=%s species=%s",
                file_path.name,
                duration_seconds,
                len(raw_detections),
                len(merged_predictions),
                ", ".join(unique_species) if unique_species else "none",
            )
            if merged_predictions:
                for prediction in merged_predictions:
                    self._logger.info(
                        "BirdNET detection species=%s scientific=%s confidence=%.3f start=%.2fs end=%.2fs",
                        prediction.common_name,
                        prediction.scientific_name or "-",
                        prediction.confidence,
                        prediction.start_offset_seconds,
                        prediction.end_offset_seconds,
                    )
            else:
                self._logger.info("BirdNET found no bird species in %s.", file_path.name)

            return merged_predictions
        except Exception as exc:
            finished_at = datetime.utcnow()
            duration_seconds = max((finished_at - analysis_started_at).total_seconds(), 0.0)
            self.last_analysis_details = {
                "started_at": analysis_started_at.isoformat() + "Z",
                "finished_at": finished_at.isoformat() + "Z",
                "duration_seconds": duration_seconds,
                "file_path": str(file_path),
                "raw_detection_count": 0,
                "merged_detection_count": 0,
                "species": [],
                "error": str(exc),
            }
            self._logger.exception("BirdNET analysis failed for %s after %.2fs.", file_path, duration_seconds)
            raise


def build_species_classifier():
    logger = get_birdnet_logger()
    logger.info("Checking BirdNET runtime availability.")
    try:
        classifier = BirdNetSpeciesClassifier()
        return classifier
    except Exception as exc:
        reason = _describe_runtime_issue(exc)
        if isinstance(exc, ModuleNotFoundError):
            logger.warning("BirdNET runtime is unavailable: %s", reason)
        else:
            logger.exception("BirdNET runtime initialization failed: %s", reason)
        return NullSpeciesClassifier(
            reason,
            runtime_details=_collect_runtime_details(available=False, reason=reason),
        )


def prediction_overlaps_event(event: BirdActivityEvent, prediction: SpeciesPrediction) -> bool:
    overlap_start = max(event.start_offset_seconds, prediction.start_offset_seconds)
    overlap_end = min(event.end_offset_seconds, prediction.end_offset_seconds)
    return overlap_end > overlap_start


def merge_species_predictions(
    predictions: list[SpeciesPrediction],
    max_gap_seconds: float = 0.75,
) -> list[SpeciesPrediction]:
    if not predictions:
        return []

    ordered = sorted(predictions, key=lambda item: (item.start_offset_seconds, item.end_offset_seconds, item.common_name))
    merged: list[SpeciesPrediction] = []

    for prediction in ordered:
        if not merged:
            merged.append(prediction)
            continue

        current = merged[-1]
        if (
            prediction.common_name == current.common_name
            and prediction.scientific_name == current.scientific_name
            and prediction.start_offset_seconds <= (current.end_offset_seconds + max_gap_seconds)
        ):
            merged[-1] = SpeciesPrediction(
                start_offset_seconds=current.start_offset_seconds,
                end_offset_seconds=max(current.end_offset_seconds, prediction.end_offset_seconds),
                common_name=current.common_name,
                scientific_name=current.scientific_name,
                confidence=max(current.confidence, prediction.confidence),
            )
            continue

        merged.append(prediction)

    return merged


def _clean_optional_text(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _describe_runtime_issue(exc: Exception) -> str:
    if isinstance(exc, ModuleNotFoundError) and getattr(exc, "name", None):
        return (
            f"Missing Python package '{exc.name}'. "
            "Run install.sh again or install the BirdNET dependencies in the server virtual environment."
        )

    message = str(exc).strip()
    if message:
        return message
    return exc.__class__.__name__


def _collect_runtime_details(
    *,
    available: bool,
    reason: str | None,
    minimum_confidence: float | None = None,
) -> dict[str, object]:
    packages = {
        "birdnetlib": _package_version("birdnetlib"),
        "librosa": _package_version("librosa"),
        "tensorflow": _package_version("tensorflow"),
        "tflite-runtime": _package_version("tflite-runtime"),
    }
    runtime_backend = "tflite-runtime" if packages["tflite-runtime"] else ("tensorflow" if packages["tensorflow"] else "missing")
    details: dict[str, object] = {
        "provider": "birdnet",
        "available": available,
        "reason": reason,
        "analysis_mode": "post-recording",
        "runtime_backend": runtime_backend,
        "python_version": platform.python_version(),
        "packages": packages,
    }
    if minimum_confidence is not None:
        details["minimum_confidence"] = minimum_confidence
    return details


def _package_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _is_audio_format_error(exc: Exception) -> bool:
    message = f"{exc.__class__.__name__}: {exc}"
    lowered = message.casefold()
    return "audioformaterror" in lowered or "generic audio read error" in lowered or "librosa" in lowered


def _safe_describe_audio_file(file_path: Path) -> dict[str, object] | None:
    try:
        return describe_audio_file(file_path)
    except Exception:
        return None
