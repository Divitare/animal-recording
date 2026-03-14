from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .detection import BirdActivityEvent


@dataclass(frozen=True)
class SpeciesPrediction:
    start_offset_seconds: float
    end_offset_seconds: float
    common_name: str
    scientific_name: str | None
    confidence: float


class NullSpeciesClassifier:
    provider_name = "disabled"

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
        from birdnetlib import Recording
        from birdnetlib.analyzer import Analyzer

        self._recording_cls = Recording
        self._analyzer = Analyzer()
        self._min_confidence = float(os.getenv("BIRD_MONITOR_SPECIES_MIN_CONFIDENCE", "0.35"))

    def available(self) -> bool:
        return True

    def classify(
        self,
        file_path: Path,
        *,
        latitude: float | None = None,
        longitude: float | None = None,
        recorded_at: datetime | None = None,
        min_confidence: float | None = None,
    ) -> list[SpeciesPrediction]:
        kwargs: dict[str, object] = {
            "min_conf": min_confidence if min_confidence is not None else self._min_confidence,
        }
        if latitude is not None and longitude is not None:
            kwargs["lat"] = latitude
            kwargs["lon"] = longitude
        if recorded_at is not None:
            kwargs["date"] = recorded_at

        recording = self._recording_cls(self._analyzer, str(file_path), **kwargs)
        recording.analyze()
        predictions: list[SpeciesPrediction] = []
        for item in getattr(recording, "detections", []):
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
        return merge_species_predictions(predictions)


def build_species_classifier():
    try:
        return BirdNetSpeciesClassifier()
    except Exception:
        return NullSpeciesClassifier()
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
