from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .detection import BirdActivityEvent


@dataclass(frozen=True)
class SpeciesPrediction:
    start_offset_seconds: float
    end_offset_seconds: float
    common_name: str
    confidence: float


class NullSpeciesClassifier:
    provider_name = "disabled"

    def available(self) -> bool:
        return False

    def classify(self, file_path: Path) -> list[SpeciesPrediction]:
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

    def classify(self, file_path: Path) -> list[SpeciesPrediction]:
        recording = self._recording_cls(self._analyzer, str(file_path), min_conf=self._min_confidence)
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
                    confidence=float(item.get("confidence", 0.0)),
                )
            )
        return predictions


def build_species_classifier():
    provider = os.getenv("BIRD_MONITOR_SPECIES_PROVIDER", "disabled").strip().casefold()
    if provider != "birdnet":
        return NullSpeciesClassifier()

    try:
        return BirdNetSpeciesClassifier()
    except Exception:
        return NullSpeciesClassifier()


def match_species_prediction(
    event: BirdActivityEvent,
    predictions: list[SpeciesPrediction],
) -> SpeciesPrediction | None:
    best_match: SpeciesPrediction | None = None
    best_overlap = 0.0

    for prediction in predictions:
        overlap_start = max(event.start_offset_seconds, prediction.start_offset_seconds)
        overlap_end = min(event.end_offset_seconds, prediction.end_offset_seconds)
        overlap = overlap_end - overlap_start
        if overlap <= 0.0:
            continue
        if overlap > best_overlap:
            best_overlap = overlap
            best_match = prediction
            continue
        if overlap == best_overlap and best_match is not None and prediction.confidence > best_match.confidence:
            best_match = prediction

    return best_match
