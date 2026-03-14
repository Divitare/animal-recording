from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from .models import Recording, utc_iso

SPECIES_EVENT_MERGE_GAP_SECONDS = 600


@dataclass(frozen=True)
class SpeciesEvent:
    species_common_name: str
    species_scientific_name: str | None
    started_at: datetime
    ended_at: datetime
    confidence: float
    average_confidence: float
    detection_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "species_common_name": self.species_common_name,
            "species_scientific_name": self.species_scientific_name,
            "started_at": utc_iso(self.started_at),
            "ended_at": utc_iso(self.ended_at),
            "confidence": self.confidence,
            "average_confidence": self.average_confidence,
            "detection_count": self.detection_count,
        }


def build_species_events(recordings: Iterable[Recording]) -> list[SpeciesEvent]:
    detections = [
        detection
        for recording in recordings
        for detection in recording.detections
    ]
    return build_species_events_from_detections(detections)


def build_species_events_from_detections(detections: Iterable[object]) -> list[SpeciesEvent]:
    species_detections = [
        detection
        for detection in detections
        if getattr(detection, "species_common_name", None)
    ]
    ordered = sorted(species_detections, key=lambda detection: getattr(detection, "started_at"))
    if not ordered:
        return []

    events: list[SpeciesEvent] = []

    for detection in ordered:
        species_common_name = str(getattr(detection, "species_common_name"))
        species_scientific_name = _optional_text(getattr(detection, "species_scientific_name", None))
        started_at = getattr(detection, "started_at")
        ended_at = getattr(detection, "ended_at")
        confidence = _species_confidence(detection)

        if not events:
            events.append(
                SpeciesEvent(
                    species_common_name=species_common_name,
                    species_scientific_name=species_scientific_name,
                    started_at=started_at,
                    ended_at=ended_at,
                    confidence=confidence,
                    average_confidence=confidence,
                    detection_count=1,
                )
            )
            continue

        current = events[-1]
        can_merge = (
            current.species_common_name == species_common_name
            and current.species_scientific_name == species_scientific_name
            and started_at <= (current.ended_at + timedelta(seconds=SPECIES_EVENT_MERGE_GAP_SECONDS))
        )

        if can_merge:
            merged_count = current.detection_count + 1
            merged_average = (
                (current.average_confidence * current.detection_count) + confidence
            ) / merged_count
            events[-1] = SpeciesEvent(
                species_common_name=current.species_common_name,
                species_scientific_name=current.species_scientific_name,
                started_at=current.started_at,
                ended_at=max(current.ended_at, ended_at),
                confidence=max(current.confidence, confidence),
                average_confidence=merged_average,
                detection_count=merged_count,
            )
            continue

        events.append(
            SpeciesEvent(
                species_common_name=species_common_name,
                species_scientific_name=species_scientific_name,
                started_at=started_at,
                ended_at=ended_at,
                confidence=confidence,
                average_confidence=confidence,
                detection_count=1,
            )
        )

    return events


def build_species_statistics(events: Iterable[SpeciesEvent]) -> list[dict[str, object]]:
    buckets: dict[tuple[str, str | None], dict[str, object]] = {}

    for event in events:
        key = (event.species_common_name, event.species_scientific_name)
        bucket = buckets.setdefault(
            key,
            {
                "species_common_name": event.species_common_name,
                "species_scientific_name": event.species_scientific_name,
                "event_count": 0,
                "detection_count": 0,
                "best_confidence": 0.0,
                "confidence_total": 0.0,
                "last_seen_at": event.ended_at,
            },
        )
        bucket["event_count"] = int(bucket["event_count"]) + 1
        bucket["detection_count"] = int(bucket["detection_count"]) + event.detection_count
        bucket["best_confidence"] = max(float(bucket["best_confidence"]), event.confidence)
        bucket["confidence_total"] = float(bucket["confidence_total"]) + event.average_confidence
        if event.ended_at > bucket["last_seen_at"]:
            bucket["last_seen_at"] = event.ended_at

    items: list[dict[str, object]] = []
    for bucket in buckets.values():
        event_count = int(bucket["event_count"])
        items.append(
            {
                "species_common_name": bucket["species_common_name"],
                "species_scientific_name": bucket["species_scientific_name"],
                "event_count": event_count,
                "detection_count": int(bucket["detection_count"]),
                "average_confidence": float(bucket["confidence_total"]) / max(event_count, 1),
                "best_confidence": float(bucket["best_confidence"]),
                "last_seen_at": utc_iso(bucket["last_seen_at"]),
            }
        )

    return sorted(
        items,
        key=lambda item: (-int(item["event_count"]), -float(item["average_confidence"]), str(item["species_common_name"])),
    )


def _species_confidence(detection: object) -> float:
    value = getattr(detection, "species_score", None)
    if value is None:
        value = getattr(detection, "confidence", 0.0)
    return float(value)


def _optional_text(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None
