from dataclasses import dataclass
from datetime import datetime, timedelta

from bird_monitor.analytics import build_species_events_from_detections, build_species_statistics


@dataclass(frozen=True)
class FakeDetection:
    started_at: datetime
    ended_at: datetime
    species_common_name: str | None
    species_scientific_name: str | None
    species_score: float | None
    confidence: float


def test_same_species_within_merge_gap_counts_as_one_event():
    start = datetime(2026, 3, 14, 6, 0)
    detections = [
      FakeDetection(start, start + timedelta(seconds=12), "European robin", "Erithacus rubecula", 0.92, 0.92),
      FakeDetection(start + timedelta(minutes=4), start + timedelta(minutes=4, seconds=18), "European robin", "Erithacus rubecula", 0.81, 0.81),
    ]

    events = build_species_events_from_detections(detections)

    assert len(events) == 1
    assert events[0].detection_count == 2
    assert events[0].confidence == 0.92


def test_species_statistics_report_event_counts():
    start = datetime(2026, 3, 14, 6, 0)
    detections = [
      FakeDetection(start, start + timedelta(seconds=10), "Great tit", "Parus major", 0.9, 0.9),
      FakeDetection(start + timedelta(minutes=20), start + timedelta(minutes=20, seconds=8), "Great tit", "Parus major", 0.82, 0.82),
      FakeDetection(start + timedelta(minutes=40), start + timedelta(minutes=40, seconds=9), "Blackbird", "Turdus merula", 0.88, 0.88),
    ]

    events = build_species_events_from_detections(detections)
    stats = build_species_statistics(events)

    assert len(events) == 3
    assert stats[0]["species_common_name"] == "Great tit"
    assert stats[0]["event_count"] == 2
    assert stats[1]["species_common_name"] == "Blackbird"
