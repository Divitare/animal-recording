from __future__ import annotations

import os
from datetime import datetime, timezone

from .extensions import db


def utcnow() -> datetime:
    return datetime.utcnow()


def utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc).isoformat()


class RecorderSettings(db.Model):
    __tablename__ = "recorder_settings"

    id = db.Column(db.Integer, primary_key=True)
    device_name = db.Column(db.String(255), nullable=True)
    device_index = db.Column(db.Integer, nullable=True)
    sample_rate = db.Column(db.Integer, nullable=False, default=32000)
    channels = db.Column(db.Integer, nullable=False, default=1)
    segment_seconds = db.Column(db.Integer, nullable=False, default=30)
    min_event_duration_seconds = db.Column(db.Float, nullable=False, default=0.2)
    location_name = db.Column(db.String(255), nullable=True)
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    species_provider = db.Column(db.String(32), nullable=False, default="disabled")
    species_min_confidence = db.Column(db.Float, nullable=False, default=0.35)
    updated_at = db.Column(db.DateTime, nullable=False, default=utcnow, onupdate=utcnow)

    @classmethod
    def get_or_create(cls) -> "RecorderSettings":
        settings = cls.query.get(1)
        if settings is None:
            device_index = os.getenv("BIRD_MONITOR_DEVICE_INDEX", "").strip()
            latitude = os.getenv("BIRD_MONITOR_LATITUDE", "").strip()
            longitude = os.getenv("BIRD_MONITOR_LONGITUDE", "").strip()
            settings = cls(
                id=1,
                device_name=os.getenv("BIRD_MONITOR_DEVICE_NAME", "").strip() or None,
                device_index=int(device_index) if device_index else None,
                sample_rate=int(os.getenv("BIRD_MONITOR_SAMPLE_RATE", "32000")),
                channels=int(os.getenv("BIRD_MONITOR_CHANNELS", "1")),
                segment_seconds=int(os.getenv("BIRD_MONITOR_SEGMENT_SECONDS", "30")),
                min_event_duration_seconds=float(os.getenv("BIRD_MONITOR_MIN_EVENT_DURATION_SECONDS", "0.2")),
                location_name=os.getenv("BIRD_MONITOR_LOCATION_NAME", "").strip() or None,
                latitude=float(latitude) if latitude else None,
                longitude=float(longitude) if longitude else None,
                species_provider=os.getenv("BIRD_MONITOR_SPECIES_PROVIDER", "birdnet").strip().casefold() or "disabled",
                species_min_confidence=float(os.getenv("BIRD_MONITOR_SPECIES_MIN_CONFIDENCE", "0.35")),
            )
            db.session.add(settings)
            db.session.commit()
        return settings

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "device_name": self.device_name,
            "device_index": self.device_index,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "segment_seconds": self.segment_seconds,
            "min_event_duration_seconds": self.min_event_duration_seconds,
            "location_name": self.location_name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "species_provider": self.species_provider,
            "species_min_confidence": self.species_min_confidence,
            "updated_at": utc_iso(self.updated_at),
        }


class RecordingSchedule(db.Model):
    __tablename__ = "recording_schedules"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    days_of_week = db.Column(db.String(32), nullable=False, default="0,1,2,3,4,5,6")
    start_time = db.Column(db.String(5), nullable=False, default="05:00")
    end_time = db.Column(db.String(5), nullable=False, default="08:00")
    enabled = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=utcnow, onupdate=utcnow)

    def days(self) -> list[int]:
        if not self.days_of_week:
            return []
        return sorted({int(item) for item in self.days_of_week.split(",") if item != ""})

    def set_days(self, values: list[int]) -> None:
        cleaned = sorted({int(value) for value in values if 0 <= int(value) <= 6})
        self.days_of_week = ",".join(str(value) for value in cleaned)

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "name": self.name,
            "days_of_week": self.days(),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "enabled": self.enabled,
            "created_at": utc_iso(self.created_at),
            "updated_at": utc_iso(self.updated_at),
        }


class Recording(db.Model):
    __tablename__ = "recordings"

    id = db.Column(db.Integer, primary_key=True)
    file_path = db.Column(db.Text, nullable=False)
    started_at = db.Column(db.DateTime, nullable=False, index=True)
    ended_at = db.Column(db.DateTime, nullable=False, index=True)
    duration_seconds = db.Column(db.Float, nullable=False)
    sample_rate = db.Column(db.Integer, nullable=False)
    channels = db.Column(db.Integer, nullable=False)
    size_bytes = db.Column(db.Integer, nullable=False)
    peak_amplitude = db.Column(db.Float, nullable=False, default=0.0)
    device_name = db.Column(db.String(255), nullable=True)
    has_bird_activity = db.Column(db.Boolean, nullable=False, default=False, index=True)
    bird_event_count = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)

    detections = db.relationship(
        "BirdDetection",
        backref="recording",
        lazy="selectin",
        cascade="all, delete-orphan",
        order_by="BirdDetection.started_at.asc()",
    )

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "file_path": self.file_path,
            "started_at": utc_iso(self.started_at),
            "ended_at": utc_iso(self.ended_at),
            "duration_seconds": self.duration_seconds,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "size_bytes": self.size_bytes,
            "peak_amplitude": self.peak_amplitude,
            "device_name": self.device_name,
            "has_bird_activity": self.has_bird_activity,
            "bird_event_count": self.bird_event_count,
            "created_at": utc_iso(self.created_at),
            "detections": [detection.to_dict() for detection in self.detections],
        }


class BirdDetection(db.Model):
    __tablename__ = "bird_detections"

    id = db.Column(db.Integer, primary_key=True)
    recording_id = db.Column(db.Integer, db.ForeignKey("recordings.id"), nullable=False, index=True)
    started_at = db.Column(db.DateTime, nullable=False, index=True)
    ended_at = db.Column(db.DateTime, nullable=False)
    confidence = db.Column(db.Float, nullable=False)
    dominant_frequency_hz = db.Column(db.Float, nullable=False)
    source = db.Column(db.String(32), nullable=False, default="activity")
    species_common_name = db.Column(db.String(255), nullable=True)
    species_scientific_name = db.Column(db.String(255), nullable=True)
    species_score = db.Column(db.Float, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "recording_id": self.recording_id,
            "started_at": utc_iso(self.started_at),
            "ended_at": utc_iso(self.ended_at),
            "confidence": self.confidence,
            "dominant_frequency_hz": self.dominant_frequency_hz,
            "source": self.source,
            "species_common_name": self.species_common_name,
            "species_scientific_name": self.species_scientific_name,
            "species_score": self.species_score,
            "created_at": utc_iso(self.created_at),
        }
