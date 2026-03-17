from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

COUNT_METRIC_FIELDS = (
    "detection_count",
    "birdnet_success_count",
    "birdnet_failure_count",
    "clipping_event_count",
    "silence_event_count",
    "overflow_event_count",
)
FLOAT_METRIC_FIELDS = (
    "recorded_seconds",
    "analyzed_seconds",
    "microphone_uptime_seconds",
)
ALL_METRIC_FIELDS = FLOAT_METRIC_FIELDS + COUNT_METRIC_FIELDS


def _zero_metric_totals() -> dict[str, float | int]:
    payload: dict[str, float | int] = {}
    for field_name in FLOAT_METRIC_FIELDS:
        payload[field_name] = 0.0
    for field_name in COUNT_METRIC_FIELDS:
        payload[field_name] = 0
    return payload


class BirdNodeStorage:
    def __init__(self, database_path: Path, status_file: Path) -> None:
        self._database_path = database_path
        self._status_file = status_file
        self._lock = threading.Lock()

    def initialize(self) -> None:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS detections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    node_id TEXT NOT NULL,
                    species_common_name TEXT NOT NULL,
                    species_scientific_name TEXT,
                    confidence REAL NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT NOT NULL,
                    clip_file_path TEXT NOT NULL,
                    clip_duration_seconds REAL NOT NULL,
                    sample_rate INTEGER NOT NULL,
                    channels INTEGER NOT NULL,
                    source_window_started_at TEXT NOT NULL,
                    source_window_ended_at TEXT NOT NULL,
                    analysis_duration_seconds REAL,
                    location_name TEXT,
                    latitude REAL,
                    longitude REAL,
                    created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_detections_event_id ON detections(event_id);
                CREATE INDEX IF NOT EXISTS idx_detections_started_at ON detections(started_at);
                CREATE INDEX IF NOT EXISTS idx_detections_species ON detections(species_common_name);
                CREATE TABLE IF NOT EXISTS node_metrics (
                    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                    total_recorded_seconds REAL NOT NULL DEFAULT 0,
                    total_analyzed_seconds REAL NOT NULL DEFAULT 0,
                    total_microphone_uptime_seconds REAL NOT NULL DEFAULT 0,
                    total_detection_count INTEGER NOT NULL DEFAULT 0,
                    total_birdnet_success_count INTEGER NOT NULL DEFAULT 0,
                    total_birdnet_failure_count INTEGER NOT NULL DEFAULT 0,
                    total_clipping_event_count INTEGER NOT NULL DEFAULT 0,
                    total_silence_event_count INTEGER NOT NULL DEFAULT 0,
                    total_overflow_event_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS daily_metrics (
                    day_utc TEXT PRIMARY KEY,
                    recorded_seconds REAL NOT NULL DEFAULT 0,
                    analyzed_seconds REAL NOT NULL DEFAULT 0,
                    microphone_uptime_seconds REAL NOT NULL DEFAULT 0,
                    detection_count INTEGER NOT NULL DEFAULT 0,
                    birdnet_success_count INTEGER NOT NULL DEFAULT 0,
                    birdnet_failure_count INTEGER NOT NULL DEFAULT 0,
                    clipping_event_count INTEGER NOT NULL DEFAULT 0,
                    silence_event_count INTEGER NOT NULL DEFAULT 0,
                    overflow_event_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(detections)")
            }
            if "event_id" not in columns:
                connection.execute("ALTER TABLE detections ADD COLUMN event_id TEXT")
                connection.execute("UPDATE detections SET event_id = 'legacy-' || id WHERE event_id IS NULL")
                connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_detections_event_id ON detections(event_id)")
            connection.execute(
                """
                INSERT OR IGNORE INTO node_metrics (
                    singleton_id,
                    updated_at
                ) VALUES (1, '')
                """
            )

    def record_detection(self, payload: dict[str, Any]) -> int:
        with self._lock:
            with self._connect() as connection:
                self._ensure_metric_rows(connection, str(payload.get("created_at") or ""))
                cursor = connection.execute(
                    """
                    INSERT INTO detections (
                        event_id,
                        node_id,
                        species_common_name,
                        species_scientific_name,
                        confidence,
                        started_at,
                        ended_at,
                        clip_file_path,
                        clip_duration_seconds,
                        sample_rate,
                        channels,
                        source_window_started_at,
                        source_window_ended_at,
                        analysis_duration_seconds,
                        location_name,
                        latitude,
                        longitude,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["event_id"],
                        payload["node_id"],
                        payload["species_common_name"],
                        payload.get("species_scientific_name"),
                        payload["confidence"],
                        payload["started_at"],
                        payload["ended_at"],
                        payload["clip_file_path"],
                        payload["clip_duration_seconds"],
                        payload["sample_rate"],
                        payload["channels"],
                        payload["source_window_started_at"],
                        payload["source_window_ended_at"],
                        payload.get("analysis_duration_seconds"),
                        payload.get("location_name"),
                        payload.get("latitude"),
                        payload.get("longitude"),
                        payload["created_at"],
                    ),
                )
                updated_at = str(payload.get("created_at") or payload.get("started_at") or "")
                day_utc = str(payload["started_at"])[:10]
                connection.execute(
                    """
                    UPDATE node_metrics
                    SET
                        total_detection_count = total_detection_count + 1,
                        updated_at = ?
                    WHERE singleton_id = 1
                    """,
                    (updated_at,),
                )
                connection.execute(
                    """
                    INSERT INTO daily_metrics (
                        day_utc,
                        detection_count,
                        updated_at
                    ) VALUES (?, 1, ?)
                    ON CONFLICT(day_utc) DO UPDATE SET
                        detection_count = daily_metrics.detection_count + 1,
                        updated_at = excluded.updated_at
                    """,
                    (day_utc, updated_at),
                )
                return int(cursor.lastrowid)

    def persist_metric_deltas(
        self,
        *,
        totals: dict[str, float | int],
        day_updates: list[dict[str, Any]],
        updated_at: str,
    ) -> None:
        if not day_updates and not any(totals.get(field_name) for field_name in ALL_METRIC_FIELDS):
            return

        with self._lock:
            with self._connect() as connection:
                self._ensure_metric_rows(connection, updated_at)
                connection.execute(
                    """
                    UPDATE node_metrics
                    SET
                        total_recorded_seconds = total_recorded_seconds + ?,
                        total_analyzed_seconds = total_analyzed_seconds + ?,
                        total_microphone_uptime_seconds = total_microphone_uptime_seconds + ?,
                        total_detection_count = total_detection_count + ?,
                        total_birdnet_success_count = total_birdnet_success_count + ?,
                        total_birdnet_failure_count = total_birdnet_failure_count + ?,
                        total_clipping_event_count = total_clipping_event_count + ?,
                        total_silence_event_count = total_silence_event_count + ?,
                        total_overflow_event_count = total_overflow_event_count + ?,
                        updated_at = ?
                    WHERE singleton_id = 1
                    """,
                    (
                        float(totals.get("recorded_seconds", 0.0) or 0.0),
                        float(totals.get("analyzed_seconds", 0.0) or 0.0),
                        float(totals.get("microphone_uptime_seconds", 0.0) or 0.0),
                        int(totals.get("detection_count", 0) or 0),
                        int(totals.get("birdnet_success_count", 0) or 0),
                        int(totals.get("birdnet_failure_count", 0) or 0),
                        int(totals.get("clipping_event_count", 0) or 0),
                        int(totals.get("silence_event_count", 0) or 0),
                        int(totals.get("overflow_event_count", 0) or 0),
                        updated_at,
                    ),
                )

                for item in day_updates:
                    connection.execute(
                        """
                        INSERT INTO daily_metrics (
                            day_utc,
                            recorded_seconds,
                            analyzed_seconds,
                            microphone_uptime_seconds,
                            detection_count,
                            birdnet_success_count,
                            birdnet_failure_count,
                            clipping_event_count,
                            silence_event_count,
                            overflow_event_count,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(day_utc) DO UPDATE SET
                            recorded_seconds = daily_metrics.recorded_seconds + excluded.recorded_seconds,
                            analyzed_seconds = daily_metrics.analyzed_seconds + excluded.analyzed_seconds,
                            microphone_uptime_seconds = daily_metrics.microphone_uptime_seconds + excluded.microphone_uptime_seconds,
                            detection_count = daily_metrics.detection_count + excluded.detection_count,
                            birdnet_success_count = daily_metrics.birdnet_success_count + excluded.birdnet_success_count,
                            birdnet_failure_count = daily_metrics.birdnet_failure_count + excluded.birdnet_failure_count,
                            clipping_event_count = daily_metrics.clipping_event_count + excluded.clipping_event_count,
                            silence_event_count = daily_metrics.silence_event_count + excluded.silence_event_count,
                            overflow_event_count = daily_metrics.overflow_event_count + excluded.overflow_event_count,
                            updated_at = excluded.updated_at
                        """,
                        (
                            item["day_utc"],
                            float(item.get("recorded_seconds", 0.0) or 0.0),
                            float(item.get("analyzed_seconds", 0.0) or 0.0),
                            float(item.get("microphone_uptime_seconds", 0.0) or 0.0),
                            int(item.get("detection_count", 0) or 0),
                            int(item.get("birdnet_success_count", 0) or 0),
                            int(item.get("birdnet_failure_count", 0) or 0),
                            int(item.get("clipping_event_count", 0) or 0),
                            int(item.get("silence_event_count", 0) or 0),
                            int(item.get("overflow_event_count", 0) or 0),
                            str(item.get("updated_at") or updated_at),
                        ),
                    )

    def load_metrics_summary(self, *, max_days: int = 14) -> dict[str, Any]:
        with self._lock:
            with self._connect() as connection:
                self._ensure_metric_rows(connection, "")
                totals_row = connection.execute(
                    """
                    SELECT
                        total_recorded_seconds,
                        total_analyzed_seconds,
                        total_microphone_uptime_seconds,
                        total_detection_count,
                        total_birdnet_success_count,
                        total_birdnet_failure_count,
                        total_clipping_event_count,
                        total_silence_event_count,
                        total_overflow_event_count,
                        updated_at
                    FROM node_metrics
                    WHERE singleton_id = 1
                    """
                ).fetchone()
                daily_rows = connection.execute(
                    """
                    SELECT
                        day_utc,
                        recorded_seconds,
                        analyzed_seconds,
                        microphone_uptime_seconds,
                        detection_count,
                        birdnet_success_count,
                        birdnet_failure_count,
                        clipping_event_count,
                        silence_event_count,
                        overflow_event_count,
                        updated_at
                    FROM daily_metrics
                    ORDER BY day_utc DESC
                    LIMIT ?
                    """,
                    (max(1, int(max_days)),),
                ).fetchall()

        totals = _zero_metric_totals()
        if totals_row is not None:
            totals.update(
                {
                    "recorded_seconds": float(totals_row["total_recorded_seconds"] or 0.0),
                    "analyzed_seconds": float(totals_row["total_analyzed_seconds"] or 0.0),
                    "microphone_uptime_seconds": float(totals_row["total_microphone_uptime_seconds"] or 0.0),
                    "detection_count": int(totals_row["total_detection_count"] or 0),
                    "birdnet_success_count": int(totals_row["total_birdnet_success_count"] or 0),
                    "birdnet_failure_count": int(totals_row["total_birdnet_failure_count"] or 0),
                    "clipping_event_count": int(totals_row["total_clipping_event_count"] or 0),
                    "silence_event_count": int(totals_row["total_silence_event_count"] or 0),
                    "overflow_event_count": int(totals_row["total_overflow_event_count"] or 0),
                }
            )

        return {
            "totals": totals,
            "updated_at": (totals_row["updated_at"] if totals_row is not None else "") or None,
            "daily": [
                {
                    "date_utc": str(row["day_utc"]),
                    "recorded_seconds": float(row["recorded_seconds"] or 0.0),
                    "analyzed_seconds": float(row["analyzed_seconds"] or 0.0),
                    "microphone_uptime_seconds": float(row["microphone_uptime_seconds"] or 0.0),
                    "detection_count": int(row["detection_count"] or 0),
                    "birdnet_success_count": int(row["birdnet_success_count"] or 0),
                    "birdnet_failure_count": int(row["birdnet_failure_count"] or 0),
                    "clipping_event_count": int(row["clipping_event_count"] or 0),
                    "silence_event_count": int(row["silence_event_count"] or 0),
                    "overflow_event_count": int(row["overflow_event_count"] or 0),
                    "updated_at": (row["updated_at"] or None),
                }
                for row in daily_rows
            ],
        }

    def write_status(self, payload: dict[str, Any]) -> None:
        self._status_file.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._status_file.with_name(f".{self._status_file.name}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temp_path.replace(self._status_file)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _ensure_metric_rows(self, connection: sqlite3.Connection, updated_at: str) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO node_metrics (
                singleton_id,
                updated_at
            ) VALUES (1, ?)
            """,
            (updated_at,),
        )
