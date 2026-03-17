from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


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

    def record_detection(self, payload: dict[str, Any]) -> int:
        with self._lock:
            with self._connect() as connection:
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
                return int(cursor.lastrowid)

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
