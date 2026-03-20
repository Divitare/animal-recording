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


def _table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({table_name})")
    }


def _ensure_columns(
    connection: sqlite3.Connection,
    table_name: str,
    column_definitions: dict[str, str],
) -> set[str]:
    existing_columns = _table_columns(connection, table_name)
    for column_name, column_definition in column_definitions.items():
        if column_name in existing_columns:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")
        existing_columns.add(column_name)
    return existing_columns


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
                CREATE TABLE IF NOT EXISTS health_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL,
                    captured_at TEXT NOT NULL,
                    time_source TEXT NOT NULL,
                    time_synchronized INTEGER NOT NULL DEFAULT 0,
                    app_commit TEXT,
                    runtime_backend TEXT,
                    birdnet_version TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_health_snapshots_captured_at ON health_snapshots(captured_at);
                CREATE TABLE IF NOT EXISTS sync_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bundle_path TEXT NOT NULL,
                    hub_url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_attempt_at TEXT,
                    next_retry_at TEXT,
                    synced_at TEXT,
                    last_error TEXT,
                    response_json TEXT,
                    detection_count INTEGER NOT NULL DEFAULT 0,
                    health_snapshot_count INTEGER NOT NULL DEFAULT 0,
                    detection_ids_json TEXT NOT NULL DEFAULT '[]',
                    health_snapshot_ids_json TEXT NOT NULL DEFAULT '[]'
                );
                """
            )
            _ensure_columns(
                connection,
                "detections",
                {
                    "event_id": "TEXT",
                    "source_window_started_at": "TEXT NOT NULL DEFAULT ''",
                    "source_window_ended_at": "TEXT NOT NULL DEFAULT ''",
                    "analysis_duration_seconds": "REAL",
                    "location_name": "TEXT",
                    "latitude": "REAL",
                    "longitude": "REAL",
                    "created_at": "TEXT NOT NULL DEFAULT ''",
                    "queued_batch_id": "INTEGER",
                    "queued_at": "TEXT",
                    "synced_at": "TEXT",
                },
            )
            connection.execute("UPDATE detections SET event_id = 'legacy-' || id WHERE COALESCE(event_id, '') = ''")
            connection.execute("UPDATE detections SET created_at = COALESCE(NULLIF(created_at, ''), started_at)")
            connection.execute(
                """
                UPDATE detections
                SET source_window_started_at = COALESCE(NULLIF(source_window_started_at, ''), started_at)
                """
            )
            connection.execute(
                """
                UPDATE detections
                SET source_window_ended_at = COALESCE(NULLIF(source_window_ended_at, ''), ended_at)
                """
            )
            connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_detections_event_id ON detections(event_id)")

            _ensure_columns(
                connection,
                "node_metrics",
                {
                    "total_recorded_seconds": "REAL NOT NULL DEFAULT 0",
                    "total_analyzed_seconds": "REAL NOT NULL DEFAULT 0",
                    "total_microphone_uptime_seconds": "REAL NOT NULL DEFAULT 0",
                    "total_detection_count": "INTEGER NOT NULL DEFAULT 0",
                    "total_birdnet_success_count": "INTEGER NOT NULL DEFAULT 0",
                    "total_birdnet_failure_count": "INTEGER NOT NULL DEFAULT 0",
                    "total_clipping_event_count": "INTEGER NOT NULL DEFAULT 0",
                    "total_silence_event_count": "INTEGER NOT NULL DEFAULT 0",
                    "total_overflow_event_count": "INTEGER NOT NULL DEFAULT 0",
                    "updated_at": "TEXT NOT NULL DEFAULT ''",
                },
            )
            _ensure_columns(
                connection,
                "daily_metrics",
                {
                    "recorded_seconds": "REAL NOT NULL DEFAULT 0",
                    "analyzed_seconds": "REAL NOT NULL DEFAULT 0",
                    "microphone_uptime_seconds": "REAL NOT NULL DEFAULT 0",
                    "detection_count": "INTEGER NOT NULL DEFAULT 0",
                    "birdnet_success_count": "INTEGER NOT NULL DEFAULT 0",
                    "birdnet_failure_count": "INTEGER NOT NULL DEFAULT 0",
                    "clipping_event_count": "INTEGER NOT NULL DEFAULT 0",
                    "silence_event_count": "INTEGER NOT NULL DEFAULT 0",
                    "overflow_event_count": "INTEGER NOT NULL DEFAULT 0",
                    "updated_at": "TEXT NOT NULL DEFAULT ''",
                },
            )

            _ensure_columns(
                connection,
                "health_snapshots",
                {
                    "time_source": "TEXT NOT NULL DEFAULT 'system'",
                    "time_synchronized": "INTEGER NOT NULL DEFAULT 0",
                    "app_commit": "TEXT",
                    "runtime_backend": "TEXT",
                    "birdnet_version": "TEXT",
                    "payload_json": "TEXT NOT NULL DEFAULT '{}'",
                    "queued_batch_id": "INTEGER",
                    "queued_at": "TEXT",
                    "synced_at": "TEXT",
                },
            )

            _ensure_columns(
                connection,
                "sync_batches",
                {
                    "attempt_count": "INTEGER NOT NULL DEFAULT 0",
                    "last_attempt_at": "TEXT",
                    "next_retry_at": "TEXT",
                    "synced_at": "TEXT",
                    "last_error": "TEXT",
                    "response_json": "TEXT",
                    "detection_count": "INTEGER NOT NULL DEFAULT 0",
                    "health_snapshot_count": "INTEGER NOT NULL DEFAULT 0",
                    "detection_ids_json": "TEXT NOT NULL DEFAULT '[]'",
                    "health_snapshot_ids_json": "TEXT NOT NULL DEFAULT '[]'",
                },
            )
            connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_detections_event_id ON detections(event_id)")
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sync_batches_status_retry
                ON sync_batches(status, next_retry_at, created_at)
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO node_metrics (
                    singleton_id,
                    updated_at
                ) VALUES (1, '')
                """
            )
            connection.execute(
                """
                UPDATE sync_batches
                SET
                    status = 'failed',
                    updated_at = COALESCE(updated_at, created_at),
                    last_error = CASE
                        WHEN COALESCE(last_error, '') = '' THEN 'Previous sync run ended unexpectedly.'
                        ELSE last_error
                    END
                WHERE status IN ('building', 'uploading')
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

    def record_health_snapshot(self, payload: dict[str, Any]) -> int:
        serialized_payload = json.dumps(payload, indent=2, sort_keys=True)
        with self._lock:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO health_snapshots (
                        node_id,
                        captured_at,
                        time_source,
                        time_synchronized,
                        app_commit,
                        runtime_backend,
                        birdnet_version,
                        payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(payload.get("node_id") or ""),
                        str(payload.get("captured_at") or ""),
                        str(payload.get("time_source") or "system"),
                        1 if bool(payload.get("time_synchronized")) else 0,
                        payload.get("app_commit"),
                        payload.get("runtime_backend"),
                        payload.get("birdnet_version"),
                        serialized_payload,
                    ),
                )
                return int(cursor.lastrowid)

    def list_detections(
        self,
        *,
        since_utc: str | None = None,
        until_utc: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if since_utc:
            clauses.append("started_at >= ?")
            parameters.append(since_utc)
        if until_utc:
            clauses.append("started_at <= ?")
            parameters.append(until_utc)

        query = """
            SELECT
                id,
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
                created_at,
                queued_batch_id,
                queued_at,
                synced_at
            FROM detections
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY started_at ASC"

        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(query, tuple(parameters)).fetchall()
        return [dict(row) for row in rows]

    def list_health_snapshots(
        self,
        *,
        since_utc: str | None = None,
        until_utc: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if since_utc:
            clauses.append("captured_at >= ?")
            parameters.append(since_utc)
        if until_utc:
            clauses.append("captured_at <= ?")
            parameters.append(until_utc)

        query = """
            SELECT
                id,
                node_id,
                captured_at,
                time_source,
                time_synchronized,
                app_commit,
                runtime_backend,
                birdnet_version,
                payload_json,
                queued_batch_id,
                queued_at,
                synced_at
            FROM health_snapshots
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY captured_at ASC"
        if limit is not None:
            query += " LIMIT ?"
            parameters.append(int(limit))

        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(query, tuple(parameters)).fetchall()

        snapshots: list[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
            snapshots.append(
                {
                    "id": int(row["id"]),
                    "node_id": row["node_id"],
                    "captured_at": row["captured_at"],
                    "time_source": row["time_source"],
                    "time_synchronized": bool(row["time_synchronized"]),
                    "app_commit": row["app_commit"],
                    "runtime_backend": row["runtime_backend"],
                    "birdnet_version": row["birdnet_version"],
                    "payload": payload,
                    "queued_batch_id": row["queued_batch_id"],
                    "queued_at": row["queued_at"],
                    "synced_at": row["synced_at"],
                }
            )
        return snapshots

    def list_detections_by_ids(self, detection_ids: list[int]) -> list[dict[str, Any]]:
        if not detection_ids:
            return []
        placeholders = ",".join("?" for _ in detection_ids)
        query = f"""
            SELECT
                id,
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
                created_at,
                queued_batch_id,
                queued_at,
                synced_at
            FROM detections
            WHERE id IN ({placeholders})
            ORDER BY started_at ASC
        """
        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(query, tuple(int(item) for item in detection_ids)).fetchall()
        return [dict(row) for row in rows]

    def list_health_snapshots_by_ids(self, snapshot_ids: list[int]) -> list[dict[str, Any]]:
        if not snapshot_ids:
            return []
        placeholders = ",".join("?" for _ in snapshot_ids)
        query = f"""
            SELECT
                id,
                node_id,
                captured_at,
                time_source,
                time_synchronized,
                app_commit,
                runtime_backend,
                birdnet_version,
                payload_json,
                queued_batch_id,
                queued_at,
                synced_at
            FROM health_snapshots
            WHERE id IN ({placeholders})
            ORDER BY captured_at ASC
        """
        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(query, tuple(int(item) for item in snapshot_ids)).fetchall()

        snapshots: list[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
            snapshots.append(
                {
                    "id": int(row["id"]),
                    "node_id": row["node_id"],
                    "captured_at": row["captured_at"],
                    "time_source": row["time_source"],
                    "time_synchronized": bool(row["time_synchronized"]),
                    "app_commit": row["app_commit"],
                    "runtime_backend": row["runtime_backend"],
                    "birdnet_version": row["birdnet_version"],
                    "payload": payload,
                    "queued_batch_id": row["queued_batch_id"],
                    "queued_at": row["queued_at"],
                    "synced_at": row["synced_at"],
                }
            )
        return snapshots

    def list_unsynced_detection_ids(self, *, limit: int) -> list[int]:
        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT id
                    FROM detections
                    WHERE synced_at IS NULL AND queued_batch_id IS NULL
                    ORDER BY started_at ASC, id ASC
                    LIMIT ?
                    """,
                    (max(1, int(limit)),),
                ).fetchall()
        return [int(row["id"]) for row in rows]

    def list_unsynced_health_snapshot_ids(self, *, limit: int) -> list[int]:
        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT id
                    FROM health_snapshots
                    WHERE synced_at IS NULL AND queued_batch_id IS NULL
                    ORDER BY captured_at ASC, id ASC
                    LIMIT ?
                    """,
                    (max(1, int(limit)),),
                ).fetchall()
        return [int(row["id"]) for row in rows]

    def create_sync_batch(
        self,
        *,
        bundle_path: str,
        hub_url: str,
        detection_ids: list[int],
        health_snapshot_ids: list[int],
        created_at: str,
    ) -> int:
        with self._lock:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO sync_batches (
                        bundle_path,
                        hub_url,
                        status,
                        created_at,
                        updated_at,
                        detection_count,
                        health_snapshot_count,
                        detection_ids_json,
                        health_snapshot_ids_json
                    ) VALUES (?, ?, 'building', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        bundle_path,
                        hub_url,
                        created_at,
                        created_at,
                        len(detection_ids),
                        len(health_snapshot_ids),
                        json.dumps(detection_ids),
                        json.dumps(health_snapshot_ids),
                    ),
                )
                batch_id = int(cursor.lastrowid)
                if detection_ids:
                    placeholders = ",".join("?" for _ in detection_ids)
                    connection.execute(
                        f"""
                        UPDATE detections
                        SET queued_batch_id = ?, queued_at = ?
                        WHERE id IN ({placeholders}) AND synced_at IS NULL AND queued_batch_id IS NULL
                        """,
                        (batch_id, created_at, *detection_ids),
                    )
                if health_snapshot_ids:
                    placeholders = ",".join("?" for _ in health_snapshot_ids)
                    connection.execute(
                        f"""
                        UPDATE health_snapshots
                        SET queued_batch_id = ?, queued_at = ?
                        WHERE id IN ({placeholders}) AND synced_at IS NULL AND queued_batch_id IS NULL
                        """,
                        (batch_id, created_at, *health_snapshot_ids),
                    )
                return batch_id

    def mark_sync_batch_pending(self, batch_id: int, *, bundle_path: str, updated_at: str) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE sync_batches
                    SET
                        bundle_path = ?,
                        status = 'pending',
                        updated_at = ?,
                        last_error = NULL
                    WHERE id = ?
                    """,
                    (bundle_path, updated_at, batch_id),
                )

    def fail_sync_batch_build(self, batch_id: int, *, error_message: str, updated_at: str) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE detections
                    SET queued_batch_id = NULL, queued_at = NULL
                    WHERE queued_batch_id = ?
                    """,
                    (batch_id,),
                )
                connection.execute(
                    """
                    UPDATE health_snapshots
                    SET queued_batch_id = NULL, queued_at = NULL
                    WHERE queued_batch_id = ?
                    """,
                    (batch_id,),
                )
                connection.execute(
                    """
                    UPDATE sync_batches
                    SET
                        status = 'abandoned',
                        updated_at = ?,
                        last_error = ?,
                        next_retry_at = NULL
                    WHERE id = ?
                    """,
                    (updated_at, error_message, batch_id),
                )

    def get_next_sync_batch(self, *, now_utc: str) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT
                        id,
                        bundle_path,
                        hub_url,
                        status,
                        created_at,
                        updated_at,
                        attempt_count,
                        last_attempt_at,
                        next_retry_at,
                        synced_at,
                        last_error,
                        response_json,
                        detection_count,
                        health_snapshot_count,
                        detection_ids_json,
                        health_snapshot_ids_json
                    FROM sync_batches
                    WHERE status = 'pending'
                       OR (status = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= ?))
                    ORDER BY
                        CASE WHEN status = 'pending' THEN 0 ELSE 1 END,
                        created_at ASC,
                        id ASC
                    LIMIT 1
                    """,
                    (now_utc,),
                ).fetchone()
        return None if row is None else self._sync_batch_row_to_dict(row)

    def mark_sync_batch_uploading(self, batch_id: int, *, attempted_at: str) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE sync_batches
                    SET
                        status = 'uploading',
                        updated_at = ?,
                        last_attempt_at = ?,
                        attempt_count = attempt_count + 1
                    WHERE id = ?
                    """,
                    (attempted_at, attempted_at, batch_id),
                )

    def mark_sync_batch_failed(
        self,
        batch_id: int,
        *,
        error_message: str,
        attempted_at: str,
        next_retry_at: str | None,
    ) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE sync_batches
                    SET
                        status = 'failed',
                        updated_at = ?,
                        last_error = ?,
                        next_retry_at = ?
                    WHERE id = ?
                    """,
                    (attempted_at, error_message, next_retry_at, batch_id),
                )

    def abandon_sync_batch(self, batch_id: int, *, error_message: str, updated_at: str) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE detections
                    SET queued_batch_id = NULL, queued_at = NULL
                    WHERE queued_batch_id = ? AND synced_at IS NULL
                    """,
                    (batch_id,),
                )
                connection.execute(
                    """
                    UPDATE health_snapshots
                    SET queued_batch_id = NULL, queued_at = NULL
                    WHERE queued_batch_id = ? AND synced_at IS NULL
                    """,
                    (batch_id,),
                )
                connection.execute(
                    """
                    UPDATE sync_batches
                    SET
                        status = 'abandoned',
                        updated_at = ?,
                        last_error = ?,
                        next_retry_at = NULL
                    WHERE id = ?
                    """,
                    (updated_at, error_message, batch_id),
                )

    def mark_sync_batch_synced(self, batch_id: int, *, synced_at: str, response_payload: dict[str, Any]) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE detections
                    SET synced_at = ?, queued_batch_id = NULL, queued_at = NULL
                    WHERE queued_batch_id = ?
                    """,
                    (synced_at, batch_id),
                )
                connection.execute(
                    """
                    UPDATE health_snapshots
                    SET synced_at = ?, queued_batch_id = NULL, queued_at = NULL
                    WHERE queued_batch_id = ?
                    """,
                    (synced_at, batch_id),
                )
                connection.execute(
                    """
                    UPDATE sync_batches
                    SET
                        status = 'synced',
                        updated_at = ?,
                        synced_at = ?,
                        response_json = ?,
                        last_error = NULL,
                        next_retry_at = NULL
                    WHERE id = ?
                    """,
                    (synced_at, synced_at, json.dumps(response_payload), batch_id),
                )

    def get_sync_summary(self) -> dict[str, Any]:
        with self._lock:
            with self._connect() as connection:
                counts_row = connection.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM detections WHERE synced_at IS NULL) AS unsynced_detection_count,
                        (SELECT COUNT(*) FROM health_snapshots WHERE synced_at IS NULL) AS unsynced_health_snapshot_count,
                        (SELECT COUNT(*) FROM sync_batches WHERE status IN ('pending', 'failed', 'uploading', 'building')) AS queued_batch_count,
                        (SELECT COUNT(*) FROM sync_batches WHERE status = 'failed') AS failed_batch_count,
                        (SELECT COUNT(*) FROM sync_batches WHERE status = 'synced') AS synced_batch_count
                    """
                ).fetchone()
                last_row = connection.execute(
                    """
                    SELECT
                        synced_at,
                        last_attempt_at,
                        last_error,
                        status
                    FROM sync_batches
                    ORDER BY COALESCE(last_attempt_at, created_at) DESC, id DESC
                    LIMIT 1
                    """
                ).fetchone()
        return {
            "unsynced_detection_count": int(counts_row["unsynced_detection_count"] if counts_row is not None else 0),
            "unsynced_health_snapshot_count": int(counts_row["unsynced_health_snapshot_count"] if counts_row is not None else 0),
            "queued_batch_count": int(counts_row["queued_batch_count"] if counts_row is not None else 0),
            "failed_batch_count": int(counts_row["failed_batch_count"] if counts_row is not None else 0),
            "synced_batch_count": int(counts_row["synced_batch_count"] if counts_row is not None else 0),
            "last_successful_sync_at": (last_row["synced_at"] if last_row is not None else None),
            "last_attempt_at": (last_row["last_attempt_at"] if last_row is not None else None),
            "last_error": (last_row["last_error"] if last_row is not None else None),
            "last_batch_status": (last_row["status"] if last_row is not None else None),
        }

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

    def _sync_batch_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "bundle_path": str(row["bundle_path"]),
            "hub_url": str(row["hub_url"]),
            "status": str(row["status"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "attempt_count": int(row["attempt_count"] or 0),
            "last_attempt_at": row["last_attempt_at"],
            "next_retry_at": row["next_retry_at"],
            "synced_at": row["synced_at"],
            "last_error": row["last_error"],
            "response": json.loads(row["response_json"]) if row["response_json"] else None,
            "detection_count": int(row["detection_count"] or 0),
            "health_snapshot_count": int(row["health_snapshot_count"] or 0),
            "detection_ids": json.loads(row["detection_ids_json"] or "[]"),
            "health_snapshot_ids": json.loads(row["health_snapshot_ids_json"] or "[]"),
        }

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
