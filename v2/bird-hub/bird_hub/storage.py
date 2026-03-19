from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Any

from .config import BirdHubConfig


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value[:-1] if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _to_float(value: object) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class BirdHubStorage:
    def __init__(self, config: BirdHubConfig) -> None:
        self.config = config
        self._lock = threading.Lock()

    def initialize(self) -> None:
        self.config.ensure_directories()
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    display_name TEXT,
                    location_name TEXT,
                    latitude REAL,
                    longitude REAL,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT,
                    last_ingest_at TEXT,
                    last_app_version TEXT,
                    last_runtime_backend TEXT,
                    last_birdnet_version TEXT,
                    latest_health_snapshot_id INTEGER,
                    is_active INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS node_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL,
                    token_hash TEXT NOT NULL UNIQUE,
                    label TEXT,
                    created_at TEXT NOT NULL,
                    revoked_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_node_tokens_node_id ON node_tokens(node_id);
                CREATE TABLE IF NOT EXISTS ingest_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    received_at TEXT NOT NULL,
                    archive_filename TEXT NOT NULL,
                    archive_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    processed_event_count INTEGER NOT NULL DEFAULT 0,
                    processed_snapshot_count INTEGER NOT NULL DEFAULT 0,
                    processed_clip_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_ingest_batches_received_at ON ingest_batches(received_at DESC);
                CREATE TABLE IF NOT EXISTS health_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingest_batch_id INTEGER,
                    node_id TEXT NOT NULL,
                    source_snapshot_id INTEGER,
                    captured_at_utc TEXT NOT NULL,
                    time_source TEXT NOT NULL,
                    time_synchronized INTEGER NOT NULL DEFAULT 0,
                    app_version TEXT,
                    runtime_backend TEXT,
                    birdnet_version TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(node_id, captured_at_utc)
                );
                CREATE INDEX IF NOT EXISTS idx_health_snapshots_node_time ON health_snapshots(node_id, captured_at_utc DESC);
                CREATE TABLE IF NOT EXISTS clips (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingest_batch_id INTEGER,
                    event_id TEXT NOT NULL UNIQUE,
                    node_id TEXT NOT NULL,
                    storage_path TEXT NOT NULL,
                    original_archive_path TEXT,
                    duration_seconds REAL,
                    sample_rate INTEGER,
                    channels INTEGER,
                    size_bytes INTEGER,
                    sha256 TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clips_node_id ON clips(node_id);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingest_batch_id INTEGER,
                    event_id TEXT NOT NULL UNIQUE,
                    node_id TEXT NOT NULL,
                    species_common_name TEXT NOT NULL,
                    species_scientific_name TEXT,
                    confidence REAL NOT NULL,
                    event_start_utc TEXT NOT NULL,
                    event_end_utc TEXT NOT NULL,
                    clip_id INTEGER,
                    health_snapshot_id INTEGER,
                    app_version TEXT,
                    runtime_backend TEXT,
                    birdnet_version TEXT,
                    time_source TEXT,
                    utc_available INTEGER NOT NULL DEFAULT 1,
                    source_window_started_at TEXT,
                    source_window_ended_at TEXT,
                    analysis_duration_seconds REAL,
                    location_name TEXT,
                    latitude REAL,
                    longitude REAL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_node_time ON events(node_id, event_start_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_events_species ON events(species_common_name);
                """
            )

    def create_node_token(self, node_id: str, *, label: str | None = None) -> str:
        raw_token = secrets.token_urlsafe(32)
        created_at = utc_now_iso()
        with self._lock:
            with self._connect() as connection:
                self.upsert_node(connection, node_id=node_id, display_name=node_id, seen_at=created_at)
                connection.execute(
                    """
                    INSERT INTO node_tokens (node_id, token_hash, label, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (node_id, _hash_token(raw_token), label, created_at),
                )
        return raw_token

    def active_token_count(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS token_count FROM node_tokens WHERE revoked_at IS NULL"
            ).fetchone()
        return int(row["token_count"] if row is not None else 0)

    def authenticate_token(self, token: str) -> str | None:
        if not token:
            return None
        with self._connect() as connection:
            row = connection.execute(
                "SELECT node_id FROM node_tokens WHERE token_hash = ? AND revoked_at IS NULL",
                (_hash_token(token),),
            ).fetchone()
        return None if row is None else str(row["node_id"])

    def create_ingest_batch(
        self,
        *,
        node_id: str | None,
        archive_filename: str,
        archive_sha256: str,
        received_at: str,
    ) -> int:
        with self._lock:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO ingest_batches (
                        node_id,
                        received_at,
                        archive_filename,
                        archive_sha256,
                        status
                    ) VALUES (?, ?, ?, ?, 'processing')
                    """,
                    (node_id, received_at, archive_filename, archive_sha256),
                )
                return int(cursor.lastrowid)

    def finish_ingest_batch(
        self,
        batch_id: int,
        *,
        status: str,
        error_message: str | None,
        processed_event_count: int,
        processed_snapshot_count: int,
        processed_clip_count: int,
    ) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE ingest_batches
                    SET
                        status = ?,
                        error_message = ?,
                        processed_event_count = ?,
                        processed_snapshot_count = ?,
                        processed_clip_count = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        error_message,
                        processed_event_count,
                        processed_snapshot_count,
                        processed_clip_count,
                        batch_id,
                    ),
                )

    def event_exists(self, event_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute("SELECT 1 FROM events WHERE event_id = ?", (event_id,)).fetchone()
        return row is not None

    def upsert_health_snapshot(
        self,
        *,
        ingest_batch_id: int,
        node_id: str,
        source_snapshot_id: int | None,
        captured_at_utc: str,
        time_source: str,
        time_synchronized: bool,
        app_version: str | None,
        runtime_backend: str | None,
        birdnet_version: str | None,
        snapshot_payload: dict[str, Any],
    ) -> int:
        created_at = utc_now_iso()
        with self._lock:
            with self._connect() as connection:
                existing = connection.execute(
                    "SELECT id FROM health_snapshots WHERE node_id = ? AND captured_at_utc = ?",
                    (node_id, captured_at_utc),
                ).fetchone()
                if existing is not None:
                    snapshot_id = int(existing["id"])
                else:
                    cursor = connection.execute(
                        """
                        INSERT INTO health_snapshots (
                            ingest_batch_id,
                            node_id,
                            source_snapshot_id,
                            captured_at_utc,
                            time_source,
                            time_synchronized,
                            app_version,
                            runtime_backend,
                            birdnet_version,
                            payload_json,
                            created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            ingest_batch_id,
                            node_id,
                            source_snapshot_id,
                            captured_at_utc,
                            time_source,
                            1 if time_synchronized else 0,
                            app_version,
                            runtime_backend,
                            birdnet_version,
                            json.dumps(snapshot_payload, indent=2, sort_keys=True),
                            created_at,
                        ),
                    )
                    snapshot_id = int(cursor.lastrowid)

                connection.execute(
                    """
                    UPDATE nodes
                    SET
                        latest_health_snapshot_id = ?,
                        last_seen_at = COALESCE(?, last_seen_at),
                        last_app_version = COALESCE(?, last_app_version),
                        last_runtime_backend = COALESCE(?, last_runtime_backend),
                        last_birdnet_version = COALESCE(?, last_birdnet_version)
                    WHERE node_id = ?
                    """,
                    (snapshot_id, captured_at_utc, app_version, runtime_backend, birdnet_version, node_id),
                )
                return snapshot_id

    def insert_clip(
        self,
        *,
        ingest_batch_id: int,
        event_id: str,
        node_id: str,
        storage_path: str,
        original_archive_path: str | None,
        duration_seconds: float | None,
        sample_rate: int | None,
        channels: int | None,
        size_bytes: int | None,
        sha256: str | None,
    ) -> int:
        created_at = utc_now_iso()
        with self._lock:
            with self._connect() as connection:
                existing = connection.execute(
                    "SELECT id FROM clips WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if existing is not None:
                    return int(existing["id"])
                cursor = connection.execute(
                    """
                    INSERT INTO clips (
                        ingest_batch_id,
                        event_id,
                        node_id,
                        storage_path,
                        original_archive_path,
                        duration_seconds,
                        sample_rate,
                        channels,
                        size_bytes,
                        sha256,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ingest_batch_id,
                        event_id,
                        node_id,
                        storage_path,
                        original_archive_path,
                        duration_seconds,
                        sample_rate,
                        channels,
                        size_bytes,
                        sha256,
                        created_at,
                    ),
                )
                return int(cursor.lastrowid)

    def insert_event(
        self,
        *,
        ingest_batch_id: int,
        node_id: str,
        event_id: str,
        species_common_name: str,
        species_scientific_name: str | None,
        confidence: float,
        event_start_utc: str,
        event_end_utc: str,
        clip_id: int | None,
        health_snapshot_id: int | None,
        app_version: str | None,
        runtime_backend: str | None,
        birdnet_version: str | None,
        time_source: str | None,
        utc_available: bool,
        source_window_started_at: str | None,
        source_window_ended_at: str | None,
        analysis_duration_seconds: float | None,
        location_name: str | None,
        latitude: float | None,
        longitude: float | None,
    ) -> int:
        created_at = utc_now_iso()
        with self._lock:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO events (
                        ingest_batch_id,
                        event_id,
                        node_id,
                        species_common_name,
                        species_scientific_name,
                        confidence,
                        event_start_utc,
                        event_end_utc,
                        clip_id,
                        health_snapshot_id,
                        app_version,
                        runtime_backend,
                        birdnet_version,
                        time_source,
                        utc_available,
                        source_window_started_at,
                        source_window_ended_at,
                        analysis_duration_seconds,
                        location_name,
                        latitude,
                        longitude,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ingest_batch_id,
                        event_id,
                        node_id,
                        species_common_name,
                        species_scientific_name,
                        confidence,
                        event_start_utc,
                        event_end_utc,
                        clip_id,
                        health_snapshot_id,
                        app_version,
                        runtime_backend,
                        birdnet_version,
                        time_source,
                        1 if utc_available else 0,
                        source_window_started_at,
                        source_window_ended_at,
                        analysis_duration_seconds,
                        location_name,
                        latitude,
                        longitude,
                        created_at,
                    ),
                )
                self.upsert_node(
                    connection,
                    node_id=node_id,
                    display_name=node_id,
                    location_name=location_name,
                    latitude=latitude,
                    longitude=longitude,
                    seen_at=event_end_utc,
                    app_version=app_version,
                    runtime_backend=runtime_backend,
                    birdnet_version=birdnet_version,
                )
                return int(cursor.lastrowid)

    def upsert_node(
        self,
        connection: sqlite3.Connection,
        *,
        node_id: str,
        display_name: str | None = None,
        location_name: str | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        seen_at: str | None = None,
        app_version: str | None = None,
        runtime_backend: str | None = None,
        birdnet_version: str | None = None,
    ) -> None:
        created_at = utc_now_iso()
        connection.execute(
            """
            INSERT INTO nodes (
                node_id,
                display_name,
                location_name,
                latitude,
                longitude,
                created_at,
                last_seen_at,
                last_ingest_at,
                last_app_version,
                last_runtime_backend,
                last_birdnet_version,
                is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(node_id) DO UPDATE SET
                display_name = COALESCE(excluded.display_name, nodes.display_name),
                location_name = COALESCE(excluded.location_name, nodes.location_name),
                latitude = COALESCE(excluded.latitude, nodes.latitude),
                longitude = COALESCE(excluded.longitude, nodes.longitude),
                last_seen_at = COALESCE(excluded.last_seen_at, nodes.last_seen_at),
                last_ingest_at = COALESCE(excluded.last_ingest_at, nodes.last_ingest_at),
                last_app_version = COALESCE(excluded.last_app_version, nodes.last_app_version),
                last_runtime_backend = COALESCE(excluded.last_runtime_backend, nodes.last_runtime_backend),
                last_birdnet_version = COALESCE(excluded.last_birdnet_version, nodes.last_birdnet_version),
                is_active = 1
            """,
            (
                node_id,
                display_name,
                location_name,
                latitude,
                longitude,
                created_at,
                seen_at,
                seen_at,
                app_version,
                runtime_backend,
                birdnet_version,
            ),
        )

    def get_hub_summary(self) -> dict[str, Any]:
        with self._connect() as connection:
            counts = connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM nodes) AS node_count,
                    (SELECT COUNT(*) FROM events) AS event_count,
                    (SELECT COUNT(*) FROM ingest_batches) AS batch_count,
                    (SELECT COUNT(*) FROM ingest_batches WHERE status = 'failed') AS failed_batch_count
                """
            ).fetchone()
            top_species_rows = connection.execute(
                """
                SELECT species_common_name, COUNT(*) AS detection_count
                FROM events
                GROUP BY species_common_name
                ORDER BY detection_count DESC, species_common_name ASC
                LIMIT 8
                """
            ).fetchall()
            recent_event_rows = connection.execute(
                """
                SELECT event_id, node_id, species_common_name, confidence, event_start_utc
                FROM events
                ORDER BY event_start_utc DESC
                LIMIT 10
                """
            ).fetchall()

        active_cutoff = (
            datetime.utcnow() - timedelta(hours=max(self.config.active_node_window_hours, 1))
        ).isoformat() + "Z"
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS active_node_count
                FROM nodes
                WHERE COALESCE(last_seen_at, created_at) >= ?
                """,
                (active_cutoff,),
            ).fetchone()

        return {
            "node_count": int(counts["node_count"] if counts is not None else 0),
            "active_node_count": int(row["active_node_count"] if row is not None else 0),
            "event_count": int(counts["event_count"] if counts is not None else 0),
            "batch_count": int(counts["batch_count"] if counts is not None else 0),
            "failed_batch_count": int(counts["failed_batch_count"] if counts is not None else 0),
            "top_species": [
                {
                    "species_common_name": str(species_row["species_common_name"]),
                    "detection_count": int(species_row["detection_count"]),
                }
                for species_row in top_species_rows
            ],
            "recent_events": [
                {
                    "event_id": str(event_row["event_id"]),
                    "node_id": str(event_row["node_id"]),
                    "species_common_name": str(event_row["species_common_name"]),
                    "confidence": float(event_row["confidence"]),
                    "event_start_utc": str(event_row["event_start_utc"]),
                }
                for event_row in recent_event_rows
            ],
        }

    def list_ingest_batches(self, *, limit: int = 25) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    id,
                    node_id,
                    received_at,
                    archive_filename,
                    archive_sha256,
                    status,
                    error_message,
                    processed_event_count,
                    processed_snapshot_count,
                    processed_clip_count
                FROM ingest_batches
                ORDER BY received_at DESC, id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_nodes(self) -> list[dict[str, Any]]:
        active_cutoff = datetime.utcnow() - timedelta(hours=max(self.config.active_node_window_hours, 1))
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    n.node_id,
                    n.display_name,
                    n.location_name,
                    n.latitude,
                    n.longitude,
                    n.created_at,
                    n.last_seen_at,
                    n.last_ingest_at,
                    n.last_app_version,
                    n.last_runtime_backend,
                    n.last_birdnet_version,
                    n.latest_health_snapshot_id,
                    (
                        SELECT COUNT(*) FROM events e WHERE e.node_id = n.node_id
                    ) AS detection_count_total,
                    (
                        SELECT MAX(e.event_end_utc) FROM events e WHERE e.node_id = n.node_id
                    ) AS last_detection_at
                FROM nodes n
                ORDER BY COALESCE(n.last_seen_at, n.created_at) DESC, n.node_id ASC
                """
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            latest_snapshot = None
            if row["latest_health_snapshot_id"] is not None:
                latest_snapshot = self.get_health_snapshot_by_id(int(row["latest_health_snapshot_id"]))
            last_seen = _parse_utc(str(row["last_seen_at"] or ""))
            items.append(
                {
                    "node_id": str(row["node_id"]),
                    "display_name": str(row["display_name"] or row["node_id"]),
                    "location_name": row["location_name"],
                    "latitude": _to_float(row["latitude"]),
                    "longitude": _to_float(row["longitude"]),
                    "created_at": row["created_at"],
                    "last_seen_at": row["last_seen_at"],
                    "last_ingest_at": row["last_ingest_at"],
                    "last_app_version": row["last_app_version"],
                    "last_runtime_backend": row["last_runtime_backend"],
                    "last_birdnet_version": row["last_birdnet_version"],
                    "detection_count_total": int(row["detection_count_total"] or 0),
                    "last_detection_at": row["last_detection_at"],
                    "active": bool(last_seen and last_seen >= active_cutoff),
                    "latest_health_snapshot": latest_snapshot,
                    "health_summary": self._health_summary(latest_snapshot["snapshot"] if latest_snapshot else None),
                }
            )
        return items

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    node_id,
                    display_name,
                    location_name,
                    latitude,
                    longitude,
                    created_at,
                    last_seen_at,
                    last_ingest_at,
                    last_app_version,
                    last_runtime_backend,
                    last_birdnet_version,
                    latest_health_snapshot_id
                FROM nodes
                WHERE node_id = ?
                """,
                (node_id,),
            ).fetchone()
        if row is None:
            return None
        latest_snapshot = None
        if row["latest_health_snapshot_id"] is not None:
            latest_snapshot = self.get_health_snapshot_by_id(int(row["latest_health_snapshot_id"]))
        return {
            "node_id": str(row["node_id"]),
            "display_name": str(row["display_name"] or row["node_id"]),
            "location_name": row["location_name"],
            "latitude": _to_float(row["latitude"]),
            "longitude": _to_float(row["longitude"]),
            "created_at": row["created_at"],
            "last_seen_at": row["last_seen_at"],
            "last_ingest_at": row["last_ingest_at"],
            "last_app_version": row["last_app_version"],
            "last_runtime_backend": row["last_runtime_backend"],
            "last_birdnet_version": row["last_birdnet_version"],
            "latest_health_snapshot": latest_snapshot,
            "health_summary": self._health_summary(latest_snapshot["snapshot"] if latest_snapshot else None),
        }

    def get_health_snapshot_by_id(self, snapshot_id: int) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    id,
                    node_id,
                    captured_at_utc,
                    time_source,
                    time_synchronized,
                    app_version,
                    runtime_backend,
                    birdnet_version,
                    payload_json
                FROM health_snapshots
                WHERE id = ?
                """,
                (snapshot_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "id": int(row["id"]),
            "node_id": str(row["node_id"]),
            "captured_at_utc": str(row["captured_at_utc"]),
            "time_source": str(row["time_source"]),
            "time_synchronized": bool(row["time_synchronized"]),
            "app_version": row["app_version"],
            "runtime_backend": row["runtime_backend"],
            "birdnet_version": row["birdnet_version"],
            "snapshot": json.loads(row["payload_json"]) if row["payload_json"] else {},
        }

    def list_events(
        self,
        *,
        node_id: str | None = None,
        since_utc: str | None = None,
        until_utc: str | None = None,
        species: str | None = None,
        min_confidence: float | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if node_id:
            clauses.append("e.node_id = ?")
            parameters.append(node_id)
        if since_utc:
            clauses.append("e.event_start_utc >= ?")
            parameters.append(since_utc)
        if until_utc:
            clauses.append("e.event_start_utc <= ?")
            parameters.append(until_utc)
        if species:
            clauses.append("LOWER(e.species_common_name) LIKE ?")
            parameters.append(f"%{species.lower()}%")
        if min_confidence is not None:
            clauses.append("e.confidence >= ?")
            parameters.append(min_confidence)

        query = """
            SELECT
                e.id,
                e.event_id,
                e.node_id,
                e.species_common_name,
                e.species_scientific_name,
                e.confidence,
                e.event_start_utc,
                e.event_end_utc,
                e.health_snapshot_id,
                e.app_version,
                e.runtime_backend,
                e.birdnet_version,
                e.time_source,
                e.utc_available,
                e.source_window_started_at,
                e.source_window_ended_at,
                e.analysis_duration_seconds,
                e.location_name,
                e.latitude,
                e.longitude,
                e.created_at,
                c.storage_path AS clip_storage_path,
                c.duration_seconds AS clip_duration_seconds,
                c.sample_rate AS clip_sample_rate,
                c.channels AS clip_channels,
                c.size_bytes AS clip_size_bytes
            FROM events e
            LEFT JOIN clips c ON c.id = e.clip_id
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY e.event_start_utc DESC, e.id DESC"
        if limit is not None:
            query += " LIMIT ?"
            parameters.append(max(1, int(limit)))

        with self._connect() as connection:
            rows = connection.execute(query, tuple(parameters)).fetchall()
        return [self._event_row_to_dict(row) for row in rows]

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    e.id,
                    e.event_id,
                    e.node_id,
                    e.species_common_name,
                    e.species_scientific_name,
                    e.confidence,
                    e.event_start_utc,
                    e.event_end_utc,
                    e.health_snapshot_id,
                    e.app_version,
                    e.runtime_backend,
                    e.birdnet_version,
                    e.time_source,
                    e.utc_available,
                    e.source_window_started_at,
                    e.source_window_ended_at,
                    e.analysis_duration_seconds,
                    e.location_name,
                    e.latitude,
                    e.longitude,
                    e.created_at,
                    c.storage_path AS clip_storage_path,
                    c.duration_seconds AS clip_duration_seconds,
                    c.sample_rate AS clip_sample_rate,
                    c.channels AS clip_channels,
                    c.size_bytes AS clip_size_bytes
                FROM events e
                LEFT JOIN clips c ON c.id = e.clip_id
                WHERE e.event_id = ?
                """,
                (event_id,),
            ).fetchone()
        if row is None:
            return None
        event = self._event_row_to_dict(row)
        event["health_snapshot"] = (
            self.get_health_snapshot_by_id(int(event["health_snapshot_id"]))
            if event["health_snapshot_id"] is not None
            else None
        )
        return event

    def list_species_stats(
        self,
        *,
        node_id: str | None = None,
        since_utc: str | None = None,
        until_utc: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if node_id:
            clauses.append("node_id = ?")
            parameters.append(node_id)
        if since_utc:
            clauses.append("event_start_utc >= ?")
            parameters.append(since_utc)
        if until_utc:
            clauses.append("event_start_utc <= ?")
            parameters.append(until_utc)
        query = """
            SELECT
                species_common_name,
                species_scientific_name,
                COUNT(*) AS detection_count,
                COUNT(DISTINCT node_id) AS node_count,
                AVG(confidence) AS average_confidence,
                MAX(confidence) AS best_confidence,
                MIN(event_start_utc) AS first_seen_utc,
                MAX(event_end_utc) AS last_seen_utc
            FROM events
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += """
            GROUP BY species_common_name, species_scientific_name
            ORDER BY detection_count DESC, best_confidence DESC, species_common_name ASC
        """
        with self._connect() as connection:
            rows = connection.execute(query, tuple(parameters)).fetchall()
        return [
            {
                "species_common_name": str(row["species_common_name"]),
                "species_scientific_name": row["species_scientific_name"],
                "detection_count": int(row["detection_count"]),
                "node_count": int(row["node_count"]),
                "average_confidence": float(row["average_confidence"] or 0.0),
                "best_confidence": float(row["best_confidence"] or 0.0),
                "first_seen_utc": row["first_seen_utc"],
                "last_seen_utc": row["last_seen_utc"],
            }
            for row in rows
        ]

    def list_health_snapshots(
        self,
        *,
        node_id: str | None = None,
        since_utc: str | None = None,
        until_utc: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if node_id:
            clauses.append("node_id = ?")
            parameters.append(node_id)
        if since_utc:
            clauses.append("captured_at_utc >= ?")
            parameters.append(since_utc)
        if until_utc:
            clauses.append("captured_at_utc <= ?")
            parameters.append(until_utc)
        query = """
            SELECT
                id,
                node_id,
                captured_at_utc,
                time_source,
                time_synchronized,
                app_version,
                runtime_backend,
                birdnet_version,
                payload_json
            FROM health_snapshots
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY captured_at_utc DESC, id DESC"
        if limit is not None:
            query += " LIMIT ?"
            parameters.append(max(1, int(limit)))

        with self._connect() as connection:
            rows = connection.execute(query, tuple(parameters)).fetchall()
        return [
            {
                "id": int(row["id"]),
                "node_id": str(row["node_id"]),
                "captured_at_utc": str(row["captured_at_utc"]),
                "time_source": str(row["time_source"]),
                "time_synchronized": bool(row["time_synchronized"]),
                "app_version": row["app_version"],
                "runtime_backend": row["runtime_backend"],
                "birdnet_version": row["birdnet_version"],
                "snapshot": json.loads(row["payload_json"]) if row["payload_json"] else {},
            }
            for row in rows
        ]

    def clip_abspath(self, storage_path: str):
        return (self.config.clip_dir / storage_path).resolve()

    def _event_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "event_id": str(row["event_id"]),
            "node_id": str(row["node_id"]),
            "species_common_name": str(row["species_common_name"]),
            "species_scientific_name": row["species_scientific_name"],
            "confidence": float(row["confidence"]),
            "event_start_utc": str(row["event_start_utc"]),
            "event_end_utc": str(row["event_end_utc"]),
            "health_snapshot_id": int(row["health_snapshot_id"]) if row["health_snapshot_id"] is not None else None,
            "app_version": row["app_version"],
            "runtime_backend": row["runtime_backend"],
            "birdnet_version": row["birdnet_version"],
            "time_source": row["time_source"],
            "utc_available": bool(row["utc_available"]),
            "source_window_started_at": row["source_window_started_at"],
            "source_window_ended_at": row["source_window_ended_at"],
            "analysis_duration_seconds": _to_float(row["analysis_duration_seconds"]),
            "location_name": row["location_name"],
            "latitude": _to_float(row["latitude"]),
            "longitude": _to_float(row["longitude"]),
            "created_at": row["created_at"],
            "clip": (
                {
                    "storage_path": row["clip_storage_path"],
                    "duration_seconds": _to_float(row["clip_duration_seconds"]),
                    "sample_rate": int(row["clip_sample_rate"]) if row["clip_sample_rate"] is not None else None,
                    "channels": int(row["clip_channels"]) if row["clip_channels"] is not None else None,
                    "size_bytes": int(row["clip_size_bytes"]) if row["clip_size_bytes"] is not None else None,
                }
                if row["clip_storage_path"] is not None
                else None
            ),
        }

    def _health_summary(self, snapshot_payload: dict[str, Any] | None) -> dict[str, Any] | None:
        if not snapshot_payload:
            return None
        health = snapshot_payload.get("health") or {}
        system = health.get("system") or {}
        microphone = health.get("microphone") or {}
        birdnet = health.get("birdnet") or {}
        return {
            "system_status": system.get("status"),
            "microphone_status": microphone.get("status"),
            "birdnet_status": birdnet.get("status"),
            "cpu_temperature_celsius": system.get("cpu_temperature_celsius"),
            "free_bytes": system.get("free_bytes"),
        }

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.config.database_path)
        connection.row_factory = sqlite3.Row
        return connection
