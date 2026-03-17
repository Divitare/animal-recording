from __future__ import annotations

import os

from sqlalchemy import inspect, text

from .extensions import db


def ensure_schema() -> None:
    inspector = inspect(db.engine)
    tables = set(inspector.get_table_names())

    if "recorder_settings" in tables:
        columns = {column["name"] for column in inspector.get_columns("recorder_settings")}
        _add_column_if_missing("recorder_settings", columns, "location_name", "location_name VARCHAR(255)")
        _add_column_if_missing("recorder_settings", columns, "latitude", "latitude REAL")
        _add_column_if_missing("recorder_settings", columns, "longitude", "longitude REAL")
        _add_column_if_missing("recorder_settings", columns, "species_provider", "species_provider VARCHAR(32)")
        _add_column_if_missing(
            "recorder_settings",
            columns,
            "species_min_confidence",
            "species_min_confidence REAL",
        )

        default_provider = os.getenv("BIRD_MONITOR_SPECIES_PROVIDER", "birdnet").strip().casefold() or "disabled"
        default_confidence = float(os.getenv("BIRD_MONITOR_SPECIES_MIN_CONFIDENCE", "0.35"))
        db.session.execute(
            text(
                """
                UPDATE recorder_settings
                SET species_provider = :provider
                WHERE species_provider IS NULL OR species_provider = ''
                """
            ),
            {"provider": default_provider},
        )
        db.session.execute(
            text(
                """
                UPDATE recorder_settings
                SET species_min_confidence = :confidence
                WHERE species_min_confidence IS NULL
                """
            ),
            {"confidence": default_confidence},
        )

    if "bird_detections" in tables:
        columns = {column["name"] for column in inspector.get_columns("bird_detections")}
        _add_column_if_missing("bird_detections", columns, "source", "source VARCHAR(32)")
        _add_column_if_missing(
            "bird_detections",
            columns,
            "species_scientific_name",
            "species_scientific_name VARCHAR(255)",
        )
        _add_column_if_missing("bird_detections", columns, "clip_file_path", "clip_file_path TEXT")
        _add_column_if_missing("bird_detections", columns, "clip_duration_seconds", "clip_duration_seconds REAL")
        db.session.execute(
            text(
                """
                UPDATE bird_detections
                SET source = 'activity'
                WHERE source IS NULL OR source = ''
                """
            )
        )

    db.session.commit()


def _add_column_if_missing(table_name: str, columns: set[str], column_name: str, ddl: str) -> bool:
    if column_name in columns:
        return False
    db.session.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {ddl}"))
    columns.add(column_name)
    return True
