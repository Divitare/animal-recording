from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _read_release_commit(app_root: Path) -> str:
    release_file = app_root / ".release-commit"
    if release_file.exists():
        return release_file.read_text(encoding="utf-8").strip() or "dev"
    return "dev"


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class BirdHubConfig:
    app_root: Path
    app_variant: str
    app_commit: str
    secret_key: str
    data_dir: Path
    log_dir: Path
    database_path: Path
    clip_dir: Path
    upload_dir: Path
    host: str
    port: int
    allow_unauthenticated_ingest: bool
    max_bundle_bytes: int
    default_event_limit: int
    active_node_window_hours: int

    @classmethod
    def from_env(cls) -> "BirdHubConfig":
        app_root = Path(__file__).resolve().parents[1]
        data_dir = Path(os.getenv("BIRD_MONITOR_DATA_DIR", app_root / "data")).resolve()
        log_dir = Path(os.getenv("BIRD_MONITOR_LOG_DIR", data_dir / "logs")).resolve()
        database_path = data_dir / "bird_hub.db"
        clip_dir = data_dir / "clips"
        upload_dir = data_dir / "uploads"

        return cls(
            app_root=app_root,
            app_variant=os.getenv("BIRD_MONITOR_APP_VARIANT", "v2-bird-hub"),
            app_commit=os.getenv("BIRD_MONITOR_APP_COMMIT", "").strip() or _read_release_commit(app_root),
            secret_key=os.getenv("BIRD_MONITOR_SECRET_KEY", "bird-hub-dev-secret"),
            data_dir=data_dir,
            log_dir=log_dir,
            database_path=database_path,
            clip_dir=clip_dir,
            upload_dir=upload_dir,
            host=os.getenv("BIRD_MONITOR_HOST", "0.0.0.0"),
            port=int(os.getenv("BIRD_MONITOR_PORT", "8080")),
            allow_unauthenticated_ingest=_env_bool("BIRD_MONITOR_ALLOW_UNAUTHENTICATED_INGEST", True),
            max_bundle_bytes=int(os.getenv("BIRD_MONITOR_MAX_BUNDLE_BYTES", str(512 * 1024 * 1024))),
            default_event_limit=int(os.getenv("BIRD_MONITOR_DEFAULT_EVENT_LIMIT", "200")),
            active_node_window_hours=int(os.getenv("BIRD_MONITOR_ACTIVE_NODE_WINDOW_HOURS", "24")),
        )

    def ensure_directories(self) -> None:
        for path in (self.data_dir, self.log_dir, self.clip_dir, self.upload_dir):
            path.mkdir(parents=True, exist_ok=True)
