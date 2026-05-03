from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


def _load_service_env(default_path: str) -> None:
    env_path = Path(os.getenv("BIRD_MONITOR_ENV_FILE", default_path)).expanduser()
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ[key] = value


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


def _load_runtime_settings(settings_path: Path) -> dict[str, str]:
    if not settings_path.exists():
        return {}
    try:
        payload = json.loads(settings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(key): str(value).strip()
        for key, value in payload.items()
        if isinstance(key, str) and isinstance(value, str) and str(value).strip()
    }


@dataclass(slots=True)
class BirdHubConfig:
    app_root: Path
    app_variant: str
    app_commit: str
    secret_key: str
    data_dir: Path
    log_dir: Path
    settings_path: Path
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
        _load_service_env("/etc/bird-hub.env")
        app_root = Path(__file__).resolve().parents[1]
        data_dir = Path(os.getenv("BIRD_MONITOR_DATA_DIR", app_root / "data")).resolve()
        log_dir = Path(os.getenv("BIRD_MONITOR_LOG_DIR", data_dir / "logs")).resolve()
        settings_path = (data_dir / "hub_settings.json").resolve()
        runtime_settings = _load_runtime_settings(settings_path)
        database_path = Path(runtime_settings.get("database_path") or (data_dir / "bird_hub.db")).expanduser().resolve()
        clip_dir = Path(runtime_settings.get("clip_dir") or (data_dir / "clips")).expanduser().resolve()
        upload_dir = Path(runtime_settings.get("upload_dir") or (data_dir / "uploads")).expanduser().resolve()

        return cls(
            app_root=app_root,
            app_variant=os.getenv("BIRD_MONITOR_APP_VARIANT", "v2-bird-hub"),
            app_commit=os.getenv("BIRD_MONITOR_APP_COMMIT", "").strip() or _read_release_commit(app_root),
            secret_key=os.getenv("BIRD_MONITOR_SECRET_KEY", "bird-hub-dev-secret"),
            data_dir=data_dir,
            log_dir=log_dir,
            settings_path=settings_path,
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
        for path in (
            self.data_dir,
            self.log_dir,
            self.settings_path.parent,
            self.database_path.parent,
            self.clip_dir,
            self.upload_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def save_runtime_settings(self) -> None:
        payload = {
            "database_path": str(self.database_path),
            "clip_dir": str(self.clip_dir),
            "upload_dir": str(self.upload_dir),
        }
        self.settings_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.settings_path.with_name(f".{self.settings_path.name}.tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temp_path.replace(self.settings_path)
