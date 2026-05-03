from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _load_default_env_files() -> None:
    service_env = Path(os.getenv("BIRD_MONITOR_ENV_FILE", "/etc/bird-node.env")).expanduser()
    if service_env.exists():
        load_dotenv(service_env, override=False)
    load_dotenv(override=False)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    return int(value) if value else default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    return float(value) if value else default


def _env_optional_int(name: str) -> int | None:
    value = os.getenv(name, "").strip()
    return int(value) if value else None


def _env_optional_float(name: str) -> float | None:
    value = os.getenv(name, "").strip()
    return float(value) if value else None


@dataclass(frozen=True)
class BirdNodeConfig:
    node_id: str
    app_commit: str
    data_dir: Path
    clips_dir: Path
    exports_dir: Path
    sync_queue_dir: Path
    log_dir: Path
    status_file: Path
    database_path: Path
    device_name: str | None
    device_index: int | None
    sample_rate: int
    channels: int
    live_window_seconds: int
    live_step_seconds: int
    minimum_live_analysis_seconds: int
    rolling_audio_buffer_seconds: int
    detection_clip_padding_seconds: float
    write_status_interval_seconds: float
    health_snapshot_interval_seconds: float
    clipping_peak_threshold: float
    silence_rms_threshold: float
    silence_alert_seconds: float
    low_disk_free_bytes: int
    status_history_days: int
    location_name: str | None
    latitude: float | None
    longitude: float | None
    hub_url: str | None
    hub_token: str | None
    cloudflare_access_client_id: str | None
    cloudflare_access_client_secret: str | None
    sync_interval_seconds: float
    sync_retry_base_seconds: float
    sync_max_events_per_bundle: int
    sync_max_health_snapshots_per_bundle: int
    species_provider: str
    species_min_confidence: float
    disable_recorder: bool


def load_config() -> BirdNodeConfig:
    _load_default_env_files()

    package_root = Path(__file__).resolve().parent.parent
    commit_file = package_root / ".release-commit"
    app_commit = (
        os.getenv("BIRD_MONITOR_APP_COMMIT", "").strip()
        or (commit_file.read_text(encoding="utf-8").strip() if commit_file.exists() else "")
        or "unknown"
    )

    data_dir = Path(os.getenv("BIRD_MONITOR_DATA_DIR", str(package_root / "data"))).resolve()
    clips_dir = data_dir / "clips"
    exports_dir = data_dir / "exports"
    sync_queue_dir = data_dir / "sync-queue"
    log_dir = Path(os.getenv("BIRD_MONITOR_LOG_DIR", str(data_dir / "logs"))).resolve()
    status_file = Path(os.getenv("BIRD_MONITOR_STATUS_FILE", str(data_dir / "status.json"))).resolve()
    database_path = data_dir / "bird_node.db"
    matplotlib_cache_dir = data_dir / "mpl-cache"

    data_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    sync_queue_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    status_file.parent.mkdir(parents=True, exist_ok=True)
    matplotlib_cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(matplotlib_cache_dir))

    return BirdNodeConfig(
        node_id=os.getenv("BIRD_MONITOR_NODE_ID", socket.gethostname()).strip() or socket.gethostname(),
        app_commit=app_commit,
        data_dir=data_dir,
        clips_dir=clips_dir,
        exports_dir=exports_dir,
        sync_queue_dir=sync_queue_dir,
        log_dir=log_dir,
        status_file=status_file,
        database_path=database_path,
        device_name=os.getenv("BIRD_MONITOR_DEVICE_NAME", "").strip() or None,
        device_index=_env_optional_int("BIRD_MONITOR_DEVICE_INDEX"),
        sample_rate=_env_int("BIRD_MONITOR_SAMPLE_RATE", 16000),
        channels=_env_int("BIRD_MONITOR_CHANNELS", 1),
        live_window_seconds=_env_int("BIRD_MONITOR_LIVE_WINDOW_SECONDS", 9),
        live_step_seconds=_env_int("BIRD_MONITOR_LIVE_STEP_SECONDS", 3),
        minimum_live_analysis_seconds=_env_int("BIRD_MONITOR_MINIMUM_LIVE_ANALYSIS_SECONDS", 3),
        rolling_audio_buffer_seconds=_env_int("BIRD_MONITOR_AUDIO_BUFFER_SECONDS", 120),
        detection_clip_padding_seconds=_env_float("BIRD_MONITOR_DETECTION_CLIP_PADDING_SECONDS", 0.4),
        write_status_interval_seconds=_env_float("BIRD_MONITOR_STATUS_WRITE_INTERVAL_SECONDS", 2.0),
        health_snapshot_interval_seconds=_env_float("BIRD_MONITOR_HEALTH_SNAPSHOT_INTERVAL_SECONDS", 300.0),
        clipping_peak_threshold=_env_float("BIRD_MONITOR_CLIPPING_PEAK_THRESHOLD", 0.98),
        silence_rms_threshold=_env_float("BIRD_MONITOR_SILENCE_RMS_THRESHOLD", 0.003),
        silence_alert_seconds=_env_float("BIRD_MONITOR_SILENCE_ALERT_SECONDS", 30.0),
        low_disk_free_bytes=_env_int("BIRD_MONITOR_LOW_DISK_FREE_BYTES", 2147483648),
        status_history_days=_env_int("BIRD_MONITOR_STATUS_HISTORY_DAYS", 14),
        location_name=os.getenv("BIRD_MONITOR_LOCATION_NAME", "").strip() or None,
        latitude=_env_optional_float("BIRD_MONITOR_LATITUDE"),
        longitude=_env_optional_float("BIRD_MONITOR_LONGITUDE"),
        hub_url=os.getenv("BIRD_MONITOR_HUB_URL", "").strip() or None,
        hub_token=os.getenv("BIRD_MONITOR_HUB_TOKEN", "").strip() or None,
        cloudflare_access_client_id=os.getenv("BIRD_MONITOR_CLOUDFLARE_ACCESS_CLIENT_ID", "").strip() or None,
        cloudflare_access_client_secret=os.getenv("BIRD_MONITOR_CLOUDFLARE_ACCESS_CLIENT_SECRET", "").strip() or None,
        sync_interval_seconds=_env_float("BIRD_MONITOR_SYNC_INTERVAL_SECONDS", 1800.0),
        sync_retry_base_seconds=_env_float("BIRD_MONITOR_SYNC_RETRY_BASE_SECONDS", 300.0),
        sync_max_events_per_bundle=_env_int("BIRD_MONITOR_SYNC_MAX_EVENTS_PER_BUNDLE", 25),
        sync_max_health_snapshots_per_bundle=_env_int("BIRD_MONITOR_SYNC_MAX_HEALTH_SNAPSHOTS_PER_BUNDLE", 12),
        species_provider=os.getenv("BIRD_MONITOR_SPECIES_PROVIDER", "birdnet").strip().casefold() or "birdnet",
        species_min_confidence=_env_float("BIRD_MONITOR_SPECIES_MIN_CONFIDENCE", 0.35),
        disable_recorder=_env_flag("BIRD_MONITOR_DISABLE_RECORDER", False),
    )
