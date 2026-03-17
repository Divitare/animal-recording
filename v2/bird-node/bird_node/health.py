from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np


def root_mean_square(samples: np.ndarray) -> float:
    prepared = np.asarray(samples, dtype=np.float32)
    if prepared.size == 0:
        return 0.0
    return float(np.sqrt(np.mean(np.square(prepared), dtype=np.float32)))


def read_cpu_temperature_celsius() -> float | None:
    candidates = (
        Path("/sys/class/thermal/thermal_zone0/temp"),
        Path("/sys/devices/virtual/thermal/thermal_zone0/temp"),
    )
    for path in candidates:
        try:
            raw_value = path.read_text(encoding="utf-8").strip()
            if not raw_value:
                continue
            value = float(raw_value)
            if value > 1000:
                value /= 1000.0
            return round(value, 2)
        except (OSError, ValueError):
            continue
    return None


def disk_usage_summary(target_path: Path, *, low_space_bytes: int) -> dict[str, object]:
    usage = shutil.disk_usage(target_path)
    total_bytes = int(usage.total)
    used_bytes = int(usage.used)
    free_bytes = int(usage.free)
    used_percent = round((used_bytes / total_bytes) * 100.0, 2) if total_bytes > 0 else 0.0

    return {
        "status": "low" if free_bytes <= max(low_space_bytes, 0) else "healthy",
        "total_bytes": total_bytes,
        "used_bytes": used_bytes,
        "free_bytes": free_bytes,
        "used_percent": used_percent,
        "low_space_threshold_bytes": int(max(low_space_bytes, 0)),
    }
