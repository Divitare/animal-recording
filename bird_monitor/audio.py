from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

try:
    import sounddevice as sd
    import soundfile as sf
except Exception as exc:  # pragma: no cover - depends on system audio libs
    sd = None
    sf = None
    AUDIO_IMPORT_ERROR = exc
else:
    AUDIO_IMPORT_ERROR = None


def ensure_audio_runtime() -> None:
    if AUDIO_IMPORT_ERROR is not None:
        raise RuntimeError(
            "Audio dependencies are not available. Install PortAudio and libsndfile, then reinstall Python packages."
        ) from AUDIO_IMPORT_ERROR


@dataclass(frozen=True)
class AudioCapture:
    samples: np.ndarray
    sample_rate: int
    channels: int
    device_index: int
    device_name: str


def list_input_devices() -> list[dict[str, object]]:
    ensure_audio_runtime()
    devices = sd.query_devices()
    items: list[dict[str, object]] = []
    for index, device in enumerate(devices):
        max_input_channels = int(device["max_input_channels"])
        if max_input_channels <= 0:
            continue
        items.append(
            {
                "index": index,
                "name": str(device["name"]),
                "max_input_channels": max_input_channels,
                "default_samplerate": int(float(device["default_samplerate"])),
            }
        )
    return items


def resolve_input_device(preferred_name: str | None, preferred_index: int | None) -> tuple[int, str]:
    devices = list_input_devices()
    if not devices:
        raise RuntimeError("No input devices were found. Check that the USB microphone is connected.")

    if preferred_index is not None:
        for item in devices:
            if item["index"] == preferred_index:
                return int(item["index"]), str(item["name"])
        raise RuntimeError(f"Configured microphone index {preferred_index} is not available.")

    if preferred_name:
        lowered = preferred_name.casefold()
        for item in devices:
            if lowered in str(item["name"]).casefold():
                return int(item["index"]), str(item["name"])
        raise RuntimeError(f"No input device matched microphone name filter '{preferred_name}'.")

    default_input = getattr(sd.default, "device", None)
    if isinstance(default_input, (tuple, list)) and default_input:
        candidate_index = default_input[0]
        if candidate_index is not None and candidate_index >= 0:
            for item in devices:
                if item["index"] == candidate_index:
                    return int(item["index"]), str(item["name"])

    first = devices[0]
    return int(first["index"]), str(first["name"])


def record_segment(
    duration_seconds: int,
    sample_rate: int,
    channels: int,
    preferred_name: str | None = None,
    preferred_index: int | None = None,
) -> AudioCapture:
    ensure_audio_runtime()
    device_index, device_name = resolve_input_device(preferred_name, preferred_index)
    frame_count = max(1, int(duration_seconds * sample_rate))
    samples = sd.rec(
        frame_count,
        samplerate=sample_rate,
        channels=channels,
        dtype="float32",
        device=device_index,
        blocking=True,
    )
    return AudioCapture(
        samples=np.asarray(samples, dtype=np.float32),
        sample_rate=sample_rate,
        channels=channels,
        device_index=device_index,
        device_name=device_name,
    )


def save_capture(capture: AudioCapture, target_path: Path) -> None:
    ensure_audio_runtime()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    sf.write(target_path, capture.samples, capture.sample_rate, subtype="PCM_16")


def peak_amplitude(samples: np.ndarray) -> float:
    if samples.size == 0:
        return 0.0
    return float(np.max(np.abs(samples)))
