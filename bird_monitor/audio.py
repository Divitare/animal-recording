from __future__ import annotations

from collections.abc import Callable
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

ChunkCallback = Callable[[np.ndarray], None]
StopCallback = Callable[[], bool]
COMMON_SAMPLE_RATES = [8000, 16000, 22050, 32000, 44100, 48000, 96000]


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
        compatibility = compatible_input_settings(index, max_input_channels, int(float(device["default_samplerate"])))
        items.append(
            {
                "index": index,
                "name": str(device["name"]),
                "max_input_channels": max_input_channels,
                "default_samplerate": int(float(device["default_samplerate"])),
                "supported_sample_rates": compatibility["sample_rates"],
                "supported_channels": compatibility["channels"],
            }
        )
    return items


def input_setting_supported(device_index: int, sample_rate: int, channels: int) -> bool:
    ensure_audio_runtime()
    try:
        sd.check_input_settings(device=device_index, samplerate=sample_rate, channels=channels)
    except Exception:
        return False
    return True


def compatible_input_settings(
    device_index: int,
    max_input_channels: int,
    default_sample_rate: int | None = None,
) -> dict[str, list[int]]:
    ensure_audio_runtime()

    candidate_rates = list(COMMON_SAMPLE_RATES)
    if default_sample_rate is not None and default_sample_rate not in candidate_rates:
        candidate_rates.insert(0, default_sample_rate)

    candidate_channels = list(range(1, min(max_input_channels, 2) + 1))
    supported_rates: list[int] = []
    supported_channels: list[int] = []

    for channel in candidate_channels:
        if any(input_setting_supported(device_index, rate, channel) for rate in candidate_rates):
            supported_channels.append(channel)

    for rate in candidate_rates:
        if any(input_setting_supported(device_index, rate, channel) for channel in candidate_channels):
            supported_rates.append(rate)

    if not supported_rates and default_sample_rate is not None and input_setting_supported(device_index, default_sample_rate, 1):
        supported_rates.append(default_sample_rate)
    if not supported_channels and input_setting_supported(device_index, default_sample_rate or 44100, 1):
        supported_channels.append(1)

    return {
        "sample_rates": supported_rates,
        "channels": supported_channels,
    }


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
    on_chunk: ChunkCallback | None = None,
    should_stop: StopCallback | None = None,
    chunk_seconds: float = 0.12,
) -> AudioCapture:
    ensure_audio_runtime()
    device_index, device_name = resolve_input_device(preferred_name, preferred_index)
    target_frames = max(1, int(duration_seconds * sample_rate))
    block_size = max(256, int(sample_rate * chunk_seconds))
    chunks: list[np.ndarray] = []
    captured_frames = 0

    with sd.InputStream(
        samplerate=sample_rate,
        channels=channels,
        dtype="float32",
        device=device_index,
        blocksize=block_size,
        latency="low",
    ) as stream:
        while captured_frames < target_frames:
            if should_stop is not None and should_stop():
                break

            frames_to_read = min(block_size, target_frames - captured_frames)
            chunk, overflowed = stream.read(frames_to_read)
            if overflowed:
                # Keep the recording and waveform flowing even if the system reports an overrun.
                pass

            chunk_array = np.asarray(chunk, dtype=np.float32)
            if chunk_array.size == 0:
                continue

            chunks.append(chunk_array.copy())
            captured_frames += int(chunk_array.shape[0])

            if on_chunk is not None:
                on_chunk(chunk_array)

            if should_stop is not None and should_stop():
                break

    if chunks:
        samples = np.concatenate(chunks, axis=0)
    else:
        samples = np.zeros((0, channels), dtype=np.float32)

    return AudioCapture(
        samples=samples,
        sample_rate=sample_rate,
        channels=channels,
        device_index=device_index,
        device_name=device_name,
    )


def save_capture(capture: AudioCapture, target_path: Path) -> None:
    ensure_audio_runtime()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    sf.write(target_path, capture.samples, capture.sample_rate, subtype="PCM_16")


def save_audio_samples(samples: np.ndarray, sample_rate: int, target_path: Path) -> None:
    ensure_audio_runtime()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    sf.write(target_path, samples, sample_rate, subtype="PCM_16")


def extract_clip_samples(
    samples: np.ndarray,
    sample_rate: int,
    start_offset_seconds: float,
    end_offset_seconds: float,
    padding_seconds: float = 0.25,
) -> np.ndarray:
    if samples.size == 0:
        return np.zeros((0,), dtype=np.float32)

    total_frames = int(samples.shape[0])
    start_frame = max(0, int(np.floor((start_offset_seconds - padding_seconds) * sample_rate)))
    end_frame = min(total_frames, int(np.ceil((end_offset_seconds + padding_seconds) * sample_rate)))
    if end_frame <= start_frame:
        return np.zeros_like(samples[:0])
    return np.asarray(samples[start_frame:end_frame]).copy()


def peak_amplitude(samples: np.ndarray) -> float:
    if samples.size == 0:
        return 0.0
    return float(np.max(np.abs(samples)))
