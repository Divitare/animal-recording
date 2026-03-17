from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class BirdActivityEvent:
    start_offset_seconds: float
    end_offset_seconds: float
    confidence: float
    dominant_frequency_hz: float


def _to_mono(samples: np.ndarray) -> np.ndarray:
    if samples.ndim == 1:
        return samples.astype(np.float32)
    return np.mean(samples.astype(np.float32), axis=1)


def detect_bird_activity(
    samples: np.ndarray,
    sample_rate: int,
    min_event_duration_seconds: float = 0.2,
) -> list[BirdActivityEvent]:
    mono = _to_mono(samples)
    if mono.size == 0:
        return []

    frame_length = max(512, int(sample_rate * 0.05))
    hop_length = max(256, int(sample_rate * 0.02))
    if mono.size < frame_length:
        return []

    window = np.hanning(frame_length)
    frequencies = np.fft.rfftfreq(frame_length, d=1.0 / sample_rate)
    bird_band = (frequencies >= 1500.0) & (frequencies <= 9500.0)

    rms_values: list[float] = []
    band_ratios: list[float] = []
    dominant_frequencies: list[float] = []
    offsets: list[int] = []

    for offset in range(0, mono.size - frame_length + 1, hop_length):
        frame = mono[offset : offset + frame_length]
        centered = frame - float(np.mean(frame))
        rms = float(np.sqrt(np.mean(np.square(centered))))
        spectrum = np.abs(np.fft.rfft(centered * window))
        total_energy = float(np.sum(spectrum)) + 1e-9
        band_energy = float(np.sum(spectrum[bird_band]))
        dominant_frequency = float(frequencies[int(np.argmax(spectrum))])

        offsets.append(offset)
        rms_values.append(rms)
        band_ratios.append(band_energy / total_energy)
        dominant_frequencies.append(dominant_frequency)

    if not offsets:
        return []

    rms_array = np.asarray(rms_values, dtype=np.float32)
    band_ratio_array = np.asarray(band_ratios, dtype=np.float32)
    dominant_array = np.asarray(dominant_frequencies, dtype=np.float32)

    adaptive_rms_threshold = max(0.012, float(np.quantile(rms_array, 0.25)) * 3.0)
    band_ratio_threshold = 0.33

    normalized_rms = np.clip(rms_array / max(adaptive_rms_threshold, 1e-4), 0.0, 2.0) / 2.0
    normalized_band = np.clip(band_ratio_array / max(band_ratio_threshold, 1e-4), 0.0, 2.0) / 2.0
    scores = (normalized_rms * 0.55) + (normalized_band * 0.45)

    bird_like = (
        (rms_array >= adaptive_rms_threshold)
        & (band_ratio_array >= band_ratio_threshold)
        & (dominant_array >= 1200.0)
        & (dominant_array <= 10000.0)
        & (scores >= 0.45)
    )

    events: list[BirdActivityEvent] = []
    group_start: int | None = None

    for index, is_positive in enumerate(bird_like):
        if is_positive and group_start is None:
            group_start = index
            continue
        if is_positive:
            continue
        if group_start is not None:
            event = _build_event(
                start_index=group_start,
                end_index=index - 1,
                offsets=offsets,
                frame_length=frame_length,
                sample_rate=sample_rate,
                scores=scores,
                dominant_array=dominant_array,
                min_event_duration_seconds=min_event_duration_seconds,
            )
            if event is not None:
                events.append(event)
            group_start = None

    if group_start is not None:
        event = _build_event(
            start_index=group_start,
            end_index=len(offsets) - 1,
            offsets=offsets,
            frame_length=frame_length,
            sample_rate=sample_rate,
            scores=scores,
            dominant_array=dominant_array,
            min_event_duration_seconds=min_event_duration_seconds,
        )
        if event is not None:
            events.append(event)

    return events


def _build_event(
    start_index: int,
    end_index: int,
    offsets: list[int],
    frame_length: int,
    sample_rate: int,
    scores: np.ndarray,
    dominant_array: np.ndarray,
    min_event_duration_seconds: float,
) -> BirdActivityEvent | None:
    start_offset = offsets[start_index] / sample_rate
    end_offset = (offsets[end_index] + frame_length) / sample_rate
    if (end_offset - start_offset) < min_event_duration_seconds:
        return None

    slice_scores = scores[start_index : end_index + 1]
    weights = slice_scores + 1e-3
    dominant_frequency = float(np.average(dominant_array[start_index : end_index + 1], weights=weights))
    confidence = float(np.clip(np.mean(slice_scores), 0.0, 1.0))
    return BirdActivityEvent(
        start_offset_seconds=start_offset,
        end_offset_seconds=end_offset,
        confidence=confidence,
        dominant_frequency_hz=dominant_frequency,
    )
