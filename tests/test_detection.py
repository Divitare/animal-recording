import numpy as np

from bird_monitor.detection import detect_bird_activity


def test_detect_bird_activity_finds_synthetic_chirp():
    sample_rate = 32000
    seconds = 2.0
    timeline = np.linspace(0, seconds, int(sample_rate * seconds), endpoint=False)
    samples = np.zeros_like(timeline, dtype=np.float32)

    chirp_start = int(0.6 * sample_rate)
    chirp_end = int(1.0 * sample_rate)
    chirp_time = timeline[: chirp_end - chirp_start]
    chirp = 0.18 * np.sin(2 * np.pi * 3200 * chirp_time).astype(np.float32)
    samples[chirp_start:chirp_end] = chirp

    events = detect_bird_activity(samples, sample_rate)

    assert events
    assert events[0].start_offset_seconds < 1.0
    assert events[0].end_offset_seconds > 0.6
    assert events[0].dominant_frequency_hz > 2000
