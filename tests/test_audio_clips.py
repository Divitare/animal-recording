import numpy as np

from bird_monitor.audio import extract_clip_samples


def test_extract_clip_samples_adds_padding_and_clamps_to_bounds():
    sample_rate = 10
    samples = np.arange(100, dtype=np.float32)

    clip = extract_clip_samples(
        samples,
        sample_rate=sample_rate,
        start_offset_seconds=1.0,
        end_offset_seconds=2.0,
        padding_seconds=0.5,
    )

    assert clip.tolist() == list(range(5, 25))


def test_extract_clip_samples_returns_empty_when_range_is_invalid():
    sample_rate = 10
    samples = np.arange(20, dtype=np.float32)

    clip = extract_clip_samples(
        samples,
        sample_rate=sample_rate,
        start_offset_seconds=1.0,
        end_offset_seconds=1.0,
        padding_seconds=0.0,
    )

    assert clip.size == 0
