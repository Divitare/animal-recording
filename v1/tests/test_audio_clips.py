import numpy as np

from bird_monitor.audio import extract_clip_samples
from bird_monitor.services import AnalysisWindowAccumulator
from bird_monitor.species import BIRDNET_SAMPLE_RATE, prepare_live_samples_for_birdnet


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


def test_analysis_window_accumulator_emits_overlapping_windows():
    accumulator = AnalysisWindowAccumulator(window_frames=9, step_frames=3)
    chunk = np.arange(12, dtype=np.float32).reshape(-1, 1)

    windows = accumulator.push(chunk)

    assert len(windows) == 2
    first_window, first_start = windows[0]
    second_window, second_start = windows[1]
    assert first_start == 0
    assert second_start == 3
    assert first_window[:, 0].tolist() == list(range(9))
    assert second_window[:, 0].tolist() == list(range(3, 12))


def test_prepare_live_samples_for_birdnet_resamples_to_48khz():
    sample_rate = 16000
    seconds = 9
    timeline = np.linspace(0, seconds, sample_rate * seconds, endpoint=False, dtype=np.float32)
    samples = (0.2 * np.sin(2 * np.pi * 1200 * timeline)).astype(np.float32)

    prepared, prepared_rate = prepare_live_samples_for_birdnet(samples, sample_rate=sample_rate)

    assert prepared_rate == BIRDNET_SAMPLE_RATE
    assert prepared.dtype == np.float32
    assert abs((prepared.shape[0] / prepared_rate) - seconds) < 0.02
    assert prepared.shape[0] > samples.shape[0]
