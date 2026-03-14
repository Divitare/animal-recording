from datetime import datetime, timezone

from bird_monitor.models import RecordingSchedule
from bird_monitor.scheduler import get_active_window


def build_schedule(start_time: str, end_time: str, days: list[int]) -> RecordingSchedule:
    schedule = RecordingSchedule(name="Test", start_time=start_time, end_time=end_time, enabled=True)
    schedule.set_days(days)
    return schedule


def test_regular_schedule_is_active_inside_window():
    schedule = build_schedule("05:00", "08:00", [5])
    moment = datetime(2026, 3, 14, 6, 30, tzinfo=timezone.utc)
    assert get_active_window(schedule, moment) is not None


def test_overnight_schedule_is_active_after_midnight():
    schedule = build_schedule("22:00", "02:00", [4])
    moment = datetime(2026, 3, 14, 1, 15, tzinfo=timezone.utc)
    assert get_active_window(schedule, moment) is not None


def test_schedule_is_inactive_outside_window():
    schedule = build_schedule("05:00", "08:00", [5])
    moment = datetime(2026, 3, 14, 9, 0, tzinfo=timezone.utc)
    assert get_active_window(schedule, moment) is None
