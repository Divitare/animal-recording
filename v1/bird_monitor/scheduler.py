from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from .models import RecordingSchedule


def parse_clock(value: str) -> time:
    hours, minutes = value.split(":", 1)
    return time(hour=int(hours), minute=int(minutes))


def _combine(day_value: date, clock: time, tzinfo) -> datetime:
    return datetime.combine(day_value, clock).replace(tzinfo=tzinfo)


@dataclass(frozen=True)
class ActiveWindow:
    schedule: RecordingSchedule
    started_at: datetime
    ends_at: datetime


def get_active_window(schedule: RecordingSchedule, moment: datetime) -> ActiveWindow | None:
    if not schedule.enabled:
        return None

    selected_days = schedule.days()
    if not selected_days:
        return None

    local_moment = moment if moment.tzinfo else moment.replace(tzinfo=datetime.now().astimezone().tzinfo)
    tzinfo = local_moment.tzinfo
    start_clock = parse_clock(schedule.start_time)
    end_clock = parse_clock(schedule.end_time)
    spans_midnight = end_clock <= start_clock

    for offset in (0, -1):
        base_day = local_moment.date() + timedelta(days=offset)
        if base_day.weekday() not in selected_days:
            continue

        start_dt = _combine(base_day, start_clock, tzinfo)
        if spans_midnight:
            end_dt = _combine(base_day + timedelta(days=1), end_clock, tzinfo)
        else:
            end_dt = _combine(base_day, end_clock, tzinfo)

        if start_dt <= local_moment < end_dt:
            return ActiveWindow(schedule=schedule, started_at=start_dt, ends_at=end_dt)

    return None


def get_active_windows(schedules: list[RecordingSchedule], moment: datetime) -> list[ActiveWindow]:
    active_windows: list[ActiveWindow] = []
    for schedule in schedules:
        window = get_active_window(schedule, moment)
        if window is not None:
            active_windows.append(window)
    return active_windows
