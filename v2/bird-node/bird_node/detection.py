from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BirdActivityEvent:
    start_offset_seconds: float
    end_offset_seconds: float
