"""可控时钟：不推进就不产生时间副作用。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class ManualClock:
    """测试与恢复场景使用的手动时钟，所有读数带时区。"""

    def __init__(self, start: datetime | None = None) -> None:
        if start is None:
            start = datetime(2026, 9, 25, 9, 0, tzinfo=timezone(timedelta(hours=8)))
        if start.tzinfo is None or start.utcoffset() is None:
            raise ValueError("时钟起点必须携带时区")
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        self._now = self._now + delta
        return self._now

    def set(self, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("时钟设置必须携带时区")
        self._now = value
        return self._now
