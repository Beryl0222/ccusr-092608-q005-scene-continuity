"""事件存储：只追加、可重放、按幂等键去重。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Any, Callable

from .errors import IdempotencyKeyMismatch


@dataclass(frozen=True)
class StoredEvent:
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: datetime
    version: int
    payload: dict[str, Any]
    idempotency_key: str | None


class EventStore:
    """内存只追加事件日志；重放由上层 fold 完成。"""

    def __init__(self) -> None:
        self._events: list[StoredEvent] = []
        self._by_aggregate: dict[tuple[str, str], list[StoredEvent]] = {}
        self._idempotency: dict[str, StoredEvent] = {}
        self._lock = Lock()

    def append(
        self,
        *,
        event_id: str,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        occurred_at: datetime,
        version: int,
        payload: dict[str, Any],
        idempotency_key: str | None,
    ) -> StoredEvent:
        with self._lock:
            if idempotency_key is not None:
                existing = self._idempotency.get(idempotency_key)
                if existing is not None:
                    signature = (event_type, aggregate_type, aggregate_id, version, payload)
                    current = (
                        existing.event_type,
                        existing.aggregate_type,
                        existing.aggregate_id,
                        existing.version,
                        existing.payload,
                    )
                    if signature != current:
                        raise IdempotencyKeyMismatch(
                            f"幂等键 {idempotency_key!r} 已用于不同事件，拒绝覆盖"
                        )
                    return existing
            event = StoredEvent(
                event_id=event_id,
                event_type=event_type,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                occurred_at=occurred_at,
                version=version,
                payload=dict(payload),
                idempotency_key=idempotency_key,
            )
            self._events.append(event)
            self._by_aggregate.setdefault((aggregate_type, aggregate_id), []).append(event)
            if idempotency_key is not None:
                self._idempotency[idempotency_key] = event
            return event

    def replay(self, fold: Callable[[Any, StoredEvent], Any], state: Any) -> Any:
        for event in self._events:
            state = fold(state, event)
        return state

    def events(self) -> list[StoredEvent]:
        return list(self._events)

    def completed_keys(self) -> set[str]:
        """已落过事件的命令幂等键（去掉同命令多事件的 ``#序号`` 后缀）。"""
        return {
            key.rsplit("#", 1)[0]
            for key in self._idempotency
            if "#" in key
        }

    def events_for(self, aggregate_type: str, aggregate_id: str) -> list[StoredEvent]:
        return list(self._by_aggregate.get((aggregate_type, aggregate_id), []))

    def __len__(self) -> int:
        return len(self._events)
