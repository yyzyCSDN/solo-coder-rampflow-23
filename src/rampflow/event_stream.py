from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from json import dumps
from threading import Condition, RLock
from typing import Iterable, Mapping

from .timeutil import utc


@dataclass(frozen=True)
class StreamEvent:
    sequence: int
    event_id: str
    occurred_at: datetime
    topic: str
    subject: str
    payload: Mapping[str, object]
    previous_hash: str
    hash: str


@dataclass(frozen=True)
class CursorPage:
    events: tuple[StreamEvent, ...]
    next_cursor: int


class CursorExpired(RuntimeError):
    """Raised when a cursor falls behind the retained floor.

    The stream never silently skips evicted events: an expired cursor is
    rejected, and the consumer must rebuild its projection from a snapshot
    and resume at ``reset_cursor``.
    """

    def __init__(self, cursor: int, floor: int):
        self.cursor = cursor
        self.floor = floor
        self.reset_cursor = floor - 1
        super().__init__(
            f"cursor {cursor} is before retained floor {floor}; "
            f"rebuild from a snapshot and resume at {self.reset_cursor}"
        )


class EventStream:
    """Hash-chained event log with a fixed retention window.

    Paging with a cursor older than the retained floor raises
    ``CursorExpired`` instead of clamping to the floor, so a stale consumer
    can never apply a partial history on top of an outdated base.
    """

    def __init__(self, retention: int = 10000):
        if retention < 1:
            raise ValueError("retention must be positive")
        self.retention = retention
        self._events: list[StreamEvent] = []
        self._ids: set[str] = set()
        self._next_sequence = 1
        self._last_hash = "0" * 64
        self._lock = RLock()
        self._changed = Condition(self._lock)

    @staticmethod
    def _canonical_payload(payload: Mapping[str, object]) -> str:
        return dumps(payload, sort_keys=True, separators=(",", ":"), default=str)

    def _hash(
        self,
        sequence: int,
        event_id: str,
        occurred_at: datetime,
        topic: str,
        subject: str,
        payload: Mapping[str, object],
        previous_hash: str,
    ) -> str:
        raw = "|".join(
            [
                str(sequence),
                event_id,
                occurred_at.isoformat(),
                topic,
                subject,
                self._canonical_payload(payload),
                previous_hash,
            ]
        ).encode("utf-8")
        return sha256(raw).hexdigest()

    def publish(
        self,
        event_id: str,
        occurred_at: datetime,
        topic: str,
        subject: str,
        payload: Mapping[str, object],
    ) -> StreamEvent:
        occurred_at = utc(occurred_at)
        with self._lock:
            if event_id in self._ids:
                return next(e for e in self._events if e.event_id == event_id)
            sequence = self._next_sequence
            digest = self._hash(
                sequence,
                event_id,
                occurred_at,
                topic,
                subject,
                payload,
                self._last_hash,
            )
            event = StreamEvent(
                sequence,
                event_id,
                occurred_at,
                topic,
                subject,
                dict(payload),
                self._last_hash,
                digest,
            )
            self._events.append(event)
            self._ids.add(event_id)
            self._next_sequence += 1
            self._last_hash = digest
            while len(self._events) > self.retention:
                evicted = self._events.pop(0)
                self._ids.discard(evicted.event_id)
            self._changed.notify_all()
            return event

    @property
    def head(self) -> int:
        with self._lock:
            return self._events[-1].sequence if self._events else 0

    @property
    def floor(self) -> int:
        with self._lock:
            return self._events[0].sequence if self._events else self._next_sequence

    def page(self, cursor: int, limit: int = 100, topics: Iterable[str] | None = None) -> CursorPage:
        if limit < 1:
            raise ValueError("limit must be positive")
        topic_set = set(topics) if topics is not None else None
        with self._lock:
            floor = self.floor
            if self._events and cursor < floor - 1:
                raise CursorExpired(cursor, floor)
            selected = [e for e in self._events if e.sequence > cursor and (topic_set is None or e.topic in topic_set)][:limit]
            next_cursor = selected[-1].sequence if selected else cursor
            return CursorPage(tuple(selected), next_cursor)

    def wait_page(
        self,
        cursor: int,
        timeout_seconds: float,
        limit: int = 100,
        topics: Iterable[str] | None = None,
    ) -> CursorPage:
        with self._changed:
            page = self.page(cursor, limit, topics)
            if page.events:
                return page
            self._changed.wait(timeout_seconds)
            return self.page(cursor, limit, topics)

    def verify_chain(self) -> bool:
        with self._lock:
            if not self._events:
                return True
            previous = self._events[0].previous_hash
            for event in self._events:
                expected = self._hash(
                    event.sequence,
                    event.event_id,
                    event.occurred_at,
                    event.topic,
                    event.subject,
                    event.payload,
                    previous,
                )
                if event.previous_hash != previous or event.hash != expected:
                    return False
                previous = event.hash
            return True

    def snapshot(self) -> tuple[StreamEvent, ...]:
        with self._lock:
            return tuple(self._events)
