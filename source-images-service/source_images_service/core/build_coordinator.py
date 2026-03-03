"""Cross-job build deduplication for concurrent image sourcing."""

from __future__ import annotations

import threading


class BuildCoordinator:
    """Tracks in-flight image builds so concurrent jobs don't duplicate work.

    Usage::

        event = coordinator.try_acquire(tag)
        if event is not None:
            # Another thread is building this tag — wait for it
            event.wait()
            # ... check registry, skip or fall through ...
        else:
            # We are the builder
            try:
                # ... build + push ...
            finally:
                coordinator.release(tag)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._in_flight: dict[str, threading.Event] = {}

    def try_acquire(self, tag: str) -> threading.Event | None:
        """Try to claim ownership of building *tag*.

        Returns ``None`` if the caller is now the builder (must call
        :meth:`release` when done).  Returns an :class:`~threading.Event`
        if another thread already owns the build — the caller should
        ``.wait()`` on it.
        """
        with self._lock:
            if tag in self._in_flight:
                return self._in_flight[tag]
            self._in_flight[tag] = threading.Event()
            return None

    def release(self, tag: str) -> None:
        """Mark *tag* as finished and wake any waiting threads.

        Idempotent — safe to call even if *tag* is not tracked.
        """
        with self._lock:
            event = self._in_flight.pop(tag, None)
        if event is not None:
            event.set()
