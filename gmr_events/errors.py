"""Exceptions raised by the runtime client."""
from __future__ import annotations


class EventLogError(RuntimeError):
    """Generic failure from the event log layer."""


class OffsetError(EventLogError):
    """Offset bookkeeping went wrong (rare; usually means
    consumer table was tampered with)."""
