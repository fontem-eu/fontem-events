"""gmr_events — producer + consumer client for the event log.

Runtime layer on top of ``gmr_event_schemas`` (typed payloads)
and Postgres (durable storage in ``events.entity_events``).
"""
from gmr_event_schemas import EventEnvelope, EventValidationError

from .consumer import EventConsumer
from .errors import EventLogError, OffsetError
from .producer import EventLog, EventBatch

__all__ = [
    "EventBatch",
    "EventConsumer",
    "EventEnvelope",
    "EventLog",
    "EventLogError",
    "EventValidationError",
    "OffsetError",
]
