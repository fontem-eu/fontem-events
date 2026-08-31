"""fontem_events — producer + consumer client for the event log.

Runtime layer on top of ``fontem_event_schemas`` (typed payloads)
and Postgres (durable storage in ``events.entity_events``).
"""
from fontem_event_schemas import EventEnvelope, EventValidationError

from .consumer import EventConsumer
from .errors import EventLogError, OffsetError
from .producer import EventLog, EventBatch
from .reaper import reap_stale_runs
from .run_log import RunLog, recent_runs

__all__ = [
    "EventBatch",
    "EventConsumer",
    "EventEnvelope",
    "EventLog",
    "EventLogError",
    "EventValidationError",
    "OffsetError",
    "RunLog",
    "reap_stale_runs",
    "recent_runs",
]
