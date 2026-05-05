"""Producer side of the event log.

The producer's job:

  * Build envelopes via the typed ``gmr_event_schemas.builders``.
  * Validate payloads against their JSON Schema before emit.
  * Insert in a single transaction per batch (all-or-nothing).
  * Stamp ``(producer, batch_id, iri)`` so consumers can dedupe
    on retry.

Concurrency: the ``EventLog`` instance owns one psycopg
connection. ETLs are single-threaded; if a producer ever needs
parallel emit, it should construct one ``EventLog`` per worker.
"""
from __future__ import annotations

import contextlib
import os
import uuid
from typing import Any, Iterator

import psycopg
from gmr_event_schemas import EventEnvelope, validate

from .errors import EventLogError


class EventLog:
    """Producer-side handle to ``events.entity_events``."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg.Connection | None = None

    @classmethod
    def from_env(
        cls, env_var: str = "EVENTS_DATABASE_URL"
    ) -> "EventLog":
        dsn = os.environ.get(env_var)
        if not dsn:
            raise EventLogError(
                f"{env_var} is not set; cannot reach the event log"
            )
        return cls(dsn)

    def connect(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, autocommit=False)
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    @contextlib.contextmanager
    def batch(
        self,
        batch_id: uuid.UUID,
        producer: str,
    ) -> Iterator["EventBatch"]:
        """Open a per-batch emit context.

        All events emitted in the block are inserted in a single
        transaction. If the block raises, nothing lands. On
        clean exit the transaction commits.
        """
        conn = self.connect()
        try:
            with conn.transaction():
                yield EventBatch(conn=conn, batch_id=batch_id, producer=producer)
        except Exception:
            # transaction() rolled back; rethrow so the caller sees it
            raise


class EventBatch:
    """Per-batch emit helper. Use via ``EventLog.batch(...)``."""

    def __init__(
        self,
        *,
        conn: psycopg.Connection,
        batch_id: uuid.UUID,
        producer: str,
    ) -> None:
        self._conn = conn
        self._batch_id = batch_id
        self._producer = producer
        self._count = 0

    @property
    def count(self) -> int:
        return self._count

    def upsert(
        self, event_type: str, *, iri: str, domain: str,
        payload: dict[str, Any], schema_version: int = 1,
    ) -> int:
        """Emit an upsert event for a single entity. Returns the
        seq the row landed at."""
        return self._emit(
            event_type=event_type, iri=iri, domain=domain, op="upsert",
            payload=payload, schema_version=schema_version,
        )

    def delete(
        self, event_type: str, *, iri: str, domain: str,
        schema_version: int = 1,
    ) -> int:
        return self._emit(
            event_type=event_type, iri=iri, domain=domain, op="delete",
            payload={"iri": iri},
            schema_version=schema_version,
        )

    def control(
        self, event_type: str, payload: dict[str, Any],
        *, schema_version: int = 1,
    ) -> int:
        """Control events (BeginGraphReplace etc.) target a graph
        rather than an entity. We still need an `iri` column so
        we use the graph IRI from the payload."""
        graph_iri = payload.get("graph_iri")
        if not graph_iri:
            raise EventLogError(
                f"{event_type} payload missing graph_iri"
            )
        return self._emit(
            event_type=event_type, iri=graph_iri,
            domain=payload.get("domain", "control"),
            op="control", payload=payload,
            schema_version=schema_version,
        )

    # ── implementation ────────────────────────────────────

    def _emit(
        self, *, event_type: str, iri: str, domain: str, op: str,
        payload: dict[str, Any], schema_version: int,
    ) -> int:
        validate(event_type, schema_version, payload)
        cur = self._conn.execute(
            """
            INSERT INTO events.entity_events (
                event_type, schema_version, iri, domain, op,
                payload, batch_id, producer
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            RETURNING seq
            """,
            (
                event_type, schema_version, iri, domain, op,
                psycopg.types.json.Jsonb(payload),
                self._batch_id, self._producer,
            ),
        )
        seq = cur.fetchone()[0]
        self._count += 1
        return seq

    def envelope_for(
        self, *, seq: int, event_type: str, iri: str, domain: str,
        op: str, payload: dict[str, Any], schema_version: int = 1,
    ) -> EventEnvelope:
        """Build an EventEnvelope from this batch's metadata. Mostly
        useful for tests."""
        return EventEnvelope(
            event_type=event_type, iri=iri, domain=domain, op=op,
            payload=payload, producer=self._producer,
            schema_version=schema_version, batch_id=self._batch_id,
            seq=seq,
        )
