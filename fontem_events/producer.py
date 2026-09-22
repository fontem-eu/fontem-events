"""Producer side of the event log.

The producer's job:

  * Build envelopes via the typed ``fontem_event_schemas.builders``.
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
import json
import os
import threading
import uuid
from typing import Any, Iterator

import psycopg
from fontem_event_schemas import EventEnvelope, validate

from .errors import EventLogError


_INSERT = """
    INSERT INTO events.entity_events (
        event_type, schema_version, iri, domain, op,
        payload, batch_id, producer
    )
    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
"""


class EventLog:
    """Producer-side handle to ``events.entity_events``."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg.Connection | None = None
        # One connection, one transaction at a time. See batch().
        self._batch_lock = threading.Lock()

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
        *,
        chunk: int = 1,
    ) -> Iterator["EventBatch"]:
        """Open a per-batch emit context.

        All events emitted in the block are inserted in a single
        transaction. If the block raises, nothing lands. On
        clean exit the transaction commits.

        ``chunk`` is how many events travel to Postgres per round trip.
        The default, 1, is one INSERT … RETURNING per event and gives
        the caller its ``seq`` back. A bulk loader should pass something
        like 1000: events are buffered and sent with executemany every
        ``chunk`` events and once more at exit, and the emit methods
        return None. Measured against prod on 2026-09-22, one round trip
        was 18 ms while the hairpin lasted and 1.1 ms after — either way
        a few million single inserts is hours, and GLEIF's 2.7 M
        companies died at their 4-hour deadline for two months. Chunked,
        the same rows cost 0.12 ms each. Ordering, atomicity and
        validation-at-call are unchanged.

        Serialised across threads, because one EventLog holds one
        connection and psycopg's ``transaction()`` is not re-entrant
        across threads on a shared one. Without the lock a thread that
        enters while another's transaction is open gets a SAVEPOINT
        instead of a transaction; the two exit out of order, psycopg
        raises OutOfOrderTransactionNesting, and the abandoned
        subtransactions hold ExclusiveLocks released only when the
        outermost transaction ends — on a long-lived producer, never.

        That took prod's Postgres down on 2026-09-02: one consolidator
        connection reached 1,045 locks, the lock table
        (max_locks_per_transaction 64 x max_connections 100 = 6,400)
        filled, and every new connection got "FATAL: out of shared
        memory". The caller swallowed emit failures, so it looked silent
        while most emits were in fact failing. Reproduced at 8 threads:
        +92 locks and 110 OutOfOrderTransactionNesting errors in 160
        emits, and with the lock removed the suite hangs outright.
        Single-threaded, neither happens — which is why a
        single-threaded test does not catch this.

        The lock makes the cost explicit rather than hiding it: emits
        queue behind one another. If that ever becomes the bottleneck
        the answer is a connection pool, not removing this.
        """
        if chunk < 1:
            raise ValueError("chunk must be >= 1")
        with self._batch_lock:
            conn = self.connect()
            with conn.transaction():
                batch = EventBatch(conn=conn, batch_id=batch_id,
                                   producer=producer, chunk=chunk)
                try:
                    yield batch
                    # Still inside the transaction: what the block
                    # buffered commits with everything else, or not at
                    # all.
                    batch.flush()
                finally:
                    # Whether the block returned or raised, the batch
                    # stops accepting events here. It is an ordinary
                    # object the caller may still hold a name for, and
                    # `flush()` is public — without this, a call after
                    # the block would write into the connection's NEXT
                    # transaction, committed by someone else, with this
                    # batch_id on the rows.
                    batch._close()  # pylint: disable=protected-access


class EventBatch:
    """Per-batch emit helper. Use via ``EventLog.batch(...)``."""

    def __init__(
        self,
        *,
        conn: psycopg.Connection,
        batch_id: uuid.UUID,
        producer: str,
        chunk: int = 1,
    ) -> None:
        self._conn = conn
        self._batch_id = batch_id
        self._producer = producer
        self._chunk = chunk
        self._count = 0
        self._pending: list[tuple] = []
        self._closed = False

    @property
    def count(self) -> int:
        """Events emitted so far, buffered ones included."""
        return self._count

    @property
    def pending(self) -> int:
        """Events buffered and not yet sent (always 0 when chunk == 1)."""
        return len(self._pending)

    def _close(self) -> None:
        """End of the batch. Anything still buffered here was not
        inserted and the transaction is on its way out, so drop it and
        refuse further use rather than write into whatever transaction
        the connection is in next."""
        self._pending = []
        self._closed = True

    def flush(self) -> None:
        """Send the buffered events.

        Called for you every ``chunk`` events and once more when the
        batch ends; call it yourself only if you need the rows visible
        to a query on this connection before the block closes.

        The buffer is dropped only once executemany has returned. Clear
        it first and a failed flush would discard rows that were never
        written — the surrounding transaction aborts either way, but
        "the rows are gone AND the error says nothing about them" is a
        bad way to find that out.
        """
        if self._closed:
            raise EventLogError(
                "this batch is closed — its transaction has already "
                "ended, so anything emitted now would land outside it"
            )
        if not self._pending:
            return
        with self._conn.cursor() as cur:
            cur.executemany(_INSERT, self._pending)
        self._pending = []

    def upsert(
        self, event_type: str, *, iri: str, domain: str,
        payload: dict[str, Any], schema_version: int = 1,
    ) -> int | None:
        """Emit an upsert event for a single entity. Returns the
        seq the row landed at, or None when the batch is chunked
        (the row is sent later, with its neighbours)."""
        return self._emit(
            event_type=event_type, iri=iri, domain=domain, op="upsert",
            payload=payload, schema_version=schema_version,
        )

    def delete(
        self, event_type: str, *, iri: str, domain: str,
        schema_version: int = 1,
    ) -> int | None:
        return self._emit(
            event_type=event_type, iri=iri, domain=domain, op="delete",
            payload={"iri": iri},
            schema_version=schema_version,
        )

    def control(
        self, event_type: str, payload: dict[str, Any],
        *, schema_version: int = 1,
    ) -> int | None:
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
    ) -> int | None:
        if self._closed:
            raise EventLogError(
                f"{event_type} emitted after its batch closed — the "
                "transaction it belonged to has already ended"
            )
        validate(event_type, schema_version, payload)
        row = (
            event_type, schema_version, iri, domain, op,
            # Serialised HERE, not handed over as a dict. psycopg's Jsonb
            # wrapper holds the object by reference and dumps it when the
            # statement executes — which for a chunked batch is up to
            # `chunk` events later. A caller that reuses one dict across a
            # loop, or edits a payload after emitting it, would then store
            # something other than what validate() just approved, and only
            # when chunked. "What was validated is what lands" is worth
            # more than the microsecond, and json.dumps is what psycopg's
            # default dumper calls anyway (verified byte-identical against
            # prod, unicode included).
            json.dumps(payload),
            self._batch_id, self._producer,
        )
        if self._chunk <= 1:
            # Counted after the INSERT, exactly as before chunking existed:
            # `count` means rows accepted by Postgres, so an emit that
            # raises does not inflate it.
            cur = self._conn.execute(_INSERT + " RETURNING seq", row)
            seq = cur.fetchone()[0]
            self._count += 1
            return seq
        self._pending.append(row)
        self._count += 1
        if len(self._pending) >= self._chunk:
            self.flush()
        return None

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
