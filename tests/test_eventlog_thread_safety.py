"""One EventLog holds one connection; concurrent batches must not corrupt it.

psycopg's `transaction()` is not re-entrant across threads on a shared
connection. A thread entering while another's transaction is open gets a
SAVEPOINT rather than a transaction; the two then exit out of order,
psycopg raises OutOfOrderTransactionNesting, and the abandoned
subtransactions hold ExclusiveLocks released only when the outermost
transaction ends — on a long-lived producer, never.

That took prod's Postgres down on 2026-09-02: one consolidator connection
reached 1,045 locks, the lock table (max_locks_per_transaction 64 x
max_connections 100 = 6,400) filled, and every new connection got
"FATAL: out of shared memory". The consolidator swallows emit failures, so
it looked silent while most emits were actually failing.

These tests drive the shared EventLog from several threads at once, which
is exactly how the consolidator uses it (asyncio.to_thread per emit).
"""
from __future__ import annotations

import threading
import uuid

import psycopg
from fontem_event_schemas import builders

from fontem_events import EventLog

_THREADS = 8
_EMITS = 160


def _locks_held(dsn: str, pid: int) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_locks WHERE pid = %s", (pid,))
        return cur.fetchone()[0]


def _emit(log: EventLog, tag: str, i: int) -> None:
    with log.batch(uuid.uuid4(), producer="thread-safety-test") as b:
        b.upsert(
            "UpsertSanctionedEntity",
            iri=f"http://data.fontem.eu/id/Sanction/{tag}-{i}",
            domain="sanctions",
            payload=builders.upsert_sanctioned_entity(
                entity_id=f"{tag}-{i}", eu_reference=f"EU.{tag}.{i}"),
        )


def _hammer(log: EventLog, tag: str) -> list[Exception]:
    errors: list[Exception] = []

    def worker(lo: int, hi: int) -> None:
        for i in range(lo, hi):
            try:
                _emit(log, tag, i)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                errors.append(exc)

    per = _EMITS // _THREADS
    threads = [
        threading.Thread(target=worker, args=(k * per, (k + 1) * per))
        for k in range(_THREADS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


def test_concurrent_batches_do_not_leak_locks(postgres_dsn):
    """The regression that filled prod's lock table.

    Asserts the property (locks stay flat) rather than the mechanism, so a
    future change that reintroduces the leak another way still fails.
    """
    tag = f"leak{uuid.uuid4().hex[:6]}"
    log = EventLog(postgres_dsn)
    pid = log.connect().info.backend_pid
    _emit(log, tag, -1)
    baseline = _locks_held(postgres_dsn, pid)

    _hammer(log, tag)
    after = _locks_held(postgres_dsn, pid)
    log.close()

    assert after <= baseline + 2, (
        f"locks grew {baseline} -> {after} across {_EMITS} concurrent emits; "
        "the producer is holding subtransaction locks again"
    )


def test_concurrent_batches_do_not_error(postgres_dsn):
    """Unserialised access raised OutOfOrderTransactionNesting on most
    emits. The consolidator swallows emit failures, so this surfaced as
    silence rather than as an error — assert it directly."""
    tag = f"err{uuid.uuid4().hex[:6]}"
    log = EventLog(postgres_dsn)
    errors = _hammer(log, tag)
    log.close()
    assert not errors, f"{len(errors)} concurrent emits failed: {errors[:3]}"


def test_every_concurrent_event_actually_lands(postgres_dsn):
    """The leak's quieter half: events that raised were never written.
    All of them must be durable, not merely most."""
    tag = f"land{uuid.uuid4().hex[:6]}"
    log = EventLog(postgres_dsn)
    _hammer(log, tag)
    log.close()
    with psycopg.connect(postgres_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM events.entity_events WHERE iri LIKE %s",
            (f"http://data.fontem.eu/id/Sanction/{tag}-%",),
        )
        assert cur.fetchone()[0] == _EMITS
