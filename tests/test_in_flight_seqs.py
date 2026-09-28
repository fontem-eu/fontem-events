"""A consumer never steps over a seq that is still in flight.

A producer takes its seq when it inserts and the row becomes visible
when it commits, so a later seq can be committed while an earlier one
is still open. Committing past it lost 9,567 of 18,957 Kohesio
projects in prod on 2026-09-28. These use two real connections to
make exactly that interleaving.
"""
from __future__ import annotations

import json
import logging

import psycopg
import pytest
from fontem_event_schemas import EventEnvelope

from fontem_events import EventConsumer
from fontem_events.consumer import ConsumerConfig


class _Sink(EventConsumer):
    def __init__(self, dsn: str, **gap_waits) -> None:
        super().__init__(ConsumerConfig(
            name="gap_sink", dsn=dsn, poll_interval_seconds=0.1,
            batch_size=50, metrics_port=None,
            gap_min_wait_seconds=gap_waits.get("min_wait", 0.0),
            gap_max_wait_seconds=gap_waits.get("max_wait", 1800.0),
        ))
        self.received: list[EventEnvelope] = []

    def handle(self, batch: list[EventEnvelope]) -> None:
        self.received.extend(batch)

    @property
    def seqs(self) -> list[int]:
        return [e.seq for e in self.received]


def _insert(conn: psycopg.Connection, n: int) -> int:
    return conn.execute(
        "INSERT INTO events.entity_events "
        "(event_type, schema_version, iri, domain, op, payload, producer) "
        "VALUES ('UpsertSanctionedEntity', 1, %s, 'sanctions', 'upsert', %s::jsonb, 't') "
        "RETURNING seq",
        (f"http://data.fontem.eu/id/Sanction/{n}", json.dumps({"entity_id": str(n)})),
    ).fetchone()[0]


@pytest.fixture(name="conns")
def _conns(postgres_dsn):
    """A slow producer (commits when told) and a fast one (autocommit)."""
    slow = psycopg.connect(postgres_dsn)
    fast = psycopg.connect(postgres_dsn, autocommit=True)
    yield slow, fast
    slow.close()
    fast.close()


def test_an_earlier_seq_still_in_flight_is_waited_for_not_stepped_over(
        postgres_dsn, conns) -> None:
    slow, fast = conns
    early = _insert(slow, 1)
    late = _insert(fast, 2)
    assert early < late
    sink = _Sink(postgres_dsn)
    assert sink.run_once() == 0
    assert sink.run_once() == 0          # still open: still waiting
    slow.commit()
    assert sink.run_once() == 2
    assert sink.seqs == [early, late]


def test_the_committed_run_before_a_gap_is_consumed(postgres_dsn, conns) -> None:
    slow, fast = conns
    first = _insert(fast, 1)
    held = _insert(slow, 2)
    last = _insert(fast, 3)
    sink = _Sink(postgres_dsn)
    assert sink.run_once() == 1 and sink.seqs == [first]
    assert sink.run_once() == 0
    slow.commit()
    assert sink.run_once() == 2 and sink.seqs == [first, held, last]


def test_a_rolled_back_seq_is_passed_once_its_transaction_has_ended(
        postgres_dsn, conns, caplog) -> None:
    slow, fast = conns
    _insert(slow, 1)
    slow.rollback()
    kept = _insert(fast, 2)
    sink = _Sink(postgres_dsn)
    caplog.set_level(logging.INFO)
    assert sink.run_once() == 0          # first sight: take the horizon
    assert sink.run_once() == 1 and sink.seqs == [kept]
    assert "never committed" in caplog.text


def test_a_gap_is_not_judged_before_the_minimum_wait(postgres_dsn, conns, monkeypatch) -> None:
    """A producer is running a moment before it has a transaction id; the
    horizon is taken only once every holder of the seq must have one."""
    slow, fast = conns
    _insert(slow, 1)
    slow.rollback()
    _insert(fast, 2)
    clock = {"t": 1000.0}
    monkeypatch.setattr("fontem_events.consumer.time.monotonic", lambda: clock["t"])
    sink = _Sink(postgres_dsn, min_wait=5.0)
    assert sink.run_once() == 0
    clock["t"] += 4.0
    assert sink.run_once() == 0          # too soon to take the horizon
    clock["t"] += 1.0
    assert sink.run_once() == 0          # horizon taken now
    assert sink.run_once() == 1


def test_a_seq_open_past_the_maximum_wait_is_passed_loudly(
        postgres_dsn, conns, caplog) -> None:
    """A session left idle in a transaction must not stall every consumer
    for ever; passing its seq is logged as the loss it is.

    With no wait at all this is how every consumer behaved before: the
    seq committed afterwards is never read.
    """
    slow, fast = conns
    lost = _insert(slow, 1)
    kept = _insert(fast, 2)
    sink = _Sink(postgres_dsn, max_wait=0.0)
    assert sink.run_once() == 1 and sink.seqs == [kept]
    assert any(r.levelno == logging.ERROR and "still missing" in r.getMessage()
               for r in caplog.records)
    slow.commit()
    assert sink.run_once() == 0 and lost not in sink.seqs


def test_the_offset_never_passes_an_open_seq(postgres_dsn, conns) -> None:
    slow, fast = conns
    held = _insert(slow, 1)
    for n in range(2, 6):
        _insert(fast, n)
    sink = _Sink(postgres_dsn)
    for _ in range(3):
        sink.run_once()
    with psycopg.connect(postgres_dsn) as c:
        row = c.execute("SELECT last_seq FROM events.consumer_offsets "
                        "WHERE consumer_name = 'gap_sink'").fetchone()
    assert row is None or row[0] < held
    slow.commit()
