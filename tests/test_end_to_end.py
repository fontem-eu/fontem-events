"""End-to-end test: produce → store → consume → ack.

This is the Phase A gate. If it goes green, the foundation works
and we can build sinks on top.
"""
from __future__ import annotations

import uuid

import psycopg
import pytest
from fontem_event_schemas import builders, EventEnvelope, EventValidationError

from fontem_events import EventConsumer, EventLog
from fontem_events.consumer import ConsumerConfig


SANCTION_GRAPH = "http://data.fontem.eu/graph/sanctions"


# ── producer-side ────────────────────────────────────────────────

def test_emit_validates_payload(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    with pytest.raises(EventValidationError):
        with log.batch(uuid.uuid4(), producer="t") as emit:
            emit.upsert(
                "UpsertSanctionedEntity",
                iri="http://x", domain="sanctions",
                payload={"entity_id": "x"},  # missing eu_reference
            )


def test_emit_inserts_row(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    with log.batch(bid, producer="load_eu_sanctions") as emit:
        emit.control(
            "BeginGraphReplace",
            builders.begin_graph_replace(
                graph_iri=SANCTION_GRAPH, label="SanctionedEntity",
                domain="sanctions",
            ),
        )
        emit.upsert(
            "UpsertSanctionedEntity",
            iri="http://data.fontem.eu/id/Sanction/abc",
            domain="sanctions",
            payload=builders.upsert_sanctioned_entity(
                entity_id="abc", eu_reference="EU.1",
            ),
        )
        emit.control(
            "EndGraphReplace",
            builders.end_graph_replace(graph_iri=SANCTION_GRAPH,
                                       domain="sanctions"),
        )

    with psycopg.connect(postgres_dsn) as c:
        rows = c.execute(
            "SELECT seq, event_type, op, domain, batch_id, producer "
            "FROM events.entity_events ORDER BY seq"
        ).fetchall()
    assert [r[1] for r in rows] == [
        "BeginGraphReplace", "UpsertSanctionedEntity", "EndGraphReplace",
    ]
    assert all(r[3] == "sanctions" for r in rows)
    assert all(r[4] == bid for r in rows)
    assert all(r[5] == "load_eu_sanctions" for r in rows)


def test_emit_rolls_back_on_exception(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    with pytest.raises(RuntimeError):
        with log.batch(uuid.uuid4(), producer="t") as emit:
            emit.upsert(
                "UpsertSanctionedEntity",
                iri="http://x", domain="sanctions",
                payload=builders.upsert_sanctioned_entity(
                    entity_id="abc", eu_reference="EU.1",
                ),
            )
            raise RuntimeError("simulated mid-batch failure")
    with psycopg.connect(postgres_dsn) as c:
        n = c.execute(
            "SELECT count(*) FROM events.entity_events"
        ).fetchone()[0]
    assert n == 0  # nothing landed; batch atomic


# ── consumer-side ────────────────────────────────────────────────

class _CapturingConsumer(EventConsumer):
    """Records every event handed to handle()."""

    def __init__(self, dsn: str, name: str = "test_sink",
                 upstream: str | None = None,
                 max_attempts: int = 5,
                 batch_size: int = 10) -> None:
        super().__init__(ConsumerConfig(
            name=name, dsn=dsn,
            poll_interval_seconds=0.1, batch_size=batch_size,
            max_attempts=max_attempts,
            upstream_consumer=upstream,
            metrics_port=None,
        ))
        self.received: list[EventEnvelope] = []
        self.fail_until: int = 0  # raise on first N batches; 0 = never
        # If set, raise whenever the batch's FIRST event has this seq.
        # Used to simulate a poison event at a specific position.
        self.poison_first_seq: int | None = None

    def handle(self, batch: list[EventEnvelope]) -> None:
        if self.fail_until > 0:
            self.fail_until -= 1
            raise RuntimeError("simulated handler failure")
        if (self.poison_first_seq is not None
                and batch and batch[0].seq == self.poison_first_seq):
            raise RuntimeError("simulated poison event")
        self.received.extend(batch)


def _emit_three(postgres_dsn) -> uuid.UUID:
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    with log.batch(bid, producer="t") as emit:
        for i in range(3):
            emit.upsert(
                "UpsertSanctionedEntity",
                iri=f"http://data.fontem.eu/id/Sanction/{i}",
                domain="sanctions",
                payload=builders.upsert_sanctioned_entity(
                    entity_id=str(i), eu_reference=f"EU.{i}",
                ),
            )
    return bid


def test_consume_advances_offset(postgres_dsn) -> None:
    _emit_three(postgres_dsn)
    sink = _CapturingConsumer(postgres_dsn, name="virtuoso_sink")
    n = sink.run_once()
    assert n == 3
    assert len(sink.received) == 3
    # second run is a no-op (offset advanced past everything)
    assert sink.run_once() == 0


def test_consume_dlq_on_handler_failure(postgres_dsn) -> None:
    _emit_three(postgres_dsn)
    sink = _CapturingConsumer(postgres_dsn, name="virtuoso_sink")
    sink.fail_until = 1
    with pytest.raises(RuntimeError):
        sink.run_once()
    with psycopg.connect(postgres_dsn) as c:
        n = c.execute(
            "SELECT count(*) FROM events.dead_letter "
            "WHERE consumer='virtuoso_sink'"
        ).fetchone()[0]
    assert n == 3


def test_poison_event_is_skipped_after_max_attempts(postgres_dsn) -> None:
    """Regression test for the 2026-06-09 virtuoso_sink jam.

    A handler that always raises on a specific seq must, after
    ``max_attempts`` consecutive failures at that same first-seq,
    advance past the poison and let the rest of the batch through.
    The dead_letter row at that seq stays as the operator-facing
    record of what was skipped.

    Before this behaviour, virtuoso_sink retried the same eu-cohesion
    Disclosure batch 15,727 times over 66 hours and never advanced.
    """
    _emit_three(postgres_dsn)

    with psycopg.connect(postgres_dsn) as c:
        seqs = [r[0] for r in c.execute(
            "SELECT seq FROM events.entity_events ORDER BY seq"
        ).fetchall()]
    assert len(seqs) == 3

    sink = _CapturingConsumer(
        postgres_dsn, name="virtuoso_sink",
        max_attempts=3, batch_size=10,
    )
    # First event of the next fetched batch is the poison.
    sink.poison_first_seq = seqs[0]

    # Failure 1, 2: handler raises, dead_letter accumulates, offset
    # unchanged.
    for _attempt in range(sink.config.max_attempts - 1):
        with pytest.raises(RuntimeError):
            sink.run_once()
    with psycopg.connect(postgres_dsn) as c:
        attempts = c.execute(
            "SELECT max(attempts) FROM events.dead_letter "
            "WHERE consumer='virtuoso_sink'"
        ).fetchone()[0]
        offset = c.execute(
            "SELECT last_seq FROM events.consumer_offsets "
            "WHERE consumer_name='virtuoso_sink'"
        ).fetchone()
    assert attempts == sink.config.max_attempts - 1
    assert offset is None  # offset never set; queue still blocked

    # Failure max_attempts: this time the consumer treats it as
    # poison and advances. No raise. The two non-poison events get
    # processed on the next iteration.
    skipped = sink.run_once()
    assert skipped == 1  # one event skipped, no sleep
    assert not sink.received  # we didn't process anything yet

    # The poison event was at seqs[0]; offset must now sit at seqs[0]
    # so the next fetch starts at seqs[1].
    with psycopg.connect(postgres_dsn) as c:
        offset = c.execute(
            "SELECT last_seq FROM events.consumer_offsets "
            "WHERE consumer_name='virtuoso_sink'"
        ).fetchone()[0]
    assert offset == seqs[0]

    # The poison's dead_letter row stays — it's the operator's record.
    with psycopg.connect(postgres_dsn) as c:
        row = c.execute(
            "SELECT attempts FROM events.dead_letter "
            "WHERE consumer='virtuoso_sink' AND seq=%s",
            (seqs[0],),
        ).fetchone()
    assert row is not None
    assert row[0] >= sink.config.max_attempts

    # Now the consumer drains the rest cleanly.
    sink.poison_first_seq = None
    n = sink.run_once()
    assert n == 2
    assert [e.seq for e in sink.received] == seqs[1:]


def test_transient_failures_at_different_seqs_do_not_count_as_poison(
    postgres_dsn,
) -> None:
    """If consecutive failures happen at different first-seqs (because
    the offset advanced between them), the poison counter must reset.
    Otherwise unrelated transient errors over time would silently skip
    healthy events."""
    _emit_three(postgres_dsn)

    sink = _CapturingConsumer(
        postgres_dsn, name="virtuoso_sink",
        max_attempts=2, batch_size=1,
    )

    # Fail at seq #1, then succeed (advance), then fail at seq #2 and
    # succeed. With independent first-seqs, the counter resets each
    # time — no skip.
    sink.fail_until = 1
    with pytest.raises(RuntimeError):
        sink.run_once()
    sink.run_once()  # seq #1 processed
    sink.fail_until = 1
    with pytest.raises(RuntimeError):
        sink.run_once()
    sink.run_once()  # seq #2 processed
    sink.run_once()  # seq #3 processed

    assert len(sink.received) == 3, (
        "all three events should be processed; no false poison-skip"
    )


def test_success_resets_failure_counter(postgres_dsn) -> None:
    """A successful batch must reset the consecutive-failure counter so
    a single transient failure later doesn't carry forward."""
    _emit_three(postgres_dsn)

    sink = _CapturingConsumer(
        postgres_dsn, name="virtuoso_sink",
        max_attempts=2, batch_size=1,
    )
    # One failure (counter=1), success (resets), one more failure
    # (counter=1 again, NOT 2), success.
    sink.fail_until = 1
    with pytest.raises(RuntimeError):
        sink.run_once()
    sink.run_once()  # success → counter resets

    # If the counter had carried forward, this next failure would
    # already trip max_attempts=2 and skip. It must not.
    sink.fail_until = 1
    with pytest.raises(RuntimeError):
        sink.run_once()

    # Final success processes the remaining events.
    while sink.run_once() > 0:
        pass
    assert len(sink.received) == 3


def test_high_watermark_gating(postgres_dsn) -> None:
    """Downstream consumer never gets past upstream's offset."""
    _emit_three(postgres_dsn)

    upstream = _CapturingConsumer(postgres_dsn, name="neo4j_sink")
    downstream = _CapturingConsumer(postgres_dsn, name="consolidator",
                                    upstream="neo4j_sink")

    # Upstream hasn't run yet → downstream sees no events even
    # though the queue has 3.
    assert downstream.run_once() == 0

    # Upstream picks up events 1 & 2 only (artificially small batch).
    upstream.config.batch_size = 2
    assert upstream.run_once() == 2

    # Downstream now sees only those 2.
    assert downstream.run_once() == 2

    # Upstream finishes; downstream catches up.
    upstream.config.batch_size = 10
    upstream.run_once()
    downstream.run_once()
    assert len(downstream.received) == 3
