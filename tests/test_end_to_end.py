"""End-to-end test: produce → store → consume → ack.

This is the Phase A gate. If it goes green, the foundation works
and we can build sinks on top.
"""
from __future__ import annotations

import uuid

import pytest
from gmr_event_schemas import builders, EventEnvelope, EventValidationError

from gmr_events import EventConsumer, EventLog
from gmr_events.consumer import ConsumerConfig


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

    import psycopg
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
    import psycopg
    with psycopg.connect(postgres_dsn) as c:
        n = c.execute(
            "SELECT count(*) FROM events.entity_events"
        ).fetchone()[0]
    assert n == 0  # nothing landed; batch atomic


# ── consumer-side ────────────────────────────────────────────────

class _CapturingConsumer(EventConsumer):
    """Records every event handed to handle()."""

    def __init__(self, dsn: str, name: str = "test_sink",
                 upstream: str | None = None) -> None:
        super().__init__(ConsumerConfig(
            name=name, dsn=dsn,
            poll_interval_seconds=0.1, batch_size=10,
            upstream_consumer=upstream,
            metrics_port=None,
        ))
        self.received: list[EventEnvelope] = []
        self.fail_until: int = 0  # raise on first N batches; 0 = never

    def handle(self, batch: list[EventEnvelope]) -> None:
        if self.fail_until > 0:
            self.fail_until -= 1
            raise RuntimeError("simulated handler failure")
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
    import psycopg
    with psycopg.connect(postgres_dsn) as c:
        n = c.execute(
            "SELECT count(*) FROM events.dead_letter "
            "WHERE consumer='virtuoso_sink'"
        ).fetchone()[0]
    assert n == 3


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
