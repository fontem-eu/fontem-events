"""``log.batch(..., chunk=N)``: bulk loaders send N events per round trip
instead of one INSERT ... RETURNING each.

What has to hold: the same rows land, once, in emit order, with every
column intact; the batch is still all-or-nothing; validation still fails
at the call; and the default (unchunked) path behaves exactly as it did
before chunking existed.
"""
from __future__ import annotations

import uuid

import psycopg
import pytest
from fontem_event_schemas import EventValidationError, builders

from fontem_events import EventLog
from fontem_events.errors import EventLogError

GRAPH = "http://data.fontem.eu/graph/sanctions"


def _payload(i: int) -> dict:
    return builders.upsert_sanctioned_entity(entity_id=f"e{i}", eu_reference=f"EU.{i}")


def _rows(dsn: str, bid: uuid.UUID) -> list[tuple]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT seq, event_type, iri, op FROM events.entity_events "
            "WHERE batch_id = %s ORDER BY seq", (bid,)).fetchall()


# ── the rows that land ────────────────────────────────────────────

def test_chunked_batch_lands_every_event_once_in_emit_order(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    with log.batch(bid, producer="t", chunk=7) as emit:
        emit.control("BeginGraphReplace", builders.begin_graph_replace(
            graph_iri=GRAPH, label="SanctionedEntity", domain="sanctions"))
        for i in range(20):        # 2 full chunks of 7, then 6 flushed at exit
            assert emit.upsert("UpsertSanctionedEntity", iri=f"http://x/{i}",
                               domain="sanctions", payload=_payload(i)) is None
        emit.control("EndGraphReplace",
                     builders.end_graph_replace(graph_iri=GRAPH, domain="sanctions"))
        assert emit.count == 22

    rows = _rows(postgres_dsn, bid)
    # Exactly 22 — a flush that ran twice, or a buffer that was not cleared,
    # would show up here and nowhere else.
    assert len(rows) == 22
    # Emit order, by content: seq is what the query sorts on, so asserting
    # that seq ascends proves nothing about the order the rows went in.
    assert [r[1] for r in rows] == (
        ["BeginGraphReplace"] + ["UpsertSanctionedEntity"] * 20 + ["EndGraphReplace"])
    assert [r[2] for r in rows[1:21]] == [f"http://x/{i}" for i in range(20)]


def test_every_column_survives_the_chunked_path(postgres_dsn) -> None:
    """The chunked path binds its own parameters. A column bound to the
    wrong position still inserts 22 rows of the right shape, so only
    reading the values back catches it."""
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    rich = builders.upsert_sanctioned_entity(
        entity_id="e7", eu_reference="EU.7",
        name="Café naïve — Россия",
        aliases=["first", "second", ""],
        nationality=None,
        designation_date="2014-07-31",
        listing_reason='Multi-line\nreason with a "quote" and a backslash \\',
    )
    with log.batch(bid, producer="load_eu_sanctions", chunk=3) as emit:
        emit.upsert("UpsertSanctionedEntity", iri="http://x/rich",
                    domain="sanctions", payload=rich, schema_version=1)

    with psycopg.connect(postgres_dsn) as conn:
        row = conn.execute(
            "SELECT payload, event_type, iri, domain, op, schema_version, producer "
            "FROM events.entity_events WHERE batch_id = %s", (bid,)).fetchone()
    assert row[0] == rich
    assert row[1:] == ("UpsertSanctionedEntity", "http://x/rich", "sanctions",
                       "upsert", 1, "load_eu_sanctions")


def test_a_payload_edited_after_emit_is_not_what_lands(postgres_dsn) -> None:
    """What validate() approved at the call is what reaches Postgres.

    A chunked row waits in the buffer until its chunk is sent, so a
    payload held by reference would be serialised in whatever state the
    caller left it — silently, and only when chunked.
    """
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    payload = _payload(1)
    with log.batch(bid, producer="t", chunk=100) as emit:
        emit.upsert("UpsertSanctionedEntity", iri="http://x/1",
                    domain="sanctions", payload=payload)
        payload["eu_reference"] = "EU.tampered"   # a dict reused by the next loop pass
        payload["entity_id"] = "tampered"

    with psycopg.connect(postgres_dsn) as conn:
        stored = conn.execute(
            "SELECT payload FROM events.entity_events WHERE batch_id = %s",
            (bid,)).fetchone()[0]
    assert stored["eu_reference"] == "EU.1" and stored["entity_id"] == "e1"


# ── all-or-nothing ────────────────────────────────────────────────

def test_an_exception_after_a_flush_still_rolls_everything_back(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    with pytest.raises(RuntimeError):
        with log.batch(bid, producer="t", chunk=5) as emit:
            for i in range(12):    # two chunks already sent to Postgres
                emit.upsert("UpsertSanctionedEntity", iri=f"http://x/{i}",
                            domain="sanctions", payload=_payload(i))
            assert emit.pending == 2
            raise RuntimeError("mid-batch failure")
    assert _rows(postgres_dsn, bid) == []


def test_a_database_error_inside_a_flush_surfaces_and_nothing_lands(postgres_dsn) -> None:
    """entity_events.iri is NOT NULL, and nothing in validate() looks at
    the iri — so this reaches Postgres and fails there, which is the one
    way a flush breaks that the caller cannot see coming."""
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    with pytest.raises(psycopg.errors.NotNullViolation):
        with log.batch(bid, producer="t", chunk=4) as emit:
            emit.upsert("UpsertSanctionedEntity", iri="http://x/ok",
                        domain="sanctions", payload=_payload(1))
            emit.upsert("UpsertSanctionedEntity", iri=None,
                        domain="sanctions", payload=_payload(2))
            emit.upsert("UpsertSanctionedEntity", iri="http://x/3",
                        domain="sanctions", payload=_payload(3))
            emit.upsert("UpsertSanctionedEntity", iri="http://x/4",
                        domain="sanctions", payload=_payload(4))
    assert _rows(postgres_dsn, bid) == []


def test_validation_fails_at_the_call_not_at_flush(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    with pytest.raises(EventValidationError):
        with log.batch(bid, producer="t", chunk=100) as emit:
            emit.upsert("UpsertSanctionedEntity", iri="http://x/ok",
                        domain="sanctions", payload=_payload(1))
            emit.upsert("UpsertSanctionedEntity", iri="http://x/bad",
                        domain="sanctions", payload={"entity_id": "x"})
    assert _rows(postgres_dsn, bid) == []


# ── the buffer ────────────────────────────────────────────────────

def test_events_wait_in_the_buffer_until_their_chunk_is_full(postgres_dsn) -> None:
    """The point of chunking, stated as behaviour rather than as a stopwatch:
    rows that have not been sent are still pending. `pending` returning to 0
    at each boundary is a round trip carrying the whole chunk."""
    log = EventLog(postgres_dsn)
    seen = []
    with log.batch(uuid.uuid4(), producer="t", chunk=5) as emit:
        for i in range(10):
            emit.upsert("UpsertSanctionedEntity", iri=f"http://b/{i}",
                        domain="sanctions", payload=_payload(i))
            seen.append(emit.pending)
    assert seen == [1, 2, 3, 4, 0, 1, 2, 3, 4, 0]


def test_flush_makes_buffered_rows_visible_on_the_same_connection(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    with log.batch(bid, producer="t", chunk=50) as emit:
        emit.upsert("UpsertSanctionedEntity", iri="http://x/1",
                    domain="sanctions", payload=_payload(1))
        assert emit.pending == 1
        emit.flush()
        assert emit.pending == 0
        # Same connection the batch holds, reached the public way.
        n = log.connect().execute(
            "SELECT count(*) FROM events.entity_events WHERE batch_id = %s",
            (bid,)).fetchone()[0]
        assert n == 1


def test_a_batch_cannot_be_used_after_its_block_ends(postgres_dsn) -> None:
    """EventBatch is an ordinary object and flush() is public, so a
    caller can hold one past its `with`. Its transaction is gone by
    then; emitting would land in whatever transaction the connection
    joins next, tagged with this batch_id."""
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    with log.batch(bid, producer="t", chunk=10) as emit:
        emit.upsert("UpsertSanctionedEntity", iri="http://x/1",
                    domain="sanctions", payload=_payload(1))
    escaped = emit
    with pytest.raises(EventLogError):
        escaped.upsert("UpsertSanctionedEntity", iri="http://x/2",
                       domain="sanctions", payload=_payload(2))
    with pytest.raises(EventLogError):
        escaped.flush()
    assert len(_rows(postgres_dsn, bid)) == 1


# ── the unchunked path is unchanged ───────────────────────────────

def test_unchunked_batch_returns_the_seq_the_row_actually_landed_at(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    bid = uuid.uuid4()
    with log.batch(bid, producer="t") as emit:
        seq = emit.upsert("UpsertSanctionedEntity", iri="http://x/1",
                          domain="sanctions", payload=_payload(1))
        assert emit.pending == 0
    rows = _rows(postgres_dsn, bid)
    assert [r[0] for r in rows] == [seq]


def test_a_payload_rejected_by_validation_is_not_counted(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    for chunk in (1, 50):
        with log.batch(uuid.uuid4(), producer="t", chunk=chunk) as emit:
            emit.upsert("UpsertSanctionedEntity", iri="http://x/1",
                        domain="sanctions", payload=_payload(1))
            with pytest.raises(EventValidationError):
                emit.upsert("UpsertSanctionedEntity", iri="http://x/2",
                            domain="sanctions", payload={"nope": True})
            assert emit.count == 1, chunk


def test_a_row_postgres_rejects_is_not_counted(postgres_dsn) -> None:
    """`count` means rows Postgres accepted, and always has. A NULL iri
    passes validate() — nothing in the schema describes the iri — and
    fails at the INSERT, which is the only way an unchunked emit gets
    that far and still fails."""
    escaped = {}
    log = EventLog(postgres_dsn)
    with pytest.raises(psycopg.errors.NotNullViolation):
        with log.batch(uuid.uuid4(), producer="t") as emit:
            escaped["batch"] = emit
            emit.upsert("UpsertSanctionedEntity", iri="http://x/1",
                        domain="sanctions", payload=_payload(1))
            emit.upsert("UpsertSanctionedEntity", iri=None,
                        domain="sanctions", payload=_payload(2))
    assert escaped["batch"].count == 1


def test_a_failed_flush_does_not_quietly_drop_the_rows_it_did_not_write(
        postgres_dsn) -> None:
    """Whatever the buffer still holds was not written. Emptying it
    before executemany returns would leave a caller inspecting the batch
    in its handler told that everything went out."""
    log = EventLog(postgres_dsn)
    with pytest.raises(RuntimeError):
        with log.batch(uuid.uuid4(), producer="t", chunk=2) as emit:
            emit.upsert("UpsertSanctionedEntity", iri="http://x/1",
                        domain="sanctions", payload=_payload(1))
            with pytest.raises(psycopg.errors.NotNullViolation):
                # Second of the chunk, so this emit triggers the flush.
                emit.upsert("UpsertSanctionedEntity", iri=None,
                            domain="sanctions", payload=_payload(2))
            assert emit.pending == 2
            raise RuntimeError("the transaction is aborted; stop here")


def test_chunk_must_be_positive(postgres_dsn) -> None:
    log = EventLog(postgres_dsn)
    with pytest.raises(ValueError):
        with log.batch(uuid.uuid4(), producer="t", chunk=0):
            pass
