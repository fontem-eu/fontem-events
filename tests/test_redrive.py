"""Tests for replaying dead-lettered events.

A redrive writes to whatever store the consumer talks to, so the tests
lean on the two properties that make it safe to run: it only touches
rows for the consumer it was asked about, and a batch's rows survive
if the batch fails. Losing a dead-letter row without the event having
been handled would destroy the only surviving record that the event was
ever skipped.
"""
from __future__ import annotations

from dataclasses import dataclass

import psycopg

from fontem_events import redrive_dead_letters


@dataclass
class _Config:  # pylint: disable=too-few-public-methods
    name: str
    dsn: str


class _RecordingConsumer:  # pylint: disable=too-few-public-methods
    """Stands in for a real sink: records what it was handed, and can
    be told to fail."""

    def __init__(self, name: str, dsn: str, fail: bool = False):
        self.config = _Config(name=name, dsn=dsn)
        self.seen: list[int] = []
        self.fail = fail

    def handle(self, batch) -> None:
        """Record the batch, or fail as configured."""
        if self.fail:
            raise RuntimeError("sink still down")
        self.seen.extend(ev.seq for ev in batch)


def _seed(dsn: str, consumer: str, *, event_type="UpsertCompany",
          error="[Errno 111] Connection refused") -> int:
    """Insert one event plus a dead-letter row pointing at it."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events.entity_events
                   (event_type, iri, domain, op, payload, producer)
            VALUES (%s, %s, 'company', 'upsert', '{}'::jsonb, 'test')
            RETURNING seq
            """,
            (event_type, f"http://data.fontem.eu/id/Company/{event_type}"),
        )
        seq = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO events.dead_letter (seq, consumer, error, attempts,
                                            first_failed_at)
            VALUES (%s, %s, %s, 1, now())
            """,
            (seq, consumer, error),
        )
        return seq


def _dead_letter(dsn: str, consumer: str, seq: int) -> dict | None:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT attempts, error FROM events.dead_letter "
            "WHERE consumer = %s AND seq = %s",
            (consumer, seq),
        )
        row = cur.fetchone()
        return {"attempts": row[0], "error": row[1]} if row else None


def test_replays_and_clears_the_row(postgres_dsn):
    """The happy path: the handler is given the event and the row goes."""
    seq = _seed(postgres_dsn, "redrive-ok")
    consumer = _RecordingConsumer("redrive-ok", postgres_dsn)
    result = redrive_dead_letters(consumer)
    assert seq in consumer.seen
    assert result["replayed"] == 1
    assert _dead_letter(postgres_dsn, "redrive-ok", seq) is None


def test_failed_batch_keeps_its_rows(postgres_dsn):
    """The row is the only record that the event was ever skipped —
    deleting it without handling the event would lose that for good."""
    seq = _seed(postgres_dsn, "redrive-down")
    consumer = _RecordingConsumer("redrive-down", postgres_dsn, fail=True)
    result = redrive_dead_letters(consumer)
    row = _dead_letter(postgres_dsn, "redrive-down", seq)
    assert row is not None
    assert result["replayed"] == 0
    assert result["still_failing"] == 1


def test_failed_redrive_is_distinguishable_from_the_original_failure(postgres_dsn):
    """An operator has to be able to tell 'failed again during a
    redrive' from 'failed when it was first processed'."""
    seq = _seed(postgres_dsn, "redrive-marked")
    consumer = _RecordingConsumer("redrive-marked", postgres_dsn, fail=True)
    redrive_dead_letters(consumer)
    row = _dead_letter(postgres_dsn, "redrive-marked", seq)
    assert row["attempts"] == 2
    assert row["error"].startswith("redrive: ")


def test_only_touches_its_own_consumer(postgres_dsn):
    """Four consumers share this table. A redrive of one must not
    replay or delete another's rows."""
    mine = _seed(postgres_dsn, "redrive-mine")
    theirs = _seed(postgres_dsn, "redrive-theirs")
    consumer = _RecordingConsumer("redrive-mine", postgres_dsn)
    redrive_dead_letters(consumer)
    assert consumer.seen == [mine]
    assert _dead_letter(postgres_dsn, "redrive-theirs", theirs) is not None


def test_error_like_selects_one_failure_family(postgres_dsn):
    """Families are redriven separately: replaying one whose cause is
    still unfixed only burns attempt counters."""
    refused = _seed(postgres_dsn, "redrive-family",
                    error="[Errno 111] Connection refused")
    unacceptable = _seed(postgres_dsn, "redrive-family",
                         error="Client error '406 Unacceptable' for url ...")
    consumer = _RecordingConsumer("redrive-family", postgres_dsn)
    result = redrive_dead_letters(consumer, error_like="%Connection refused%")
    assert consumer.seen == [refused]
    assert result["replayed"] == 1
    assert _dead_letter(postgres_dsn, "redrive-family", unacceptable) is not None


def test_dry_run_changes_nothing(postgres_dsn):
    """Operators look before they leap; a dry run must not hand the
    handler anything or clear a row."""
    seq = _seed(postgres_dsn, "redrive-dry")
    consumer = _RecordingConsumer("redrive-dry", postgres_dsn)
    result = redrive_dead_letters(consumer, dry_run=True)
    assert not consumer.seen
    assert result["selected"] == 1
    assert result["replayed"] == 0
    assert _dead_letter(postgres_dsn, "redrive-dry", seq) is not None


def test_one_bad_batch_does_not_abort_the_rest(postgres_dsn):
    """A redrive of thousands of rows must not stop at the first
    unrecoverable one."""
    seqs = [_seed(postgres_dsn, "redrive-partial") for _ in range(4)]

    class _FailsFirstBatch(_RecordingConsumer):  # pylint: disable=too-few-public-methods
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            self.calls = 0

        def handle(self, batch):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("first batch fails")
            self.seen.extend(ev.seq for ev in batch)

    consumer = _FailsFirstBatch("redrive-partial", postgres_dsn)
    result = redrive_dead_letters(consumer, batch_size=1)
    assert result["batches_failed"] == 1
    assert result["replayed"] == 3
    assert sorted(consumer.seen) == sorted(seqs[1:])


def test_no_rows_is_a_clean_noop(postgres_dsn):
    """A consumer with nothing dead-lettered is not an error."""
    consumer = _RecordingConsumer("redrive-empty", postgres_dsn)
    result = redrive_dead_letters(consumer)
    assert result["selected"] == 0
    assert result["replayed"] == 0


def test_limit_caps_the_batch(postgres_dsn):
    """Redriving thousands at once against a live sink is how you cause
    the next outage; the cap is the throttle."""
    for _ in range(5):
        _seed(postgres_dsn, "redrive-limit")
    consumer = _RecordingConsumer("redrive-limit", postgres_dsn)
    result = redrive_dead_letters(consumer, limit=2)
    assert result["selected"] == 2
    assert len(consumer.seen) == 2
