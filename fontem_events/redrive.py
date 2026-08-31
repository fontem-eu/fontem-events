"""Replay dead-lettered events back through a consumer's handler.

When a batch fails ``max_attempts`` times the consumer treats it as
poison: it advances the offset past the event and records a row in
``events.dead_letter``. That keeps one bad event from jamming the queue
forever, but it also means the event is *skipped* — the offset has
moved on and nothing will ever come back for it on its own.

Nothing does come back. Sampling the 1,008 ``neo4j_sink`` rows from the
2026-08-16 transaction-timeout burst, none of the ``Filing`` nodes they
carried exist in the graph today: the loaders use ``--since`` windows,
so a later run re-emits new filings, not the ones that were dropped.
Those events are lost until something replays them, and the
dead-letter row is the only surviving record that they existed.

Most dead letters are worth replaying because most are transient — a
sink that was restarting, a DNS blip, a slow transaction. They failed
on infrastructure, not on their content, so the same event handled
again simply succeeds. That is what this does: re-read the events named
by the dead-letter rows, hand them to the consumer's own ``handle()``,
and delete the rows that go through.

Deliberately not automatic. A redrive against a still-broken sink just
burns the rows' attempt counters and relogs the same failure, so this
is an operator action taken once the underlying cause is fixed. Filter
with ``error_like`` to replay one failure family at a time, and use
``dry_run`` to see what would be attempted first.

Handlers must be idempotent for this to be safe, which is already
required of them — the consumer commits its offset after ``handle()``,
so a crash in between redelivers the same batch on restart. Replaying a
dead letter is the same redelivery by another route.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import psycopg
from fontem_event_schemas import EventEnvelope

if TYPE_CHECKING:  # pragma: no cover
    from .consumer import EventConsumer

logger = logging.getLogger(__name__)


# Keep the failure note short and prefixed, so a row that fails again
# during a redrive is distinguishable from its original failure.
_REDRIVE_PREFIX = "redrive: "

_SELECT_SQL = """
SELECT d.seq, e.ts, e.event_type, e.schema_version, e.iri, e.domain,
       e.op, e.payload, e.batch_id, e.producer
  FROM events.dead_letter d
  JOIN events.entity_events e USING (seq)
 WHERE d.consumer = %(consumer)s
   AND (%(error_like)s::text IS NULL OR d.error LIKE %(error_like)s::text)
 ORDER BY d.seq
 LIMIT %(limit)s
"""

_DELETE_SQL = """
DELETE FROM events.dead_letter
 WHERE consumer = %(consumer)s AND seq = ANY(%(seqs)s)
"""

_MARK_FAILED_SQL = """
UPDATE events.dead_letter
   SET attempts = attempts + 1,
       error    = %(error)s
 WHERE consumer = %(consumer)s AND seq = ANY(%(seqs)s)
"""


def redrive_dead_letters(
    consumer: "EventConsumer",
    *,
    error_like: str | None = None,
    limit: int = 1000,
    batch_size: int = 50,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Replay this consumer's dead letters through its own handler.

    ``error_like`` is a SQL LIKE pattern against the recorded error, so
    one failure family can be replayed at a time — replaying a family
    whose cause is still unfixed only burns attempt counters.

    Rows are replayed in ``seq`` order and in batches, and a batch is
    deleted only once ``handle()`` returns for it. A batch that raises
    leaves its rows in place with ``attempts`` incremented and the error
    recorded under a ``redrive:`` prefix, so a row that fails again here
    is distinguishable from its original failure.

    Returns counts of what was replayed, kept and skipped.
    """
    dsn = consumer.config.dsn
    name = consumer.config.name
    events = _load(dsn, name, error_like=error_like, limit=limit)

    result: dict[str, Any] = {
        "consumer": name,
        "selected": len(events),
        "replayed": 0,
        "still_failing": 0,
        "batches_failed": 0,
        "dry_run": dry_run,
    }
    if dry_run or not events:
        return result

    for start in range(0, len(events), batch_size):
        chunk = events[start:start + batch_size]
        seqs = [ev.seq for ev in chunk]
        try:
            consumer.handle(chunk)
        # The point of a redrive is to survive the rows that are still
        # broken and keep going, so a failing batch is recorded rather
        # than allowed to abort the remaining ones.
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(
                "%s: redrive batch seq=%s..%s still failing: %s",
                name, seqs[0], seqs[-1], exc,
            )
            result["still_failing"] += len(chunk)
            result["batches_failed"] += 1
            _mark_failed(dsn, name, seqs, exc)
            continue
        _delete(dsn, name, seqs)
        result["replayed"] += len(chunk)
        logger.info(
            "%s: redrove %d event(s) seq=%s..%s",
            name, len(chunk), seqs[0], seqs[-1],
        )

    return result


def _load(
    dsn: str, consumer: str, *, error_like: str | None, limit: int,
) -> list[EventEnvelope]:
    """Read the events named by this consumer's dead-letter rows."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            _SELECT_SQL,
            {"consumer": consumer, "error_like": error_like, "limit": limit},
        )
        rows = cur.fetchall()
    return [
        EventEnvelope(
            seq=r[0], ts=r[1], event_type=r[2], schema_version=r[3],
            iri=r[4], domain=r[5], op=r[6], payload=r[7],
            batch_id=r[8], producer=r[9],
        )
        for r in rows
    ]


def _delete(dsn: str, consumer: str, seqs: list[int]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_DELETE_SQL, {"consumer": consumer, "seqs": seqs})


def _mark_failed(dsn: str, consumer: str, seqs: list[int], exc: Exception) -> None:
    # Truncated for the same reason RunLog truncates: the table is for
    # triage, the full trace belongs in the pod logs.
    message = f"{_REDRIVE_PREFIX}{exc}"[:2000]
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            _MARK_FAILED_SQL,
            {"consumer": consumer, "seqs": seqs, "error": message},
        )
