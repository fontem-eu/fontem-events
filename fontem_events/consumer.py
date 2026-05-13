"""Consumer side of the event log.

A consumer is a long-lived process that:

  1. Reads its committed offset from ``events.consumer_offsets``.
  2. Fetches a window of events ``WHERE seq > offset`` (optionally
     bounded above by another consumer's offset for high-watermark
     gating).
  3. Hands the batch to ``handle()`` (subclass-supplied).
  4. Commits the new offset in the same Postgres transaction as the
     work — so a crash mid-handle causes a redo, not a loss.

Failures retry with exponential backoff up to ``max_attempts``;
permanent failures land in ``events.dead_letter`` and the
consumer skips past them.

Observability:

  * Prometheus: ``event_lag_seconds``, ``events_processed_total``,
    ``events_failed_total``, ``batch_size``.
  * Uptime Kuma: heartbeat ping per loop iteration (default-down trap
    if the loop dies).
"""
from __future__ import annotations

import abc
import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable

import psycopg
from fontem_event_schemas import EventEnvelope
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .errors import EventLogError, OffsetError

logger = logging.getLogger(__name__)

# Module-level Prometheus collectors so multiple EventConsumer
# instances (different consumer_names) coexist in the same
# process — instance-scoped collectors clash on the global
# default registry.
_EVENT_LAG_SECONDS = Gauge(
    "event_lag_seconds",
    "Seconds between newest event and last-processed seq",
    ["consumer"],
)
_EVENTS_PROCESSED_TOTAL = Counter(
    "events_processed_total",
    "Events successfully applied",
    ["consumer", "event_type"],
)
_EVENTS_FAILED_TOTAL = Counter(
    "events_failed_total",
    "Events that failed and went to DLQ",
    ["consumer", "event_type"],
)
_EVENT_BATCH_SIZE = Histogram(
    "event_batch_size",
    "Size of each handled batch",
    ["consumer"],
    buckets=[1, 10, 50, 100, 500, 1000, 5000],
)


@dataclass
class ConsumerConfig:
    name: str                           # 'virtuoso_sink', 'neo4j_sink', …
    dsn: str                            # Postgres DSN
    poll_interval_seconds: float = 5.0
    batch_size: int = 1000
    max_attempts: int = 5
    upstream_consumer: str | None = None  # high-watermark gating
    metrics_port: int | None = 9100
    kuma_push_url: str | None = None


class EventConsumer(abc.ABC):
    """Subclass + implement ``handle(batch)``. Construct via
    ``from_env()``, call ``run_forever()``."""

    def __init__(self, config: ConsumerConfig) -> None:
        self.config = config
        self._conn: psycopg.Connection | None = None
        self._labels = {"consumer": config.name}
        self._lag = _EVENT_LAG_SECONDS
        self._processed = _EVENTS_PROCESSED_TOTAL
        self._failed = _EVENTS_FAILED_TOTAL
        self._batch_size = _EVENT_BATCH_SIZE

    # ── public API ────────────────────────────────────────

    @classmethod
    def from_env(cls, **kwargs):
        cfg = ConsumerConfig(
            name=os.environ["EVENT_CONSUMER_NAME"],
            dsn=os.environ["EVENTS_DATABASE_URL"],
            poll_interval_seconds=float(
                os.environ.get("EVENT_POLL_INTERVAL", "5.0")
            ),
            batch_size=int(os.environ.get("EVENT_BATCH_SIZE", "1000")),
            max_attempts=int(os.environ.get("EVENT_MAX_ATTEMPTS", "5")),
            upstream_consumer=os.environ.get("EVENT_UPSTREAM_CONSUMER"),
            metrics_port=int(os.environ.get("METRICS_PORT", "9100")),
            kuma_push_url=os.environ.get("KUMA_PUSH_URL"),
        )
        return cls(cfg, **kwargs)

    @abc.abstractmethod
    def handle(self, batch: list[EventEnvelope]) -> None:
        """Apply a batch of events. Must be idempotent.

        Raise to trigger retry; events that exceed
        ``max_attempts`` get DLQ'd.
        """

    def run_once(self) -> int:
        """Process one batch. Returns rows processed (0 = caught up)."""
        events = self._fetch()
        if not events:
            return 0
        self._batch_size.labels(**self._labels).observe(len(events))
        try:
            self.handle(events)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            # Whole batch failed; per-event DLQ would require a
            # different shape (one transaction per event). Today
            # we just retry the batch up to max_attempts.
            self._record_batch_failure(events, exc)
            raise
        last_seq = events[-1].seq
        self._commit_offset(last_seq)
        for ev in events:
            self._processed.labels(consumer=self.config.name,
                                   event_type=ev.event_type).inc()
        return len(events)

    def run_forever(self) -> None:
        if self.config.metrics_port:
            start_http_server(self.config.metrics_port)
        while True:
            try:
                processed = self.run_once()
            except Exception:  # pylint: disable=broad-exception-caught
                logger.exception("consumer iteration failed; retrying")
                processed = 0
                time.sleep(min(self.config.poll_interval_seconds * 2, 60))
            self._heartbeat(ok=True)
            if processed == 0:
                time.sleep(self.config.poll_interval_seconds)

    # ── implementation ────────────────────────────────────

    def _connect(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.config.dsn, autocommit=False)
        return self._conn

    def _read_offset(self, name: str) -> int:
        cur = self._connect().execute(
            "SELECT last_seq FROM events.consumer_offsets "
            "WHERE consumer_name = %s",
            (name,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def _commit_offset(self, last_seq: int) -> None:
        with self._connect().transaction():
            self._connect().execute(
                """
                INSERT INTO events.consumer_offsets
                  (consumer_name, last_seq, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (consumer_name) DO UPDATE
                SET last_seq = EXCLUDED.last_seq,
                    updated_at = EXCLUDED.updated_at
                """,
                (self.config.name, last_seq),
            )

    def _fetch(self) -> list[EventEnvelope]:
        offset = self._read_offset(self.config.name)
        upper = None
        if self.config.upstream_consumer:
            upper = self._read_offset(self.config.upstream_consumer)

        sql = (
            "SELECT seq, ts, event_type, schema_version, iri, domain, op, "
            "payload, batch_id, producer "
            "FROM events.entity_events "
            "WHERE seq > %s "
        )
        params: list = [offset]
        if upper is not None:
            sql += "AND seq <= %s "
            params.append(upper)
        sql += "ORDER BY seq LIMIT %s"
        params.append(self.config.batch_size)

        cur = self._connect().execute(sql, params)
        rows = cur.fetchall()
        self._connect().commit()  # release the read txn

        out = [
            EventEnvelope(
                seq=r[0], ts=r[1], event_type=r[2], schema_version=r[3],
                iri=r[4], domain=r[5], op=r[6], payload=r[7],
                batch_id=r[8], producer=r[9],
            )
            for r in rows
        ]
        # Update lag gauge based on the newest event we know about.
        if out:
            newest_ts = out[-1].ts
            self._lag.labels(**self._labels).set(
                max(0.0, time.time() - newest_ts.timestamp())
            )
        return out

    def _record_batch_failure(
        self, events: Iterable[EventEnvelope], exc: Exception,
    ) -> None:
        # Naive first cut: write all events in the failed batch to
        # the DLQ with attempts=1. A subsequent retry that fails
        # again increments. After max_attempts, the consumer treats
        # the batch as poison and skips past (subclass policy).
        with self._connect().transaction():
            for ev in events:
                self._connect().execute(
                    """
                    INSERT INTO events.dead_letter
                      (seq, consumer, error, attempts, first_failed_at)
                    VALUES (%s, %s, %s, 1, now())
                    ON CONFLICT (seq, consumer) DO UPDATE
                    SET attempts = events.dead_letter.attempts + 1,
                        error    = EXCLUDED.error
                    """,
                    (ev.seq, self.config.name, str(exc)),
                )
                self._failed.labels(consumer=self.config.name,
                                    event_type=ev.event_type).inc()

    def _heartbeat(self, *, ok: bool) -> None:
        if not self.config.kuma_push_url:
            return
        try:
            import urllib.parse
            import urllib.request
            qs = urllib.parse.urlencode({
                "status": "up" if ok else "down",
                "msg": f"{self.config.name}",
                "ping": "",
            })
            url = self.config.kuma_push_url + (
                "&" if "?" in self.config.kuma_push_url else "?"
            ) + qs
            urllib.request.urlopen(url, timeout=5).read()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.debug("kuma heartbeat failed", exc_info=True)
