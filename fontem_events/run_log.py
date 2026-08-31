"""Execution-log writer for the ETL CronJobs.

Wraps a loader run with a row in ``events.etl_run`` (in the same
schema/tablespace as ``entity_events``). The row starts as
``status='running'`` and is updated to ``success`` or ``failed`` on
exit. The summary line — usually the loader's ``"Done: ..."``
output that today gets pulled into the Uptime-Kuma ping — lives in
the ``summary`` column so the data-quality dashboard can show "etl-
gleif: loaded 2.4M LEIs at 03:14 UTC" without parsing logs.

Usage::

    from fontem_events.run_log import RunLog

    with RunLog.from_env(cronjob_name="etl-gleif", image_tag=os.environ["IMAGE_TAG"]) as run:
        # ... actual loader ...
        run.set_summary("loaded 2.4M LEIs")

The context manager updates the row to ``success`` on clean exit
and ``failed`` (with ``error_message=truncated_traceback``) on any
exception. The exception still propagates so the CronJob fails as
usual — the row exists purely so the dashboard can render
without grepping logs.

Crashes that kill the process before ``__exit__`` runs (SIGKILL on
OOM or activeDeadlineSeconds, node eviction) leave the row in
``status='running'``. ``fontem_events.reaper`` closes those out to
``crashed``, using the ``deadline_seconds`` recorded here: past
``started_at + deadline_seconds`` Kubernetes has already killed the
pod, so the row is provably dead rather than merely old. Pass the
CronJob's ``activeDeadlineSeconds`` via ``RUN_DEADLINE_SECONDS`` (the
chart templates both from one value) or crash detection falls back to
a conservative default.
"""
from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone
from typing import Any

import psycopg

from .errors import EventLogError


# Keep `error_message` under 2 KB so a runaway stack trace doesn't
# bloat the table. Operators get the truncation marker; full traces
# live in the pod logs (kept for failedJobsHistoryLimit cycles).
_ERROR_MESSAGE_MAX_LEN = 2000
_SUMMARY_MAX_LEN = 500


class RunLog:
    """Context manager that writes one row per ETL run."""

    def __init__(
        self,
        dsn: str,
        *,
        cronjob_name: str,
        image_tag: str | None = None,
        deadline_seconds: int | None = None,
    ) -> None:
        self._dsn = dsn
        self._cronjob = cronjob_name
        self._image_tag = image_tag
        self._deadline_seconds = deadline_seconds
        self._run_id: int | None = None
        self._summary: str | None = None

    @classmethod
    def from_env(
        cls,
        cronjob_name: str,
        env_var: str = "EVENTS_DATABASE_URL",
        image_tag_var: str = "IMAGE_TAG",
        deadline_var: str = "RUN_DEADLINE_SECONDS",
    ) -> "RunLog":
        """Build from the standard CronJob env.

        ``RUN_DEADLINE_SECONDS`` mirrors the CronJob's
        ``activeDeadlineSeconds``; it is what lets the reaper prove a
        row is dead. Absent or unparseable, it stays NULL and the
        reaper falls back to its conservative default — a worse
        signal, never a wrong one.
        """
        dsn = os.environ.get(env_var)
        if not dsn:
            raise EventLogError(
                f"{env_var} is not set; cannot reach the run log"
            )
        raw_deadline = os.environ.get(deadline_var)
        try:
            deadline = int(raw_deadline) if raw_deadline else None
        except ValueError:
            # A malformed deadline must not stop the loader from
            # running; degrade to the reaper's default instead.
            deadline = None
        return cls(
            dsn,
            cronjob_name=cronjob_name,
            image_tag=os.environ.get(image_tag_var),
            deadline_seconds=deadline,
        )

    def set_summary(self, summary: str) -> None:
        """Loader calls this once it has a human-friendly count to record."""
        self._summary = summary[:_SUMMARY_MAX_LEN]

    # ── Context manager ────────────────────────────────────────────

    def __enter__(self) -> "RunLog":
        with psycopg.connect(self._dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                _INSERT_RUNNING_SQL,
                (self._cronjob, self._image_tag, self._deadline_seconds),
            )
            self._run_id = cur.fetchone()[0]
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        status = "success" if exc is None else "failed"
        error_message: str | None = None
        if exc is not None:
            error_message = "".join(
                traceback.format_exception(exc_type, exc, tb)
            )[:_ERROR_MESSAGE_MAX_LEN]
        with psycopg.connect(self._dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                _FINISH_RUN_SQL,
                (status, self._summary, error_message, self._run_id),
            )
        # Don't swallow — the cronjob exit code drives the rest of
        # the alerting pipeline.


_INSERT_RUNNING_SQL = """
INSERT INTO events.etl_run
       (cronjob_name, image_tag, started_at, status, deadline_seconds)
VALUES (%s, %s, now(), 'running', %s)
RETURNING run_id
"""

_FINISH_RUN_SQL = """
UPDATE events.etl_run
   SET status         = %s,
       summary        = %s,
       error_message  = %s,
       finished_at    = now()
 WHERE run_id = %s
"""


# Convenience for callers that want to query recent runs without
# wiring their own psycopg cursor. Kept tiny on purpose — the
# read-side dashboard owns the richer SQL.
def recent_runs(
    dsn: str | None = None, *, limit: int = 50
) -> list[dict[str, Any]]:
    """Return the last ``limit`` rows from ``events.etl_run``.

    Mostly for fontem-api's `/atlas/etl-runs` endpoint and ad-hoc
    operator queries. Not a replacement for proper SQL — anything
    fancier than "show me the last N" should hit the table directly.
    """
    if dsn is None:
        dsn = os.environ.get("EVENTS_DATABASE_URL")
    if not dsn:
        raise EventLogError("EVENTS_DATABASE_URL not set")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, cronjob_name, image_tag, started_at,
                   finished_at, status, summary, error_message
            FROM events.etl_run
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
