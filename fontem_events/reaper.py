"""Finalizer for ETL runs whose process died without closing its row.

``RunLog`` writes ``status='running'`` on entry and rewrites it on
exit. When the process is killed outright — OOM, ``activeDeadlineSeconds``
expiry, node eviction, SIGKILL — ``__exit__`` never runs and the row
stays ``running`` forever. Those rows are indistinguishable from a
genuinely in-flight run, so the dashboard cannot tell "started ten
minutes ago and still working" from "died three weeks ago", and every
stale row hides the one that matters.

The trick is that we do not have to guess a timeout. Every CronJob in
the chart sets ``activeDeadlineSeconds``, and Kubernetes *guarantees*
the pod is gone once that elapses. So a row still marked ``running``
past ``started_at + activeDeadlineSeconds`` is not "probably dead" —
it is provably dead, and can be closed without racing a live job.

``RunLog`` records that deadline in ``deadline_seconds`` at insert
time (see ``RUN_DEADLINE_SECONDS`` in the chart), which keeps this
module pure SQL: no Kubernetes API access, no RBAC, no coupling to
the cluster it happens to be reaping. Rows written before that column
existed carry NULL and fall back to ``default_deadline_seconds``.

``finished_at`` is set to the deadline expiry rather than ``now()``.
We know the pod was dead by then; we do not know it was alive until
the reaper happened to run. Stamping ``now()`` would invent a 35-day
"duration" for a job that died in its first minute and would poison
every duration average on the dashboard.
"""
from __future__ import annotations

import os
from typing import Any

import psycopg

from .errors import EventLogError


# Falls back for rows written before deadline_seconds existed. Four
# hours is the longest activeDeadlineSeconds among the ETL cronjobs
# (etl-gleif, etl-eu-knowledge-graph), so it cannot close a legacy row
# that some slower job might still legitimately be inside.
DEFAULT_DEADLINE_SECONDS = 14400

# Kubernetes kills at the deadline, but the kill, the kubelet's status
# write and our clock are not the same instant. A few minutes of slack
# keeps the reaper off the heels of a job being torn down right now.
DEFAULT_GRACE_SECONDS = 300

_REAP_SQL = """
UPDATE events.etl_run
   SET status        = 'crashed',
       finished_at   = started_at
                     + make_interval(secs => coalesce(deadline_seconds, %(default)s)),
       error_message = coalesce(error_message, '')
                     || 'No completion recorded. The run was still marked running '
                     || age(now(), started_at)::text
                     || ' after it started, past its '
                     || coalesce(deadline_seconds, %(default)s)::text
                     || 's deadline, so the pod was already killed by then '
                     || '(OOM, activeDeadlineSeconds, or eviction). '
                     || 'Closed by the run reaper; finished_at is the deadline '
                     || 'expiry, not the observed end.'
 WHERE status = 'running'
   AND now() > started_at
             + make_interval(secs => coalesce(deadline_seconds, %(default)s) + %(grace)s)
RETURNING run_id, cronjob_name, started_at, deadline_seconds
"""


def reap_stale_runs(
    dsn: str | None = None,
    *,
    default_deadline_seconds: int = DEFAULT_DEADLINE_SECONDS,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
) -> list[dict[str, Any]]:
    """Close every ``running`` row whose deadline has provably expired.

    Returns one dict per reaped row so the caller can log what it
    closed. Safe to run concurrently with live ETL jobs: the deadline
    window means an in-flight run is never touched, and the
    ``status = 'running'`` predicate makes a double run a no-op.
    """
    if dsn is None:
        dsn = os.environ.get("EVENTS_DATABASE_URL")
    if not dsn:
        raise EventLogError("EVENTS_DATABASE_URL not set; cannot reach the run log")

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            _REAP_SQL,
            {"default": default_deadline_seconds, "grace": grace_seconds},
        )
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def main() -> None:
    """Entry point for the etl-run-reaper CronJob.

    Returns nothing and lets failures raise: the CronJob's exit code is
    what the alerting pipeline reads, and an EventLogError here means
    the reaper could not reach the run log at all — which should fail
    the job loudly rather than be flattened into a status code.
    """
    default = int(
        os.environ.get("REAP_DEFAULT_DEADLINE_SECONDS", DEFAULT_DEADLINE_SECONDS)
    )
    grace = int(os.environ.get("REAP_GRACE_SECONDS", DEFAULT_GRACE_SECONDS))
    reaped = reap_stale_runs(default_deadline_seconds=default, grace_seconds=grace)

    if not reaped:
        print("Done: no stale runs to reap")
        return

    by_job: dict[str, int] = {}
    for row in reaped:
        by_job[row["cronjob_name"]] = by_job.get(row["cronjob_name"], 0) + 1
    for name, count in sorted(by_job.items(), key=lambda kv: -kv[1]):
        print(f"  reaped {count:3d}  {name}")
    print(f"Done: reaped {len(reaped)} stale run(s) across {len(by_job)} cronjob(s)")


if __name__ == "__main__":  # pragma: no cover
    main()
