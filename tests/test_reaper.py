"""Tests for the stale-run finalizer.

The reaper rewrites rows that no process will ever close. The risk it
carries is the mirror of the bug it fixes: closing a run that is still
alive would report a false crash and, worse, teach operators to
distrust the status column. So the tests here lean hardest on what it
must *not* touch.
"""
from __future__ import annotations

from datetime import timedelta

import psycopg
import pytest

from fontem_events import RunLog, reap_stale_runs
from fontem_events.errors import EventLogError
from fontem_events.reaper import DEFAULT_DEADLINE_SECONDS, main


def _insert_running(dsn: str, cronjob: str, *, age_seconds: int,
                    deadline_seconds: int | None) -> int:
    """Insert a 'running' row that started age_seconds ago.

    Backdating started_at is how we simulate a process that was killed
    without waiting out a real deadline.
    """
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events.etl_run
                   (cronjob_name, started_at, status, deadline_seconds)
            VALUES (%s, now() - make_interval(secs => %s), 'running', %s)
            RETURNING run_id
            """,
            (cronjob, age_seconds, deadline_seconds),
        )
        return cur.fetchone()[0]


def _row(dsn: str, run_id: int) -> dict:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, started_at, finished_at, error_message, "
            "deadline_seconds FROM events.etl_run WHERE run_id = %s",
            (run_id,),
        )
        cols = [c.name for c in cur.description]
        return dict(zip(cols, cur.fetchone()))


def test_reaps_run_past_its_own_deadline(postgres_dsn):
    """A row whose recorded deadline has expired is provably dead."""
    run_id = _insert_running(
        postgres_dsn, "etl-reap-expired", age_seconds=7200, deadline_seconds=3600
    )
    reaped = reap_stale_runs(postgres_dsn)
    assert run_id in [r["run_id"] for r in reaped]
    assert _row(postgres_dsn, run_id)["status"] == "crashed"


def test_leaves_live_run_alone(postgres_dsn):
    """The one thing the reaper must never do: close a running job.

    Started 30 minutes ago against a 2-hour deadline — well inside its
    window, so it is still legitimately working.
    """
    run_id = _insert_running(
        postgres_dsn, "etl-reap-live", age_seconds=1800, deadline_seconds=7200
    )
    reaped = reap_stale_runs(postgres_dsn)
    assert run_id not in [r["run_id"] for r in reaped]
    assert _row(postgres_dsn, run_id)["status"] == "running"


def test_grace_period_protects_a_run_at_its_deadline(postgres_dsn):
    """Right at the deadline the pod may still be being torn down.

    Kubernetes' kill, the kubelet's status write and our clock are not
    the same instant, so the reaper waits out a grace period rather
    than racing the teardown.
    """
    run_id = _insert_running(
        postgres_dsn, "etl-reap-grace", age_seconds=3610, deadline_seconds=3600
    )
    reaped = reap_stale_runs(postgres_dsn, grace_seconds=300)
    assert run_id not in [r["run_id"] for r in reaped]
    assert _row(postgres_dsn, run_id)["status"] == "running"


def test_legacy_row_without_deadline_uses_the_default(postgres_dsn):
    """Rows predating deadline_seconds still get closed eventually.

    These are the 30-day-old rows already in prod. They carry NULL, so
    the reaper falls back to a default long enough that it cannot
    outrun the slowest job.
    """
    stale = _insert_running(
        postgres_dsn, "etl-reap-legacy-old",
        age_seconds=DEFAULT_DEADLINE_SECONDS * 3, deadline_seconds=None,
    )
    recent = _insert_running(
        postgres_dsn, "etl-reap-legacy-recent",
        age_seconds=60, deadline_seconds=None,
    )
    reap_stale_runs(postgres_dsn)
    assert _row(postgres_dsn, stale)["status"] == "crashed"
    assert _row(postgres_dsn, recent)["status"] == "running"


def test_finished_at_is_the_deadline_not_the_reap_time(postgres_dsn):
    """We know the pod was dead by the deadline; we do not know it was
    alive until the reaper ran. Stamping now() would invent a
    multi-week duration for a job that died in its first minute and
    poison every duration average on the dashboard."""
    run_id = _insert_running(
        postgres_dsn, "etl-reap-finished-at",
        age_seconds=30 * 86400, deadline_seconds=3600,
    )
    reap_stale_runs(postgres_dsn)
    row = _row(postgres_dsn, run_id)
    duration = row["finished_at"] - row["started_at"]
    assert duration == timedelta(seconds=3600)


def test_error_message_explains_why_the_row_was_closed(postgres_dsn):
    """A crashed row with no explanation is barely better than a stuck
    one — the operator still has to reconstruct what happened."""
    run_id = _insert_running(
        postgres_dsn, "etl-reap-message", age_seconds=7200, deadline_seconds=3600
    )
    reap_stale_runs(postgres_dsn)
    message = _row(postgres_dsn, run_id)["error_message"]
    assert "deadline" in message
    assert "run reaper" in message


def test_reaping_is_idempotent(postgres_dsn):
    """It runs on a schedule; a second pass must not re-close or
    re-append to rows it already handled."""
    run_id = _insert_running(
        postgres_dsn, "etl-reap-idempotent", age_seconds=7200, deadline_seconds=3600
    )
    reap_stale_runs(postgres_dsn)
    first = _row(postgres_dsn, run_id)
    second_pass = reap_stale_runs(postgres_dsn)
    assert run_id not in [r["run_id"] for r in second_pass]
    assert _row(postgres_dsn, run_id) == first


def test_does_not_touch_terminal_rows(postgres_dsn):
    """Only 'running' is ambiguous. A row that already reported its own
    outcome is the truth and must survive untouched."""
    with RunLog(postgres_dsn, cronjob_name="etl-reap-succeeded") as run:
        run.set_summary("loaded 7 records")
        run_id = run._run_id  # pylint: disable=protected-access
    before = _row(postgres_dsn, run_id)
    reap_stale_runs(postgres_dsn)
    assert _row(postgres_dsn, run_id) == before


def test_run_log_records_the_deadline_from_env(monkeypatch, postgres_dsn):
    """The chart templates RUN_DEADLINE_SECONDS from the same value as
    activeDeadlineSeconds, so the two cannot drift."""
    monkeypatch.setenv("EVENTS_DATABASE_URL", postgres_dsn)
    monkeypatch.setenv("RUN_DEADLINE_SECONDS", "5400")
    with RunLog.from_env(cronjob_name="etl-reap-env") as run:
        run_id = run._run_id  # pylint: disable=protected-access
    assert _row(postgres_dsn, run_id)["deadline_seconds"] == 5400


def test_malformed_deadline_does_not_break_the_loader(monkeypatch, postgres_dsn):
    """A typo in the chart must degrade crash detection, never stop the
    ETL run itself."""
    monkeypatch.setenv("EVENTS_DATABASE_URL", postgres_dsn)
    monkeypatch.setenv("RUN_DEADLINE_SECONDS", "not-a-number")
    with RunLog.from_env(cronjob_name="etl-reap-bad-env") as run:
        run_id = run._run_id  # pylint: disable=protected-access
    row = _row(postgres_dsn, run_id)
    assert row["deadline_seconds"] is None
    assert row["status"] == "success"


def test_requires_a_dsn(monkeypatch):
    """Misconfigured reaper fails loudly rather than silently reaping
    nothing and reporting success."""
    monkeypatch.delenv("EVENTS_DATABASE_URL", raising=False)
    with pytest.raises(EventLogError, match="EVENTS_DATABASE_URL"):
        reap_stale_runs()


def test_main_reports_what_it_closed(postgres_dsn, monkeypatch, capsys):
    """The CronJob's log is the only place an operator sees this run,
    so the summary has to name the cronjobs it touched and the count."""
    monkeypatch.setenv("EVENTS_DATABASE_URL", postgres_dsn)
    _insert_running(postgres_dsn, "etl-reap-main-a",
                    age_seconds=7200, deadline_seconds=3600)
    _insert_running(postgres_dsn, "etl-reap-main-a",
                    age_seconds=7200, deadline_seconds=3600)
    _insert_running(postgres_dsn, "etl-reap-main-b",
                    age_seconds=7200, deadline_seconds=3600)
    main()
    out = capsys.readouterr().out
    assert "etl-reap-main-a" in out
    assert "etl-reap-main-b" in out
    assert "Done: reaped" in out


def test_main_says_so_when_there_is_nothing_to_do(postgres_dsn, monkeypatch, capsys):
    """The quiet path still has to print something. A silent run is
    indistinguishable from a run that never happened."""
    monkeypatch.setenv("EVENTS_DATABASE_URL", postgres_dsn)
    reap_stale_runs(postgres_dsn)  # drain anything left by earlier tests
    main()
    assert "no stale runs to reap" in capsys.readouterr().out


def test_main_honours_the_env_overrides(postgres_dsn, monkeypatch, capsys):
    """The chart tunes the reaper through these two variables; if they
    were ignored, a misconfigured default could close live runs."""
    monkeypatch.setenv("EVENTS_DATABASE_URL", postgres_dsn)
    monkeypatch.setenv("REAP_DEFAULT_DEADLINE_SECONDS", "60")
    monkeypatch.setenv("REAP_GRACE_SECONDS", "0")
    run_id = _insert_running(postgres_dsn, "etl-reap-main-env",
                             age_seconds=600, deadline_seconds=None)
    main()
    assert _row(postgres_dsn, run_id)["status"] == "crashed"
    assert "etl-reap-main-env" in capsys.readouterr().out


def test_main_propagates_a_missing_dsn(monkeypatch):
    """Misconfiguration must fail the CronJob, not exit 0 quietly."""
    monkeypatch.delenv("EVENTS_DATABASE_URL", raising=False)
    with pytest.raises(EventLogError):
        main()
