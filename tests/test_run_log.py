"""Tests for the ETL execution-log writer.

Uses the same session-scoped postgres_dsn fixture as test_end_to_end:
real Postgres, bootstrap SQL applied. We exercise the surface that
the loaders + dashboard depend on, not the wire format.
"""
from __future__ import annotations

import os

import psycopg
import pytest

from fontem_events import RunLog, recent_runs


def _row(dsn: str, run_id: int) -> dict:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, summary, error_message, image_tag, "
            "finished_at IS NOT NULL AS finished "
            "FROM events.etl_run WHERE run_id = %s",
            (run_id,),
        )
        cols = [c.name for c in cur.description]
        return dict(zip(cols, cur.fetchone()))


def test_run_log_success_path(postgres_dsn):
    """Clean exit → status='success', summary preserved, finished_at set."""
    with RunLog(postgres_dsn, cronjob_name="etl-test-success",
                image_tag="vtest") as run:
        run.set_summary("loaded 42 records")
        run_id = run._run_id  # pylint: disable=protected-access
    row = _row(postgres_dsn, run_id)
    assert row["status"] == "success"
    assert row["summary"] == "loaded 42 records"
    assert row["error_message"] is None
    assert row["image_tag"] == "vtest"
    assert row["finished"] is True


def test_run_log_failure_path_records_truncated_traceback(postgres_dsn):
    """Exception inside the context → status='failed', traceback in
    error_message, exception still propagates."""
    with pytest.raises(RuntimeError, match="boom"):
        with RunLog(postgres_dsn, cronjob_name="etl-test-fail") as run:
            run_id = run._run_id  # pylint: disable=protected-access
            raise RuntimeError("boom")
    row = _row(postgres_dsn, run_id)
    assert row["status"] == "failed"
    assert row["error_message"] is not None
    assert "RuntimeError: boom" in row["error_message"]
    # 2000 char cap — a deeply-nested traceback shouldn't bloat the table.
    assert len(row["error_message"]) <= 2000


def test_run_log_leaves_row_running_on_hard_crash(postgres_dsn):
    """If __exit__ never runs (sigkill, OOM), the row stays at
    'running' — dashboard treats that as 'crashed'. Verified here by
    just not closing the context."""
    rl = RunLog(postgres_dsn, cronjob_name="etl-test-crashed")
    rl.__enter__()
    run_id = rl._run_id  # pylint: disable=protected-access
    # Deliberately skip __exit__ to simulate sigkill.
    row = _row(postgres_dsn, run_id)
    assert row["status"] == "running"
    assert row["finished"] is False


def test_recent_runs_returns_newest_first(postgres_dsn):
    """The fontem-api endpoint reads from this; order has to be
    deterministic so the dashboard's 'latest run' column makes sense."""
    with RunLog(postgres_dsn, cronjob_name="etl-test-recent-a") as r:
        r.set_summary("first")
    with RunLog(postgres_dsn, cronjob_name="etl-test-recent-b") as r:
        r.set_summary("second")
    rows = recent_runs(postgres_dsn, limit=5)
    # We may share the fixture with other tests so we can't assert
    # exact length — instead verify the order and presence of our two.
    summaries = [r["summary"] for r in rows]
    assert "second" in summaries
    assert "first" in summaries
    assert summaries.index("second") < summaries.index("first")


def test_from_env_reads_events_database_url(monkeypatch, postgres_dsn):
    monkeypatch.setenv("EVENTS_DATABASE_URL", postgres_dsn)
    monkeypatch.setenv("IMAGE_TAG", "v123abc")
    with RunLog.from_env(cronjob_name="etl-test-env") as run:
        run.set_summary("from env")
        run_id = run._run_id  # pylint: disable=protected-access
    row = _row(postgres_dsn, run_id)
    assert row["status"] == "success"
    assert row["image_tag"] == "v123abc"


def test_summary_is_truncated_to_500_chars(postgres_dsn):
    long_summary = "x" * 1000
    with RunLog(postgres_dsn, cronjob_name="etl-test-trunc") as run:
        run.set_summary(long_summary)
        run_id = run._run_id  # pylint: disable=protected-access
    row = _row(postgres_dsn, run_id)
    assert row["summary"] is not None
    assert len(row["summary"]) == 500
