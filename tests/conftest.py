"""Spin up an ephemeral Postgres in Docker for the suite.

We use the same golden image the cluster runs (postgres-fontem)
so the SQL we test against matches prod byte-for-byte. The
fixture is session-scoped — one container for the whole suite,
torn down at exit.
"""
from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import pytest


_IMAGE = "contribute.void42.internal/fontem/postgres-fontem:16.13-pgv0.8.1"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    if os.environ.get("EVENTS_DATABASE_URL"):
        # CI provides one — use it.
        yield os.environ["EVENTS_DATABASE_URL"]
        return

    port = _free_port()
    name = f"gmr-events-it-{uuid.uuid4().hex[:8]}"
    pwd = "events-test"
    subprocess.run(
        ["docker", "run", "-d", "--name", name,
         "-e", f"POSTGRES_PASSWORD={pwd}",
         "-p", f"{port}:5432",
         _IMAGE],
        check=True, capture_output=True,
    )
    try:
        # Wait for ready
        deadline = time.monotonic() + 60
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                with psycopg.connect(
                    f"postgresql://postgres:{pwd}@127.0.0.1:{port}/postgres",
                    connect_timeout=2,
                ) as c:
                    c.execute("SELECT 1")
                    break
            except Exception as e:  # pylint: disable=broad-exception-caught
                last_exc = e
                time.sleep(0.5)
        else:
            raise RuntimeError(f"postgres never came up: {last_exc}")

        # Apply the bootstrap schema. Tablespace creation needs a
        # real path; the test postgres is using its own data dir,
        # so we point at a subfolder that exists + is empty.
        subprocess.run(
            ["docker", "exec", name, "mkdir", "-p",
             "/var/lib/postgresql/events"],
            check=True,
        )
        subprocess.run(
            ["docker", "exec", name, "chown", "postgres:postgres",
             "/var/lib/postgresql/events"],
            check=True,
        )
        bootstrap = (
            Path(__file__).parent / "fixtures" / "bootstrap.sql"
        ).read_text()
        with psycopg.connect(
            f"postgresql://postgres:{pwd}@127.0.0.1:{port}/postgres",
            autocommit=True,
        ) as c:
            c.execute(
                "CREATE TABLESPACE events_ts LOCATION '/var/lib/postgresql/events'"
            )
        with psycopg.connect(
            f"postgresql://postgres:{pwd}@127.0.0.1:{port}/postgres",
        ) as c:
            c.execute(bootstrap)
        yield f"postgresql://postgres:{pwd}@127.0.0.1:{port}/postgres"
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False,
                       capture_output=True)


@pytest.fixture(autouse=True)
def _truncate_events(postgres_dsn: str) -> None:
    """Each test starts on a clean event log."""
    with psycopg.connect(postgres_dsn) as c:
        c.execute(
            "TRUNCATE events.entity_events, events.consumer_offsets, "
            "events.dead_letter RESTART IDENTITY"
        )
        c.commit()
