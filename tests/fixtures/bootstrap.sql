-- Mirrors gitops/infra/prod.yaml events-bootstrap-sql.schema.sql.
-- If you change one, change the other.
CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.entity_events (
    seq            BIGSERIAL PRIMARY KEY,
    ts             TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_type     TEXT NOT NULL,
    schema_version INT  NOT NULL DEFAULT 1,
    iri            TEXT NOT NULL,
    domain         TEXT NOT NULL,
    op             TEXT NOT NULL,
    payload        JSONB NOT NULL,
    batch_id       UUID,
    producer       TEXT NOT NULL
) TABLESPACE events_ts;

CREATE INDEX IF NOT EXISTS entity_events_domain_seq
    ON events.entity_events (domain, seq) TABLESPACE events_ts;
CREATE INDEX IF NOT EXISTS entity_events_iri_seq
    ON events.entity_events (iri, seq)    TABLESPACE events_ts;
CREATE INDEX IF NOT EXISTS entity_events_batch
    ON events.entity_events (batch_id)    TABLESPACE events_ts;

CREATE TABLE IF NOT EXISTS events.consumer_offsets (
    consumer_name TEXT PRIMARY KEY,
    last_seq      BIGINT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
) TABLESPACE events_ts;

CREATE TABLE IF NOT EXISTS events.dead_letter (
    seq             BIGINT NOT NULL,
    consumer        TEXT NOT NULL,
    error           TEXT NOT NULL,
    attempts        INT NOT NULL,
    first_failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (seq, consumer)
) TABLESPACE events_ts;

CREATE INDEX IF NOT EXISTS dead_letter_consumer
    ON events.dead_letter (consumer) TABLESPACE events_ts;

-- ── ETL execution log ──────────────────────────────────────────
-- One row per ETL CronJob invocation. Written by
-- fontem_events.run_log.RunLog: status='running' on entry,
-- 'success'|'failed' on clean exit, left at 'running' on hard
-- crash (OOM, deadline kill) until the reaper rewrites it to
-- 'crashed'. Drives the data-quality dashboard.
--
-- deadline_seconds mirrors the CronJob's activeDeadlineSeconds and is
-- what makes crash detection provable rather than guessed: past
-- started_at + deadline_seconds, Kubernetes has already killed the
-- pod, so a row still marked 'running' cannot be alive. NULL for rows
-- written before the column existed; the reaper falls back to a
-- default for those.
CREATE TABLE IF NOT EXISTS events.etl_run (
    run_id           BIGSERIAL PRIMARY KEY,
    cronjob_name     TEXT        NOT NULL,
    image_tag        TEXT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    status           TEXT        NOT NULL
                     CHECK (status IN ('running','success','failed','crashed')),
    summary          TEXT,
    error_message    TEXT,
    deadline_seconds INTEGER
) TABLESPACE events_ts;

-- Both statements are for databases created before these existed;
-- they are no-ops on a fresh bootstrap.
ALTER TABLE events.etl_run
    ADD COLUMN IF NOT EXISTS deadline_seconds INTEGER;

DO $$
BEGIN
    ALTER TABLE events.etl_run DROP CONSTRAINT IF EXISTS etl_run_status_check;
    ALTER TABLE events.etl_run ADD CONSTRAINT etl_run_status_check
        CHECK (status IN ('running','success','failed','crashed'));
END $$;

CREATE INDEX IF NOT EXISTS etl_run_cronjob_started
    ON events.etl_run (cronjob_name, started_at DESC) TABLESPACE events_ts;
CREATE INDEX IF NOT EXISTS etl_run_status_started
    ON events.etl_run (status, started_at DESC) TABLESPACE events_ts
    WHERE status IN ('running','failed','crashed');
