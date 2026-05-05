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
