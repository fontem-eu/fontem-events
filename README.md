# gmr-events

Runtime Python lib that producers and consumers use to talk to
the `events.entity_events` log in `gmr_app`. Sister package to
[gmr-event-schemas](https://contribute.void42.internal/fontem/gmr-event-schemas)
(which only ships JSON Schemas + validators).

See [fontem-ontology/MIGRATION.md](https://contribute.void42.internal/fontem/fontem-ontology/src/branch/main/MIGRATION.md) for the architecture.

## What you get

```python
from gmr_events import EventLog, EventConsumer
from gmr_event_schemas import builders

# ── producer side: in an ETL loader ────────────────────────
log = EventLog.from_env()                          # reads DATABASE_URL
batch_id = uuid.uuid4()

with log.batch(batch_id, producer="load_eu_sanctions") as emit:
    emit.control("BeginGraphReplace",
                 builders.begin_graph_replace(
                     graph_iri="http://data.fontem.eu/graph/sanctions",
                     label="SanctionedEntity",
                     domain="sanctions",
                 ))
    for ent in entities:
        emit.upsert("UpsertSanctionedEntity",
                    iri=f"http://data.fontem.eu/id/Sanction/{ent.id}",
                    domain="sanctions",
                    payload=builders.upsert_sanctioned_entity(**ent))
    emit.control("EndGraphReplace",
                 builders.end_graph_replace(
                     graph_iri="http://data.fontem.eu/graph/sanctions",
                     domain="sanctions",
                 ))

# ── consumer side: in the Virtuoso sink ────────────────────
class VirtuosoSink(EventConsumer):
    consumer_name = "virtuoso_sink"
    domains       = ["sanctions", "company", "contract", …]

    def handle(self, batch: list[EventEnvelope]) -> None:
        # apply each event; fail loudly on unknown event_type
        ...

VirtuosoSink.from_env().run_forever()
```

## Layered guarantees

* Every emitted payload is validated against
  `gmr-event-schemas` before insert. Producers can't ship a
  malformed payload.
* Insert + idempotency-key write are in the same transaction,
  so a crash mid-emit either lands the whole batch or none of
  it.
* Consumers read with `SELECT … FOR UPDATE SKIP LOCKED` and
  commit the offset advance in the same transaction as the
  work. At-least-once delivery; idempotent sinks see no
  difference.
* Failed events go to `events.dead_letter` after N retries
  with exponential backoff. The DLQ row carries the full error
  message; replay tooling reads from there.
* Every consumer ships a Prometheus exporter and an Uptime Kuma
  push by default — opt-out, not opt-in.

## Used by

* **Producers**: every ETL loader in `edgar-gmr-etl`, the
  consolidator in `gmr-consolidator`.
* **Consumers**: `gmr-virtuoso-sink`, `gmr-neo4j-sink`, and
  `gmr-consolidator` (which reads from the log to drive
  detection rules — gated by the Neo4j sink's offset).
