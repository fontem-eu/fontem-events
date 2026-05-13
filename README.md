# fontem-events

Python client library (package: fontem_events — to be renamed). Exposes log.batch() / emit.* for producers and a generic event consumer base class for sinks. Wraps the events.entity_events table in Postgres with batch-id semantics and dead-letter handling.

## Deploy

CI auto-deploys to the testing env on every merge to main. Promotion to staging / prod is **manual** — bump the version in `gitops/<env>/<service>.yaml` to land it in a given environment.

## Convention

See [/config/repos/CLAUDE.md](https://contribute.void42.internal/fontem/gitops) for workspace-wide rules (feature branches + CI gate, no direct push to main, full gate before declaring done, conventional commits).
