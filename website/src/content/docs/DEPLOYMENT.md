---
title: Unattended container deployment
description: Run Ceres as a finite scheduled harvest job or a long-lived API server.
---

Ceres ships one multi-stage image containing the `ceres` CLI, `ceres-server`,
all SQL migrations, and the `ceres-migrate` helper. The image defaults to the
server, while a scheduler overrides the command with a finite CLI invocation.

## Managed PostgreSQL choice

For the maintained deployment, use **Supabase Postgres**. Both Supabase and
Neon support PostgreSQL and pgvector, but Supabase is the practical first choice
when the project and credentials already live there. Ceres remains portable: a
Neon connection string can be substituted without code changes.

Supabase documents direct connections for long-lived backends and migrations,
and its session pooler for persistent clients on IPv4-only networks. Use one of
those modes for `ceres-migrate` and scheduled harvest containers; avoid a
transaction-mode URL for migrations. The initial Ceres migration enables the
`vector` extension automatically.

- [Supabase connection modes](https://supabase.com/docs/guides/database/connecting-to-postgres)
- [Supabase Postgres extensions](https://supabase.com/docs/guides/database/extensions)
- [Neon pgvector support](https://neon.com/docs/extensions/pgvector)

A broad Ceres index will outgrow small free database quotas. Size storage and
compute for the number of harvested records, raw JSON metadata, and any HNSW
embedding index before making the job unattended.

## Build or pull the image

Build the current checkout:

```bash
docker build -t ceres .
```

Release tags publish the image to GHCR as
`ghcr.io/andreabozzo/ceres:<version>` and `:latest`. The existing
`ghcr.io/andreabozzo/ceres-server` name remains as a compatibility alias.

## Initialize the database

Inject the database URL at runtime. Never bake it into the image or commit it to
an env file.

```bash
docker run --rm \
  -e DATABASE_URL="$DATABASE_URL" \
  ceres ceres-migrate
```

`ceres-migrate` applies the bundled migrations in filename order and records
each completed file in `schema_migrations`. It is safe to run before every
deployment.

## Run one scheduled harvest

Mount `portals.toml` read-only and keep embedding out of the harvesting hot path:

```bash
docker run --rm \
  -e DATABASE_URL="$DATABASE_URL" \
  -e PORTALS_CONFIG=/etc/ceres/portals.toml \
  -e CERES_LOG_FORMAT=json \
  -e CERES_BATCH_CONCURRENCY=4 \
  -v "$PWD/examples/portals.toml:/etc/ceres/portals.toml:ro" \
  ceres ceres harvest --metadata-only
```

The command is finite and continues after individual portal failures. Exit `0`
means all portals succeeded, `2` means the batch finished with failed portals,
and `1` means a fatal configuration/database/command error. JSON logs include a
`portal_outcome` event per configured portal and one final `batch_summary`.

Configure any standard container-job scheduler with:

- the image and `ceres harvest --metadata-only` command;
- `DATABASE_URL` from its secret manager;
- a read-only `portals.toml` mount or equivalent injected file;
- enough timeout for the widest portal set;
- no retries for exit `2` unless repeated portal traffic is intentional;
- alerting for exit `1` and retention for JSON logs.

Supabase Cron schedules SQL, database functions, HTTP requests, or Edge
Functions; it does not run this Rust container. Use the container scheduler of
your host or cloud platform and use Supabase as the managed PostgreSQL service.

## Run the API server

The default image command is `ceres-server`:

```bash
docker run --rm -p 3000:3000 \
  -e DATABASE_URL="$DATABASE_URL" \
  -e CERES_ADMIN_TOKEN="$CERES_ADMIN_TOKEN" \
  ceres
```

Configure container-platform probes as follows:

- liveness: `GET /api/v1/health/live` — process-only, always independent of PostgreSQL;
- readiness: `GET /api/v1/health/ready` — returns 200 when PostgreSQL is reachable and 503 otherwise;
- compatibility: `GET /api/v1/health` — the same readiness behavior.

## Secrets and configuration

Pass credentials through the scheduler or platform secret manager. Common
runtime variables are `DATABASE_URL`, `CERES_ADMIN_TOKEN`, `GEMINI_API_KEY`,
`OPENAI_API_KEY`, `SOCRATA_APP_TOKEN`, and `ODS_API_KEY`. Keep `portals.toml`
non-secret where possible and mount it read-only. The image contains no `.env`
file and `.dockerignore` excludes local env files from the build context.
