# sequin (TripTech fork)

TripTech's maintained fork of Sequin, Postgres change data capture to sinks
(Typesense, webhooks, Postgres, queues). Elixir/Phoenix app in `lib/`, Svelte UI
in `assets/`, Go CLI in `cli/`. Codebase guide and the commands that must pass
before a commit: [AGENTS.md](AGENTS.md). Fork scope: [README.md](README.md) banner.
This repo is **public**: no hostnames, account ids, Doppler or Railway details of
TripTech's deployment go in it. Those live in TripTech's private repos.

## Branches and releases

- `main` is the trunk. Work goes on a feature branch with a PR to `main`.
- **Release:** push an annotated tag `v0.14.6-ttN` on `main`.
  `.github/workflows/tt-docker-build.yml` builds `ghcr.io/triptechtravel/sequin:<tag>`
  (linux/amd64, ~7 min). Add the release to [CHANGELOG.md](CHANGELOG.md) under a
  dated `(ttN)` heading with its compare link, as the existing entries do.
- `signoff-commit.yml` and the other upstream workflows use upstream's warp runners
  and never run here. CI does not test this fork: run the suite locally (below).
- Upstream's inherited branches are deleted; their tips are kept under hidden
  `refs/archive/<name>` on origin. Restore one with
  `git push origin refs/archive/<name>:refs/heads/<name>`. Upstream has nothing
  this fork lacks.

## Running tests without a local Elixir

`config/test.exs` hardcodes `localhost` for Postgres and Redis, so Postgres and Redis
share the Elixir container's network namespace. Versions match `Dockerfile` and
`signoff-commit.yml`:

```
docker run -d --name sequin-test-elixir -v "$PWD":/app -w /app -e MIX_ENV=test -e MIX_BUILD_PATH=/build/test \
  -e HEX_HOME=/hex -e MIX_HOME=/mix hexpm/elixir:1.19.4-erlang-28.3-debian-trixie-20251208-slim sleep infinity
docker run -d --name sequin-test-pg --network container:sequin-test-elixir -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=sequin_test pgvector/pgvector:0.8.0-pg17 \
  postgres -c wal_level=logical -c max_connections=300 -c max_replication_slots=50 -c max_wal_senders=50
docker run -d --name sequin-test-redis --network container:sequin-test-elixir redis:7
docker exec sequin-test-elixir sh -c 'apt-get update -qq && apt-get install -y -qq build-essential cmake git curl && \
  mix local.hex --force && mix local.rebar --force && mix deps.get && mix compile'
docker exec sequin-test-elixir sh -c 'mix test path/to/test.exs:LINE --include unboxed'
```

- `MIX_BUILD_PATH` keeps Linux build output out of the repo's `_build`. `cmake` is
  for the `crc32cer` NIF. Replication tests are tagged `:unboxed` and need
  `--include unboxed`. The container has `grep`, not `rg`. First compile ~5 min.
- **Resetting the test DB** (e.g. after bumping `my_version` in the UnboxedRepo
  test-tables migration): `mix ecto.drop` hangs because replication slots pin the
  DB. Drop the slots first
  (`psql -U postgres -c "select pg_drop_replication_slot(slot_name) from pg_replication_slots"`
  in `sequin-test-pg`), then `drop database sequin_test`; the next `mix test`
  recreates it.
- A killed `mix` can leave a zombie holding the build-dir lock ("Waiting for lock on
  the build directory"): `kill -9` its pid inside the container.

## Operating gotchas

- **Redis database 0 selects the cluster client** (`set_redis_client/0` in
  `lib/sequin/redis.ex`). `eredis_cluster` ignores the configured host and dials
  whatever `CLUSTER SLOTS` announces. A Dragonfly in `cluster_mode=emulated` with no
  announce flag advertises an IPv4-mapped local address, which is unroutable on an
  IPv6-only private network: every connection times out forever while DNS and a
  direct `:eredis.start_link` by hostname still work (that asymmetry is the
  fingerprint). Set `DFLY_cluster_announce_ip=<private hostname>` (a hostname is
  accepted; a literal IPv6 changes on every redeploy), then restart Sequin, which
  re-reads `CLUSTER SLOTS` only on boot.
- **Short Redis outages are survivable.** `SlotProducer` retries persisting the
  restart WAL cursor ("Failed to persist restart WAL cursor, will retry") and
  `Sequin.Redis` catches client exits, so a Redis restart logs reconnects and
  recovers without tearing down replication. `Sequin.TestSupport.RedisOutage`
  reproduces an outage in tests (`test/sequin/redis_outage_test.exs`); keep that
  test green when touching Redis paths.
- **Typesense sinks can gzip** (`gzip: true`, off by default) for a proxy that
  inflates them. Typesense itself rejects gzip: never point a gzip sink at it directly.
- `out of walsender slots` comes from the **replication source** database, not
  Sequin's own config database. After a crash-loop it usually clears once the old
  instance releases its walsender; confirm with `[SlotProducer] Sending ack for LSN`
  advancing rather than by counting slots.

## Where to look

- [AGENTS.md](AGENTS.md): codebase layout, commands, style
- [CHANGELOG.md](CHANGELOG.md): what each `ttN` release changed
- [CONTRIBUTING.md](CONTRIBUTING.md), [STYLE.md](STYLE.md), [ADD-SINK.md](ADD-SINK.md),
  [ADD-SINK-ROUTING.md](ADD-SINK-ROUTING.md): upstream developer guides
- [docs/](docs/): upstream product docs, preserved as published
- How this fork fits with TripTech's other systems: the system map in TripTech's private repos (`docs/system-map.md`); deliberately not copied into this public repo
