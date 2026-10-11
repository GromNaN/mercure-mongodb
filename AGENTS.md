# AGENTS.md

Implementation notes for coding agents. The [README](README.md) is the
user-facing documentation. This file holds the details that only matter when you
work on the code, the release pipeline or the container image.

## What this is

A MongoDB transport for the Mercure hub, packaged as a Caddy module. It
registers `http.handlers.mercure.mongodb`, so a hub configuration can use
`transport mongodb { ... }`. It stores the update history in MongoDB and uses a
change stream for cross instance delivery.

The design record is
[`docs/adr/0001-mongodb-transport.md`](docs/adr/0001-mongodb-transport.md). Read
it before you change the ordering, replay or retention logic.

Target versions: Mercure v1.1.0, Caddy v2.11.7, Go 1.27, MongoDB Go driver v2.

## Repository layout

| Path | Role |
| --- | --- |
| `caddy.go` | Caddy module: config parsing, defaults, provisioning, transport pool key |
| `transport.go` | `MongoDBTransport`, the change stream consumer, the replay and live boundary |
| `store.go` | `store` interface, plus the in memory implementation used by the unit tests |
| `mongo_store.go` | MongoDB storage: sequence allocation, collections, indexes, change stream |
| `document.go` | Mapping between a Mercure update and its MongoDB document |
| `errors.go` | Sentinel errors |
| `caddy/mercure/main.go` | Hub entry point; blank imports the Mercure handler and this transport |
| `load/` | SSE load generator |
| `Caddyfile` | Default configuration baked into the image |
| `examples/Caddyfile` | Example configuration used by the quick start |
| `docker-compose.yml` | Development replica set (MongoDB Atlas Local) |
| `.github/scripts/smoke-test-image.sh` | CI smoke test of the built image |
| `.github/scripts/collect-licenses.sh` | Builds the license tree for the archives and the image |

## Build and test

```console
make build              # go build ./...
make test               # go test -race ./...
make test-integration   # needs a replica set
make docker             # hub binary for the host architecture, then the image
make licenses           # regenerates the license tree
make fmt vet tidy
```

Unit tests need no MongoDB. They run against the `memoryStore` from `store.go`,
which is what makes the ordering and boundary logic testable without a replica
set. Integration tests are skipped unless `MONGODB_URI` is set, and they need a
replica set, because change streams do not exist on a standalone server.

```console
docker compose up -d --wait
make test-integration
```

`docker-compose.yml` publishes MongoDB Atlas Local on `localhost:27017`. It is a
single node replica set that initializes itself, so change streams work
immediately.

The load generator opens many concurrent subscriptions, publishes a stream of
updates and reports the publish to client latency.

```console
go run ./load \
  -hub http://localhost:3000/.well-known/mercure \
  -publish-token "$PUBLISHER_JWT" \
  -subscribe-token "$SUBSCRIBER_JWT" \
  -clients 2000 -messages 2000
```

Run it against the hardware and configuration you plan to use, and publish the
results with that context. The numbers depend on the machine, the network, the
MongoDB deployment and the topic fan-out. They are not a guarantee for other
setups.

## How it works

Three collections in one database: `updates` holds the durable history,
`checkpoints` holds the resume token and applied watermark of each hub,
`seen_ids` holds the visible ID markers and stays bounded to the retention
window.

`_id` is the internal gapless sequence. Writes use `w:majority` and reads go to
the primary, so the sequence stays gapless across a failover.

Indexes, created by `ensureIndexes`: the default `_id` index serves replay,
retention and cursor order; a compound `{mercure_id: 1, _id: 1}` resolves a
repeated Mercure ID to its earliest occurrence in the window in one ordered
lookup; `seen_ids` gets `{last_at: 1}` so retention can drop the markers that
left the window. Creating an index also creates the collection, so the change
stream always has a namespace to watch.

Sequence allocation: a local mutex serializes allocation and insert inside one
process. Collisions between processes are resolved by the unique `_id` index and
a retry. `hint` is the highest sequence this process knows to be committed, and
it is always at or below the real maximum, so `hint+1` never skips a value.

The applied watermark `committed` only advances once the consumer has delivered
an event, so a local publish never hides its own event from the consumer.

`liveFloors` holds, per subscriber, the highest sequence that must not be
delivered live. It covers both the replay snapshot and the sequence resolved
from the requested `Last-Event-ID`. Replay sends history up to the snapshot,
live delivery starts above the floor, and the consumer fills any gap from the
durable collection. That is the no gap, no duplicate boundary.

A dropped or renamed `updates` collection sends an invalidate event. The
transport logs it and resets its watermark through `ResetSequence`. Retention
that outruns a subscriber produces `errGapExpired`, and the hub disconnects the
subscribers that are behind so they replay from their `Last-Event-ID`.

Provisioning never contacts MongoDB. `storeOpener` defers the connection, so the
hub starts while the database is down and the consumer retries with exponential
backoff. `Ready` reports not ready until the change stream is up.

## Build tags

`deprecated_transport`, `deprecated_topic` and `deprecated_claim` compile in the
Mercure v0.x compatibility mode, which only takes effect when a configuration
sets `protocol_version_compatibility`. `nobadger`, `nomysql` and `nopgx` match
the upstream Mercure release build; Mercure v1.1.0 no longer reads them.

The same list appears in `.goreleaser.yml`, in the `HUB_TAGS` variable of the
`Makefile` and in the `docker` job of `.github/workflows/ci.yml`. Keep the three
in sync.

## Release pipeline

1. A `v*.*.*` tag is pushed. That is the only automatic trigger;
   `workflow_dispatch` must also target a version tag, and the job fails
   otherwise.
2. `.github/workflows/cd.yml` runs the `check` job: build, vet, unit tests and
   `goreleaser check`. The `release` job needs it.
3. The `release` job runs GoReleaser, which builds both platforms, creates the
   archives, generates one SPDX SBOM per archive with syft, and builds and
   pushes the multi architecture image to `ghcr.io/gromnan/mercure-mongodb`.
4. `release.draft: true` creates the GitHub release as a draft. The image is
   pushed in the same run, so it is public before the release is published.

Details worth knowing:

- The image tag set is the version tag plus `latest`, and `latest` is suppressed
  on a prerelease so it keeps pointing at the newest stable release.
- `dockers_v2` builds from the repository `Dockerfile`. `extra_files` populates
  a temporary build context; it takes no wildcards, so a directory is listed
  whole, and `gio.Copy` copies it recursively with the layout preserved.
- `sboms` produces the archive documents and needs syft, which the workflow
  installs. `dockers_v2.sbom: true` attaches an SBOM attestation to the image
  manifest.
- GoReleaser works with an existing release instead of failing, and
  `release.mode` defaults to `keep-existing`, so it does not overwrite release
  notes you wrote by hand.

## License bundling

The project is AGPL 3.0, because the hub binary links the AGPL Mercure hub.
`LICENSE` is the AGPL text and `COPYRIGHT` carries the notice.

A distribution must also carry the notices of everything it links.
`.github/scripts/collect-licenses.sh` walks the dependency graph of
`./caddy/mercure` with `go list -deps`, copies the license and notice files of
each module into `licenses/<module path>/`, and prints a summary. The tree is
generated, never committed: `licenses/` is in `.gitignore`.

The tree reaches both distributions. In the archives, the `before` hook runs the
script and `archives.files` picks up `licenses/**/*`. In the image,
`dockers_v2.extra_files` copies the tree into the build context and the
Dockerfile copies it to `/usr/share/licenses/mercure-mongodb/`, with `LICENSE`
and `COPYRIGHT` at the top of that directory.

The Mercure `COPYRIGHT` must be preserved in any redistribution. The script
copies it along with the module licenses. Do not remove it.

The `org.opencontainers.image.licenses` label is an SPDX expression, so it uses
the SPDX identifiers rather than the plain names in the README table.

## Container image

`FROM caddy:2-alpine`, plus the hub binary at `/usr/bin/caddy` and the default
`Caddyfile` at `/etc/caddy/Caddyfile`. The binary is built by GoReleaser for
each platform and laid out in the build context under `${TARGETPLATFORM}`.

The healthcheck queries the Caddy admin API on
`127.0.0.1:2019/mercure/health/ready`, which reports transport readiness rather
than process liveness.

`.dockerignore` allows only the files the Dockerfile copies. When you add a
`COPY`, add the path there too. Otherwise the local and CI builds fail while the
GoReleaser build keeps working, because GoReleaser builds from its own temporary
context and never reads `.dockerignore`.

## Conventions

- Comments, commit messages and PR titles and bodies in plain language English.
  No em dashes anywhere.
- Go code is standard `gofmt`. Run `make fmt vet` before you commit.
- Every text file ends with a newline.
- A pull request body never contains a "Test plan" section and never starts with
  a "Summary" heading.
- `caddy.go` builds a transport key from every effective option, with the URI
  hashed so credentials never appear. An omitted option and its explicit default
  must produce the same key. Otherwise equivalent handlers open duplicate change
  streams and duplicate checkpoint and cleanup loops.

## Gotchas

- The transport refuses to start on a standalone MongoDB and does not support a
  sharded cluster. `checkChangeStreamSupport` enforces both.
- Retention is count based only. A TTL index is not an option, because it could
  restart the internal sequence.
- Do not drop or rename the `updates` collection while hubs run.
- `GetSubscribers` reports only the subscribers of the local instance. The
  Enterprise Redis and Valkey transport shares the list across the cluster; this
  one does not.
- The CI `docker` job builds the binary with the release build tags before it
  builds the image, so the smoke test exercises what actually ships. Keep it in
  sync with `HUB_TAGS`.
- The image is public as soon as the release workflow pushes it, even while the
  GitHub release stays a draft.
