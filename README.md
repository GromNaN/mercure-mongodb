# mercure-mongodb

[![CI](https://github.com/GromNaN/mercure-mongodb/actions/workflows/ci.yml/badge.svg)](https://github.com/GromNaN/mercure-mongodb/actions/workflows/ci.yml)
[![License: AGPL-3.0-or-later](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue)](LICENSE)
[![Go 1.27](https://img.shields.io/badge/go-1.27-00ADD8)](https://go.dev/)

A [MongoDB](https://www.mongodb.com/) transport for the [Mercure](https://mercure.rocks) hub.

It stores the update history in MongoDB and uses a MongoDB change stream to
synchronize hub instances. MongoDB is the only shared service: there is no
Redis, no Kafka, and no other broker.

- Durable history. A client that reconnects with `Last-Event-ID` is replayed
  from MongoDB, even if it lands on another hub instance.
- Multi instance by default. Every hub reads the same change stream, so a
  publish on one instance reaches subscribers on all of them.
- One dependency to operate. The same MongoDB deployment serves storage and fan
  out.

The transport plugs into the standard Mercure hub. The protocol, the HTTP
endpoints, JWT authorization and the SSE server stay in Mercure. This project
only provides storage and cross instance synchronization.

## Table of contents

- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Guarantees and limits](#guarantees-and-limits)
- [Multi instance](#multi-instance)
- [Production notes](#production-notes)
- [Load testing](#load-testing)
- [Development](#development)
- [License](#license)

## How it works

Each update is one immutable document in an `updates` collection. Every hub
instance opens a change stream on that collection. A publish appends a document;
the change stream delivers it to every hub, including the one that published, so
there is a single ordered delivery path and no duplicate.

```mermaid
flowchart TB
    Pub["Publisher"]

    subgraph Hubs["Caddy hubs (Mercure)"]
        A["hub A<br/>mercure handler<br/>mongodb transport"]
        B["hub B<br/>mercure handler<br/>mongodb transport"]
    end

    S1["Subscribers"]
    S2["Subscribers"]

    M[("MongoDB replica set<br/>updates + checkpoints")]

    Pub -->|"POST /hub"| A
    Pub -->|"POST /hub"| B

    A -->|"insert one event"| M
    B -->|"insert one event"| M

    M -.->|"change stream"| A
    M -.->|"change stream"| B

    A -->|SSE| S1
    B -->|SSE| S2
```

The internal cursor is a gapless sequence stored as `_id`. A hub keeps an
applied watermark and, for each subscriber, a live floor. Replay sends history
up to the snapshot, live delivery starts above the floor, and the change stream
consumer fills any gap from the durable collection. This gives a clean
replay/live boundary with no gap and no duplicate. See
[`docs/adr/0001-mongodb-transport.md`](docs/adr/0001-mongodb-transport.md) for
the full design and its trade-offs.

## Requirements

- A MongoDB replica set or a sharded cluster. Change streams are not available
  on a standalone server, so the transport refuses to start on one.
- A Caddy build that includes the Mercure handler and this transport.

Supported versions:

| Component           | Version          |
| ------------------- | ---------------- |
| Mercure             | v1.1.0           |
| Caddy               | v2.11.7          |
| Go                  | 1.27             |
| MongoDB Go driver   | v2.9.2           |
| MongoDB server      | 6.0 or newer     |

## Quick start

### 1. Start a development replica set

```console
docker compose up -d --wait
```

This starts [MongoDB Atlas Local](https://www.mongodb.com/docs/atlas/cli/current/atlas-cli-local-deploy/)
on `localhost:27017`. It is a single node replica set that initializes itself, so
change streams work out of the box. The `--wait` flag returns once the image
healthcheck passes.

### 2. Build the hub

```console
xcaddy build \
  --with github.com/dunglas/mercure/caddy \
  --with github.com/GromNaN/mercure-mongodb
```

### 3. Configure the hub

Use the example configuration, or write your own:

```caddyfile
http://localhost:3000 {
	mercure {
		transport mongodb {
			uri {$MERCURE_MONGODB_URI}
			database mercure
			collection updates
			retention 10000
		}

		publisher_jwt {$MERCURE_PUBLISHER_JWT_KEY} HS256
		subscriber_jwt {$MERCURE_SUBSCRIBER_JWT_KEY} HS256
	}
}
```

See [`examples/Caddyfile`](examples/Caddyfile) for the full file.

### 4. Run the hub

```console
export MERCURE_MONGODB_URI='mongodb://localhost:27017/?directConnection=true'
export MERCURE_PUBLISHER_JWT_KEY='change-me'
export MERCURE_SUBSCRIBER_JWT_KEY='change-me'
./caddy run --config examples/Caddyfile
```

The hub is now available at `http://localhost:3000`.

### 5. Publish and subscribe

Mint two tokens for the development keys. The example uses a short Python
snippet, because the JSON Web Token format is standard.

```console
mint() {
  python3 - "$1" "$2" <<'PY'
import base64, hashlib, hmac, json, sys
key, claim = sys.argv[1].encode(), sys.argv[2]
b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
head = b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
body = b64(json.dumps({"mercure": json.loads(claim)}, separators=(",", ":")).encode())
sig = b64(hmac.new(key, f"{head}.{body}".encode(), hashlib.sha256).digest())
print(f"{head}.{body}.{sig}")
PY
}

PUBLISHER_JWT=$(mint "$MERCURE_PUBLISHER_JWT_KEY" '{"publish":["*"]}')
SUBSCRIBER_JWT=$(mint "$MERCURE_SUBSCRIBER_JWT_KEY" '{"subscribe":["*"]}')
```

Subscribe in one terminal:

```console
curl -N http://localhost:3000/hub \
  -H "Authorization: Bearer $SUBSCRIBER_JWT" \
  --get --data-urlencode 'topic=https://example.com/books/1'
```

Publish in another:

```console
curl -X POST http://localhost:3000/hub \
  -H "Authorization: Bearer $PUBLISHER_JWT" \
  --data-urlencode 'topic=https://example.com/books/1' \
  --data-urlencode 'data=hello'
```

The subscriber receives the update through MongoDB.

## Configuration

The transport is configured with the `mongodb` block inside `mercure`:

```caddyfile
mercure {
	transport mongodb {
		uri mongodb://localhost:27017/?directConnection=true
		database mercure
		collection updates
		retention 10000
	}
}
```

| Directive             | Default       | Description                                                        |
| --------------------- | ------------- | ------------------------------------------------------------------ |
| `uri`                 |               | MongoDB connection string. Required. It may contain credentials.   |
| `database`            | `mercure`     | Database that holds the events and checkpoints.                    |
| `collection`          | `updates`     | Collection of update documents.                                    |
| `checkpoints`         | `checkpoints` | Collection of hub checkpoints (change stream resume tokens).      |
| `retention`           | `0`           | Number of newest updates kept. `0` keeps everything.               |
| `cleanup_interval`    | `30s`         | How often the retention cleanup runs.                              |
| `checkpoint_interval` | `1s`          | How often the resume token is saved.                               |
| `watch_await_time`    | `1s`          | Maximum time the change stream waits for a batch.                  |
| `connect_timeout`     | `10s`         | Connection and server selection timeout.                           |

Keep credentials out of the Caddyfile. Pass the connection string through the
`MERCURE_MONGODB_URI` environment variable, as in the example above. The
transport never logs the connection string.

## Guarantees and limits

- **At least once.** An update is persisted once and delivered at least once to
  a connected subscriber. A publisher retry after an ambiguous write error can
  store the same update twice. Duplicates are possible; gaps are not.
- **`Last-Event-ID`.** A reconnect replays the updates published after the given
  ID. When the ID is unknown, or was removed by retention, the hub answers with
  `earliest` so the client can reload its state. This matches the Bolt
  transport.
- **Change stream dependency.** Delivery needs a healthy change stream. The
  transport reports its health through `Ready` and `Live`, and reconnects with
  exponential backoff. A hub that loses the stream catches up from the durable
  collection.
- **Count-based retention only.** Retention keeps the newest N updates. There is
  no age-based expiry, because a TTL index could restart the internal sequence
  and break the replay/live boundary. Use count-based `retention` instead.
- **Do not drop the collection while hubs run.** Dropping or renaming the
  `updates` collection restarts the internal sequence. The transport logs an
  error and resets its watermark. This is a destructive operator action.

Non-goals:

- This project does not implement the Mercure protocol, the HTTP endpoints, JWT
  authorization or the SSE server. Those stay in the Mercure hub.
- It does not persist subscribers. Subscribers and their SSE connections live in
  hub memory and are lost on restart, as in every Mercure transport.
- It does not provide exactly-once delivery.

## Multi instance

Run as many hub instances as you need, all pointing at the same database and
collection. Each instance keeps its own SSE connections in memory and its own
change stream. A subscriber connects to one instance; a publish on any instance
reaches it through MongoDB.

Give each instance a distinct Caddy `name` so their checkpoints do not collide.
Two instances with the same name share one checkpoint, which only affects resume
after a restart, not correctness.

## Production notes

- **Write concern.** The transport writes with `w:majority` and reads from the
  primary, so the internal sequence stays gapless across a failover.
- **Minimal MongoDB permissions.** The transport needs `readWrite` on the
  database that holds the events and checkpoints collections, and `read` on the
  `admin` database to detect a standalone deployment. Grant nothing more.
- **Indexes.** The transport creates a compound index on
  `{mercure_id: 1, _id: 1}` to resolve `Last-Event-ID` lookups. The default
  `_id` index serves replay, retention and cursor order.
- **Oplog window.** A longer oplog lets a hub resume a change stream after a
  longer outage without a full catch up. Size the oplog for the outage you want
  to tolerate.
- **Backups.** The `updates` collection is the durable history. Back it up with
  your usual MongoDB tooling. Restoring it restores replay for `Last-Event-ID`.
- **Reverse proxy.** Configure your proxy to keep SSE connections open: disable
  response buffering and raise the read timeout above the hub heartbeat.
- **Observability.** The transport exposes `Ready`, `Live` and `Lag`. A hub that
  reports not ready, or a growing lag, points to a change stream or MongoDB
  problem.
- **Troubleshooting.** If the hub refuses to start, check that the deployment is
  a replica set and that `uri` is set. If clients stop receiving updates, check
  the change stream health and the MongoDB logs.

## Load testing

The repository includes a small SSE load generator in [`load`](load). It opens
many concurrent subscriptions, publishes a stream of updates, and reports the
publish to client latency.

```console
go run ./load \
  -hub http://localhost:3000/hub \
  -publish-token "$PUBLISHER_JWT" \
  -subscribe-token "$SUBSCRIBER_JWT" \
  -clients 2000 -messages 2000
```

Run it against the hardware and configuration you plan to use, and publish the
results with that context. The numbers depend on the machine, the network, the
MongoDB deployment and the topic fan-out. They are not a guarantee for other
setups.

## Development

Run the unit tests. They need no MongoDB:

```console
make test
```

Run the integration tests against a replica set:

```console
docker compose up -d --wait
make test-integration
```

The integration tests are skipped unless `MONGODB_URI` is set. They require a
replica set, because change streams are not available on a standalone server.

## License

[AGPL-3.0-or-later](LICENSE), the same license as Mercure. If you run a
modified version of this software as a network service, the AGPL requires you to
offer the corresponding source to its users.
