# ADR 0001: MongoDB transport for the Mercure hub

Status: proposed

Date: 2026-10-09

## Context

Mercure hubs are Caddy modules. The hub core handles the Mercure protocol, the
HTTP endpoints, JWT authorization and the SSE server. Storage and cross instance
synchronization are delegated to a `mercure.Transport` implementation.

The built in transports are `local` (memory only) and `bolt` (a single node file
database). Neither one synchronizes several hubs. The goal is a transport that
uses MongoDB as the only shared service: a durable update history and a fan out
channel between hub instances, with no Redis, Kafka or other broker.

Target versions: Mercure v1.1.0, Caddy v2.11.7, Go 1.27, MongoDB Go driver v2.

## Transport contract (observed in Mercure v1.1.0)

`mercure.Transport` (transport.go) has four methods:

* `Dispatch(ctx, *Update) error`
* `AddSubscriber(ctx, *LocalSubscriber) error`
* `RemoveSubscriber(ctx, *LocalSubscriber) error`
* `Close(ctx) error`

An optional `TransportSubscribers` adds `GetSubscribers(ctx) (string, []*Subscriber, error)`.
An optional `TransportHealthChecker` adds `Ready(ctx) error` and `Live(ctx) error`.

Contract details that constrain the design:

* `Dispatch` takes ownership of the update and may set its ID with `AssignUUID`.
  A custom `Last-Event-ID` set by the publisher must be kept as is.
* `AddSubscriber` must call `s.HistoryDispatched(id)` exactly once when
  `s.RequestLastEventIDSet` is true, even for an empty requested ID. The
  subscribe handler blocks on this call before writing the response headers.
* History events are sent with `s.Dispatch(ctx, update, true)`, live events with
  `s.Dispatch(ctx, update, false)`. A subscriber that is not yet `Ready` buffers
  live events in an internal queue, so the transport can replay history first and
  then call `s.Ready(ctx)` to flush live events in order.
* `SubscriberList.MatchAny` and `Subscriber.Match` do topic and private topic
  authorization. The transport must use them, never its own matcher.

## Decisions

### 1. Append only event document

Each update is one immutable document in an events collection:

```
{
  _id:            int64,   // internal cursor, gapless and monotonic
  mercure_id:     string,  // visible Mercure update ID (custom or urn:uuid:...)
  topics:         [string],
  private:        bool,
  data:           string,
  type:           string,  // optional
  retry:          int64,   // optional
  debug:          bool,
  created_at:     date,
  schema_version: int32
}
```

The visible Mercure ID (`mercure_id`) is separate from the internal cursor
(`_id`). A publisher may send its own ID, so the visible ID is not unique and
not ordered. The cursor is always server assigned.

Documents are never updated. Retention deletes whole documents. The schema
version allows a future migration.

### 2. Write ordering

The cursor `_id` is a gapless sequence assigned at insert time. It is the total
order for replay and for the replay/live boundary.

The sequence is assigned optimistically: read the current maximum `_id`, try to
insert with `max + 1`, and on a duplicate key error read the maximum again and
retry. A unique index on `_id` (the default) enforces this. Two properties hold:

* The committed set of `_id` values is always contiguous (`1..M`). An insert of
  `k` only succeeds when `k - 1` already exists, because the candidate is always
  `observedMax + 1` and `observedMax` never exceeds the committed maximum.
* Because the set is contiguous, commit order equals `_id` order.

The contiguity argument needs two reads to observe the latest committed state,
so the transport reads from the primary and writes with `w:majority`. A
secondary read could return a stale maximum and an acknowledgement from a rolled
back write would break the proof.

Neither an ObjectID nor a wall clock timestamp gives this property. An ObjectID
carries a random part and a per process counter, and a timestamp is not
monotonic with the commit order. They are not used as the order.

Cost: concurrent publishers on the same collection collide on the candidate and
retry. Under a single hub the transport serializes local candidate allocation
with a mutex, so local publishes do not collide with each other. Across hubs,
collisions are resolved by the unique index and a bounded retry with backoff.
Publish throughput is therefore bounded by insert latency times the retry rate,
not by fan out. Fan out happens outside the allocation lock (see decision 6).
Under cross hub contention the retry rate grows with the number of concurrent
publishers, so this transport favors correctness over raw publish throughput.

### 3. Replay and retention

Indexes:

* `_id` (default, unique): cursor order, range replay, retention.
* `{mercure_id: 1, _id: 1}`: resolve a requested `Last-Event-ID` to its earliest
  occurrence in one ordered lookup.

Retention is count based only (`retention`, the number of newest events kept).
The default is 0, meaning unlimited, matching the Bolt transport default of
`size = 0`. A cleanup job runs on a timer and deletes the events older than the
newest `retention` events.

There is no age based retention. A TTL index would delete the newest document
when it is also the oldest, which could make the sequence restart at 1. A
restart would break the replay/live boundary and the watermark. Retention is
therefore always `DELETE WHERE _id < boundary` with the newest document kept.

When the events collection is dropped or renamed while a hub is running, the
change stream sends an `invalidate` event. The hub logs an error and resets its
watermark, because the sequence restarted at 1. Dropping the collection is a
destructive operator action: history is lost by design.

A requested `Last-Event-ID` that resolves to no document (never seen, or deleted
by retention) makes the transport answer `earliest`. This tells the client to
reload its state and reconnect, which is the documented behavior of the Bolt
transport. The advertised contract matches the data that is actually kept: if
the operator sets a small retention, reconnects beyond that window get `earliest`.

A requested `Last-Event-ID` that resolves to a document resolves to the earliest
occurrence of that ID. If a publisher reused an ID, the client is replayed from
the first occurrence, so it can receive an update it already had. This is a
deliberate at least once choice: replaying too much is safe, skipping is not.

### 4. Change streams and checkpoints

Each hub opens one change stream on the events collection. The stream is not
filtered by operation type, because the consumer must also see `invalidate`
events (decision 3) and skip retention `delete` events. Insert events carry the
full document.

The hub persists a checkpoint `{resume_token, seq, updated_at}` in a checkpoints
collection, keyed by the hub name (the Caddy `name` directive, `default` when
unset). This keeps two differently named hubs on the same collection isolated.
The checkpoint is written on a timer (`checkpoint_interval`, default 1s) and on
close, not on every event, to bound write load.

On startup the hub resumes the change stream with `resumeAfter` when a token
exists. When the token is no longer in the oplog, MongoDB returns a change
stream history lost error. The hub then catches up from the durable collection:
it reads the current maximum `_id`, delivers every event in
`(appliedWatermark, max]` to the connected subscribers, and opens a fresh change
stream from the current time. Connected subscribers do not lose the events
published during the interruption, because the catch up delivers them.

Whenever the hub opens a stream without a resume token, it opens the stream
first and then catches up. The stream captures events committed from that point
on, and the catch up fills the range that predates the stream. The order matters:
a catch up that ran before the stream opened could miss an event committed in
between, and one that ran concurrently with stream delivery could let a high
sequence overtake a low one. Opening first, then catching up sequentially,
leaves no such window. Overlap at the boundary is dropped by the watermark.

The consumer also detects gaps without a stream error. Because the sequence is
gapless, an insert whose `seq` is above `appliedWatermark + 1` means the hub
missed events. The consumer reads the missing range from the durable collection
and delivers it in order before the event that revealed the gap. This makes
delivery robust to a resume that silently skipped documents.

A gap that the durable collection can no longer fill is handled explicitly. When
retention removed events a lagging hub still needs, the missing range is gone for
good. Retrying would loop, so the consumer disconnects every subscriber and fast
forwards the watermark past the hole. Each client reconnects with its
`Last-Event-ID` and replays from the durable collection, or is answered
`earliest` when its position was removed. The transport never advances the
watermark past a gap it could not fill, and it never delivers the event that
revealed the gap before the range is complete. A catch up that fails for a
transient reason is retried, and the hub does not report itself ready until the
catch up succeeded.

The hub exposes `Lag(ctx)`, the number of committed events it has not applied
yet, so an operator can see a lagging instance.

### 5. Guarantees after a crash

Delivery to SSE clients is best effort with at least once semantics, never
exactly once. Subscribers live in hub memory and are lost on a crash, so a crash
cannot duplicate a delivery to a live client. After a restart the hub replays
from a checkpoint, or from the durable maximum when the token expired. Because
the change stream is read with a single consumer and an applied watermark
(`appliedWatermark`), a resumed stream cannot dispatch the same event twice to
the same subscriber: an event with `seq <= appliedWatermark` is dropped.

A publisher retry after an ambiguous write error can store the same update twice,
under two sequences and one visible ID. This is the source of the end to end at
least once behavior, and it is why a repeated ID replays from its earliest
occurrence (decision 3).

The checkpoint is written periodically, so a crash loses at most
`checkpoint_interval` of checkpoints. On resume the hub replays the durable
events after the checkpoint to rebuild its watermark. These events are not sent
to any client, because there are no clients yet at startup. The only observable
effect is a bounded startup catch up.

No exactly once claim is made. The README states the at least once model and the
`Last-Event-ID` reconnect contract.

### 6. Multi instance behavior and the local/change stream duplicate

Every hub keeps its own SSE connections and subscriptions in memory. All hubs
consume the same change stream, so a publish on hub A reaches a subscriber on
hub B through MongoDB.

All delivery, including delivery on the publishing hub, goes through the change
stream. The publishing hub does not broadcast to its local subscribers inside
`Dispatch`. This removes the local publish / change stream duplicate by
construction: there is a single ordered delivery path, so an update is never
delivered twice and never out of order. It also decouples publish throughput
from fan out, because the fan out runs in the change stream consumer and not in
the publish path.

Trade off: delivery latency includes the change stream latency (typically a few
milliseconds), and delivery requires a healthy change stream. The transport
implements `TransportHealthChecker` so a hub can report not ready when the stream
is down, and it reconnects with exponential backoff. The README documents this.

`GetSubscribers` reports only the subscribers connected to this hub instance,
because the subscriber list is local. This matches the Bolt transport.

### 7. Replay/live coherence

`AddSubscriber` runs as follows:

1. Resolve the requested `Last-Event-ID` to the highest sequence the client
   already has, outside the lock, because it needs a database lookup.
2. Take a snapshot `bound` of the applied watermark and register the subscriber
   in the same critical section, shared with the consumer. This makes the
   snapshot and the registration atomic: no event can be delivered between them.
3. Set the subscriber live floor to `max(bound, resolvedSeq)`. The floor covers
   both the replay snapshot and the position the client already reached. A hub
   that lags behind the client (its `bound` is below `resolvedSeq`) must not
   redeliver events the client already has, so the floor is the maximum.
4. If `RequestLastEventIDSet`, replay the durable range `(resolvedSeq, bound]`
   in order and dispatch matching events as history. When the requested ID is
   unknown, answer `earliest` without replay.
5. Call `s.Ready(ctx)` to flush buffered live events.

The change stream consumer dispatches a live event to a subscriber only when
`seq > liveFloor`. Events with `seq <= bound` belong to history and are replayed.
Because the committed set is contiguous, there is no event with `seq <= bound`
that is not already in the collection, so there is no gap. Because replay stops
at `bound` and live delivery starts above the floor, there is no duplicate.
During replay the subscriber is not ready, so any live event is buffered and
flushed after history, in order.

### 8. MongoDB dependency

The transport requires a replica set or a sharded cluster. Change streams are
not available on a standalone server. `Provision` verifies this: it reads the
topology and fails with an actionable error when the deployment is a standalone.
Integration tests run against a replica set, not a standalone.

## Consequences

* A durable, ordered history with an exact, tested reconnect contract.
* Multi instance synchronization with one shared MongoDB deployment and no extra
  broker.
* Publish throughput is bounded by the optimistic sequence under cross hub
  contention. This favors correctness over throughput and is documented.
* Delivery depends on the change stream. The transport surfaces its health and
  its lag.

## Alternatives rejected

* ObjectID or timestamp ordering: no proof of a total order consistent with
  commits (see decision 2).
* A global counter document with `$inc`: simple, but the counter value is
  assigned before the insert commits, so concurrent inserts can commit out of
  counter order. That breaks the contiguity needed by the replay/live boundary.
* Local immediate broadcast plus change stream: reintroduces the duplicate and
  the cross source ordering problem (see decision 6).
* Age based TTL retention: can restart the sequence at 1 (see decision 3).
