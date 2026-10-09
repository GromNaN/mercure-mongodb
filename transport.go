package mercuremongodb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dunglas/mercure"
)

// Default configuration values.
const (
	DefaultDatabase           = "mercure"
	DefaultCollection         = "updates"
	DefaultCheckpoints        = "checkpoints"
	DefaultSeen               = "seen_ids"
	DefaultCleanupInterval    = 30 * time.Second
	DefaultCheckpointInterval = time.Second
	DefaultWatchAwaitTime     = time.Second
	DefaultConnectTimeout     = 10 * time.Second

	// minEarliestReplayWindow bounds how many events an "earliest" request
	// replays when retention is unlimited, so a reconnect cannot read the whole
	// collection.
	minEarliestReplayWindow = 10000

	// reconnect backoff bounds for the change stream consumer.
	reconnectMin = 250 * time.Millisecond
	reconnectMax = 30 * time.Second
)

// errInvalidate signals that the change stream sent an invalidate event, which
// means the events collection was dropped or renamed and the sequence restarted.
var errInvalidate = errors.New("mercure mongodb transport: events collection was dropped or renamed")

// errGapExpired signals that the durable events needed to fill a change stream
// gap were removed by retention. The hub cannot serve a contiguous stream to
// the subscribers that are behind, so it disconnects them and they replay from
// their Last-Event-ID.
var errGapExpired = errors.New("mercure mongodb transport: the events needed to fill a change stream gap were removed by retention")

// Config is the transport configuration. It is filled by the Caddy module and
// can also be used to build a transport directly.
type Config struct {
	// URI is the MongoDB connection string. It may contain credentials and is
	// never logged.
	URI string

	Database    string
	Collection  string
	Checkpoints string

	// Seen is the collection that records which visible Mercure IDs have ever
	// been published. It is not subject to retention, so a repeated ID is never
	// mistaken for a first occurrence after the earlier one was deleted.
	Seen string

	// Retention is the number of newest events kept. 0 keeps everything.
	Retention int64

	CleanupInterval    time.Duration
	CheckpointInterval time.Duration
	WatchAwaitTime     time.Duration
	ConnectTimeout     time.Duration

	// HubName isolates checkpoints of hubs that share a database.
	HubName string
}

func (c *Config) withDefaults() {
	if c.Database == "" {
		c.Database = DefaultDatabase
	}

	if c.Collection == "" {
		c.Collection = DefaultCollection
	}

	if c.Checkpoints == "" {
		c.Checkpoints = DefaultCheckpoints
	}

	if c.Seen == "" {
		c.Seen = DefaultSeen
	}

	if c.CleanupInterval <= 0 {
		c.CleanupInterval = DefaultCleanupInterval
	}

	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = DefaultCheckpointInterval
	}

	if c.WatchAwaitTime <= 0 {
		c.WatchAwaitTime = DefaultWatchAwaitTime
	}

	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
}

// MongoDBTransport implements mercure.Transport on top of MongoDB. It stores a
// durable, ordered history of updates and uses a change stream to deliver them
// to every hub instance.
//
// Delivery goes through the change stream for all hubs, including the one that
// published. This keeps a single ordered delivery path, so an update is never
// delivered twice and never out of order, and it decouples publish throughput
// from fan out.
type MongoDBTransport struct {
	store       store
	subscribers *mercure.SubscriberList
	logger      *slog.Logger

	retention          int64
	cleanupInterval    time.Duration
	checkpointInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closed     chan struct{}
	closedOnce sync.Once

	// mu guards committed, lastEventID, lastToken and liveFloors.
	//
	// committed is the applied watermark. It only advances when the consumer has
	// delivered an event, so a local publish never hides its own event from the
	// consumer. liveFloors holds, per subscriber, the highest sequence the
	// subscriber must not receive live: it covers both the replay snapshot and
	// the sequence resolved from the requested Last-Event-ID.
	mu          sync.Mutex
	committed   int64
	lastEventID string
	lastToken   []byte
	liveFloors  map[*mercure.LocalSubscriber]int64

	healthy atomic.Bool
}

// NewMongoDBTransport connects to MongoDB and starts the change stream consumer.
func NewMongoDBTransport(ctx context.Context, subscribers *mercure.SubscriberList, logger *slog.Logger, cfg Config) (*MongoDBTransport, error) {
	cfg.withDefaults()

	st, err := newMongoStore(ctx, mongoConfig{
		URI:            cfg.URI,
		Database:       cfg.Database,
		Collection:     cfg.Collection,
		Checkpoints:    cfg.Checkpoints,
		Seen:           cfg.Seen,
		HubName:        cfg.HubName,
		WatchAwaitTime: cfg.WatchAwaitTime,
		ConnectTimeout: cfg.ConnectTimeout,
	})
	if err != nil {
		return nil, err
	}

	t, err := newTransportWithStore(ctx, st, subscribers, logger, cfg)
	if err != nil {
		_ = st.Close(context.Background())

		return nil, err
	}

	return t, nil
}

// newTransportWithStore builds the transport on an existing store. It is shared
// by the public constructor and by the unit tests, which inject a memory store.
func newTransportWithStore(ctx context.Context, st store, subscribers *mercure.SubscriberList, logger *slog.Logger, cfg Config) (*MongoDBTransport, error) {
	cfg.withDefaults()

	consumerCtx, cancel := context.WithCancel(context.Background())

	t := &MongoDBTransport{
		store:              st,
		subscribers:        subscribers,
		logger:             logger,
		retention:          cfg.Retention,
		cleanupInterval:    cfg.CleanupInterval,
		checkpointInterval: cfg.CheckpointInterval,
		ctx:                consumerCtx,
		cancel:             cancel,
		closed:             make(chan struct{}),
		lastEventID:        mercure.EarliestLastEventID,
		liveFloors:         make(map[*mercure.LocalSubscriber]int64),
	}

	// The applied watermark starts at the durable maximum. Anything committed
	// before this point is history for a future subscriber, not a live event.
	max, err := st.MaxSeq(ctx)
	if err != nil {
		cancel()
		_ = st.Close(context.Background())

		return nil, err
	}

	t.committed = max

	latest, err := st.Latest(ctx)
	if err != nil {
		cancel()
		_ = st.Close(context.Background())

		return nil, err
	}

	if latest != nil {
		t.lastEventID = latest.MercureID
	}

	t.wg.Add(2)
	go t.consume()
	go t.checkpointLoop()

	if cfg.Retention > 0 {
		t.wg.Add(1)
		go t.cleanupLoop()
	}

	return t, nil
}

// Dispatch persists the update and assigns its sequence. Delivery to
// subscribers happens through the change stream, so Dispatch returns once the
// update is durable.
//
// A publisher retry after an ambiguous write error can store the same update
// twice, with two sequences. Delivery is therefore at least once end to end.
func (t *MongoDBTransport) Dispatch(ctx context.Context, update *mercure.Update) error {
	select {
	case <-t.closed:
		return mercure.ErrClosedTransport
	default:
	}

	update.AssignUUID()

	doc := newEventDoc(update, time.Now().UTC())
	if err := t.store.Append(ctx, doc); err != nil {
		return fmt.Errorf("persisting update: %w", err)
	}

	return nil
}

// AddSubscriber registers the subscriber and replays its history.
func (t *MongoDBTransport) AddSubscriber(ctx context.Context, s *mercure.LocalSubscriber) error {
	select {
	case <-t.closed:
		return mercure.ErrClosedTransport
	default:
	}

	// Resolve the client position before taking the lock, because it needs a
	// database lookup.
	haveSeq, found, first, resolveErr := t.resolveRequestedID(ctx, s)
	if resolveErr != nil {
		s.HistoryDispatched(mercure.EarliestLastEventID)

		return resolveErr
	}

	// Snapshot and register in one critical section, shared with the consumer,
	// so no event can slip between the snapshot and the registration.
	//
	// The live floor is the resolved client position only when that position is
	// trustworthy, that is when the requested ID still maps to its first
	// occurrence. Otherwise the subscriber is answered "earliest" and the floor
	// is the snapshot.
	t.mu.Lock()
	bound := t.committed
	liveFloor := bound
	if found && first && haveSeq > liveFloor {
		liveFloor = haveSeq
	}

	t.liveFloors[s] = liveFloor
	t.subscribers.Add(s)
	t.mu.Unlock()

	// Guards a race where Close() between the first select and Add() would miss
	// this subscriber during its disconnect walk.
	select {
	case <-t.closed:
		t.removeSubscriber(s)
		s.Disconnect()

		return mercure.ErrClosedTransport
	default:
	}

	if s.RequestLastEventIDSet {
		if err := t.replay(ctx, s, bound, haveSeq, found, first); err != nil {
			return err
		}
	}

	s.Ready(ctx)

	return nil
}

// resolveRequestedID resolves the requested Last-Event-ID to the highest
// sequence the client already has. found is false when the id is unknown, was
// removed by retention, or is empty. first is false when the earliest retained
// occurrence is not the first occurrence of the id, which means an earlier one
// was removed and the client position is ambiguous.
func (t *MongoDBTransport) resolveRequestedID(ctx context.Context, s *mercure.LocalSubscriber) (seq int64, found, first bool, err error) {
	requested := s.RequestLastEventID
	if requested == "" || requested == mercure.EarliestLastEventID {
		return 0, false, false, nil
	}

	seq, found, first, err = t.store.FindByMercureID(ctx, requested)
	if err != nil {
		return 0, false, false, err
	}

	return seq, found, first, nil
}

// replay sends the history the subscriber is missing, then calls
// HistoryDispatched exactly once.
func (t *MongoDBTransport) replay(ctx context.Context, s *mercure.LocalSubscriber, bound, haveSeq int64, found, first bool) error {
	requested := s.RequestLastEventID

	switch {
	case requested == mercure.EarliestLastEventID:
		from := int64(0)
		if limit := t.earliestReplayWindow(); bound > limit {
			from = bound - limit
		}

		return t.replayRange(ctx, s, from, bound, mercure.EarliestLastEventID)

	case !found || !first:
		// The requested id is unknown, was removed by retention, or resolves to
		// a later occurrence whose first occurrence was removed. In all these
		// cases the client position is unknown, so answer "earliest" and let the
		// client reload its state and reconnect.
		if t.logger != nil && t.logger.Enabled(ctx, slog.LevelInfo) {
			t.logger.LogAttrs(ctx, slog.LevelInfo, "Requested Last-Event-ID does not resolve to a known position, answering earliest",
				slog.String("mercure.last_event_id", requested))
		}

		s.HistoryDispatched(mercure.EarliestLastEventID)

		return nil

	default:
		// The client already has everything up to haveSeq, and that occurrence
		// is the first one, so replay (haveSeq, bound]. This is empty when this
		// hub lags behind the client.
		return t.replayRange(ctx, s, haveSeq, bound, requested)
	}
}

// replayRange dispatches matching events in (fromExclusive, toInclusive] as
// history, then calls HistoryDispatched exactly once.
func (t *MongoDBTransport) replayRange(ctx context.Context, s *mercure.LocalSubscriber, fromExclusive, toInclusive int64, responseLastEventID string) error {
	err := t.store.Scan(ctx, fromExclusive, toInclusive, func(doc *eventDoc) bool {
		update := doc.update()
		if !s.Match(update) {
			return true
		}

		return s.Dispatch(ctx, update, true)
	})
	if err != nil {
		s.HistoryDispatched(mercure.EarliestLastEventID)

		return err
	}

	s.HistoryDispatched(responseLastEventID)

	return nil
}

// earliestReplayWindow bounds an "earliest" replay. A configured retention is
// the operator's window; minEarliestReplayWindow is the floor when retention is
// unlimited.
func (t *MongoDBTransport) earliestReplayWindow() int64 {
	if t.retention > minEarliestReplayWindow {
		return t.retention
	}

	return minEarliestReplayWindow
}

// RemoveSubscriber unregisters the subscriber.
func (t *MongoDBTransport) RemoveSubscriber(_ context.Context, s *mercure.LocalSubscriber) error {
	select {
	case <-t.closed:
		return mercure.ErrClosedTransport
	default:
	}

	t.removeSubscriber(s)

	return nil
}

func (t *MongoDBTransport) removeSubscriber(s *mercure.LocalSubscriber) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.liveFloors, s)
	t.subscribers.Remove(s)
}

// GetSubscribers returns the last event ID and the active subscribers. The
// subscriber list is local to this hub instance, so in a multi-instance
// deployment it reports only the subscribers connected to this instance.
func (t *MongoDBTransport) GetSubscribers(_ context.Context) (string, []*mercure.Subscriber, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var subscribers []*mercure.Subscriber

	t.subscribers.Walk(0, func(s *mercure.LocalSubscriber) bool {
		subscribers = append(subscribers, &s.Subscriber)

		return true
	})

	return t.lastEventID, subscribers, nil
}

// Lag returns how many committed events this hub has not applied yet.
func (t *MongoDBTransport) Lag(ctx context.Context) (int64, error) {
	max, err := t.store.MaxSeq(ctx)
	if err != nil {
		return 0, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if max <= t.committed {
		return 0, nil
	}

	return max - t.committed, nil
}

// Ready reports whether the change stream consumer is connected.
func (t *MongoDBTransport) Ready(context.Context) error {
	select {
	case <-t.closed:
		return mercure.ErrClosedTransport
	default:
	}

	if !t.healthy.Load() {
		return errors.New("change stream is not connected")
	}

	return nil
}

// Live pings MongoDB.
func (t *MongoDBTransport) Live(ctx context.Context) error {
	select {
	case <-t.closed:
		return mercure.ErrClosedTransport
	default:
	}

	if err := t.store.Ping(ctx); err != nil {
		return fmt.Errorf("MongoDB ping failed: %w", err)
	}

	return nil
}

// Close stops the consumer, disconnects subscribers and releases MongoDB.
func (t *MongoDBTransport) Close(ctx context.Context) error {
	t.closedOnce.Do(func() {
		close(t.closed)
		t.cancel()
		t.wg.Wait()

		t.subscribers.Walk(0, func(s *mercure.LocalSubscriber) bool {
			s.Disconnect()

			return true
		})

		t.saveCheckpoint(ctx)

		if err := t.store.Close(ctx); err != nil && t.logger != nil {
			t.logger.LogAttrs(ctx, slog.LevelError, "Unable to close MongoDB", slog.Any("error", err))
		}
	})

	return nil
}

// consume reads the change stream and delivers events, reconnecting with
// backoff. When the resume token can no longer be used, it catches up from the
// durable collection and restarts the stream.
func (t *MongoDBTransport) consume() {
	defer t.wg.Done()

	backoff := reconnectMin
	resumeToken := t.loadResumeToken()

	for {
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		err := t.consumeOnce(resumeToken)
		if t.ctx.Err() != nil {
			return
		}

		t.healthy.Store(false)

		switch {
		case errors.Is(err, errInvalidate):
			if t.logger != nil {
				t.logger.LogAttrs(t.ctx, slog.LevelError, "Events collection was dropped or renamed, the sequence restarted", slog.Any("error", err))
			}

			t.reset()
			resumeToken = nil
			backoff = reconnectMin

		case isResumeTokenError(err):
			if t.logger != nil {
				t.logger.LogAttrs(t.ctx, slog.LevelWarn, "Change stream resume token is no longer valid, catching up from the durable collection", slog.Any("error", err))
			}

			// consumeOnce catches up after reopening the stream without a token.
			resumeToken = nil
			backoff = reconnectMin

		case errors.Is(err, errGapExpired):
			if t.logger != nil {
				t.logger.LogAttrs(t.ctx, slog.LevelError, "Change stream gap could not be filled from the durable collection", slog.Any("error", err))
			}

			// The missing events are gone for good, so retrying the same range
			// would loop. Disconnect the subscribers and fast forward instead.
			if recoverErr := t.recoverGap(); recoverErr != nil && t.logger != nil {
				t.logger.LogAttrs(t.ctx, slog.LevelError, "Unable to recover from an expired change stream gap", slog.Any("error", recoverErr))
			}

			resumeToken = nil
			backoff = reconnectMin

		default:
			if t.logger != nil {
				t.logger.LogAttrs(t.ctx, slog.LevelError, "Change stream interrupted, reconnecting", slog.Any("error", err), slog.Duration("backoff", backoff))
			}

			select {
			case <-t.ctx.Done():
				return
			case <-time.After(backoff):
			}

			backoff = min(backoff*2, reconnectMax)
			resumeToken = t.currentToken()
		}
	}
}

// consumeOnce opens the change stream and delivers events until it fails or the
// context is cancelled.
func (t *MongoDBTransport) consumeOnce(resumeToken []byte) error {
	stream, err := t.store.Watch(t.ctx, resumeToken)
	if err != nil {
		return err
	}

	defer stream.Close(context.Background()) //nolint:errcheck

	// Open the stream before catching up, so events committed from now on are
	// captured by the stream while the catch up fills the range that predates
	// the stream. Without a resume token, the stream only sees events after it
	// opened, so anything committed in between must come from the collection.
	if len(resumeToken) == 0 {
		if err := t.catchUp(); err != nil {
			return err
		}
	}

	// The stream is connected and caught up, so the hub can serve subscribers.
	// Reporting ready before the catch up would advertise a hub that is still
	// behind.
	t.healthy.Store(true)

	for {
		ev, ok, err := stream.Next(t.ctx)
		if err != nil {
			return err
		}

		if !ok {
			continue
		}

		switch ev.OperationType {
		case opInvalidate:
			return errInvalidate
		case opInsert:
			if err := t.apply(ev.Doc); err != nil {
				return err
			}
		default:
			// A retention delete. It does not change the applied watermark.
		}

		// Advance the resume token only after the event was applied, so a
		// failed apply re-reads the same event after the reconnect instead of
		// skipping it.
		t.mu.Lock()
		t.lastToken = ev.Token
		t.mu.Unlock()
	}
}

// apply delivers one event, first filling any gap between the applied watermark
// and the event sequence from the durable collection. It returns an error when
// the gap cannot be filled, so the caller does not advance past missing events.
func (t *MongoDBTransport) apply(doc *eventDoc) error {
	t.mu.Lock()
	committed := t.committed
	t.mu.Unlock()

	if doc.Seq <= committed {
		return nil // duplicate, for example after a resume
	}

	if doc.Seq > committed+1 {
		if err := t.backfill(committed, doc.Seq-1); err != nil {
			return fmt.Errorf("backfilling change stream gap (%d, %d]: %w", committed, doc.Seq-1, err)
		}
	}

	t.deliver(doc)

	return nil
}

// backfill delivers the durable events in (fromExclusive, toInclusive] to live
// subscribers, in order. It returns errGapExpired when the range is not fully
// covered, which means retention removed events that are still needed. In that
// case nothing is delivered, so the caller can reset instead of exposing a hole
// in the stream.
func (t *MongoDBTransport) backfill(fromExclusive, toInclusive int64) error {
	expected := fromExclusive + 1
	expired := false

	err := t.store.Scan(t.ctx, fromExclusive, toInclusive, func(doc *eventDoc) bool {
		if doc.Seq != expected {
			// The range has a hole, which only retention can cause. Stop before
			// delivering anything, so subscribers never see a partial range.
			expired = true

			return false
		}

		expected = doc.Seq + 1

		t.deliver(doc)

		return true
	})
	if err != nil {
		return err
	}

	if expired || expected <= toInclusive {
		return errGapExpired
	}

	return nil
}

// deliver advances the watermark and dispatches the event to matching
// subscribers whose live floor is below it.
func (t *MongoDBTransport) deliver(doc *eventDoc) {
	t.mu.Lock()
	if doc.Seq <= t.committed {
		t.mu.Unlock()

		return
	}

	t.committed = doc.Seq
	t.lastEventID = doc.MercureID

	update := doc.update()
	matched := t.subscribers.MatchAny(update)

	type target struct {
		subscriber *mercure.LocalSubscriber
		liveFloor  int64
	}

	targets := make([]target, 0, len(matched))

	for _, s := range matched {
		targets = append(targets, target{subscriber: s, liveFloor: t.liveFloors[s]})
	}

	t.mu.Unlock()

	for _, tg := range targets {
		if doc.Seq <= tg.liveFloor {
			continue // covered by history replay or already known by the client
		}

		tg.subscriber.Dispatch(t.ctx, update, false)
	}
}

// catchUp delivers every committed event the hub has not applied yet. It is
// used when the resume token is no longer valid, so connected subscribers do
// not lose the events published during the interruption. It returns an error
// when the range cannot be read or is no longer retained, so the consumer can
// retry or reset instead of silently staying behind.
func (t *MongoDBTransport) catchUp() error {
	max, err := t.store.MaxSeq(t.ctx)
	if err != nil {
		return fmt.Errorf("reading the maximum sequence while catching up: %w", err)
	}

	t.mu.Lock()
	from := t.committed
	t.mu.Unlock()

	if max <= from {
		return nil
	}

	if err := t.backfill(from, max); err != nil {
		return fmt.Errorf("catching up from the durable collection: %w", err)
	}

	return nil
}

// recoverGap handles a gap that retention made unfillable. The subscribers
// cannot be served a contiguous stream, so they are disconnected and the
// watermark is fast forwarded past the missing events. Each client reconnects
// with its Last-Event-ID and replays from the durable collection.
func (t *MongoDBTransport) recoverGap() error {
	max, err := t.store.MaxSeq(t.ctx)
	if err != nil {
		return fmt.Errorf("reading the maximum sequence while recovering a gap: %w", err)
	}

	latest, err := t.store.Latest(t.ctx)
	if err != nil {
		return fmt.Errorf("reading the latest event while recovering a gap: %w", err)
	}

	var subscribers []*mercure.LocalSubscriber

	t.mu.Lock()
	t.committed = max
	if latest != nil {
		t.lastEventID = latest.MercureID
	}

	t.liveFloors = make(map[*mercure.LocalSubscriber]int64)

	t.subscribers.Walk(0, func(s *mercure.LocalSubscriber) bool {
		subscribers = append(subscribers, s)

		return true
	})
	t.mu.Unlock()

	for _, s := range subscribers {
		s.Disconnect()
	}

	if t.logger != nil {
		t.logger.LogAttrs(t.ctx, slog.LevelError, "Change stream gap could not be filled from the durable collection, disconnecting subscribers to force a replay",
			slog.Int64("committed", max), slog.Int("subscribers", len(subscribers)))
	}

	return nil
}

// reset restarts the watermark after the events collection was dropped.
func (t *MongoDBTransport) reset() {
	t.mu.Lock()
	t.committed = 0
	t.lastToken = nil
	t.lastEventID = mercure.EarliestLastEventID
	t.mu.Unlock()
}

// loadResumeToken returns the stored resume token, or nil when none exists.
func (t *MongoDBTransport) loadResumeToken() []byte {
	cp, err := t.store.LoadCheckpoint(t.ctx)
	if err != nil {
		if t.logger != nil {
			t.logger.LogAttrs(t.ctx, slog.LevelWarn, "Unable to read checkpoint, starting from the current position", slog.Any("error", err))
		}

		return nil
	}

	if cp == nil {
		return nil
	}

	t.mu.Lock()
	t.lastToken = cp.ResumeToken
	t.mu.Unlock()

	return cp.ResumeToken
}

func (t *MongoDBTransport) currentToken() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.lastToken
}

// checkpointLoop persists the resume token and watermark on a timer.
func (t *MongoDBTransport) checkpointLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(t.checkpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.saveCheckpoint(t.ctx)
		}
	}
}

func (t *MongoDBTransport) saveCheckpoint(ctx context.Context) {
	t.mu.Lock()
	token := t.lastToken
	seq := t.committed
	t.mu.Unlock()

	if len(token) == 0 {
		return
	}

	cp := &checkpoint{ResumeToken: token, Seq: seq, UpdatedAt: time.Now().UTC()}

	if err := t.store.SaveCheckpoint(ctx, cp); err != nil && t.logger != nil {
		t.logger.LogAttrs(ctx, slog.LevelWarn, "Unable to save checkpoint", slog.Any("error", err))
	}
}

// cleanupLoop applies count based retention on a timer.
func (t *MongoDBTransport) cleanupLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(t.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			if err := t.store.Cleanup(t.ctx, t.retention); err != nil && t.logger != nil {
				t.logger.LogAttrs(t.ctx, slog.LevelWarn, "Retention cleanup failed", slog.Any("error", err))
			}
		}
	}
}

// Interface guards.
var (
	_ mercure.Transport              = (*MongoDBTransport)(nil)
	_ mercure.TransportSubscribers   = (*MongoDBTransport)(nil)
	_ mercure.TransportHealthChecker = (*MongoDBTransport)(nil)
)
