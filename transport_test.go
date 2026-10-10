package mercuremongodb

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTopic = "https://example.com/foo"

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func exactMatchers(topics ...string) []mercure.TopicMatcher {
	matchers := make([]mercure.TopicMatcher, len(topics))
	for i, topic := range topics {
		matchers[i] = mercure.TopicMatcher{Type: mercure.MatcherTypeExact, Pattern: topic}
	}

	return matchers
}

func newTestSubscriber(lastEventID string, topics, allowedPrivate []string) *mercure.LocalSubscriber {
	s := mercure.NewLocalSubscriber(lastEventID, testLogger(), &mercure.TopicMatcherStore{})
	s.SetMatchers(exactMatchers(topics...), exactMatchers(allowedPrivate...))

	return s
}

// newStartedTransport builds a transport on a memory store, with the change
// stream consumer running.
func newStartedTransport(t *testing.T, retention int64) (*MongoDBTransport, *memoryStore) {
	t.Helper()

	st := newMemoryStore()

	tr, err := newTransportWithStore(context.Background(), st, mercure.NewSubscriberList(0), testLogger(), Config{Retention: retention})
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, tr.Close(context.Background())) })

	return tr, st
}

// newBareTransport builds a transport without starting the consumer, so a test
// can drive apply and AddSubscriber directly.
func newBareTransport(st store) *MongoDBTransport {
	return &MongoDBTransport{
		store:       st,
		subscribers: mercure.NewSubscriberList(0),
		logger:      testLogger(),
		ctx:         context.Background(),
		closed:      make(chan struct{}),
		lastEventID: mercure.EarliestLastEventID,
		liveFloors:  make(map[*mercure.LocalSubscriber]int64),
	}
}

func waitCommitted(t *testing.T, tr *MongoDBTransport, want int64) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)

	for {
		tr.mu.Lock()
		got := tr.committed
		tr.mu.Unlock()

		if got >= want {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the committed watermark %d, got %d", want, got)
		}

		time.Sleep(time.Millisecond)
	}
}

func readIDs(t *testing.T, s *mercure.LocalSubscriber, n int) []string {
	t.Helper()

	ids := make([]string, 0, n)

	for i := range n {
		select {
		case u, ok := <-s.Receive():
			require.True(t, ok, "subscriber channel closed")
			ids = append(ids, u.ID)
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for event %d of %d, got %v", i+1, n, ids)
		}
	}

	return ids
}

func assertNoEvent(t *testing.T, s *mercure.LocalSubscriber) {
	t.Helper()

	select {
	case u, ok := <-s.Receive():
		if ok {
			t.Fatalf("unexpected event received: %s", u.ID)
		}
	case <-time.After(150 * time.Millisecond):
	}
}

func dispatchIDs(t *testing.T, tr *MongoDBTransport, topics []string, ids ...string) {
	t.Helper()

	for _, id := range ids {
		require.NoError(t, tr.Dispatch(t.Context(), &mercure.Update{
			Event:  mercure.Event{ID: id},
			Topics: topics,
		}))
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.withDefaults()

	assert.Equal(t, DefaultDatabase, cfg.Database)
	assert.Equal(t, DefaultCollection, cfg.Collection)
	assert.Equal(t, DefaultCheckpoints, cfg.Checkpoints)
	assert.Equal(t, DefaultSeen, cfg.Seen)
	assert.Equal(t, DefaultCleanupInterval, cfg.CleanupInterval)
	assert.Equal(t, DefaultCheckpointInterval, cfg.CheckpointInterval)
	assert.Equal(t, DefaultWatchAwaitTime, cfg.WatchAwaitTime)
	assert.Equal(t, DefaultConnectTimeout, cfg.ConnectTimeout)
}

func TestEventDocRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	u := &mercure.Update{
		Event:   mercure.Event{ID: "id1", Data: "data", Type: "type", Retry: 5},
		Topics:  []string{"a", "b"},
		Private: true,
		Debug:   true,
	}

	doc := newEventDoc(u, now)

	assert.Equal(t, currentSchemaVersion, doc.SchemaVersion)
	assert.Equal(t, "id1", doc.MercureID)
	assert.Equal(t, []string{"a", "b"}, doc.Topics)
	assert.True(t, doc.Private)
	assert.True(t, doc.Debug)
	assert.Equal(t, now, doc.CreatedAt)

	got := doc.update()

	assert.Equal(t, u.ID, got.ID)
	assert.Equal(t, u.Data, got.Data)
	assert.Equal(t, u.Type, got.Type)
	assert.Equal(t, u.Retry, got.Retry)
	assert.Equal(t, u.Topics, got.Topics)
	assert.Equal(t, u.Private, got.Private)
	assert.Equal(t, u.Debug, got.Debug)
}

func TestReplayEarliest(t *testing.T) {
	t.Parallel()

	tr, _ := newStartedTransport(t, 0)
	dispatchIDs(t, tr, []string{testTopic}, "1", "2", "3")
	waitCommitted(t, tr, 3)

	s := newTestSubscriber(mercure.EarliestLastEventID, []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(t.Context(), s))

	assert.Equal(t, []string{"1", "2", "3"}, readIDs(t, s, 3))
	assertNoEvent(t, s)
}

func TestReplayAfterID(t *testing.T) {
	t.Parallel()

	tr, _ := newStartedTransport(t, 0)
	dispatchIDs(t, tr, []string{testTopic}, "1", "2", "3", "4", "5")
	waitCommitted(t, tr, 5)

	s := newTestSubscriber("3", []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(t.Context(), s))

	assert.Equal(t, []string{"4", "5"}, readIDs(t, s, 2))
	assertNoEvent(t, s)
}

func TestReplayUnknownIDAnswersEarliest(t *testing.T) {
	t.Parallel()

	tr, _ := newStartedTransport(t, 0)
	dispatchIDs(t, tr, []string{testTopic}, "1", "2")
	waitCommitted(t, tr, 2)

	s := newTestSubscriber("unknown", []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(t.Context(), s))

	// An unknown id is not replayed. The response is "earliest", which the
	// subscriber exposes through its response channel.
	assertNoEvent(t, s)
}

func TestEmptyRequestedIDIsAnswered(t *testing.T) {
	t.Parallel()

	tr, _ := newStartedTransport(t, 0)
	dispatchIDs(t, tr, []string{testTopic}, "1", "2")
	waitCommitted(t, tr, 2)

	s := newTestSubscriber("placeholder", []string{testTopic}, nil)
	// A request that carried the header with an empty value.
	s.RequestLastEventID = ""
	s.RequestLastEventIDSet = true

	require.NoError(t, tr.AddSubscriber(t.Context(), s))
	assertNoEvent(t, s)
}

func TestReplayFiltersPrivate(t *testing.T) {
	t.Parallel()

	tr, _ := newStartedTransport(t, 0)

	require.NoError(t, tr.Dispatch(t.Context(), &mercure.Update{
		Event:  mercure.Event{ID: "public"},
		Topics: []string{testTopic},
	}))
	require.NoError(t, tr.Dispatch(t.Context(), &mercure.Update{
		Event:   mercure.Event{ID: "private"},
		Topics:  []string{testTopic},
		Private: true,
	}))
	waitCommitted(t, tr, 2)

	// The subscriber is allowed on the topic but not on any private topic.
	s := newTestSubscriber(mercure.EarliestLastEventID, []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(t.Context(), s))

	assert.Equal(t, []string{"public"}, readIDs(t, s, 1))
	assertNoEvent(t, s)
}

func TestLiveDelivery(t *testing.T) {
	t.Parallel()

	tr, _ := newStartedTransport(t, 0)

	s := newTestSubscriber(mercure.EarliestLastEventID, []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(t.Context(), s))

	dispatchIDs(t, tr, []string{testTopic}, "live")

	assert.Equal(t, []string{"live"}, readIDs(t, s, 1))
}

func TestGapIsBackfilledInOrder(t *testing.T) {
	t.Parallel()

	st := newMemoryStore()
	tr := newBareTransport(st)
	ctx := t.Context()

	docs := make([]*eventDoc, 0, 3)

	for i := 1; i <= 3; i++ {
		doc := &eventDoc{MercureID: strconv.Itoa(i), Topics: []string{testTopic}, Data: "x"}
		require.NoError(t, st.Append(ctx, doc))
		docs = append(docs, doc)
	}

	s := newTestSubscriber(mercure.EarliestLastEventID, []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(ctx, s))

	// The consumer sees only the third event. The two missing events must be
	// backfilled from the durable collection, in order, before it.
	require.NoError(t, tr.apply(st, docs[2]))

	assert.Equal(t, []string{"1", "2", "3"}, readIDs(t, s, 3))

	// A redelivered event is dropped.
	require.NoError(t, tr.apply(st, docs[2]))
	assertNoEvent(t, s)
}

func TestLaggingHubDoesNotRedeliver(t *testing.T) {
	t.Parallel()

	st := newMemoryStore()
	tr := newBareTransport(st)
	ctx := t.Context()

	docs := make([]*eventDoc, 0, 6)

	for i := 1; i <= 6; i++ {
		doc := &eventDoc{MercureID: strconv.Itoa(i), Topics: []string{testTopic}, Data: "x"}
		require.NoError(t, st.Append(ctx, doc))
		docs = append(docs, doc)
	}

	// The hub has applied only the first three events, but the client already
	// has everything up to event 5.
	tr.committed = 3

	s := newTestSubscriber("5", []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(ctx, s))
	assert.Equal(t, int64(5), tr.liveFloors[s])

	require.NoError(t, tr.apply(st, docs[3])) // sequence 4, below the floor
	require.NoError(t, tr.apply(st, docs[4])) // sequence 5, at the floor
	assertNoEvent(t, s)

	require.NoError(t, tr.apply(st, docs[5])) // sequence 6, above the floor
	assert.Equal(t, []string{"6"}, readIDs(t, s, 1))
}

func TestUnfillableGapIsReported(t *testing.T) {
	t.Parallel()

	st := newMemoryStore()
	tr := newBareTransport(st)
	ctx := t.Context()

	docs := make([]*eventDoc, 0, 3)

	for i := 1; i <= 3; i++ {
		doc := &eventDoc{MercureID: strconv.Itoa(i), Topics: []string{testTopic}, Data: "x"}
		require.NoError(t, st.Append(ctx, doc))
		docs = append(docs, doc)
	}

	// Retention removed the first two events, so the gap before the third event
	// cannot be filled from the durable collection.
	st.docs = st.docs[2:]

	err := tr.apply(st, docs[2])
	require.ErrorIs(t, err, errGapExpired)

	// The watermark must not advance past the missing events.
	assert.Equal(t, int64(0), tr.committed)
}

func TestRecoverGapDisconnectsSubscribers(t *testing.T) {
	t.Parallel()

	st := newMemoryStore()
	tr := newBareTransport(st)
	ctx := t.Context()

	for i := 1; i <= 3; i++ {
		require.NoError(t, st.Append(ctx, &eventDoc{MercureID: strconv.Itoa(i), Topics: []string{testTopic}, Data: "x"}))
	}

	s := newTestSubscriber(mercure.EarliestLastEventID, []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(ctx, s))

	require.NoError(t, tr.recoverGap(st))

	// The watermark fast forwards to the latest event and the subscriber is
	// disconnected, so the client reconnects and replays from its last event ID.
	assert.Equal(t, int64(3), tr.committed)

	select {
	case _, ok := <-s.Receive():
		require.False(t, ok, "the subscriber channel must be closed after a forced reset")
	case <-time.After(3 * time.Second):
		t.Fatal("the subscriber was not disconnected")
	}
}

func TestRepeatedIDIsAmbiguous(t *testing.T) {
	t.Parallel()

	st := newMemoryStore()
	tr := newBareTransport(st)
	ctx := t.Context()

	// X is published twice, and the second occurrence is still in the window.
	require.NoError(t, st.Append(ctx, &eventDoc{MercureID: "X", Topics: []string{testTopic}, Data: "1"}))

	for i := 2; i <= 5; i++ {
		require.NoError(t, st.Append(ctx, &eventDoc{MercureID: strconv.Itoa(i), Topics: []string{testTopic}, Data: "x"}))
	}

	second := &eventDoc{MercureID: "X", Topics: []string{testTopic}, Data: "2"}
	require.NoError(t, st.Append(ctx, second))
	require.False(t, second.First, "a repeated ID must not be the first occurrence while an earlier one is retained")

	// Retention removes the first occurrence of X but keeps the second.
	require.NoError(t, st.Cleanup(ctx, 4))

	tr.committed = 6

	// A client that saw the first occurrence of X reconnects. The earliest
	// retained occurrence is the second one, and the marker still records the
	// earlier occurrence, so the position is ambiguous: answer earliest, not the
	// events after the second occurrence.
	s := newTestSubscriber("X", []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(ctx, s))
	assertNoEvent(t, s)
}

func TestRepeatedIDAfterRetentionIsTreatedAsNew(t *testing.T) {
	t.Parallel()

	st := newMemoryStore()
	ctx := t.Context()

	require.NoError(t, st.Append(ctx, &eventDoc{MercureID: "X", Topics: []string{testTopic}, Data: "1"}))

	for i := 2; i <= 5; i++ {
		require.NoError(t, st.Append(ctx, &eventDoc{MercureID: strconv.Itoa(i), Topics: []string{testTopic}, Data: "x"}))
	}

	// Retention removes the only occurrence of X, so its seen marker is dropped
	// with it.
	require.NoError(t, st.Cleanup(ctx, 3))

	second := &eventDoc{MercureID: "X", Topics: []string{testTopic}, Data: "2"}
	require.NoError(t, st.Append(ctx, second))

	// A reuse after the ID left the window is treated as a new ID. This is the
	// documented trade-off of bounding the seen collection to the window.
	assert.True(t, second.First)
}

func TestReplayRangeWithRetentionHoleAnswersEarliest(t *testing.T) {
	t.Parallel()

	st := newMemoryStore()
	tr := newBareTransport(st)
	ctx := t.Context()

	for i := 1; i <= 5; i++ {
		require.NoError(t, st.Append(ctx, &eventDoc{MercureID: strconv.Itoa(i), Topics: []string{testTopic}, Data: "x"}))
	}

	// Retention removed the prefix that the replay range needed, leaving only
	// the newest events. This is the race between the Last-Event-ID lookup and
	// the scan: the position was found, then the events after it were deleted.
	st.docs = st.docs[3:] // keeps sequences 4 and 5

	s := newTestSubscriber("2", []string{testTopic}, nil)

	// The requested position and the events that follow it were removed, so the
	// range is not contiguous: the subscriber is answered earliest instead of
	// being served a suffix with a hole.
	require.NoError(t, tr.replayRange(ctx, st, s, 2, 5, "2", true))
	assertNoEvent(t, s)
}

func TestClosedTransportRejectsDispatch(t *testing.T) {
	t.Parallel()

	tr, _ := newStartedTransport(t, 0)
	require.NoError(t, tr.Close(t.Context()))

	err := tr.Dispatch(t.Context(), &mercure.Update{Topics: []string{testTopic}})
	require.ErrorIs(t, err, mercure.ErrClosedTransport)
}
