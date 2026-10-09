package mercuremongodb

import (
	"context"
	"sync"
	"time"
)

// checkpoint is the durable position of a hub in the change stream. The resume
// token is opaque to the transport. The sequence is the applied watermark used
// to rebuild state when the token can no longer be used.
type checkpoint struct {
	ResumeToken []byte
	Seq         int64
	UpdatedAt   time.Time
}

// watchedEvent is one event coming from the change stream, with the resume
// token valid just after it. OperationType is "insert", "delete" or
// "invalidate".
type watchedEvent struct {
	OperationType string
	Doc           *eventDoc
	Token         []byte
}

// eventStream is an ordered stream of change events. Next blocks until an event
// is available or ctx is cancelled. It returns false with a nil error on a
// normal end of stream and false with an error when the stream failed.
type eventStream interface {
	Next(ctx context.Context) (watchedEvent, bool, error)
	Close(ctx context.Context) error
}

// store abstracts durable event storage and the change notification channel.
// The MongoDB implementation is in mongo_store.go. A memory implementation is
// used by unit tests, so the ordering and boundary logic can be tested without
// a replica set.
type store interface {
	// Append persists doc and assigns its sequence. It must fail rather than
	// leave a gap in the sequence.
	Append(ctx context.Context, doc *eventDoc) error

	// MaxSeq returns the highest committed sequence, or 0 when empty.
	MaxSeq(ctx context.Context) (int64, error)

	// FindByMercureID returns the sequence of the earliest document with the
	// given visible Mercure ID.
	FindByMercureID(ctx context.Context, mercureID string) (int64, bool, error)

	// Scan calls fn for every document with fromExclusive < Seq <= toInclusive,
	// in sequence order. It stops early when fn returns false.
	Scan(ctx context.Context, fromExclusive, toInclusive int64, fn func(*eventDoc) bool) error

	// Latest returns the most recent document, or nil when empty.
	Latest(ctx context.Context) (*eventDoc, error)

	// Cleanup keeps only the newest retention documents. A retention <= 0 keeps
	// everything.
	Cleanup(ctx context.Context, retention int64) error

	// Watch opens the change stream. A non nil resumeToken resumes after it.
	Watch(ctx context.Context, resumeToken []byte) (eventStream, error)

	// LoadCheckpoint returns the stored checkpoint, or nil when none exists.
	LoadCheckpoint(ctx context.Context) (*checkpoint, error)

	// SaveCheckpoint overwrites the stored checkpoint.
	SaveCheckpoint(ctx context.Context, cp *checkpoint) error

	// Ping checks that the backing service is reachable.
	Ping(ctx context.Context) error

	// Close releases the resources held by the store.
	Close(ctx context.Context) error
}

// memoryStream is the eventStream returned by memoryStore.Watch.
type memoryStream struct {
	ch     chan watchedEvent
	store  *memoryStore
	key    int
	closed bool
}

func (s *memoryStream) Next(ctx context.Context) (watchedEvent, bool, error) {
	select {
	case <-ctx.Done():
		return watchedEvent{}, false, ctx.Err() //nolint:wrapcheck
	case ev, ok := <-s.ch:
		if !ok {
			return watchedEvent{}, false, nil
		}

		return ev, true, nil
	}
}

func (s *memoryStream) Close(context.Context) error {
	s.store.removeWatcher(s.key)

	return nil
}

// memoryStore is an in memory store used by unit tests.
type memoryStore struct {
	mu        sync.Mutex
	docs      []*eventDoc
	byID      map[string]int64
	cp        *checkpoint
	watchers  map[int]chan watchedEvent
	nextWatch int
	closed    bool
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		byID:     make(map[string]int64),
		watchers: make(map[int]chan watchedEvent),
	}
}

func (s *memoryStore) Append(_ context.Context, doc *eventDoc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errStoreClosed
	}

	// The sequence is gapless: the next value is always len+1.
	seq := int64(len(s.docs)) + 1
	doc.Seq = seq

	stored := *doc
	s.docs = append(s.docs, &stored)

	if _, ok := s.byID[doc.MercureID]; !ok {
		s.byID[doc.MercureID] = seq
	}

	token := []byte{byte(seq)}

	for _, ch := range s.watchers {
		select {
		case ch <- watchedEvent{OperationType: opInsert, Doc: &stored, Token: token}:
		default:
		}
	}

	return nil
}

func (s *memoryStore) MaxSeq(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return int64(len(s.docs)), nil
}

func (s *memoryStore) FindByMercureID(_ context.Context, mercureID string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seq, ok := s.byID[mercureID]

	return seq, ok, nil
}

func (s *memoryStore) Scan(_ context.Context, fromExclusive, toInclusive int64, fn func(*eventDoc) bool) error {
	s.mu.Lock()

	var batch []*eventDoc

	for _, d := range s.docs {
		if d.Seq > fromExclusive && d.Seq <= toInclusive {
			batch = append(batch, d)
		}
	}

	s.mu.Unlock()

	for _, d := range batch {
		if !fn(d) {
			break
		}
	}

	return nil
}

func (s *memoryStore) Latest(context.Context) (*eventDoc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.docs) == 0 {
		return nil, nil
	}

	return s.docs[len(s.docs)-1], nil
}

func (s *memoryStore) Cleanup(_ context.Context, retention int64) error {
	if retention <= 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if int64(len(s.docs)) <= retention {
		return nil
	}

	drop := int64(len(s.docs)) - retention

	for _, d := range s.docs[:drop] {
		if s.byID[d.MercureID] == d.Seq {
			delete(s.byID, d.MercureID)
		}
	}

	s.docs = s.docs[drop:]

	return nil
}

func (s *memoryStore) Watch(_ context.Context, _ []byte) (eventStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errStoreClosed
	}

	ch := make(chan watchedEvent, 1024)
	key := s.nextWatch
	s.nextWatch++
	s.watchers[key] = ch

	return &memoryStream{ch: ch, store: s, key: key}, nil
}

func (s *memoryStore) removeWatcher(key int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ch, ok := s.watchers[key]; ok {
		delete(s.watchers, key)
		close(ch)
	}
}

func (s *memoryStore) LoadCheckpoint(context.Context) (*checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cp == nil {
		return nil, nil
	}

	cp := *s.cp

	return &cp, nil
}

func (s *memoryStore) SaveCheckpoint(_ context.Context, cp *checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	saved := *cp
	s.cp = &saved

	return nil
}

func (s *memoryStore) Ping(context.Context) error {
	return nil
}

func (s *memoryStore) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	for key, ch := range s.watchers {
		delete(s.watchers, key)
		close(ch)
	}

	return nil
}
