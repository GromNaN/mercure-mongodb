package mercuremongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// Change stream operation types the consumer reacts to.
const (
	opInsert     = "insert"
	opDelete     = "delete"
	opInvalidate = "invalidate"
)

// maxAppendRetries bounds the optimistic sequence retries before Append gives
// up. Each retry means another publisher committed the candidate first.
const maxAppendRetries = 100

// mongoStore is the MongoDB backed store.
type mongoStore struct {
	client      *mongo.Client
	events      *mongo.Collection
	checkpoints *mongo.Collection
	seen        *mongo.Collection
	hubName     string
	watchAwait  time.Duration

	// seqMu serializes local sequence allocation and insert, so two local
	// publishes never collide on the same candidate. It does not coordinate
	// with other hubs; the unique index does that.
	seqMu sync.Mutex

	// hint is the highest sequence this process knows to be committed. It is
	// always <= the real maximum, so hint+1 never skips a value.
	hint atomic.Int64

	closed atomic.Bool
}

// mongoConfig holds the connection and collection settings of the store.
type mongoConfig struct {
	URI            string
	Database       string
	Collection     string
	Checkpoints    string
	Seen           string
	HubName        string
	WatchAwaitTime time.Duration
	ConnectTimeout time.Duration
}

// newMongoStore connects to MongoDB, verifies change stream support and creates
// the required indexes.
func newMongoStore(ctx context.Context, cfg mongoConfig) (*mongoStore, error) {
	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetServerSelectionTimeout(cfg.ConnectTimeout).
		// Majority writes make a publish durable before Dispatch returns, and
		// make the gapless sequence safe across a failover. Primary reads make
		// the max sequence and the replay range reflect the latest commit.
		SetWriteConcern(writeconcern.Majority()).
		SetReadPreference(readpref.Primary()).
		SetRetryWrites(true)

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())

		return nil, fmt.Errorf("pinging MongoDB: %w", err)
	}

	s := &mongoStore{
		client:      client,
		events:      client.Database(cfg.Database).Collection(cfg.Collection),
		checkpoints: client.Database(cfg.Database).Collection(cfg.Checkpoints),
		seen:        client.Database(cfg.Database).Collection(cfg.Seen),
		hubName:     cfg.HubName,
		watchAwait:  cfg.WatchAwaitTime,
	}

	// Create the collections before probing the change stream, so the probe has
	// a namespace to open on.
	if err := s.ensureIndexes(ctx); err != nil {
		_ = client.Disconnect(context.Background())

		return nil, err
	}

	if err := s.checkChangeStreamSupport(ctx); err != nil {
		_ = client.Disconnect(context.Background())

		return nil, err
	}

	max, err := s.MaxSeq(ctx)
	if err != nil {
		_ = client.Disconnect(context.Background())

		return nil, err
	}

	s.hint.Store(max)

	return s, nil
}

// checkChangeStreamSupport verifies that the deployment can serve change
// streams by opening one and closing it. A standalone server rejects the
// $changeStream stage, so the transport refuses to start on one. A sharded
// cluster accepts the stream, so it is not detected here, but it is not
// supported: the gapless sequence relies on the global uniqueness of _id,
// which a sharded collection only enforces per shard unless _id is the shard
// key.
func (s *mongoStore) checkChangeStreamSupport(ctx context.Context) error {
	stream, err := s.Watch(ctx, nil)
	if err != nil {
		return fmt.Errorf("the deployment cannot serve change streams, use a replica set: %w", err)
	}

	if err := stream.Close(ctx); err != nil {
		return err
	}

	return nil
}

// ensureIndexes creates the secondary indexes used by the Last-Event-ID
// lookup. The compound index resolves a repeated Mercure ID to its earliest
// occurrence in one ordered lookup. The _id index exists by default and serves
// the cursor order, replay ranges and retention. The seen collection gets an
// index on last_at, so retention can drop the markers that left the window.
//
// Creating an index also creates the collection when it does not exist yet, so
// the change stream always has a namespace to watch.
func (s *mongoStore) ensureIndexes(ctx context.Context) error {
	if _, err := s.events.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "mercure_id", Value: 1}, {Key: "_id", Value: 1}},
		Options: options.Index().SetName("mercure_id_1__id_1"),
	}); err != nil {
		return fmt.Errorf("creating mercure_id index: %w", err)
	}

	if _, err := s.seen.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "last_at", Value: 1}},
		Options: options.Index().SetName("last_at_1"),
	}); err != nil {
		return fmt.Errorf("creating seen index: %w", err)
	}

	return nil
}

// Append assigns a gapless sequence and inserts the document.
func (s *mongoStore) Append(ctx context.Context, doc *eventDoc) error {
	if s.closed.Load() {
		return errStoreClosed
	}

	// Record whether this is the first occurrence of the visible ID, so a
	// Last-Event-ID that resolves to a later occurrence can be detected as
	// ambiguous after retention removed the first one. The marker lives in its
	// own collection, outside the retention window, so a later occurrence is
	// never mistaken for the first one. The upsert is atomic on a single
	// document, so concurrent publishers of the same new ID agree on which
	// occurrence is the first. A failed insert can leave a marker behind, which
	// only makes the transport answer "earliest" more often, never skip events.
	first, err := s.markSeen(ctx, doc.MercureID)
	if err != nil {
		return err
	}

	doc.First = first

	// Serialize local allocation so local publishes never collide with each
	// other. Cross hub collisions are resolved by the retry loop below.
	s.seqMu.Lock()
	defer s.seqMu.Unlock()

	for attempt := 0; attempt < maxAppendRetries; attempt++ {
		candidate := s.hint.Load() + 1
		doc.Seq = candidate

		_, err := s.events.InsertOne(ctx, doc)
		if err == nil {
			s.hint.Store(candidate)

			return nil
		}

		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("inserting event: %w", err)
		}

		// Another hub committed the candidate first. Re-read the maximum.
		max, err := s.MaxSeq(ctx)
		if err != nil {
			return err
		}

		if max > s.hint.Load() {
			s.hint.Store(max)
		}
	}

	return errTooMuchContention
}

// MaxSeq returns the highest committed sequence, or 0 when the collection is
// empty.
func (s *mongoStore) MaxSeq(ctx context.Context) (int64, error) {
	var doc eventDoc

	err := s.events.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("reading max sequence: %w", err)
	}

	return doc.Seq, nil
}

// FindByMercureID returns the sequence of the earliest retained document with
// the given visible Mercure ID, and whether that document is the first
// occurrence of the ID. When First is false, an earlier occurrence was removed
// by retention, so the client position is ambiguous.
func (s *mongoStore) FindByMercureID(ctx context.Context, mercureID string) (int64, bool, bool, error) {
	var doc eventDoc

	err := s.events.FindOne(ctx,
		bson.D{{Key: "mercure_id", Value: mercureID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}}),
	).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, false, false, nil
	}

	if err != nil {
		return 0, false, false, fmt.Errorf("looking up Last-Event-ID: %w", err)
	}

	return doc.Seq, true, doc.First, nil
}

// markSeen records the visible Mercure ID in the seen collection and reports
// whether this call created the marker, that is whether the ID has no occurrence
// in the retention window yet. The upsert is atomic on a single document, so two
// publishers of the same new ID cannot both observe it as new. last_at tracks the
// most recent occurrence, so Cleanup can drop the marker once the ID leaves the
// window and the collection stays bounded.
func (s *mongoStore) markSeen(ctx context.Context, mercureID string) (bool, error) {
	now := time.Now().UTC()

	res, err := s.seen.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: mercureID}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "last_at", Value: now}}},
			{Key: "$setOnInsert", Value: bson.D{{Key: "created_at", Value: now}}},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return false, fmt.Errorf("marking the visible ID as seen: %w", err)
	}

	return res.UpsertedCount > 0, nil
}

// Scan calls fn for every document with fromExclusive < Seq <= toInclusive, in
// sequence order.
func (s *mongoStore) Scan(ctx context.Context, fromExclusive, toInclusive int64, fn func(*eventDoc) bool) error {
	if toInclusive <= fromExclusive {
		return nil
	}

	cursor, err := s.events.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{
			{Key: "$gt", Value: fromExclusive},
			{Key: "$lte", Value: toInclusive},
		}}},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return fmt.Errorf("scanning history: %w", err)
	}

	defer cursor.Close(ctx) //nolint:errcheck

	for cursor.Next(ctx) {
		var doc eventDoc
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("decoding history event: %w", err)
		}

		if !fn(&doc) {
			return nil
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("scanning history: %w", err)
	}

	return nil
}

// Latest returns the most recent document, or nil when empty.
func (s *mongoStore) Latest(ctx context.Context) (*eventDoc, error) {
	var doc eventDoc

	err := s.events.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("reading latest event: %w", err)
	}

	return &doc, nil
}

// Cleanup keeps only the newest retention documents.
func (s *mongoStore) Cleanup(ctx context.Context, retention int64) error {
	if retention <= 0 {
		return nil
	}

	var boundary eventDoc

	err := s.events.FindOne(ctx, bson.D{},
		options.FindOne().
			SetSort(bson.D{{Key: "_id", Value: -1}}).
			SetSkip(retention-1),
	).Decode(&boundary)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil // fewer documents than the retention window
	}

	if err != nil {
		return fmt.Errorf("finding retention boundary: %w", err)
	}

	if _, err := s.events.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: boundary.Seq}}}}); err != nil {
		return fmt.Errorf("deleting events past retention: %w", err)
	}

	// Drop the seen markers whose last occurrence predates the oldest retained
	// event, so the collection stays bounded by the retention window.
	if _, err := s.seen.DeleteMany(ctx, bson.D{{Key: "last_at", Value: bson.D{{Key: "$lt", Value: boundary.CreatedAt}}}}); err != nil {
		return fmt.Errorf("deleting seen markers past retention: %w", err)
	}

	return nil
}

// ResetSequence discards the cached sequence after the events collection was
// dropped or renamed. The next Append reads the empty collection and starts at
// 1 again.
func (s *mongoStore) ResetSequence() {
	s.hint.Store(0)
}

// Watch opens the change stream, optionally resuming after the given token.
//
// The stream is not filtered by operation type: the consumer must also see
// "invalidate" events, which signal that the collection was dropped or renamed
// and that the sequence restarted. Retention deletes are seen as "delete" and
// ignored.
func (s *mongoStore) Watch(ctx context.Context, resumeToken []byte) (eventStream, error) {
	opts := options.ChangeStream().SetMaxAwaitTime(s.watchAwait)

	if len(resumeToken) > 0 {
		opts = opts.SetResumeAfter(bson.Raw(resumeToken))
	}

	cs, err := s.events.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return nil, fmt.Errorf("opening change stream: %w", err)
	}

	return &mongoStream{cs: cs}, nil
}

// LoadCheckpoint returns the stored checkpoint, or nil when none exists.
func (s *mongoStore) LoadCheckpoint(ctx context.Context) (*checkpoint, error) {
	var doc struct {
		ResumeToken []byte    `bson:"resume_token"`
		Seq         int64     `bson:"seq"`
		UpdatedAt   time.Time `bson:"updated_at"`
	}

	err := s.checkpoints.FindOne(ctx, bson.D{{Key: "_id", Value: s.hubName}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("reading checkpoint: %w", err)
	}

	return &checkpoint{ResumeToken: doc.ResumeToken, Seq: doc.Seq, UpdatedAt: doc.UpdatedAt}, nil
}

// SaveCheckpoint overwrites the stored checkpoint for this hub.
func (s *mongoStore) SaveCheckpoint(ctx context.Context, cp *checkpoint) error {
	_, err := s.checkpoints.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: s.hubName}},
		bson.D{
			{Key: "_id", Value: s.hubName},
			{Key: "resume_token", Value: cp.ResumeToken},
			{Key: "seq", Value: cp.Seq},
			{Key: "updated_at", Value: cp.UpdatedAt},
		},
		options.Replace().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("saving checkpoint: %w", err)
	}

	return nil
}

// Ping checks that MongoDB is reachable.
func (s *mongoStore) Ping(ctx context.Context) error {
	if err := s.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("pinging MongoDB: %w", err)
	}

	return nil
}

// Close disconnects the client.
func (s *mongoStore) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}

	if err := s.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("disconnecting MongoDB: %w", err)
	}

	return nil
}

// mongoStream wraps a driver change stream as an eventStream.
type mongoStream struct {
	cs *mongo.ChangeStream
}

func (s *mongoStream) Next(ctx context.Context) (watchedEvent, bool, error) {
	if !s.cs.Next(ctx) {
		if err := s.cs.Err(); err != nil {
			return watchedEvent{}, false, err //nolint:wrapcheck
		}

		// MaxAwaitTime elapsed with no new event.
		return watchedEvent{}, false, nil
	}

	var ev struct {
		OperationType string    `bson:"operationType"`
		FullDocument  *eventDoc `bson:"fullDocument"`
	}

	if err := s.cs.Decode(&ev); err != nil {
		return watchedEvent{}, false, fmt.Errorf("decoding change event: %w", err)
	}

	token := make([]byte, len(s.cs.ResumeToken()))
	copy(token, s.cs.ResumeToken())

	if ev.OperationType != opInsert || ev.FullDocument == nil {
		// Retention deletes and other non insert events carry no update.
		return watchedEvent{OperationType: ev.OperationType, Token: token}, true, nil
	}

	return watchedEvent{OperationType: opInsert, Doc: ev.FullDocument, Token: token}, true, nil
}

func (s *mongoStream) Close(ctx context.Context) error {
	if err := s.cs.Close(ctx); err != nil {
		return fmt.Errorf("closing change stream: %w", err)
	}

	return nil
}

// isResumeTokenError reports whether err means the resume token can no longer
// be used, so the hub must resynchronize from the durable collection.
func isResumeTokenError(err error) bool {
	if err == nil {
		return false
	}

	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		switch cmdErr.Code {
		case 260, // InvalidResumeToken
			280, // ChangeStreamFatalError
			286: // ChangeStreamHistoryLost
			return true
		}
	}

	return false
}

// Interface guard.
var _ store = (*mongoStore)(nil)
