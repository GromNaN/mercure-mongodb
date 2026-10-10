package mercuremongodb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// integrationURI returns the replica set connection string. The integration
// tests are skipped when MONGODB_URI is not set. The target must be a replica
// set, because change streams are required.
func integrationURI(t *testing.T) string {
	t.Helper()

	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		t.Skip("set MONGODB_URI to a replica set to run the MongoDB integration tests")
	}

	return uri
}

func dropDatabase(t *testing.T, uri, database string) {
	t.Helper()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, client.Database(database).Drop(context.Background()))
		require.NoError(t, client.Disconnect(context.Background()))
	})
}

func newIntegrationTransport(t *testing.T, uri, database, hubName string) *MongoDBTransport {
	t.Helper()

	return NewMongoDBTransport(mercure.NewSubscriberList(0), testLogger(), Config{
		URI:         uri,
		Database:    database,
		Collection:  "updates",
		Checkpoints: "checkpoints",
		HubName:     hubName,
	})
}

func TestIntegrationReplay(t *testing.T) {
	uri := integrationURI(t)
	database := fmt.Sprintf("mercure_it_%d", time.Now().UnixNano())
	dropDatabase(t, uri, database)

	ctx := t.Context()
	tr := newIntegrationTransport(t, uri, database, "hub-a")
	t.Cleanup(func() { require.NoError(t, tr.Close(context.Background())) })

	dispatchIDs(t, tr, []string{testTopic}, "1", "2", "3")
	waitCommitted(t, tr, 3)

	s := newTestSubscriber(mercure.EarliestLastEventID, []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(ctx, s))

	assert.Equal(t, []string{"1", "2", "3"}, readIDs(t, s, 3))
	assertNoEvent(t, s)
}

func TestIntegrationCrossInstanceLive(t *testing.T) {
	uri := integrationURI(t)
	database := fmt.Sprintf("mercure_it_%d", time.Now().UnixNano())
	dropDatabase(t, uri, database)

	ctx := t.Context()

	// Two hub instances sharing the same collection.
	publisher := newIntegrationTransport(t, uri, database, "hub-a")
	subscriber := newIntegrationTransport(t, uri, database, "hub-b")

	t.Cleanup(func() {
		require.NoError(t, publisher.Close(context.Background()))
		require.NoError(t, subscriber.Close(context.Background()))
	})

	s := newTestSubscriber(mercure.EarliestLastEventID, []string{testTopic}, nil)
	require.NoError(t, subscriber.AddSubscriber(ctx, s))

	// A publish on hub A must reach a subscriber connected to hub B.
	dispatchIDs(t, publisher, []string{testTopic}, "cross")

	assert.Equal(t, []string{"cross"}, readIDs(t, s, 1))
}

func TestIntegrationReconnectReplaysFromLastEventID(t *testing.T) {
	uri := integrationURI(t)
	database := fmt.Sprintf("mercure_it_%d", time.Now().UnixNano())
	dropDatabase(t, uri, database)

	ctx := t.Context()

	tr := newIntegrationTransport(t, uri, database, "hub-a")
	dispatchIDs(t, tr, []string{testTopic}, "1", "2", "3")
	waitCommitted(t, tr, 3)

	// A client that already has event 2 reconnects and must receive 3.
	s := newTestSubscriber("2", []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(ctx, s))
	assert.Equal(t, []string{"3"}, readIDs(t, s, 1))
	assertNoEvent(t, s)

	require.NoError(t, tr.Close(context.Background()))
}

func TestIntegrationUnknownLastEventIDAnswersEarliest(t *testing.T) {
	uri := integrationURI(t)
	database := fmt.Sprintf("mercure_it_%d", time.Now().UnixNano())
	dropDatabase(t, uri, database)

	ctx := t.Context()
	tr := newIntegrationTransport(t, uri, database, "hub-a")
	t.Cleanup(func() { require.NoError(t, tr.Close(context.Background())) })

	dispatchIDs(t, tr, []string{testTopic}, "1", "2")
	waitCommitted(t, tr, 2)

	s := newTestSubscriber("does-not-exist", []string{testTopic}, nil)
	require.NoError(t, tr.AddSubscriber(ctx, s))
	assertNoEvent(t, s)
}
