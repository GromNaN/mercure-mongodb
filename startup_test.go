package mercuremongodb

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errUnreachable stands for any connection failure reported by the opener.
var errUnreachable = errors.New("server unreachable")

// flakyOpener fails its first failures calls, then delegates to inner. It makes
// a MongoDB that is not ready yet deterministic to test.
type flakyOpener struct {
	mu       sync.Mutex
	failures int
	calls    int
	inner    storeOpener
}

func (o *flakyOpener) open(ctx context.Context) (store, error) {
	o.mu.Lock()
	o.calls++
	fail := o.calls <= o.failures
	o.mu.Unlock()

	if fail {
		return nil, errUnreachable
	}

	return o.inner.open(ctx)
}

func (o *flakyOpener) callCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.calls
}

// silentServer accepts TCP connections and never answers, so the driver waits
// for its handshake timeout. It stands for a MongoDB that is reachable but not
// ready.
func silentServer(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var conns []net.Conn

	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			conn, acceptErr := l.Accept()
			if acceptErr != nil {
				return
			}

			conns = append(conns, conn)
		}
	}()

	t.Cleanup(func() {
		_ = l.Close()
		<-done

		for _, c := range conns {
			_ = c.Close()
		}
	})

	return l.Addr().String()
}

// TestStartupDoesNotWaitForMongoDB checks that the hub provisions while the
// connection is not up, so a MongoDB that is unreachable at startup cannot stop
// the hub from starting.
func TestStartupDoesNotWaitForMongoDB(t *testing.T) {
	addr := silentServer(t)

	start := time.Now()

	tr := NewMongoDBTransport(mercure.NewSubscriberList(0), testLogger(), Config{
		URI:            "mongodb://" + addr + "/?directConnection=true",
		ConnectTimeout: 10 * time.Second,
	})

	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("the constructor waited %s for MongoDB", elapsed)
	}

	closed := make(chan error, 1)

	go func() { closed <- tr.Close(context.Background()) }()

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return while the connection attempt was in flight")
	}
}

// TestConsumerRetriesUntilMongoDBAnswers checks that the connection is retried
// until the server answers, which is what lets a hub start before MongoDB.
func TestConsumerRetriesUntilMongoDBAnswers(t *testing.T) {
	opener := &flakyOpener{failures: 1, inner: staticOpener{st: newMemoryStore()}}

	tr := newTransport(opener, mercure.NewSubscriberList(0), testLogger(), Config{})
	tr.start()

	t.Cleanup(func() { require.NoError(t, tr.Close(context.Background())) })

	require.Eventually(t, func() bool {
		return tr.Ready(t.Context()) == nil
	}, 10*time.Second, 10*time.Millisecond)

	assert.GreaterOrEqual(t, opener.callCount(), 2, "the consumer must retry a failed connection")
}

// TestOperationsFailWhileMongoDBIsUnreachable checks that a publish and a
// subscriber report the failure instead of blocking, so the client can retry.
func TestOperationsFailWhileMongoDBIsUnreachable(t *testing.T) {
	opener := &flakyOpener{failures: 1 << 30, inner: staticOpener{st: newMemoryStore()}}

	tr := newTransport(opener, mercure.NewSubscriberList(0), testLogger(), Config{})
	tr.start()

	t.Cleanup(func() { require.NoError(t, tr.Close(context.Background())) })

	err := tr.Dispatch(t.Context(), &mercure.Update{
		Event:  mercure.Event{ID: "1"},
		Topics: []string{testTopic},
	})
	require.ErrorIs(t, err, errUnreachable)

	s := newTestSubscriber(mercure.EarliestLastEventID, []string{testTopic}, nil)
	require.ErrorIs(t, tr.AddSubscriber(t.Context(), s), errUnreachable)

	// Nothing is connected, so the health checks report it.
	require.ErrorIs(t, tr.Ready(t.Context()), errNotConnected)
	require.ErrorIs(t, tr.Live(t.Context()), errNotConnected)
	_, err = tr.Lag(t.Context())
	require.ErrorIs(t, err, errNotConnected)
}

func TestSleepStopsOnContextDone(t *testing.T) {
	assert.True(t, sleep(t.Context(), time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.False(t, sleep(ctx, time.Minute))
}
