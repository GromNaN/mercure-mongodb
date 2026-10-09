package mercuremongodb

import (
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalCaddyfile(t *testing.T) {
	t.Parallel()

	d := caddyfile.NewTestDispenser(`mongodb {
		uri mongodb://localhost:27017/?replicaSet=rs0
		database mercuredb
		collection events
		checkpoints cp
		retention 100
		cleanup_interval 1m
		checkpoint_interval 2s
		watch_await_time 500ms
		connect_timeout 5s
	}`)

	m := new(MongoDB)
	require.NoError(t, m.UnmarshalCaddyfile(d))

	assert.Equal(t, "mongodb://localhost:27017/?replicaSet=rs0", m.URI)
	assert.Equal(t, "mercuredb", m.Database)
	assert.Equal(t, "events", m.Collection)
	assert.Equal(t, "cp", m.Checkpoints)
	assert.Equal(t, int64(100), m.Retention)
	assert.Equal(t, caddy.Duration(time.Minute), m.CleanupInterval)
	assert.Equal(t, caddy.Duration(2*time.Second), m.CheckpointInterval)
	assert.Equal(t, caddy.Duration(500*time.Millisecond), m.WatchAwaitTime)
	assert.Equal(t, caddy.Duration(5*time.Second), m.ConnectTimeout)
}

func TestUnmarshalCaddyfileRejectsUnknownDirective(t *testing.T) {
	t.Parallel()

	d := caddyfile.NewTestDispenser(`mongodb {
		nope 1
	}`)

	require.Error(t, new(MongoDB).UnmarshalCaddyfile(d))
}

func TestUnmarshalCaddyfileRejectsNegativeRetention(t *testing.T) {
	t.Parallel()

	d := caddyfile.NewTestDispenser(`mongodb {
		retention -1
	}`)

	require.Error(t, new(MongoDB).UnmarshalCaddyfile(d))
}

func TestKeyDoesNotLeakCredentials(t *testing.T) {
	t.Parallel()

	m := &MongoDB{
		URI:        "mongodb://user:secret@example.com:27017/?replicaSet=rs0",
		Database:   "mercure",
		Collection: "updates",
	}

	key := m.key("hub-a")

	assert.NotContains(t, key, "secret")
	assert.NotContains(t, key, "user")
	assert.True(t, strings.HasPrefix(key, "mongodb:hub-a:mercure:updates:"))
}
