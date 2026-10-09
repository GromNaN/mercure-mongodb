// Package mercuremongodb provides a MongoDB transport for the Mercure hub,
// packaged as a Caddy module. It registers the http.handlers.mercure.mongodb
// module so a hub can use `transport mongodb { ... }` in a Caddyfile.
package mercuremongodb

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
	mercurecaddy "github.com/dunglas/mercure/caddy"
)

func init() { //nolint:gochecknoinits
	caddy.RegisterModule(MongoDB{})
}

// MongoDB is the Caddy module that builds the MongoDB transport.
type MongoDB struct {
	// URI is the MongoDB connection string. It may contain credentials, so it
	// is not logged and not stored in clear text in the transport key.
	URI string `json:"uri,omitempty"`

	Database    string `json:"database,omitempty"`
	Collection  string `json:"collection,omitempty"`
	Checkpoints string `json:"checkpoints,omitempty"`
	Seen        string `json:"seen,omitempty"`

	Retention int64 `json:"retention,omitempty"`

	CleanupInterval    caddy.Duration `json:"cleanup_interval,omitempty"`
	CheckpointInterval caddy.Duration `json:"checkpoint_interval,omitempty"`
	WatchAwaitTime     caddy.Duration `json:"watch_await_time,omitempty"`
	ConnectTimeout     caddy.Duration `json:"connect_timeout,omitempty"`

	transport    *MongoDBTransport
	transportKey string
}

// CaddyModule returns the Caddy module information.
func (MongoDB) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.mercure.mongodb",
		New: func() caddy.Module { return new(MongoDB) },
	}
}

// GetTransport returns the transport built at provision time.
func (m *MongoDB) GetTransport() mercure.Transport { //nolint:ireturn
	return m.transport
}

// Provision builds or reuses the transport.
func (m *MongoDB) Provision(ctx caddy.Context) error {
	if m.URI == "" {
		return fmt.Errorf("the mongodb transport requires a connection string, set the \"uri\" directive or the MERCURE_MONGODB_URI environment variable")
	}

	hubName, _ := ctx.Value(mercurecaddy.HubNameContextKey).(string)
	m.transportKey = m.key(hubName)

	destructor, _, err := mercurecaddy.TransportUsagePool.LoadOrNew(m.transportKey, func() (caddy.Destructor, error) {
		t, err := NewMongoDBTransport(
			ctx,
			mercure.NewSubscriberList(ctx.Value(mercurecaddy.SubscriberListCacheSizeContextKey).(int)), //nolint:forcetypeassert
			ctx.Slogger(),
			Config{
				URI:                m.URI,
				Database:           m.Database,
				Collection:         m.Collection,
				Checkpoints:        m.Checkpoints,
				Seen:               m.Seen,
				Retention:          m.Retention,
				CleanupInterval:    time.Duration(m.CleanupInterval),
				CheckpointInterval: time.Duration(m.CheckpointInterval),
				WatchAwaitTime:     time.Duration(m.WatchAwaitTime),
				ConnectTimeout:     time.Duration(m.ConnectTimeout),
				HubName:            hubName,
			},
		)
		if err != nil {
			return nil, err
		}

		return mercurecaddy.TransportDestructor[*MongoDBTransport]{Transport: t}, nil
	})
	if err != nil {
		return err //nolint:wrapcheck
	}

	m.transport = destructor.(mercurecaddy.TransportDestructor[*MongoDBTransport]).Transport

	return nil
}

// key builds a stable transport key that identifies the connection and the hub
// without exposing credentials. Every option that changes the behavior of the
// transport is part of the key, so two different configurations never share a
// transport instance. The URI is hashed, so credentials never appear in the key.
func (m *MongoDB) key(hubName string) string {
	sum := sha256.Sum256([]byte(m.URI))

	return fmt.Sprintf("mongodb:%s:%s:%s:%s:%s:%s:%d:%d:%d:%d:%d",
		hubName, m.Database, m.Collection, m.Checkpoints, m.Seen, hex.EncodeToString(sum[:8]),
		m.Retention, m.CleanupInterval, m.CheckpointInterval, m.WatchAwaitTime, m.ConnectTimeout)
}

// Cleanup releases the transport.
func (m *MongoDB) Cleanup() error {
	_, err := mercurecaddy.TransportUsagePool.Delete(m.transportKey)

	return err //nolint:wrapcheck
}

// UnmarshalCaddyfile parses the transport block.
func (m *MongoDB) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "uri":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.URI = d.Val()

			case "database":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.Database = d.Val()

			case "collection":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.Collection = d.Val()

			case "checkpoints":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.Checkpoints = d.Val()

			case "seen":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.Seen = d.Val()

			case "retention":
				if !d.NextArg() {
					return d.ArgErr()
				}

				retention, err := strconv.ParseInt(d.Val(), 10, 64)
				if err != nil {
					return d.WrapErr(err)
				}

				if retention < 0 {
					return d.Errf("retention must be >= 0, got %d", retention)
				}

				m.Retention = retention

			case "cleanup_interval":
				duration, err := parseDurationParameter(d)
				if err != nil {
					return err
				}

				m.CleanupInterval = duration

			case "checkpoint_interval":
				duration, err := parseDurationParameter(d)
				if err != nil {
					return err
				}

				m.CheckpointInterval = duration

			case "watch_await_time":
				duration, err := parseDurationParameter(d)
				if err != nil {
					return err
				}

				m.WatchAwaitTime = duration

			case "connect_timeout":
				duration, err := parseDurationParameter(d)
				if err != nil {
					return err
				}

				m.ConnectTimeout = duration

			default:
				return d.Errf("unknown mongodb transport directive: %s", d.Val())
			}
		}
	}

	return nil
}

var (
	_ caddy.Provisioner      = (*MongoDB)(nil)
	_ caddy.CleanerUpper     = (*MongoDB)(nil)
	_ caddyfile.Unmarshaler  = (*MongoDB)(nil)
	_ mercurecaddy.Transport = (*MongoDB)(nil)
)

// parseDurationParameter reads the next argument as a Caddy duration.
func parseDurationParameter(d *caddyfile.Dispenser) (caddy.Duration, error) {
	if !d.NextArg() {
		return 0, d.ArgErr() //nolint:wrapcheck
	}

	duration, err := caddy.ParseDuration(d.Val())
	if err != nil {
		return 0, d.WrapErr(err) //nolint:wrapcheck
	}

	return caddy.Duration(duration), nil
}
