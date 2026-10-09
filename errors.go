package mercuremongodb

import "errors"

var (
	// errStoreClosed is returned when an operation runs after Close.
	errStoreClosed = errors.New("mercure mongodb transport: store is closed")

	// errTooMuchContention is returned when the gapless sequence cannot be
	// assigned after several retries. It usually means a very high publish rate
	// across several hubs on the same collection.
	errTooMuchContention = errors.New("mercure mongodb transport: too much contention assigning the event sequence")

	// errStandalone is returned at provision time when the MongoDB deployment
	// cannot serve change streams.
	errStandalone = errors.New("mercure mongodb transport: the MongoDB deployment is a standalone server, which does not support change streams; use a replica set or a sharded cluster")
)
