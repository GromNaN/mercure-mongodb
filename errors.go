package mercuremongodb

import "errors"

var (
	// errStoreClosed is returned when an operation runs after Close.
	errStoreClosed = errors.New("mercure mongodb transport: store is closed")

	// errTooMuchContention is returned when the gapless sequence cannot be
	// assigned after several retries. It usually means a very high publish rate
	// across several hubs on the same collection.
	errTooMuchContention = errors.New("mercure mongodb transport: too much contention assigning the event sequence")

	// errNotConnected is returned when an operation needs MongoDB while the
	// connection is not up yet, either during startup or while the server is
	// unreachable.
	errNotConnected = errors.New("mercure mongodb transport: MongoDB is not connected")
)
