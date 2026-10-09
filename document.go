package mercuremongodb

import (
	"time"

	"github.com/dunglas/mercure"
)

// currentSchemaVersion is the version of the event document layout. It is
// stored on every document so a future change can migrate or read old events.
const currentSchemaVersion int32 = 1

// eventDoc is the durable representation of a Mercure update. It is append
// only: an event is written once and never modified.
type eventDoc struct {
	// Seq is the internal cursor. It is the total order used for replay and
	// for the replay/live boundary. It is stored as the document _id.
	Seq int64 `bson:"_id"`

	SchemaVersion int32 `bson:"schema_version"`

	// MercureID is the visible Mercure update ID. It can be set by the
	// publisher, so it is neither unique nor ordered.
	MercureID string `bson:"mercure_id"`

	// First is true when this document is the first occurrence of its visible
	// Mercure ID. A Last-Event-ID that resolves to a document with First false
	// had an earlier occurrence that was removed, so the client position is
	// ambiguous and the transport answers "earliest".
	First bool `bson:"first"`

	Topics  []string `bson:"topics"`
	Private bool     `bson:"private"`
	Data    string   `bson:"data"`
	Type    string   `bson:"type,omitempty"`
	Retry   int64    `bson:"retry,omitempty"`
	Debug   bool     `bson:"debug,omitempty"`

	CreatedAt time.Time `bson:"created_at"`
}

// newEventDoc builds a document from an update. The caller must have called
// update.AssignUUID beforehand so that MercureID is set.
func newEventDoc(u *mercure.Update, now time.Time) *eventDoc {
	topics := make([]string, len(u.Topics))
	copy(topics, u.Topics)

	return &eventDoc{
		SchemaVersion: currentSchemaVersion,
		MercureID:     u.ID,
		Topics:        topics,
		Private:       u.Private,
		Data:          u.Data,
		Type:          u.Type,
		Retry:         int64(u.Retry), //nolint:gosec // Mercure retry is a small delay in milliseconds.
		Debug:         u.Debug,
		CreatedAt:     now,
	}
}

// update rebuilds a Mercure update from a stored document.
func (d *eventDoc) update() *mercure.Update {
	topics := make([]string, len(d.Topics))
	copy(topics, d.Topics)

	u := &mercure.Update{
		Event: mercure.Event{
			Data:  d.Data,
			ID:    d.MercureID,
			Type:  d.Type,
			Retry: uint64(d.Retry), //nolint:gosec // Stored from a uint64 Mercure retry.
		},
		Topics:  topics,
		Private: d.Private,
		Debug:   d.Debug,
	}

	return u
}
