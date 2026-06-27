// Package event defines the wire format for input events and output records.
//
// Phase 0-2 use JSON on the Kafka wire for debuggability. The spec calls for
// Protobuf on the wire (proto/streamforge.proto) which is introduced alongside
// gRPC in Phase 3; the encode/decode helpers below give us a single seam to
// swap the codec without touching the pipeline.
package event

import (
	"encoding/json"
	"hash/fnv"
)

// Event is the input record produced by the generator and carried by Kafka.
type Event struct {
	EventID    string  `json:"event_id"`    // unique; used for idempotency + reconciliation
	Key        string  `json:"key"`         // the partition key (keyBy field)
	EventType  string  `json:"event_type"`  // categorical
	Amount     float64 `json:"amount"`      // numeric payload for aggregation
	EventTime  int64   `json:"event_time"`  // epoch millis, EVENT time, set by producer
	IngestTime int64   `json:"ingest_time"` // epoch millis, filled by source operator on consume
}

// Encode serializes an event for the Kafka value.
func (e *Event) Encode() ([]byte, error) { return json.Marshal(e) }

// Decode parses a Kafka value into an event.
func Decode(b []byte) (*Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// Bucket maps a key onto one of numBuckets hash buckets. This is the basis for
// keyBy routing/shuffle in Phase 3; in single-worker phases every bucket is
// owned locally, but using the same function now keeps the answer identical
// once distribution is added.
func Bucket(key string, numBuckets int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()) % numBuckets
}
