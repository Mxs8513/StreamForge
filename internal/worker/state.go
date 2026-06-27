package worker

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"

	badger "github.com/dgraph-io/badger/v4"
)

// PartialAgg is the in-flight aggregate for one (window, key). It is the unit
// of keyed state the engine snapshots in Phase 4.
type PartialAgg struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64
	Types map[string]struct{} // distinct event types (exact set; low cardinality)
}

// Add folds one event's payload into the aggregate.
func (p *PartialAgg) Add(amount float64, eventType string) {
	if p.Count == 0 {
		p.Min = amount
		p.Max = amount
	} else {
		if amount < p.Min {
			p.Min = amount
		}
		if amount > p.Max {
			p.Max = amount
		}
	}
	p.Count++
	p.Sum += amount
	if p.Types == nil {
		p.Types = make(map[string]struct{})
	}
	p.Types[eventType] = struct{}{}
}

func (p *PartialAgg) encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(p); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeAgg(b []byte) (*PartialAgg, error) {
	var p PartialAgg
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

// State is a BadgerDB-backed keyed state store. Keys are (windowStart, key);
// values are encoded PartialAgg. BadgerDB is a pure-Go embedded KV store whose
// snapshot support the Phase 4 checkpointer relies on.
type State struct {
	db *badger.DB
}

// OpenState opens (or creates) a BadgerDB instance at dir. Pass an empty dir for
// an in-memory store (used by tests).
func OpenState(dir string) (*State, error) {
	var opts badger.Options
	if dir == "" {
		opts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		opts = badger.DefaultOptions(dir)
	}
	opts = opts.WithLoggingLevel(badger.WARNING)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &State{db: db}, nil
}

// DB exposes the underlying handle for the Phase 4 snapshot/restore layer.
func (s *State) DB() *badger.DB { return s.db }

func (s *State) Close() error { return s.db.Close() }

// Snapshot serializes the entire keyed state into a self-describing KV dump:
// a sequence of [uvarint keyLen][key][uvarint valLen][val]. Unlike an opaque
// BadgerDB backup, this format can be restored selectively (RestoreFiltered),
// which Phase 5 recovery needs to redistribute keyed state by bucket. The caller
// must ensure no concurrent writes (the single aggregator goroutine takes the
// snapshot, so state is quiescent). This image plus the offsets it corresponds
// to form one atomic checkpoint unit.
func (s *State) Snapshot() ([]byte, error) {
	var buf bytes.Buffer
	var hdr [binary.MaxVarintLen64]byte
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			if verr := item.Value(func(v []byte) error {
				n := binary.PutUvarint(hdr[:], uint64(len(k)))
				buf.Write(hdr[:n])
				buf.Write(k)
				n = binary.PutUvarint(hdr[:], uint64(len(v)))
				buf.Write(hdr[:n])
				buf.Write(v)
				return nil
			}); verr != nil {
				return verr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Restore loads every entry from a snapshot into this store.
func (s *State) Restore(data []byte) error {
	return s.RestoreFiltered(data, func([]byte) bool { return true })
}

// RestoreFiltered loads only entries whose stored key satisfies keep. Phase 5
// recovery uses this to import, from each worker's checkpoint snapshot, just the
// keys whose bucket the restoring worker now owns — so each owned bucket's state
// is gathered from wherever it lived, with no duplication.
func (s *State) RestoreFiltered(data []byte, keep func(stateKey []byte) bool) error {
	r := bytes.NewReader(data)
	return s.db.Update(func(txn *badger.Txn) error {
		for r.Len() > 0 {
			kl, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			k := make([]byte, kl)
			if _, err := io.ReadFull(r, k); err != nil {
				return err
			}
			vl, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			v := make([]byte, vl)
			if _, err := io.ReadFull(r, v); err != nil {
				return err
			}
			if keep(k) {
				if err := txn.Set(k, v); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// KeyOf returns the user key portion of a stored state key (drops the 8-byte
// window prefix), so callers can compute its bucket for restore filtering.
func KeyOf(stateKey []byte) string {
	if len(stateKey) < 8 {
		return ""
	}
	return string(stateKey[8:])
}

// stateKey encodes (windowStart, key) as 8-byte big-endian start || key bytes.
// Big-endian ordering makes a window-prefix scan possible later if needed.
func stateKey(windowStart int64, key string) []byte {
	b := make([]byte, 8+len(key))
	binary.BigEndian.PutUint64(b[:8], uint64(windowStart))
	copy(b[8:], key)
	return b
}

func splitKey(b []byte) (int64, string) {
	return int64(binary.BigEndian.Uint64(b[:8])), string(b[8:])
}

// Get returns the aggregate for (windowStart, key), or nil if absent.
func (s *State) Get(windowStart int64, key string) (*PartialAgg, error) {
	var agg *PartialAgg
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(stateKey(windowStart, key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			a, derr := decodeAgg(v)
			if derr != nil {
				return derr
			}
			agg = a
			return nil
		})
	})
	return agg, err
}

// Put writes the aggregate for (windowStart, key).
func (s *State) Put(windowStart int64, key string, agg *PartialAgg) error {
	enc, err := agg.encode()
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(stateKey(windowStart, key), enc)
	})
}

// WindowEntry pairs a stored aggregate with its (windowStart, key) identity.
type WindowEntry struct {
	WindowStart int64
	Key         string
	Agg         *PartialAgg
}

// ScanClosed returns and deletes every entry whose window has closed, i.e.
// windowStart+windowSize <= now. This is how flushed windows are emitted and
// their state reclaimed.
func (s *State) ScanClosed(now, windowSize int64) ([]WindowEntry, error) {
	var out []WindowEntry
	var toDelete [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			windowStart, key := splitKey(k)
			if windowStart+windowSize > now {
				continue // window still open
			}
			err := item.Value(func(v []byte) error {
				a, derr := decodeAgg(v)
				if derr != nil {
					return derr
				}
				out = append(out, WindowEntry{WindowStart: windowStart, Key: key, Agg: a})
				return nil
			})
			if err != nil {
				return err
			}
			toDelete = append(toDelete, k)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(toDelete) > 0 {
		err = s.db.Update(func(txn *badger.Txn) error {
			for _, k := range toDelete {
				if derr := txn.Delete(k); derr != nil {
					return derr
				}
			}
			return nil
		})
	}
	return out, err
}

// ScanAll returns and deletes every entry regardless of window state. Used to
// drain remaining windows at shutdown or end-of-bounded-stream.
func (s *State) ScanAll() ([]WindowEntry, error) {
	// A window size of 0 makes every window "closed" (start+0 <= now for any
	// now >= start); use max now to be safe.
	return s.ScanClosed(int64(^uint64(0)>>1), 0)
}
