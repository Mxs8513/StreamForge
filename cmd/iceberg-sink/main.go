// Command iceberg-sink reads StreamForge's committed checkpoint output from
// S3/MinIO and appends it into an Apache Iceberg table — one Iceberg snapshot
// per checkpoint — then demonstrates a time-travel read (spec Phase 8 / §13).
//
// This is the "stream -> lakehouse" pattern: committed Parquet is registered
// into an Iceberg table whose optimistic-concurrency commit gives atomic,
// snapshot-isolated table updates and time-travel for free. Iceberg is used as
// the transactional sink; it is not reimplemented.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/Mxs8513/StreamForge/internal/checkpoint"
	"github.com/Mxs8513/StreamForge/internal/config"
	"github.com/Mxs8513/StreamForge/internal/event"
	"github.com/Mxs8513/StreamForge/internal/iceberg"
	"github.com/Mxs8513/StreamForge/internal/storage"
)

func main() {
	var (
		s3Endpoint = flag.String("s3-endpoint", config.Env("S3_ENDPOINT", config.DefaultS3Endpoint), "S3/MinIO endpoint (committed output source)")
		s3Bucket   = flag.String("s3-bucket", config.Env("S3_BUCKET", config.DefaultS3Bucket), "S3 bucket")
		warehouse  = flag.String("warehouse", "./data/iceberg/warehouse", "local Iceberg warehouse directory")
		catalogDB  = flag.String("catalog-db", "./data/iceberg/catalog.db", "SQLite catalog file")
		namespace  = flag.String("namespace", "streamforge", "Iceberg namespace")
		tableName  = flag.String("table", "aggregates", "Iceberg table name")
	)
	flag.Parse()

	ctx := context.Background()
	whAbs, err := filepath.Abs(*warehouse)
	if err != nil {
		log.Fatalf("warehouse path: %v", err)
	}
	dbAbs, err := filepath.Abs(*catalogDB)
	if err != nil {
		log.Fatalf("catalog path: %v", err)
	}
	if err := os.MkdirAll(whAbs, 0o755); err != nil {
		log.Fatalf("warehouse dir: %v", err)
	}

	store, err := storage.New(ctx, storage.Config{
		Endpoint:  *s3Endpoint,
		Region:    config.DefaultS3Region,
		Bucket:    *s3Bucket,
		AccessKey: config.Env("S3_ACCESS_KEY", config.DefaultS3AccessKey),
		SecretKey: config.Env("S3_SECRET_KEY", config.DefaultS3SecretKey),
	})
	if err != nil {
		log.Fatalf("storage: %v", err)
	}

	metas, err := checkpoint.NewStore(store).AllCompleted(ctx)
	if err != nil {
		log.Fatalf("read checkpoints: %v", err)
	}
	if len(metas) == 0 {
		log.Fatal("no completed checkpoints found — run a pipeline first (e.g. make p6-test)")
	}

	sink, err := iceberg.Open(ctx, "file://"+dbAbs, whAbs, *namespace, *tableName)
	if err != nil {
		log.Fatalf("iceberg open: %v", err)
	}

	log.Printf("appending %d committed checkpoints into iceberg table %s.%s", len(metas), *namespace, *tableName)
	for _, m := range metas {
		rows, rerr := readCheckpointRows(ctx, store, m)
		if rerr != nil {
			log.Fatalf("read checkpoint %d: %v", m.CheckpointID, rerr)
		}
		if err := sink.AppendCheckpoint(ctx, m.CheckpointID, rows); err != nil {
			log.Fatalf("append: %v", err)
		}
		log.Printf("  checkpoint %d -> snapshot (%d rows)", m.CheckpointID, len(rows))
	}

	// Snapshot history (one per checkpoint) + time-travel demo.
	snaps := sink.Snapshots()
	fmt.Println("\n=== Iceberg snapshot history (one snapshot per checkpoint) ===")
	for i, s := range snaps {
		fmt.Printf("  #%d  snapshot_id=%d  at=%s  total_records=%s\n",
			i+1, s.ID, time.UnixMilli(s.TimestampMs).Format("15:04:05.000"), s.TotalRecords)
	}

	if len(snaps) >= 2 {
		first, last := snaps[0], snaps[len(snaps)-1]
		firstN, err := sink.CountAsOf(ctx, first.ID)
		if err != nil {
			log.Fatalf("time-travel scan: %v", err)
		}
		lastN, err := sink.CountAsOf(ctx, last.ID)
		if err != nil {
			log.Fatalf("time-travel scan: %v", err)
		}
		fmt.Println("\n=== Time-travel query ===")
		fmt.Printf("  rows AS OF first snapshot (%d): %d\n", first.ID, firstN)
		fmt.Printf("  rows AS OF latest snapshot (%d): %d\n", last.ID, lastN)
		fmt.Printf("  -> the table grew by %d rows across %d checkpoint snapshots; each prior\n", lastN-firstN, len(snaps))
		fmt.Println("     state remains queryable by snapshot id (time-travel).")
	}
}

// readCheckpointRows reads one checkpoint's committed staged Parquet, deduped by
// (key, window_start), tagging each row with the checkpoint id.
func readCheckpointRows(ctx context.Context, store *storage.Client, m checkpoint.Metadata) ([]event.OutputRecord, error) {
	seen := map[string]bool{}
	var out []event.OutputRecord
	for _, key := range m.StagedOutputs {
		data, err := store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		recs, err := storage.DecodeParquet(data)
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			id := fmt.Sprintf("%s@%d", r.Key, r.WindowStart)
			if seen[id] {
				continue
			}
			seen[id] = true
			r.CheckpointID = m.CheckpointID
			out = append(out, r)
		}
	}
	return out, nil
}
