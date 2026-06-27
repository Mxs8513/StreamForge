// Command verify reads StreamForge output from S3/MinIO and reports totals,
// per-(key,window) duplicate counts, and a per-key rollup.
//
//	--prefix output/run/   scans a raw output prefix (Phases 1-5, at-least-once)
//	--committed            reads ONLY the staged files referenced by COMPLETED
//	                       checkpoints, deduped by (key,window) — the Phase 6
//	                       exactly-once committed view.
//
// The rollup is window-assignment-independent (per-key count/sum/min/max), so
// runs that window events differently still compare equal when no event is lost
// or double-counted.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/Mxs8513/StreamForge/internal/checkpoint"
	"github.com/Mxs8513/StreamForge/internal/config"
	"github.com/Mxs8513/StreamForge/internal/event"
	"github.com/Mxs8513/StreamForge/internal/storage"
)

type keyRollup struct {
	count int64
	sum   float64
	min   float64
	max   float64
	init  bool
}

func main() {
	var (
		s3Endpoint = flag.String("s3-endpoint", config.Env("S3_ENDPOINT", config.DefaultS3Endpoint), "S3/MinIO endpoint")
		s3Bucket   = flag.String("s3-bucket", config.Env("S3_BUCKET", config.DefaultS3Bucket), "S3 bucket")
		prefix     = flag.String("prefix", "output/", "object prefix to scan (raw output mode)")
		committed  = flag.Bool("committed", false, "read only staged files from COMPLETED checkpoints (exactly-once view)")
		rollupOut  = flag.String("rollup", "", "write per-key rollup to this file (for cross-run comparison)")
	)
	flag.Parse()

	ctx := context.Background()
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

	var objKeys []string
	source := "prefix " + *prefix
	if *committed {
		metas, merr := checkpoint.NewStore(store).AllCompleted(ctx)
		if merr != nil {
			log.Fatalf("read checkpoints: %v", merr)
		}
		for _, m := range metas {
			objKeys = append(objKeys, m.StagedOutputs...)
		}
		source = fmt.Sprintf("committed checkpoints (%d)", len(metas))
	} else {
		keys, lerr := store.List(ctx, *prefix)
		if lerr != nil {
			log.Fatalf("list: %v", lerr)
		}
		for _, k := range keys {
			if strings.HasSuffix(k, ".parquet") {
				objKeys = append(objKeys, k)
			}
		}
	}

	// Dedup by (key, window): each window contributes its aggregate exactly once.
	seen := map[string]event.OutputRecord{}
	dupes := 0
	for _, k := range objKeys {
		data, err := store.Get(ctx, k)
		if err != nil {
			log.Fatalf("get %s: %v", k, err)
		}
		rows, err := storage.DecodeParquet(data)
		if err != nil {
			log.Fatalf("decode %s: %v", k, err)
		}
		for _, r := range rows {
			id := fmt.Sprintf("%s@%d", r.Key, r.WindowStart)
			if _, ok := seen[id]; ok {
				dupes++
				continue
			}
			seen[id] = r
		}
	}

	var totalEvents int64
	rollup := map[string]*keyRollup{}
	for _, r := range seen {
		totalEvents += r.Count
		foldRollup(rollup, r)
	}

	fmt.Printf("source:                     %s\n", source)
	fmt.Printf("parquet files:              %d\n", len(objKeys))
	fmt.Printf("total events counted:       %d\n", totalEvents)
	fmt.Printf("distinct keys:              %d\n", len(rollup))
	fmt.Printf("distinct (key,window) rows: %d\n", len(seen))
	fmt.Printf("duplicate (key,window) rows: %d\n", dupes)

	if *rollupOut != "" {
		if err := writeRollup(*rollupOut, rollup); err != nil {
			log.Fatalf("write rollup: %v", err)
		}
		fmt.Printf("rollup written:             %s (%d keys)\n", *rollupOut, len(rollup))
	}
}

func foldRollup(m map[string]*keyRollup, r event.OutputRecord) {
	kr := m[r.Key]
	if kr == nil {
		kr = &keyRollup{}
		m[r.Key] = kr
	}
	kr.count += r.Count
	kr.sum += r.SumAmount
	if !kr.init {
		kr.min, kr.max, kr.init = r.MinAmount, r.MaxAmount, true
	} else {
		if r.MinAmount < kr.min {
			kr.min = r.MinAmount
		}
		if r.MaxAmount > kr.max {
			kr.max = r.MaxAmount
		}
	}
}

// writeRollup emits sorted "key\tcount\tsum_cents\tmin\tmax". sum is rounded to
// integer cents so float addition order (which differs between 1 and 3 workers)
// cannot cause a spurious mismatch; the true sum is an exact multiple of 0.01.
func writeRollup(path string, m map[string]*keyRollup) error {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		kr := m[k]
		cents := int64(math.Round(kr.sum * 100))
		fmt.Fprintf(&b, "%s\t%d\t%d\t%.2f\t%.2f\n", k, kr.count, cents, kr.min, kr.max)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
