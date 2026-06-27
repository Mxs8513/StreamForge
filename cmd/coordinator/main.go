// Command coordinator runs the StreamForge coordinator (spec §6.1): it accepts
// worker registrations, hands back partition + key-bucket assignments plus the
// shuffle routing table, and (Phase 4) drives periodic aligned checkpoints.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mxs8513/StreamForge/internal/checkpoint"
	"github.com/Mxs8513/StreamForge/internal/config"
	"github.com/Mxs8513/StreamForge/internal/coordinator"
	"github.com/Mxs8513/StreamForge/internal/metrics"
	"github.com/Mxs8513/StreamForge/internal/storage"
)

func main() {
	var (
		addr        = flag.String("addr", config.Env("COORDINATOR_ADDR", ":7070"), "gRPC listen address")
		workers     = flag.Int("workers", config.EnvInt("WORKERS", 1), "expected worker count (assignment finalizes once all register)")
		partitions  = flag.Int("kafka-partitions", config.EnvInt("KAFKA_PARTITIONS", 6), "total Kafka partitions to distribute")
		buckets     = flag.Int("buckets", config.EnvInt("BUCKETS", config.DefaultBuckets), "total key-bucket space")
		ckptMS      = flag.Int("checkpoint-interval-ms", config.EnvInt("CHECKPOINT_INTERVAL_MS", 0), "checkpoint interval (ms); 0 disables checkpointing")
		ckptTimeout = flag.Int("checkpoint-timeout-ms", config.EnvInt("CHECKPOINT_TIMEOUT_MS", 5000), "per-worker checkpoint RPC timeout (ms)")
		failTimeout = flag.Int("failure-timeout-ms", config.EnvInt("FAILURE_TIMEOUT_MS", 4000), "declare a worker dead after this long without a heartbeat")
		detectMS    = flag.Int("detect-interval-ms", config.EnvInt("DETECT_INTERVAL_MS", 1000), "failure-detector scan interval (ms)")
		metricsAddr = flag.String("metrics-addr", config.Env("COORD_METRICS_ADDR", ":2120"), "Prometheus /metrics listen addr")
		s3Endpoint  = flag.String("s3-endpoint", config.Env("S3_ENDPOINT", config.DefaultS3Endpoint), "S3/MinIO endpoint")
		s3Bucket    = flag.String("s3-bucket", config.Env("S3_BUCKET", config.DefaultS3Bucket), "S3 bucket")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := coordinator.New(*workers, *partitions, *buckets, time.Duration(*failTimeout)*time.Millisecond)
	metrics.Serve(*metricsAddr)
	log.Printf("coordinator: addr=%s workers=%d partitions=%d buckets=%d checkpoint=%dms failure-timeout=%dms metrics=%s",
		*addr, *workers, *partitions, *buckets, *ckptMS, *failTimeout, *metricsAddr)

	// Failure detector: declares unseen workers dead and triggers rebalance.
	stopDetect := make(chan struct{})
	defer close(stopDetect)
	go c.DetectFailures(time.Duration(*detectMS)*time.Millisecond, stopDetect)

	if *ckptMS > 0 {
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
		if err := store.EnsureBucket(ctx); err != nil {
			log.Fatalf("ensure bucket: %v", err)
		}
		orch := coordinator.NewOrchestrator(c, checkpoint.NewStore(store), time.Duration(*ckptTimeout)*time.Millisecond)
		go orch.Run(ctx, time.Duration(*ckptMS)*time.Millisecond)
	}

	if err := coordinator.Serve(ctx, *addr, c); err != nil && ctx.Err() == nil {
		log.Fatalf("coordinator: %v", err)
	}
	log.Printf("coordinator: shut down")
}
