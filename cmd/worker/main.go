// Command worker runs a single StreamForge worker: consume assigned Kafka
// partitions, keyBy-route/shuffle events to the owning worker, run stateful
// tumbling-window aggregation in BadgerDB, and write Parquet results to
// S3/MinIO (spec Phases 1-3).
//
// Offsets are owned in-process (groupless partition readers); nothing is
// committed back to Kafka's consumer group. Phase 4 folds those offsets into
// checkpoints.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Mxs8513/StreamForge/internal/config"
	"github.com/Mxs8513/StreamForge/internal/metrics"
	"github.com/Mxs8513/StreamForge/internal/storage"
	"github.com/Mxs8513/StreamForge/internal/worker"
)

func main() {
	var (
		brokers     = flag.String("brokers", config.Env("KAFKA_BROKERS", config.DefaultKafkaBrokers), "comma-separated Kafka brokers")
		topic       = flag.String("topic", config.Env("KAFKA_TOPIC", config.DefaultTopic), "Kafka topic")
		workerID    = flag.String("id", config.Env("WORKER_ID", "worker-1"), "worker id")
		shuffleAddr = flag.String("shuffle-addr", config.Env("SHUFFLE_ADDR", "127.0.0.1:7100"), "this worker's Worker gRPC (shuffle) address")
		coordAddr   = flag.String("coordinator", config.Env("COORDINATOR_ADDR", ""), "coordinator address; empty = standalone (own everything)")
		partitions  = flag.Int("kafka-partitions", config.EnvInt("KAFKA_PARTITIONS", 6), "total Kafka partitions in the topic")
		buckets     = flag.Int("buckets", config.EnvInt("BUCKETS", config.DefaultBuckets), "total key-bucket space (must match coordinator)")
		windowMS    = flag.Int("window-size-ms", config.EnvInt("WINDOW_SIZE_MS", config.DefaultWindowSizeMS), "tumbling window size (ms)")
		heartbeatMS = flag.Int("heartbeat-interval-ms", config.EnvInt("HEARTBEAT_INTERVAL_MS", 1000), "heartbeat interval (ms)")
		startEarly  = flag.Bool("from-earliest", true, "start consuming at the earliest offset (true for bounded runs)")
		restore     = flag.Bool("restore", false, "restore state + offsets from the latest completed checkpoint on start")
		runID       = flag.String("run-id", config.Env("RUN_ID", ""), "output namespace under output/<run-id>/")
		stateDir    = flag.String("state-dir", config.Env("STATE_DIR", ""), "BadgerDB dir (empty = in-memory)")
		metricsAddr = flag.String("metrics-addr", config.Env("METRICS_ADDR", ":2112"), "Prometheus /metrics listen addr")
		s3Endpoint  = flag.String("s3-endpoint", config.Env("S3_ENDPOINT", config.DefaultS3Endpoint), "S3/MinIO endpoint")
		s3Bucket    = flag.String("s3-bucket", config.Env("S3_BUCKET", config.DefaultS3Bucket), "S3 bucket")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	metrics.Serve(*metricsAddr)

	start := worker.StartLatest
	if *startEarly {
		start = worker.StartEarliest
	}
	prefix := "output"
	if *runID != "" {
		prefix = "output/" + *runID
	}

	sink := worker.NewSink(store, *workerID, prefix)

	rt := worker.NewRuntime(worker.Config{
		WorkerID:          *workerID,
		Brokers:           strings.Split(*brokers, ","),
		Topic:             *topic,
		ShuffleAddr:       *shuffleAddr,
		CoordinatorAddr:   *coordAddr,
		KafkaPartitions:   *partitions,
		NumBuckets:        *buckets,
		WindowSizeMS:      int64(*windowMS),
		FlushInterval:     time.Duration(*windowMS) * time.Millisecond,
		HeartbeatInterval: time.Duration(*heartbeatMS) * time.Millisecond,
		StartOffset:       start,
		Restore:           *restore,
		StateDir:          *stateDir,
	}, sink, store)

	log.Printf("worker %s: topic=%s window=%dms coord=%q run=%q", *workerID, *topic, *windowMS, *coordAddr, *runID)
	if err := rt.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("runtime: %v", err)
	}
}
