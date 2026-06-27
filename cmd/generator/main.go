// Command generator produces synthetic events to Kafka for benchmarking and
// testing (spec §6.3). It logs the events it produced so the Phase 6
// reconciliation test can verify exactly-once.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/Mxs8513/StreamForge/internal/config"
	"github.com/Mxs8513/StreamForge/internal/event"
)

func main() {
	var (
		brokers  = flag.String("brokers", config.Env("KAFKA_BROKERS", config.DefaultKafkaBrokers), "comma-separated Kafka brokers")
		topic    = flag.String("topic", config.Env("KAFKA_TOPIC", config.DefaultTopic), "Kafka topic")
		eps      = flag.Int("eps", 1000, "events per second")
		keys     = flag.Int("keys", 100, "distinct key cardinality")
		total    = flag.Int("total", 0, "total events to produce then exit (0 = run forever)")
		lateness = flag.Int("max-lateness-ms", 0, "max out-of-order skew applied to event_time")
		seed     = flag.Int64("seed", 42, "PRNG seed for reproducible datasets")
	)
	flag.Parse()

	// Synchronous, acked writes (reliable + deterministic delivery). Throughput
	// comes from writing a batch of messages per WriteMessages call, not from
	// async fire-and-forget, so every produced event is durably in Kafka before
	// the call returns — both runs see the same N events.
	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(*brokers, ",")...),
		Topic:        *topic,
		Balancer:     &kafka.Hash{}, // hash by key => same key always same partition
		BatchSize:    10000,         // don't sub-batch our explicit batches
		BatchTimeout: time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rng := rand.New(rand.NewSource(*seed))
	eventTypes := []string{"purchase", "view", "click", "refund", "add_to_cart"}

	// Pace at ~100 ticks/sec, writing a batch per tick so we can sustain high
	// rates (one synchronous, acked WriteMessages per batch — fast and reliable,
	// unlike one network round-trip per event).
	const ticksPerSec = 100
	batch := *eps / ticksPerSec
	if batch < 1 {
		batch = 1
	}
	ticker := time.NewTicker(time.Second / ticksPerSec)
	defer ticker.Stop()

	produced := 0
	log.Printf("generator: eps=%d keys=%d total=%d topic=%s (batch=%d)", *eps, *keys, *total, *topic, batch)

	msgs := make([]kafka.Message, 0, batch)
	for {
		select {
		case <-ctx.Done():
			log.Printf("generator: stopped after %d events", produced)
			return
		case <-ticker.C:
			n := batch
			if *total > 0 && produced+n > *total {
				n = *total - produced
			}
			now := time.Now().UnixMilli()
			msgs = msgs[:0]
			for i := 0; i < n; i++ {
				skew := int64(0)
				if *lateness > 0 {
					skew = int64(rng.Intn(*lateness))
				}
				e := event.Event{
					EventID:   uuid.NewString(),
					Key:       fmt.Sprintf("user_%d", rng.Intn(*keys)),
					EventType: eventTypes[rng.Intn(len(eventTypes))],
					Amount:    float64(rng.Intn(10000)) / 100.0,
					EventTime: now - skew,
				}
				val, err := e.Encode()
				if err != nil {
					log.Fatalf("encode: %v", err)
				}
				msgs = append(msgs, kafka.Message{Key: []byte(e.Key), Value: val})
			}
			if err := w.WriteMessages(ctx, msgs...); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("write error: %v", err)
				continue
			}
			produced += n
			if *total > 0 && produced >= *total {
				log.Printf("generator: produced target %d events", produced)
				return
			}
		}
	}
}
