# StreamForge — Distributed Stream Processing Engine

A from-scratch distributed stream processing engine in Go. It consumes a
high-volume event stream from Kafka, partitions work across worker nodes by key,
runs **stateful windowed aggregations**, **checkpoints** state durably for
**exactly-once** recovery, and writes results as Parquet to object storage. In
miniature, the same shape as Flink / Spark Structured Streaming.

> **Credibility rule:** infrastructure tools (Kafka, Parquet, S3, BadgerDB,
> Iceberg) are *used as libraries*. The engine internals — partitioning, worker
> coordination, checkpointing, fault-tolerant keyed state, exactly-once — are
> *built here*. "I built it" applies only to the engine logic.

## Scope (honest)

**Built:** distribution, keyed state, windowing, fault tolerance, exactly-once,
observability — measured, not asserted.
**Not built / not claimed:** production cluster manager, millions of events/sec,
a SQL planner, a custom Kafka/storage format, consensus (Raft/Paxos) from
scratch, multi-region. See the spec for the full goals/non-goals.

## Architecture

```
generator ──> Kafka (Redpanda) ──> worker(s) ──> Parquet ──> MinIO / S3
                                      │ keyed state (BadgerDB)
coordinator ── gRPC ──> workers       │ tumbling-window aggregation
(assignment, health, checkpoints)     └─> Prometheus ──> Grafana
```

## Build status

| Phase | What | Status |
|------|------|--------|
| P0 | Scaffolding + local infra (Redpanda, MinIO, Prometheus, Grafana) | ✅ done |
| P1 | Single worker: Kafka → deserialize → Parquet → MinIO | ✅ done |
| P2 | Stateful processing-time tumbling windows (BadgerDB), count/sum/min/max/distinct | ✅ done |
| P3 | Coordinator + N workers + keyBy shuffle over gRPC/Protobuf | ✅ done |
| P4 | Checkpointing (state + offsets, two-phase aligned barrier, atomic commit) | ✅ done |
| P5 | Failure detect (heartbeat) → reassign → restore from checkpoint → resume | ✅ done |
| P6 | Event-time windows + staged exactly-once output commit + reconciliation | ✅ done |
| P7 | Prometheus/Grafana metrics + benchmark harness + [RESULTS.md](bench/RESULTS.md) | ✅ done |
| P8 | Event-time watermarks (in P6) + Iceberg lakehouse sink + time-travel | ✅ done |

The proto (`proto/streamforge.proto`) declares the full control plane: P3 wires
Register / GetAssignment / Heartbeat / Shuffle; P4 wires PrepareCheckpoint /
CommitCheckpoint.

## Quick start

Prerequisites: Go 1.22+, Docker (Colima works on macOS: `colima start`).

```bash
make up                              # Redpanda + MinIO + Prometheus + Grafana, creates topic + bucket
make test                            # unit + Phase 2 aggregation exit test

# single standalone worker (Phases 1-2), in two terminals:
make worker                          # consume -> aggregate -> Parquet to MinIO
make gen TOTAL=20000 EPS=2000 KEYS=100   # produce a bounded dataset

# distributed run (Phase 3): coordinator + 3 workers + keyBy shuffle
make p3-test                         # runs 1-worker vs 3-worker and diffs per-key output

make down                            # tear everything down
```

### Phase 3 exit test

`bench/p3_exit_test.sh` produces one bounded dataset, consumes it with 1 worker
then with 3 workers (each owning a slice of partitions/key-buckets, shuffling
non-owned keys to their owner over gRPC), and diffs the per-key rollups:

```
baseline (1 worker): 10000 events, 200 keys, 0 duplicate (key,window) rows
triple   (3 workers): 10000 events, 200 keys, 0 duplicate (key,window) rows
EXIT TEST PASS: 3-worker output == 1-worker output (per-key rollups identical)
```

Rollups are compared window-independently (per-key count/sum/min/max) because
windowing is processing-time and therefore timing-dependent; the per-key answer
must not change with worker count, and it doesn't.

### Phase 4 — checkpointing (two-phase aligned barrier)

The coordinator drives periodic checkpoints (spec §7). Because keys shuffle
across workers, a consistent global snapshot needs a *global* quiescent point,
so the barrier is two-phase:

1. **Prepare** (all workers): pause sources — offsets stop advancing, no new
   shuffles are generated; in-flight steps drain. The coordinator waits for
   *every* worker to prepare.
2. **Commit** (all workers): drain the inbox into state, then snapshot BadgerDB
   state **and** the partition offsets together at that frozen point, and ack.

The checkpoint becomes `COMPLETED` via a **single atomic metadata PUT**, only
after every worker acks. State and offsets are one unit; an aborted round writes
no metadata and the system keeps running on the previous checkpoint.

`bench/p4_checkpoint_test.sh` runs it live and asserts:

```
checkpoints/7/metadata.json  status COMPLETED
  kafka_offsets sum = 8000      # == events produced: every event accounted for once
  state_snapshots: w0, w1, w2   # one per worker
worker w0: restored from checkpoint 7   # restart restores state+offsets
```

The offsets summing exactly to the produced count is the visible proof that
state and offsets were captured as one atomic unit.

### Phase 5 — failure detection & recovery

Workers heartbeat the coordinator. When one is unseen for `failure-timeout`, the
coordinator declares it dead, **bumps an epoch**, and recomputes the assignment
over the survivors. Because key-buckets are hashed independently of Kafka
partitions, a dead worker's bucket state can't be rebuilt from its partitions
alone — so recovery follows Flink's model: **the whole job restarts from the last
completed checkpoint** with the new assignment. On the epoch bump every survivor:

- rebuilds the keyed state for its now-owned buckets by importing, from *every*
  worker's checkpoint snapshot, only the keys whose bucket it now owns
  (`State.RestoreFiltered`);
- resumes each assigned partition from the **checkpoint offsets**, reprocessing
  the tail the dead worker hadn't checkpointed — so no event is lost.

`bench/p5_recovery_test.sh` runs a clean baseline, then a chaos run that SIGKILLs
a worker mid-stream, and asserts per-key **chaos count ≥ baseline count** (no
loss) while recording recovery time:

```
coordinator: w0 declared DEAD (unseen 2.5s) -> epoch 2 (2 live)
worker w1: resetting to epoch 2 (partitions=[1 3 5], 32 buckets)
worker w1: restored owned buckets from checkpoint 2 (3 snapshots scanned), resume offsets=...
baseline keys=200 events=12000 ; chaos keys=200 ; missing=0 ; keys-with-fewer-events=0
recovery time: ~2.8 s
EXIT TEST PASS: worker crash recovered, NO DATA LOSS
```

Output is at-least-once across recovery (replay can re-emit windows); exact
exactly-once output is Phase 6.

### Phase 6 — event-time windows + exactly-once staged output

Two changes make the output committed-once (spec §8, §9):

- **Event-time windowing with a watermark.** A window is derived from the
  event's own `event_time` and closes when the watermark (max event-time seen,
  minus an allowed-lateness buffer) passes its end — deterministic and
  independent of wall-clock, so replay assigns an event to the *same* window.
- **Staged two-phase commit.** Flushed windows are buffered, written to
  `staging/<checkpoint_id>/` at the checkpoint's commit, and become *committed*
  only when that checkpoint's metadata is written (the atomic PUT). Output read
  back as committed = the staged files referenced by COMPLETED checkpoints,
  deduped by `(key, window_start)`. Uncommitted staging from an aborted
  checkpoint is never referenced, and replayed windows commit once.

`bench/p6_reconciliation_test.sh` produces a known dataset (deterministic per-key
ground truth), runs a clean baseline and a chaos run (worker SIGKILLed
mid-stream), and reads back the **committed** output via `verify --committed`:

```
baseline (no failure): 200 keys, 20000 events, duplicate windows=0   <- bit-exact exactly-once
chaos   (crash+recover): 200 keys, missing_keys=0, keys_below_baseline=0,
                         duplicate windows=0, aggregate within ~0.05% of ground truth
EXIT TEST PASS
```

**Honest scope of the guarantee.** With no failure the committed output is
**bit-exact exactly-once**. Under a mid-stream crash, recovery commits **zero
duplicate windows and loses zero data**, with per-key aggregates within ~0.05% of
ground truth — i.e. *effectively-once via idempotent (key,window) commits* (spec
§9). The small residual is events double-aggregated into windows that straddle
the recovered checkpoint; making it bit-exact under crashes requires
barrier-marker (Chandy-Lamport) snapshotting instead of the aligned snapshot used
here, which the spec names (§7) as the production evolution and is the documented
next step.

### Phase 7 — observability & benchmark

Every process exposes Prometheus metrics (throughput, end-to-end event latency
histogram, checkpoint duration, last-completed checkpoint, shuffle bytes);
`make up` provisions a Grafana dashboard ([deploy/grafana](deploy/grafana)) over
them. `make bench` runs the harness ([bench/run_benchmark.sh](bench/run_benchmark.sh))
— a worker-count scaling sweep and a latency-vs-load sweep, reading numbers
straight off `/metrics` — and writes [bench/RESULTS.md](bench/RESULTS.md). On a
2-vCPU laptop VM it sustains **~40k events/sec at p99 ≤ 25 ms**, saturating near
**~70k/sec**; throughput scales 2.3× from 1→2 workers (then CPU-bound at 2 cores).

### Phase 8 — Iceberg lakehouse sink + time-travel

`make iceberg` ([cmd/iceberg-sink](cmd/iceberg-sink), [internal/iceberg](internal/iceberg))
reads the committed checkpoint output and appends it into an **Apache Iceberg**
table via `iceberg-go` — **one Iceberg snapshot per checkpoint** — then runs a
**time-travel** query:

```
checkpoint 3 -> snapshot (200 rows) ... checkpoint 6 -> snapshot (200 rows)
snapshot history: total_records 200 -> 333 -> 400 -> 600
time-travel: rows AS OF first snapshot = 200 ; AS OF latest = 600
```

This is the real stream→lakehouse pattern: Iceberg's optimistic-concurrency
commit gives atomic, snapshot-isolated table updates and time-travel for free.
**Iceberg is used as the transactional sink — not reimplemented.** (Event-time
windowing + watermarks, the other half of P8, already landed in P6.) The demo
uses a SQLite catalog + local-filesystem warehouse; the same code points at a
REST/Glue catalog with an S3 warehouse by changing the catalog properties.

Outputs land in MinIO at `s3://streamforge/output/<worker-id>/part-*.parquet`
(browse at http://localhost:9001, `minioadmin`/`minioadmin`). Worker metrics are
at http://localhost:2112/metrics.

## Layout

```
cmd/coordinator coordinator: membership, assignment, checkpoints, failure detection
cmd/generator   synthetic event producer (--eps --keys --total --max-lateness-ms)
cmd/worker      worker: source -> keyBy/shuffle -> aggregate -> sink, recovery generations
cmd/verify      reconciliation aid: totals + per-key rollup (--committed for exactly-once view)
cmd/chaos       failure injector: SIGKILL a worker pid after a delay
cmd/iceberg-sink (P8) append committed checkpoints into an Iceberg table + time-travel
internal/coordinator membership, assignment, gRPC server, checkpoint orchestration
internal/worker source (groupless, offset-owned), router+shuffle, runtime, aggregator, BadgerDB state, barrier, sink
internal/checkpoint metadata + store (atomic COMPLETED commit, latest-completed)
internal/iceberg (P8) Iceberg table sink via iceberg-go (snapshot per checkpoint, time-travel)
internal/storage S3/MinIO client + Parquet encode/decode
internal/metrics Prometheus collectors (throughput, latency, checkpoint, shuffle)
internal/proto  generated gRPC/Protobuf
proto           control plane definition (assignment, shuffle, Prepare/CommitCheckpoint)
test            aggregation, assignment, snapshot/restore, bucket-filtered-restore tests
bench           p3_exit_test.sh, p4_checkpoint_test.sh, p5_recovery_test.sh, p6_reconciliation_test.sh
```

### Offset ownership

Workers consume with **groupless per-partition readers** — no `GroupID`, no
`CommitMessages`, no auto-commit. Offsets live only in the in-process
`OffsetTracker`. Kafka never tracks our progress, which is exactly what lets P4
fold offsets into checkpoints so state + offsets advance as a single atomic unit
(output commit joins them in P6).

## Design notes for later phases

- **Window assignment** takes the timestamp as a parameter (`Aggregator.Update(e, ts)`),
  so P2 passes ingest time (processing-time) and P8 can pass event-time + watermarks
  without changing the aggregation logic.
- **Offsets** are committed via `Source.Commit` after state update; P4 moves
  offset ownership into checkpoints so state+offset+output advance as one unit.
- **Sink** writes to `output/` now; P6 redirects to `staging/<checkpoint_id>/`
  with a commit step for exactly-once.
