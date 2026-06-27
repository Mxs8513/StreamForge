#!/usr/bin/env bash
# StreamForge benchmark harness (spec Phase 7). Two experiments:
#   A) Throughput ceiling + horizontal scaling: pre-load N events, then measure
#      how fast 1, 2, 3 workers drain them (events/sec).
#   B) Latency vs load: live-inject at increasing rates with 3 workers and watch
#      sustained throughput and p99 event latency until backlog forms.
# Numbers are read straight off the workers' /metrics. Results -> bench/RESULTS.md.
set -euo pipefail
export PATH="/opt/homebrew/bin:$PATH"
export GOTOOLCHAIN=local
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"

N=${N:-300000}
KEYS=${KEYS:-500}
PARTS=6
BUCKETS=64
WIN=1000
CKPT_MS=2000
OUT="bench/RESULTS.md"
TMP="$(mktemp -d)"
MET="python3 bench/metrics.py"

echo ">> building"
pkill -f 'bin/coordinator' 2>/dev/null || true
pkill -f 'bin/worker' 2>/dev/null || true
sleep 1
for c in coordinator worker generator; do go build -o "bin/$c" "./cmd/$c"; done

ms() { python3 -c 'import time;print(int(time.time()*1000))'; }
reset() { docker run --rm --entrypoint /bin/sh --network streamforge_default minio/mc:RELEASE.2024-09-16T17-43-14Z -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && (mc rm --recursive --force local/streamforge/checkpoints/ >/dev/null 2>&1 || true) && (mc rm --recursive --force local/streamforge/output/ >/dev/null 2>&1 || true)"; }
ports() { local n=$1 p=""; for i in $(seq 0 $((n-1))); do p="$p,$((2112+i))"; done; echo "${p#,}"; }

start_cluster() { # nworkers topic
  local nw=$1 topic=$2
  bin/coordinator --addr :7070 --workers "$nw" --kafka-partitions "$PARTS" --buckets "$BUCKETS" \
    --checkpoint-interval-ms "$CKPT_MS" >"$TMP/coord.log" 2>&1 &
  CO=$!; sleep 1
  WPIDS=()
  for i in $(seq 0 $((nw-1))); do
    bin/worker --id "w$i" --topic "$topic" --coordinator 127.0.0.1:7070 \
      --shuffle-addr "127.0.0.1:$((7100+i))" --metrics-addr ":$((2112+i))" \
      --kafka-partitions "$PARTS" --buckets "$BUCKETS" --window-size-ms "$WIN" \
      --heartbeat-interval-ms 1000 --from-earliest >"$TMP/w$i.log" 2>&1 &
    WPIDS+=($!)
  done
}
stop_cluster() { for p in "${WPIDS[@]:-}"; do kill -TERM "$p" 2>/dev/null || true; done; kill -TERM "${CO:-}" 2>/dev/null || true; wait 2>/dev/null || true; sleep 1; }
wait_consumed() { local pts=$1 t0=$2; for _ in $(seq 1 400); do [ "$($MET consumed "$pts")" -ge "$N" ] && return 0; sleep 0.25; done; return 1; }

mktopic() { docker exec streamforge-redpanda-1 rpk topic delete "$1" >/dev/null 2>&1 || true; docker exec streamforge-redpanda-1 rpk topic create "$1" --partitions "$PARTS" --replicas 1 --brokers redpanda:9092 >/dev/null; }

echo "# StreamForge benchmark results" >"$OUT"
echo >>"$OUT"
echo "Single machine (Apple Silicon, Colima/Docker), Redpanda + MinIO, $PARTS Kafka partitions, $BUCKETS key-buckets, ${WIN}ms windows, ${CKPT_MS}ms checkpoints. N=$N events, $KEYS keys. Engine processing throughput = N / time to consume all N (events/sec); latency = event_time -> aggregation." >>"$OUT"
echo >>"$OUT"

# ---------- Experiment A: throughput ceiling + scaling (pre-loaded backlog) ----------
echo ">> Experiment A: pre-load $N events, measure drain rate for 1/2/3 workers"
echo "## A. Throughput & horizontal scaling (drain a pre-loaded backlog)" >>"$OUT"
echo >>"$OUT"
echo "Raw processing throughput: a full backlog is pre-loaded, then the engine drains it as fast as it can. (Latency is omitted here — a pre-loaded backlog makes every event 'old' by construction; see experiment B for live latency.)" >>"$OUT"
echo >>"$OUT"
echo "| workers | drain time (s) | throughput (events/s) | speedup vs 1w |" >>"$OUT"
echo "|--:|--:|--:|--:|" >>"$OUT"
BASE_TPUT=0
for NW in 1 2 3; do
  TOPIC="benchA-$NW-$(date +%s)"; mktopic "$TOPIC"
  bin/generator --topic "$TOPIC" --eps 1000000 --keys "$KEYS" --total "$N" --seed 7 >"$TMP/gen.log" 2>&1
  reset
  PTS=$(ports "$NW")
  start_cluster "$NW" "$TOPIC"
  T0=$(ms)
  wait_consumed "$PTS" "$T0" || echo "   (timeout draining with $NW workers)"
  T1=$(ms)
  DUR=$(python3 -c "print(f'{($T1-$T0)/1000:.2f}')")
  TPUT=$(python3 -c "print(int($N/(($T1-$T0)/1000)))")
  [ "$NW" = "1" ] && BASE_TPUT=$TPUT
  SPD=$(python3 -c "print(f'{$TPUT/$BASE_TPUT:.2f}x')")
  echo "   ${NW}w: ${DUR}s, ${TPUT}/s (${SPD})"
  echo "| $NW | $DUR | $TPUT | $SPD |" >>"$OUT"
  stop_cluster
done
echo >>"$OUT"

# ---------- Experiment B: latency vs injected load (3 workers, live stream) ----------
echo ">> Experiment B: live-inject at increasing rates (3 workers), watch p99"
echo "## B. Latency vs load (3 workers, live injection)" >>"$OUT"
echo >>"$OUT"
echo "| injected (events/s) | sustained (events/s) | kept up? | p50 latency (ms) | p99 latency (ms) |" >>"$OUT"
echo "|--:|--:|:--:|--:|--:|" >>"$OUT"
PTS=$(ports 3)
for R in 5000 10000 20000 40000 80000; do
  TOPIC="benchB-$R-$(date +%s)"; mktopic "$TOPIC"
  reset
  start_cluster 3 "$TOPIC"
  T0=$(ms)
  bin/generator --topic "$TOPIC" --eps "$R" --keys "$KEYS" --total "$N" --seed 7 >"$TMP/gen.log" 2>&1 &
  GEN=$!
  wait_consumed "$PTS" "$T0" || true
  T1=$(ms); wait "$GEN" 2>/dev/null || true
  SUS=$(python3 -c "print(int($N/(($T1-$T0)/1000)))")
  KEPT=$(python3 -c "print('yes' if $SUS >= $R*0.9 else 'no (backlog)')")
  P50=$(python3 -c "print(int(float('$($MET latency 0.50 "$PTS")')*1000))")
  P99=$(python3 -c "print(int(float('$($MET latency 0.99 "$PTS")')*1000))")
  echo "   inject ${R}/s -> sustained ${SUS}/s, p99=${P99}ms ($KEPT)"
  echo "| $R | $SUS | $KEPT | $P50 | $P99 |" >>"$OUT"
  stop_cluster
done

echo >>"$OUT"
echo "_Generated by bench/run_benchmark.sh on $(date)._" >>"$OUT"
echo
echo ">> wrote $OUT"
cat "$OUT"
