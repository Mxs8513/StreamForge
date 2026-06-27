#!/usr/bin/env bash
# Phase 3 exit test: 3-worker output must equal 1-worker output.
#
# Produces one bounded dataset to a fresh topic, consumes it twice (1 worker,
# then 3 workers with keyBy shuffle), and diffs the per-key rollups. Identical
# rollups prove distribution does not change the answer: every key aggregated on
# exactly one worker, no loss, no duplication.
set -euo pipefail

export PATH="/opt/homebrew/bin:$PATH"
export GOTOOLCHAIN=local
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

N=${N:-10000}
KEYS=${KEYS:-200}
PARTS=6
BUCKETS=64
WIN=1000
CONSUME_SECS=${CONSUME_SECS:-10}
TOPIC="p3-$(date +%s)"
TMP="$(mktemp -d)"

echo ">> building binaries"
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/worker ./cmd/worker
go build -o bin/generator ./cmd/generator
go build -o bin/verify ./cmd/verify

echo ">> creating fresh topic $TOPIC ($PARTS partitions)"
docker exec streamforge-redpanda-1 rpk topic create "$TOPIC" --partitions "$PARTS" --replicas 1 --brokers redpanda:9092 >/dev/null

echo ">> producing $N events ($KEYS keys)"
bin/generator --topic "$TOPIC" --eps 50000 --keys "$KEYS" --total "$N" --seed 99

run_scenario() {
  local runid=$1 nworkers=$2
  echo ">> scenario '$runid' with $nworkers worker(s)"
  bin/coordinator --addr :7070 --workers "$nworkers" --kafka-partitions "$PARTS" --buckets "$BUCKETS" >"$TMP/coord-$runid.log" 2>&1 &
  local coord=$!
  sleep 1
  local pids=()
  for i in $(seq 0 $((nworkers-1))); do
    bin/worker --id "w$i" --topic "$TOPIC" --coordinator 127.0.0.1:7070 \
      --shuffle-addr "127.0.0.1:$((7100+i))" --metrics-addr ":$((2112+i))" \
      --kafka-partitions "$PARTS" --buckets "$BUCKETS" --window-size-ms "$WIN" \
      --from-earliest --run-id "$runid" >"$TMP/$runid-w$i.log" 2>&1 &
    pids+=($!)
  done
  echo "   consuming for ${CONSUME_SECS}s..."
  sleep "$CONSUME_SECS"
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  kill -TERM "$coord" 2>/dev/null || true
  wait "$coord" 2>/dev/null || true
  sleep 1
}

run_scenario baseline 1
run_scenario triple 3

echo ">> reconciling"
echo "--- baseline (1 worker) ---"
bin/verify --prefix "output/baseline/" --rollup "$TMP/baseline.rollup"
echo "--- triple (3 workers) ---"
bin/verify --prefix "output/triple/" --rollup "$TMP/triple.rollup"

echo ">> diffing per-key rollups"
if diff -u "$TMP/baseline.rollup" "$TMP/triple.rollup"; then
  echo
  echo "EXIT TEST PASS: 3-worker output == 1-worker output (per-key rollups identical)"
else
  echo
  echo "EXIT TEST FAIL: rollups differ"
  exit 1
fi
