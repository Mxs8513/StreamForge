#!/usr/bin/env bash
# Phase 4 exit test: checkpoints appear in storage on schedule, marked
# COMPLETED, containing per-partition offsets and per-worker state snapshots;
# and a restarted worker restores its state from the latest checkpoint.
set -euo pipefail

export PATH="/opt/homebrew/bin:$PATH"
export GOTOOLCHAIN=local
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

N=${N:-8000}
KEYS=${KEYS:-200}
PARTS=6
BUCKETS=64
WIN=1000
CKPT_MS=1500
RUN_SECS=${RUN_SECS:-10}
TOPIC="p4-$(date +%s)"
TMP="$(mktemp -d)"
# mc's entrypoint is `mc`; override to /bin/sh so we can run shell pipelines.
MC="docker run --rm --entrypoint /bin/sh --network streamforge_default minio/mc:RELEASE.2024-09-16T17-43-14Z -c"
MCALIAS="mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null"

echo ">> killing any stale coordinator/worker processes"
pkill -f 'bin/coordinator' 2>/dev/null || true
pkill -f 'bin/worker' 2>/dev/null || true
sleep 1

echo ">> building binaries"
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/worker ./cmd/worker
go build -o bin/generator ./cmd/generator

echo ">> fresh topic $TOPIC; clearing prior checkpoints"
docker exec streamforge-redpanda-1 rpk topic create "$TOPIC" --partitions "$PARTS" --replicas 1 --brokers redpanda:9092 >/dev/null
$MC "$MCALIAS && (mc rm --recursive --force local/streamforge/checkpoints/ >/dev/null 2>&1 || true)"

echo ">> producing $N events"
bin/generator --topic "$TOPIC" --eps 50000 --keys "$KEYS" --total "$N" --seed 99

echo ">> starting coordinator (3 workers, checkpoint every ${CKPT_MS}ms)"
bin/coordinator --addr :7070 --workers 3 --kafka-partitions "$PARTS" --buckets "$BUCKETS" \
  --checkpoint-interval-ms "$CKPT_MS" >"$TMP/coord.log" 2>&1 &
COORD=$!
sleep 1

PIDS=()
for i in 0 1 2; do
  bin/worker --id "w$i" --topic "$TOPIC" --coordinator 127.0.0.1:7070 \
    --shuffle-addr "127.0.0.1:$((7100+i))" --metrics-addr ":$((2112+i))" \
    --kafka-partitions "$PARTS" --buckets "$BUCKETS" --window-size-ms "$WIN" \
    --from-earliest --run-id p4 >"$TMP/w$i.log" 2>&1 &
  PIDS+=($!)
done

echo ">> running for ${RUN_SECS}s to accumulate checkpoints"
sleep "$RUN_SECS"

echo
echo "=== committed checkpoints in object storage ==="
$MC "$MCALIAS && mc ls --recursive local/streamforge/checkpoints/" | tee "$TMP/ls.txt"

LATEST=$($MC "$MCALIAS && mc ls local/streamforge/checkpoints/" \
  | awk '{print $NF}' | tr -d '/' | sort -n | tail -1)
echo
echo "=== latest checkpoint metadata (checkpoints/$LATEST/metadata.json) ==="
$MC "$MCALIAS && mc cat local/streamforge/checkpoints/$LATEST/metadata.json" | tee "$TMP/meta.json"

echo
echo "=== assertions ==="
FAIL=0
grep -q '"status": "COMPLETED"' "$TMP/meta.json" && echo "  [ok] status COMPLETED" || { echo "  [FAIL] not COMPLETED"; FAIL=1; }
grep -q '"kafka_offsets"' "$TMP/meta.json" && python3 - "$TMP/meta.json" <<'PY' || FAIL=1
import json,sys
m=json.load(open(sys.argv[1]))
offs=m.get("kafka_offsets") or {}
snaps=m.get("state_snapshots") or {}
total=sum(offs.values())
print(f"  [{'ok' if total>0 else 'FAIL'}] offsets present: {offs} (sum={total})")
print(f"  [{'ok' if len(snaps)==3 else 'FAIL'}] state snapshots for {len(snaps)} workers: {list(snaps)}")
sys.exit(0 if (total>0 and len(snaps)==3) else 1)
PY
SNAPCOUNT=$(grep -c '\.snap$' "$TMP/ls.txt" || true)
echo "  [info] total .snap objects across checkpoints: $SNAPCOUNT"

echo
echo ">> restart-restore demo: killing w0, restarting with --restore"
kill -TERM "${PIDS[0]}" 2>/dev/null || true
wait "${PIDS[0]}" 2>/dev/null || true
bin/worker --id w0 --topic "$TOPIC" --coordinator 127.0.0.1:7070 \
  --shuffle-addr 127.0.0.1:7100 --metrics-addr :2112 \
  --kafka-partitions "$PARTS" --buckets "$BUCKETS" --window-size-ms "$WIN" \
  --from-earliest --restore --run-id p4 >"$TMP/w0-restore.log" 2>&1 &
RESTORE_PID=$!
sleep 4
if grep -q "restored from checkpoint" "$TMP/w0-restore.log"; then
  echo "  [ok] $(grep 'restored from checkpoint' "$TMP/w0-restore.log" | head -1)"
else
  echo "  [FAIL] worker did not log a restore"; FAIL=1
fi

echo
echo ">> shutting down"
kill -TERM "$RESTORE_PID" "${PIDS[1]}" "${PIDS[2]}" "$COORD" 2>/dev/null || true
wait 2>/dev/null || true

echo
if [ "$FAIL" -eq 0 ]; then
  echo "EXIT TEST PASS: checkpoints COMPLETED with offsets+snapshots; worker restores from checkpoint"
else
  echo "EXIT TEST FAIL"; exit 1
fi
