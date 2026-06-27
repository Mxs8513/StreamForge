#!/usr/bin/env bash
# Phase 5 exit test: kill a worker mid-stream; the coordinator detects the death,
# reassigns its partitions/key-buckets to survivors, who restore state from the
# last completed checkpoint and resume from the checkpointed offsets. We prove
# NO DATA LOSS by comparing per-key counts to a clean baseline (chaos >= baseline
# for every key), and we record the recovery time.
#
# Output is at-least-once here (replay after recovery can re-emit windows);
# exact exactly-once is Phase 6. The invariant proven now is: no event is lost.
set -euo pipefail

export PATH="/opt/homebrew/bin:$PATH"
export GOTOOLCHAIN=local
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

N=${N:-12000}
KEYS=${KEYS:-200}
PARTS=6
BUCKETS=64
WIN=1000
CKPT_MS=1500
TOPIC="p5-$(date +%s)"
TMP="$(mktemp -d)"
MC="docker run --rm --entrypoint /bin/sh --network streamforge_default minio/mc:RELEASE.2024-09-16T17-43-14Z -c"
MCALIAS="mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null"

now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }  # macOS date lacks %3N

echo ">> killing stale processes; building"
pkill -f 'bin/coordinator' 2>/dev/null || true
pkill -f 'bin/worker' 2>/dev/null || true
sleep 1
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/worker ./cmd/worker
go build -o bin/generator ./cmd/generator
go build -o bin/verify ./cmd/verify

echo ">> fresh topic $TOPIC; producing $N events"
docker exec streamforge-redpanda-1 rpk topic create "$TOPIC" --partitions "$PARTS" --replicas 1 --brokers redpanda:9092 >/dev/null
bin/generator --topic "$TOPIC" --eps 50000 --keys "$KEYS" --total "$N" --seed 99

clear_storage() {
  $MC "$MCALIAS && (mc rm --recursive --force local/streamforge/checkpoints/ >/dev/null 2>&1 || true) && (mc rm --recursive --force local/streamforge/output/$1/ >/dev/null 2>&1 || true)"
}

start_coord() { # args: failure_timeout_ms detect_ms
  bin/coordinator --addr :7070 --workers 3 --kafka-partitions "$PARTS" --buckets "$BUCKETS" \
    --checkpoint-interval-ms "$CKPT_MS" --failure-timeout-ms "$1" --detect-interval-ms "$2" \
    >"$TMP/coord.log" 2>&1 &
  echo $!
}

start_worker() { # args: index runid
  bin/worker --id "w$1" --topic "$TOPIC" --coordinator 127.0.0.1:7070 \
    --shuffle-addr "127.0.0.1:$((7100+$1))" --metrics-addr ":$((2112+$1))" \
    --kafka-partitions "$PARTS" --buckets "$BUCKETS" --window-size-ms "$WIN" \
    --heartbeat-interval-ms 500 --from-earliest --run-id "$2" >"$TMP/$2-w$1.log" 2>&1 &
  echo $!
}

# ---------- baseline: clean run, ground truth ----------
echo
echo ">> BASELINE run (no failure)"
clear_storage baseline
COORD=$(start_coord 60000 1000)   # high failure timeout: no false deaths
sleep 1
B0=$(start_worker 0 baseline); B1=$(start_worker 1 baseline); B2=$(start_worker 2 baseline)
sleep 9
kill -TERM "$B0" "$B1" "$B2" 2>/dev/null || true; wait "$B0" "$B1" "$B2" 2>/dev/null || true
kill -TERM "$COORD" 2>/dev/null || true; wait "$COORD" 2>/dev/null || true
sleep 1
bin/verify --prefix "output/baseline/" --rollup "$TMP/baseline.rollup" | sed 's/^/   /'

# ---------- chaos: kill a worker mid-stream ----------
echo
echo ">> CHAOS run (kill w0 mid-stream)"
clear_storage chaos
COORD=$(start_coord 2000 500)     # detect death within ~2s
sleep 1
C0=$(start_worker 0 chaos); C1=$(start_worker 1 chaos); C2=$(start_worker 2 chaos)

sleep 3   # consume + accumulate >=1 checkpoint
echo "   >> CRASH: SIGKILL w0 (pid $C0)"
TS_KILL=$(now_ms)
kill -9 "$C0" 2>/dev/null || true

# Wait for a survivor to restore from checkpoint, and time it.
echo "   >> waiting for survivors to recover..."
TS_RECOVER=0
for _ in $(seq 1 100); do
  if grep -q "restored owned buckets" "$TMP/chaos-w1.log" "$TMP/chaos-w2.log" 2>/dev/null; then
    TS_RECOVER=$(now_ms); break
  fi
  sleep 0.2
done

sleep 8   # survivors reprocess from checkpoint + drain windows
kill -TERM "$C1" "$C2" 2>/dev/null || true; wait "$C1" "$C2" 2>/dev/null || true
kill -TERM "$COORD" 2>/dev/null || true; wait "$COORD" 2>/dev/null || true
sleep 1
bin/verify --prefix "output/chaos/" --rollup "$TMP/chaos.rollup" | sed 's/^/   /'

echo
echo "=== coordinator failure-detection log ==="
grep -E 'DEAD|epoch' "$TMP/coord.log" | sed 's/^/   /' || true
echo "=== survivor recovery log ==="
grep -hE 'restored owned buckets|resetting to epoch' "$TMP"/chaos-w1.log "$TMP"/chaos-w2.log | sed 's/^/   /' || true

RECOVERY_MS=0
if [ "$TS_RECOVER" -gt 0 ]; then RECOVERY_MS=$((TS_RECOVER - TS_KILL)); fi

echo
echo "=== no-loss assertion (chaos count >= baseline count for every key) ==="
python3 - "$TMP/baseline.rollup" "$TMP/chaos.rollup" "$RECOVERY_MS" <<'PY'
import sys
def load(p):
    d={}
    for line in open(p):
        k,c,cents,mn,mx=line.rstrip("\n").split("\t")
        d[k]=int(c)
    return d
base=load(sys.argv[1]); chaos=load(sys.argv[2]); rec=int(sys.argv[3])
missing=[k for k in base if k not in chaos]
lost=[k for k in base if chaos.get(k,0) < base[k]]
base_total=sum(base.values()); chaos_total=sum(chaos.values())
print(f"   baseline keys={len(base)} events={base_total}")
print(f"   chaos    keys={len(chaos)} events={chaos_total}")
print(f"   missing keys: {len(missing)}   keys with fewer events than baseline: {len(lost)}")
print(f"   duplicate (replayed) events: {chaos_total-base_total}  (expected; Phase 6 makes output exactly-once)")
print(f"   recovery time: {rec} ms" if rec>0 else "   recovery time: (not captured)")
ok = (len(missing)==0 and len(lost)==0 and len(chaos)>=len(base))
print()
print("EXIT TEST PASS: worker crash recovered, NO DATA LOSS (every key count >= baseline)" if ok
      else "EXIT TEST FAIL: data loss detected")
sys.exit(0 if ok else 1)
PY
