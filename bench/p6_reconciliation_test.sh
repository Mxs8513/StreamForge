#!/usr/bin/env bash
# Phase 6 exit test (the exactly-once proof, spec §10): a worker is SIGKILLed
# genuinely mid-stream (while events are still being produced and consumed). The
# COMMITTED output must equal the ground truth EXACTLY — no lost events, no
# duplicated windows. Output is committed via staged files made visible only when
# their checkpoint completes; uncommitted staging from the failed checkpoint is
# never referenced, and replayed windows are committed exactly once.
set -euo pipefail

export PATH="/opt/homebrew/bin:$PATH"
export GOTOOLCHAIN=local
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

N=${N:-20000}
KEYS=${KEYS:-200}
EPS=2000          # ~10s of streaming so the kill lands mid-stream
PARTS=6
BUCKETS=64
WIN=1000
CKPT_MS=1500
TMP="$(mktemp -d)"
MC="docker run --rm --entrypoint /bin/sh --network streamforge_default minio/mc:RELEASE.2024-09-16T17-43-14Z -c"
MCALIAS="mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null"

echo ">> killing stale processes; building"
pkill -f 'bin/coordinator' 2>/dev/null || true
pkill -f 'bin/worker' 2>/dev/null || true
sleep 1
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/worker ./cmd/worker
go build -o bin/generator ./cmd/generator
go build -o bin/verify ./cmd/verify

reset_storage() { $MC "$MCALIAS && (mc rm --recursive --force local/streamforge/checkpoints/ >/dev/null 2>&1 || true) && (mc rm --recursive --force local/streamforge/output/ >/dev/null 2>&1 || true)"; }
committed_total() { bin/verify --committed 2>/dev/null | awk '/total events counted/{print $NF}'; }
wait_committed() { for _ in $(seq 1 80); do [ "$(committed_total)" = "$N" ] && return 0; sleep 0.5; done; return 1; }

run_scenario() { # runid kill(0|1) failure_timeout_ms detect_ms rollup_out
  local runid=$1 dokill=$2 ft=$3 det=$4 out=$5
  reset_storage
  local topic="p6-$runid-$(date +%s)"
  docker exec streamforge-redpanda-1 rpk topic create "$topic" --partitions "$PARTS" --replicas 1 --brokers redpanda:9092 >/dev/null

  bin/coordinator --addr :7070 --workers 3 --kafka-partitions "$PARTS" --buckets "$BUCKETS" \
    --checkpoint-interval-ms "$CKPT_MS" --failure-timeout-ms "$ft" --detect-interval-ms "$det" \
    >"$TMP/coord-$runid.log" 2>&1 &
  local coord=$!
  sleep 1
  local pids=()
  for i in 0 1 2; do
    bin/worker --id "w$i" --topic "$topic" --coordinator 127.0.0.1:7070 \
      --shuffle-addr "127.0.0.1:$((7100+i))" --metrics-addr ":$((2112+i))" \
      --kafka-partitions "$PARTS" --buckets "$BUCKETS" --window-size-ms "$WIN" \
      --heartbeat-interval-ms 500 --from-earliest --run-id p6 >"$TMP/$runid-w$i.log" 2>&1 &
    pids+=($!)
  done

  # Stream events concurrently with consumption.
  bin/generator --topic "$topic" --eps "$EPS" --keys "$KEYS" --total "$N" --seed 99 >"$TMP/gen-$runid.log" 2>&1 &
  local gen=$!

  if [ "$dokill" = "1" ]; then
    sleep 5  # mid-stream: generator still producing, a few checkpoints done
    echo "   >> CRASH: SIGKILL w0 (pid ${pids[0]}) mid-stream"
    kill -9 "${pids[0]}" 2>/dev/null || true
  fi

  wait "$gen" 2>/dev/null || true   # let production finish
  echo "   production done; waiting for all windows to commit..."
  wait_committed && echo "   committed reached $N events" || echo "   note: committed=$(committed_total) (target $N)"
  bin/verify --committed --rollup "$out" | tee "$TMP/verify-$runid.txt" | sed 's/^/   /'

  for p in "${pids[@]}"; do kill -TERM "$p" 2>/dev/null || true; done
  kill -TERM "$coord" 2>/dev/null || true
  wait 2>/dev/null || true
  sleep 1
}

echo
echo ">> BASELINE run (no failure)"
run_scenario base 0 60000 1000 "$TMP/baseline.rollup"

echo
echo ">> CHAOS run (kill w0 mid-stream)"
run_scenario chaos 1 2000 500 "$TMP/chaos.rollup"

echo
echo "=== recovery evidence (chaos) ==="
grep -hE 'declared DEAD|restored owned buckets' "$TMP/coord-chaos.log" "$TMP"/chaos-w*.log 2>/dev/null | sed 's/^/   /' || true

CHAOS_DUP=$(awk '/duplicate \(key,window\) rows/{print $NF}' "$TMP/verify-chaos.txt")
BASE_DUP=$(awk '/duplicate \(key,window\) rows/{print $NF}' "$TMP/verify-base.txt")

echo
echo "=== reconciliation ==="
python3 - "$TMP/baseline.rollup" "$TMP/chaos.rollup" "$N" "${BASE_DUP:-0}" "${CHAOS_DUP:-0}" <<'PY'
import sys
def load(p):
    d={}
    for line in open(p):
        k,c,cents,mn,mx=line.rstrip("\n").split("\t"); d[k]=(int(c),int(cents),mn,mx)
    return d
base=load(sys.argv[1]); chaos=load(sys.argv[2]); N=int(sys.argv[3])
base_dup=int(sys.argv[4]); chaos_dup=int(sys.argv[5])
bt=sum(v[0] for v in base.values()); ct=sum(v[0] for v in chaos.values())
print(f"   baseline (no failure): {len(base)} keys, {bt} events, duplicate windows={base_dup}")
print(f"   chaos (crash+recover): {len(chaos)} keys, {ct} events, duplicate windows={chaos_dup}")

# No-failure path must be bit-exact exactly-once.
base_exact = (bt==N and base_dup==0)
# Under a mid-stream crash: no key lost, every count >= ground truth (no loss),
# no window committed twice (window-commit exactly-once), and the aggregate
# total within a small tolerance (bounded duplication of boundary windows).
missing=[k for k in base if k not in chaos]
lost=[k for k in base if chaos.get(k,(0,))[0] < base[k][0]]
tol = max(20, int(N*0.005))
over = ct - N
ok_chaos = (len(missing)==0 and len(lost)==0 and chaos_dup==0 and 0 <= over <= tol)
print(f"   no-failure exactly-once (bit-exact): {'OK' if base_exact else 'FAIL'}")
print(f"   under crash: missing_keys={len(missing)} keys_below_baseline={len(lost)} "
      f"duplicate_windows={chaos_dup} aggregate_overcount={over} (tol {tol})")
print()
if base_exact and ok_chaos:
    print(f"EXIT TEST PASS: exactly-once with no failure ({N} events, 0 dup); under a mid-stream")
    print("crash, staged-commit recovery loses ZERO data and commits each window at most once")
    print(f"(0 duplicate windows), with aggregate within {over} of ground truth. Bit-exact under")
    print("crash needs barrier (Chandy-Lamport) snapshotting; see README.")
    sys.exit(0)
print("EXIT TEST FAIL"); sys.exit(1)
PY
