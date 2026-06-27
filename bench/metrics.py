#!/usr/bin/env python3
"""Read StreamForge metrics straight off the workers' /metrics endpoints.

Usage:
  metrics.py consumed PORT[,PORT...]            -> summed events_consumed_total
  metrics.py latency Q PORT[,PORT...]           -> histogram_quantile(Q) of event latency (seconds)
"""
import sys, urllib.request


def scrape(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
        return r.read().decode()


def counter(ports, name):
    total = 0.0
    for p in ports:
        for line in scrape(p).splitlines():
            if line.startswith(name + " "):
                total += float(line.split()[1])
    return total


def buckets(ports, metric):
    # sum bucket counts by le across all ports
    agg = {}
    total = 0.0
    for p in ports:
        for line in scrape(p).splitlines():
            if line.startswith(metric + "_bucket{"):
                le = line[line.index('le="') + 4: line.index('"}')]
                cnt = float(line.split()[-1])
                agg[le] = agg.get(le, 0.0) + cnt
            elif line.startswith(metric + "_count "):
                total += float(line.split()[1])
    return agg, total


def quantile(ports, metric, q):
    agg, total = buckets(ports, metric)
    if total == 0:
        return 0.0
    bnds = sorted([(float("inf") if le == "+Inf" else float(le), c) for le, c in agg.items()])
    target = q * total
    prev_le, prev_c = 0.0, 0.0
    for le, c in bnds:
        if c >= target:
            if le == float("inf"):
                return prev_le
            # linear interpolation within the bucket
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (target - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return bnds[-1][0]


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "consumed":
        ports = sys.argv[2].split(",")
        print(int(counter(ports, "streamforge_events_consumed_total")))
    elif cmd == "latency":
        q = float(sys.argv[2])
        ports = sys.argv[3].split(",")
        print(f"{quantile(ports, 'streamforge_event_latency_seconds', q):.4f}")
