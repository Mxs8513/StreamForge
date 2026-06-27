# StreamForge portfolio demo

This renderer produces the silent inline walkthrough used on madhavsuri.com.
It records a real browser timeline rather than stitching terminal screenshots:
commands type, logs stream, worker ownership changes, checkpoint objects appear,
and reconciliation results resolve while the video is recording.

The terminal content is grounded in the repository's live Phase 6 run from
2026-06-27: Kafka records consumed with `rpk`, worker/coordinator logs, checkpoint
3 snapshot and staged-output evidence, SIGKILL recovery, and committed-output
verification. The measured boundary remains explicit: the no-failure run is
bit-exact; the recorded crash run has no lost keys or duplicate committed
windows, with 17 replayed boundary events.

```bash
npm install
npm run record
```

Outputs are written to `out/` as a 4K master, a web-optimized 1080p MP4, and an
architecture poster. Playwright records the animated browser session, then
FFmpeg creates the portfolio-ready H.264 files.
