# StreamForge portfolio demo

This deterministic renderer produces the silent inline walkthrough used on
madhavsuri.com. The scenes visualize the engine's documented architecture,
latest crash reconciliation, and measured benchmark results.

```bash
npm install
npm run record
```

Outputs are written to `out/` as a 4K master and a web-optimized 1080p MP4.
Each scene is rendered to a fixed-duration clip so browser recording speed
cannot change the walkthrough pacing.
