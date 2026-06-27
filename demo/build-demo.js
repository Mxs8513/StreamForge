#!/usr/bin/env node
const fs = require("fs");
const http = require("http");
const path = require("path");
const { spawnSync } = require("child_process");
const { chromium } = require("playwright");
const ffmpeg = require("@ffmpeg-installer/ffmpeg").path;

const ROOT = __dirname;
const OUT = path.join(ROOT, "out");
const FRAMES = path.join(OUT, "frames");
const CLIPS = path.join(OUT, "clips");
const PORT = 4175;
fs.rmSync(FRAMES, { recursive: true, force: true });
fs.rmSync(CLIPS, { recursive: true, force: true });
fs.mkdirSync(FRAMES, { recursive: true });
fs.mkdirSync(CLIPS, { recursive: true });

const server = http.createServer((req, res) => {
  const file = path.join(ROOT, req.url === "/" ? "index.html" : req.url.replace(/^\//, ""));
  if (!file.startsWith(ROOT) || !fs.existsSync(file)) {
    res.writeHead(404);
    return res.end("Not found");
  }
  res.setHeader("Content-Type", file.endsWith(".html") ? "text/html; charset=utf-8" : "application/octet-stream");
  fs.createReadStream(file).pipe(res);
});
function run(args) {
  const result = spawnSync(ffmpeg, args, { stdio: "inherit" });
  if (result.status !== 0) throw new Error(`ffmpeg failed: ${args.join(" ")}`);
}

async function mountOverlay(page) {
  await page.evaluate(() => {
    const root = document.createElement("div");
    root.id = "demo-overlay";
    root.style.cssText = "position:fixed;inset:0;z-index:99999;pointer-events:none;font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif";
    root.innerHTML = `<div id="demo-card" style="position:absolute;inset:0;display:none;align-items:center;justify-content:center;background:radial-gradient(circle at 50% 35%,#251b42,#090b10 58%)"><div style="text-align:center"><div style="width:104px;height:104px;margin:0 auto 30px;display:grid;place-items:center;border-radius:12px;background:#8b5cf6;color:white;font-size:38px;font-weight:850">SF</div><div id="demo-title" style="font-size:74px;font-weight:850;color:white">StreamForge</div><div id="demo-subtitle" style="margin-top:16px;font-size:31px;color:#b7c0d1">Fault-Tolerant Distributed Stream Processing</div><div style="width:128px;height:5px;background:#8b5cf6;border-radius:5px;margin:32px auto 0"></div></div></div><div id="demo-step" style="position:absolute;right:28px;top:25px;padding:9px 16px;border-radius:999px;background:rgba(9,11,16,.86);border:1px solid #3a4355;color:#fff;font-size:17px;font-weight:700"></div><div id="demo-caption" style="position:absolute;left:50%;bottom:34px;transform:translateX(-50%);width:min(76%,1200px);padding:15px 24px;border-radius:12px;border:1px solid #333b4b;border-left:5px solid #8b5cf6;background:rgba(9,11,16,.92);color:#fff;font-size:22px;line-height:1.3;font-weight:650;text-align:center"></div>`;
    document.documentElement.appendChild(root);
    window.setDemoOverlay = ({ card = false, outro = false, step = "", caption = "" }) => {
      document.querySelector("#demo-card").style.display = card ? "flex" : "none";
      document.querySelector("#demo-title").textContent = "StreamForge";
      document.querySelector("#demo-subtitle").textContent = outro ? "Measured. Recoverable. Defensible." : "Fault-Tolerant Distributed Stream Processing";
      document.querySelector("#demo-step").textContent = step;
      document.querySelector("#demo-step").style.display = step ? "block" : "none";
      document.querySelector("#demo-caption").textContent = caption;
      document.querySelector("#demo-caption").style.display = caption ? "block" : "none";
    };
  });
}

const scenes = [
  { name: "00-intro", seconds: 3, card: true },
  { name: "01-architecture", seconds: 6, id: "architecture", step: "1 / 5 · Architecture", caption: "Kafka input is partitioned by key across a coordinator-managed worker pool, then committed to Parquet and Iceberg." },
  { name: "02-processing", seconds: 6, id: "processing", step: "2 / 5 · Stateful Processing", caption: "Event-time watermarks and keyed BadgerDB state keep window assignment stable when events are replayed." },
  { name: "03-recovery", seconds: 7, id: "recovery", step: "3 / 5 · Worker Recovery", caption: "The chaos test kills w0 mid-stream. Heartbeats detect the failure, ownership moves, and survivors restore checkpointed state." },
  { name: "04-correctness", seconds: 7, id: "correctness", step: "4 / 5 · Reconciliation", caption: "The clean run is bit-exact. Under crash, no keys are lost and no window is committed twice; the residual boundary is reported honestly." },
  { name: "05-benchmark", seconds: 7, id: "benchmark", step: "5 / 5 · Measured Performance", caption: "The engine holds p99 latency to 24 ms at about 40,000 events per second and exposes the saturation knee at higher load." },
  { name: "06-outro", seconds: 3, card: true, outro: true }
];

(async () => {
  await new Promise(resolve => server.listen(PORT, "127.0.0.1", resolve));
  const browser = await chromium.launch({ headless: true, args: ["--force-color-profile=srgb", "--hide-scrollbars"] });
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 }, colorScheme: "dark" });
  await page.goto(`http://127.0.0.1:${PORT}`, { waitUntil: "networkidle" });
  await mountOverlay(page);

  for (const scene of scenes) {
    if (scene.id) await page.evaluate(id => window.showScene(id), scene.id);
    await page.evaluate(config => window.setDemoOverlay(config), scene);
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(FRAMES, `${scene.name}.png`) });
  }
  await browser.close();
  server.close();

  const clipPaths = [];
  for (const scene of scenes) {
    const frame = path.join(FRAMES, `${scene.name}.png`);
    const clip = path.join(CLIPS, `${scene.name}.mp4`);
    const fadeOut = Math.max(0, scene.seconds - 0.35);
    run(["-y", "-loop", "1", "-framerate", "25", "-t", String(scene.seconds), "-i", frame, "-vf", `scale=1920:1080:flags=lanczos,format=yuv420p,fade=t=in:st=0:d=0.35,fade=t=out:st=${fadeOut}:d=0.35`, "-r", "25", "-c:v", "libx264", "-preset", "slow", "-crf", "22", "-pix_fmt", "yuv420p", "-an", clip]);
    clipPaths.push(clip);
  }

  const list = path.join(CLIPS, "concat.txt");
  fs.writeFileSync(list, clipPaths.map(file => `file '${file.replace(/'/g, "'\\''")}'`).join("\n") + "\n");
  const web = path.join(OUT, "streamforge-demo-web.mp4");
  run(["-y", "-f", "concat", "-safe", "0", "-i", list, "-c", "copy", "-movflags", "+faststart", web]);
  run(["-y", "-i", web, "-vf", "scale=3840:2160:flags=lanczos,format=yuv420p", "-c:v", "libx264", "-preset", "slow", "-crf", "18", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-an", path.join(OUT, "streamforge-demo-4k.mp4")]);
  console.log(`Demo complete: ${web}`);
})().catch(error => {
  server.close();
  console.error(error);
  process.exit(1);
});
