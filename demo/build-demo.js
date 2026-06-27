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
  const type = file.endsWith(".html") ? "text/html; charset=utf-8" : file.endsWith(".js") ? "text/javascript; charset=utf-8" : "application/octet-stream";
  res.setHeader("Content-Type", type);
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
    root.innerHTML = `<div id="demo-card" style="position:absolute;inset:0;display:none;align-items:center;justify-content:center;background-color:#fbfcfe;background-image:radial-gradient(#dfe5ef 1px,transparent 1px);background-size:20px 20px"><div style="text-align:center"><div style="width:104px;height:104px;margin:0 auto 30px;display:grid;place-items:center;border-radius:16px;background:linear-gradient(135deg,#286cff,#7c3aed);color:white;font-size:52px;font-weight:900;box-shadow:0 16px 40px rgba(76,80,220,.22)">S</div><div id="demo-title" style="font-size:74px;font-weight:900;color:#07132f">StreamForge</div><div id="demo-subtitle" style="margin-top:16px;font-size:31px;color:#53627d;font-weight:650">Distributed Stream Processing, Made Visible</div><div style="width:128px;height:5px;background:linear-gradient(90deg,#1473e6,#7137d8,#078640);border-radius:5px;margin:32px auto 0"></div></div></div><div id="demo-step" style="position:absolute;right:28px;top:25px;padding:9px 16px;border-radius:7px;background:rgba(255,255,255,.96);border:1px solid #d7deea;color:#07132f;box-shadow:0 8px 24px rgba(30,55,90,.08);font-size:17px;font-weight:800"></div><div id="demo-caption" style="position:absolute;left:50%;bottom:34px;transform:translateX(-50%);width:min(78%,1260px);padding:15px 24px;border-radius:8px;border:1px solid #d7deea;border-left:5px solid #7137d8;background:rgba(255,255,255,.96);box-shadow:0 10px 30px rgba(30,55,90,.1);color:#07132f;font-size:21px;line-height:1.3;font-weight:750;text-align:center"></div>`;
    document.documentElement.appendChild(root);
    window.setDemoOverlay = ({ card = false, outro = false, step = "", caption = "" }) => {
      document.querySelector("#demo-card").style.display = card ? "flex" : "none";
      document.querySelector("#demo-title").textContent = "StreamForge";
      document.querySelector("#demo-subtitle").textContent = outro ? "Measured. Recoverable. Defensible." : "Distributed Stream Processing, Made Visible";
      document.querySelector("#demo-step").textContent = step;
      document.querySelector("#demo-step").style.display = step ? "block" : "none";
      document.querySelector("#demo-caption").textContent = caption;
      document.querySelector("#demo-caption").style.display = caption ? "block" : "none";
    };
  });
}

const scenes = [
  { name: "00-intro", seconds: 3, card: true },
  { name: "01-overview", seconds: 5, id: "overview", step: "1 / 6 · System Overview", caption: "A replayable Kafka stream enters the engine, stateful workers process it, and completed checkpoints publish lakehouse output." },
  { name: "02-architecture", seconds: 7, id: "architecture", step: "2 / 6 · Architecture", caption: "The Go coordinator owns assignment, heartbeats, epochs, and checkpoint barriers while three workers own disjoint key ranges." },
  { name: "03-live-a", seconds: 2.5, id: "live", workerFrame: 0, step: "3 / 6 · Workers Processing", caption: "user_42 hashes to bucket 12, so Worker 1 updates the only authoritative window state for that key." },
  { name: "04-live-b", seconds: 2.5, id: "live", workerFrame: 1, step: "3 / 6 · Workers Processing", caption: "The next key maps to bucket 37 and crosses the gRPC shuffle to Worker 2." },
  { name: "05-live-c", seconds: 3, id: "live", workerFrame: 2, step: "3 / 6 · Workers Processing", caption: "All workers reach the barrier; state, offsets, and staged Parquet commit together as checkpoint 42." },
  { name: "06-recovery", seconds: 7, id: "recovery", step: "4 / 6 · Fault Recovery", caption: "A heartbeat timeout advances the epoch, reassigns ownership, restores state, and resumes from checkpointed offsets." },
  { name: "07-correctness", seconds: 6, id: "correctness", step: "5 / 6 · Reconciliation", caption: "The clean run is bit-exact. Under crash, no keys are lost and no window is committed twice; the measured boundary stays explicit." },
  { name: "08-benchmark", seconds: 6, id: "benchmark", step: "6 / 6 · Measured Performance", caption: "The engine holds p99 latency to 24 ms around 40,000 events per second and exposes its saturation knee." },
  { name: "09-outro", seconds: 3, card: true, outro: true }
];

(async () => {
  await new Promise(resolve => server.listen(PORT, "127.0.0.1", resolve));
  const browser = await chromium.launch({ headless: true, args: ["--force-color-profile=srgb", "--hide-scrollbars"] });
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 }, colorScheme: "light" });
  await page.goto(`http://127.0.0.1:${PORT}`, { waitUntil: "networkidle" });
  await mountOverlay(page);

  for (const scene of scenes) {
    if (scene.id) await page.evaluate(id => window.showScene(id), scene.id);
    if (Number.isInteger(scene.workerFrame)) await page.evaluate(index => window.setWorkerFrame(index), scene.workerFrame);
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
