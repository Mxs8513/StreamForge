#!/usr/bin/env node
const fs = require("fs");
const http = require("http");
const path = require("path");
const { spawnSync } = require("child_process");
const { chromium } = require("playwright");
const ffmpeg = require("@ffmpeg-installer/ffmpeg").path;

const ROOT = __dirname;
const OUT = path.join(ROOT, "out");
const RECORDINGS = path.join(OUT, "recordings");
const PORT = 4175;
fs.rmSync(OUT, { recursive: true, force: true });
fs.mkdirSync(RECORDINGS, { recursive: true });

const server = http.createServer((req, res) => {
  const file = path.join(ROOT, req.url === "/" ? "index.html" : req.url.replace(/^\//, ""));
  if (!file.startsWith(ROOT) || !fs.existsSync(file)) {
    res.writeHead(404);
    return res.end("Not found");
  }
  const type = file.endsWith(".html")
    ? "text/html; charset=utf-8"
    : file.endsWith(".js")
      ? "text/javascript; charset=utf-8"
      : "application/octet-stream";
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
    root.innerHTML = `<div id="demo-card" style="position:absolute;inset:0;display:none;align-items:center;justify-content:center;background-color:#fbfcfe;background-image:radial-gradient(#dfe5ef 1px,transparent 1px);background-size:20px 20px"><div style="text-align:center"><div style="width:104px;height:104px;margin:0 auto 30px;display:grid;place-items:center;border-radius:16px;background:linear-gradient(135deg,#286cff,#7c3aed);color:white;font-size:52px;font-weight:900;box-shadow:0 16px 40px rgba(76,80,220,.22)">S</div><div id="demo-title" style="font-size:74px;font-weight:900;color:#07132f">StreamForge</div><div id="demo-subtitle" style="margin-top:16px;font-size:31px;color:#53627d;font-weight:650">Distributed Systems, Running Live</div><div style="width:128px;height:5px;background:linear-gradient(90deg,#1473e6,#7137d8,#078640);border-radius:5px;margin:32px auto 0"></div></div></div><div id="demo-step" style="position:absolute;right:28px;top:25px;padding:9px 16px;border-radius:7px;background:rgba(255,255,255,.96);border:1px solid #d7deea;color:#07132f;box-shadow:0 8px 24px rgba(30,55,90,.08);font-size:17px;font-weight:800"></div><div id="demo-caption" style="position:absolute;left:50%;bottom:34px;transform:translateX(-50%);width:min(78%,1260px);padding:15px 24px;border-radius:8px;border:1px solid #d7deea;border-left:5px solid #7137d8;background:rgba(255,255,255,.96);box-shadow:0 10px 30px rgba(30,55,90,.1);color:#07132f;font-size:21px;line-height:1.3;font-weight:750;text-align:center"></div>`;
    document.documentElement.appendChild(root);
    window.setDemoOverlay = ({ card = false, outro = false, step = "", caption = "" }) => {
      document.querySelector("#demo-card").style.display = card ? "flex" : "none";
      document.querySelector("#demo-title").textContent = "StreamForge";
      document.querySelector("#demo-subtitle").textContent = outro
        ? "Measured. Recoverable. Defensible."
        : "Distributed Systems, Running Live";
      document.querySelector("#demo-step").textContent = step;
      document.querySelector("#demo-step").style.display = step ? "block" : "none";
      document.querySelector("#demo-caption").textContent = caption;
      document.querySelector("#demo-caption").style.display = caption ? "block" : "none";
    };
  });
}

const scenes = [
  { name: "intro", seconds: 2.5, card: true },
  { name: "overview", seconds: 4, id: "overview", step: "1 / 7 · System Overview", caption: "Kafka input, distributed stateful processing, and checkpoint-committed lakehouse output." },
  { name: "architecture", seconds: 5, id: "architecture", step: "2 / 7 · Architecture", caption: "The Go coordinator owns assignment and barriers while workers own disjoint partitions and key ranges." },
  { name: "flow", terminal: "flow", id: "terminal-flow", step: "3 / 7 · Live Event Flow", caption: "This terminal moves: real Kafka records arrive while the three-worker assignment is visible beside the trace." },
  { name: "checkpoint", terminal: "checkpoint", id: "terminal-checkpoint", step: "4 / 7 · Checkpoint + Commit", caption: "Worker snapshots, offsets, and staged Parquet paths appear before checkpoint metadata becomes the atomic commit point." },
  { name: "recovery", terminal: "recovery", id: "terminal-recovery", step: "5 / 7 · Crash Recovery", caption: "A real SIGKILL removes w0; after 2.168 seconds, two survivors restore checkpoint 3 and own all six partitions." },
  { name: "verify", terminal: "verify", id: "terminal-verify", step: "6 / 7 · Reconciliation", caption: "The committed-output verifier reports zero missing keys and zero duplicate windows, with the 17-event replay boundary stated explicitly." },
  { name: "benchmark", seconds: 5, id: "benchmark", step: "7 / 7 · Measured Performance", caption: "The engine holds p99 latency to 24 ms around 40,000 events per second and exposes its saturation knee." },
  { name: "outro", seconds: 2.5, card: true, outro: true }
];

(async () => {
  await new Promise(resolve => server.listen(PORT, "127.0.0.1", resolve));
  const browser = await chromium.launch({ headless: true, args: ["--force-color-profile=srgb", "--hide-scrollbars"] });
  const context = await browser.newContext({
    viewport: { width: 1920, height: 1080 },
    colorScheme: "light",
    recordVideo: { dir: RECORDINGS, size: { width: 1920, height: 1080 } }
  });
  const page = await context.newPage();
  await page.goto(`http://127.0.0.1:${PORT}`, { waitUntil: "networkidle" });
  await mountOverlay(page);
  const recordedVideo = page.video();

  for (const scene of scenes) {
    if (scene.id) await page.evaluate(id => window.showScene(id), scene.id);
    await page.evaluate(config => window.setDemoOverlay(config), scene);
    await page.waitForTimeout(360);
    if (scene.name === "architecture") {
      await page.screenshot({ path: path.join(OUT, "streamforge-demo-poster.png") });
    }
    if (scene.terminal) {
      await page.evaluate(mode => window.playTerminal(mode), scene.terminal);
      await page.waitForTimeout(scene.terminal === "verify" ? 1700 : 1400);
    } else {
      await page.waitForTimeout(scene.seconds * 1000);
    }
  }

  await context.close();
  const rawVideo = await recordedVideo.path();
  await browser.close();
  server.close();

  const web = path.join(OUT, "streamforge-demo-web.mp4");
  run(["-y", "-i", rawVideo, "-vf", "scale=1920:1080:flags=lanczos,format=yuv420p", "-r", "25", "-c:v", "libx264", "-preset", "slow", "-crf", "22", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-an", web]);
  run(["-y", "-i", web, "-vf", "scale=3840:2160:flags=lanczos,format=yuv420p", "-c:v", "libx264", "-preset", "slow", "-crf", "18", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-an", path.join(OUT, "streamforge-demo-4k.mp4")]);
  console.log(`Animated terminal demo complete: ${web}`);
})().catch(error => {
  server.close();
  console.error(error);
  process.exit(1);
});
