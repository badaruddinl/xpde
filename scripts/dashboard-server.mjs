import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { startProdServer } from "../node_modules/vinext/dist/server/prod-server.js";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, "..");
const outDir = path.join(repoRoot, "dist");
const clientDir = path.join(outDir, "client");
const publicPort = Number(process.env.XPDE_DASHBOARD_PORT ?? 3000);
const retryScript = path.join(scriptDir, "retry-mt5-bridge.ps1");
const allowedOrigins = new Set([
  `http://127.0.0.1:${publicPort}`,
  `http://localhost:${publicPort}`,
]);
let retryInFlight = null;

if (!fs.existsSync(path.join(outDir, "server", "index.js"))) {
  throw new Error("Dashboard build is missing. Run `npm run build` first.");
}

const upstream = await startProdServer({
  port: 0,
  host: "127.0.0.1",
  outDir,
  purpose: "XPDE internal renderer",
});

const contentTypes = new Map([
  [".css", "text/css; charset=utf-8"],
  [".js", "application/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".svg", "image/svg+xml"],
  [".png", "image/png"],
  [".jpg", "image/jpeg"],
  [".jpeg", "image/jpeg"],
  [".webp", "image/webp"],
  [".woff", "font/woff"],
  [".woff2", "font/woff2"],
]);

function localAsset(pathname) {
  if (!(pathname.startsWith("/assets/") || /^\/[^/]+\.[a-z0-9]+$/i.test(pathname))) {
    return null;
  }
  let decoded;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    return null;
  }
  const candidate = path.resolve(clientDir, `.${decoded}`);
  const prefix = `${path.resolve(clientDir)}${path.sep}`;
  if (!candidate.startsWith(prefix) || !fs.existsSync(candidate) || !fs.statSync(candidate).isFile()) {
    return null;
  }
  return candidate;
}

function sendJson(response, status, payload) {
  const body = JSON.stringify(payload);
  response.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
    "Cache-Control": "no-store",
  });
  response.end(body);
}

async function runBridgeRetry() {
  return new Promise((resolve, reject) => {
    const child = spawn(
      "powershell.exe",
      [
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        retryScript,
      ],
      {
        cwd: repoRoot,
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let stdout = "";
    let stderr = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill();
      reject(new Error(`bridge supervisor timed out: ${stderr.trim() || "no details"}`));
    }, 30_000);

    const finish = (callback) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      callback();
      child.kill();
    };
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
      try {
        const result = JSON.parse(stdout.trim());
        finish(() => resolve(result));
      } catch {
        // PowerShell may split the single JSON line across chunks.
      }
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", (error) => {
      finish(() => reject(error));
    });
    child.on("close", (code) => {
      if (settled) return;
      try {
        const result = JSON.parse(stdout.trim());
        finish(() => resolve(result));
      } catch {
        finish(() =>
          reject(
            new Error(
              `bridge supervisor exited ${code}: ${stderr.trim() || stdout.trim() || "no details"}`,
            ),
          ),
        );
      }
    });
  });
}

async function handleBridgeRetry(request, response) {
  if (request.method !== "POST") {
    sendJson(response, 405, { error: "POST required" });
    return;
  }
  const origin = request.headers.origin;
  if (
    request.headers["x-xpde-action"] !== "retry-mt5-bridge" ||
    (origin && !allowedOrigins.has(origin))
  ) {
    sendJson(response, 403, { error: "local retry request rejected" });
    return;
  }

  try {
    retryInFlight ??= runBridgeRetry().finally(() => {
      retryInFlight = null;
    });
    const result = await retryInFlight;
    sendJson(response, 200, result);
  } catch (error) {
    const stdout = typeof error?.stdout === "string" ? error.stdout.trim() : "";
    let detail = error instanceof Error ? error.message : String(error);
    if (stdout) {
      try {
        detail = JSON.parse(stdout).error ?? detail;
      } catch {
        detail = stdout;
      }
    }
    sendJson(response, 500, { status: "failed", error: detail });
  }
}

const server = http.createServer((request, response) => {
  const pathname = new URL(request.url ?? "/", "http://localhost").pathname;
  if (pathname === "/api/local/retry-mt5-bridge") {
    void handleBridgeRetry(request, response);
    return;
  }
  const asset = localAsset(pathname);
  if (asset) {
    const stat = fs.statSync(asset);
    response.writeHead(200, {
      "Content-Type": contentTypes.get(path.extname(asset).toLowerCase()) ?? "application/octet-stream",
      "Content-Length": stat.size,
      "Cache-Control": pathname.startsWith("/assets/")
        ? "public, max-age=31536000, immutable"
        : "public, max-age=3600",
    });
    if (request.method === "HEAD") {
      response.end();
    } else {
      fs.createReadStream(asset).pipe(response);
    }
    return;
  }

  const proxy = http.request(
    {
      hostname: "127.0.0.1",
      port: upstream.port,
      method: request.method,
      path: request.url,
      headers: request.headers,
    },
    (upstreamResponse) => {
      response.writeHead(upstreamResponse.statusCode ?? 502, upstreamResponse.headers);
      upstreamResponse.pipe(response);
    },
  );
  proxy.on("error", (error) => {
    response.writeHead(502, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ error: `dashboard renderer unavailable: ${error.message}` }));
  });
  request.pipe(proxy);
});

server.listen(publicPort, "127.0.0.1", () => {
  console.log(`XPDE dashboard ready at http://127.0.0.1:${publicPort}/`);
});

function shutdown() {
  server.close(() => upstream.server.close(() => process.exit(0)));
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
