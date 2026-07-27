import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { startProdServer } from "../node_modules/vinext/dist/server/prod-server.js";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, "..");
const outDir = path.join(repoRoot, "dist");
const clientDir = path.join(outDir, "client");
const publicPort = Number(process.env.XPDE_DASHBOARD_PORT ?? 3000);

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

const server = http.createServer((request, response) => {
  const pathname = new URL(request.url ?? "/", "http://localhost").pathname;
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
