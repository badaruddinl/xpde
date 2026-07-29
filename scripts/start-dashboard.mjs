import path from "node:path";
import process from "node:process";

import { startProdServer } from "vinext/server/prod-server";
import { StaticFileCache } from "../node_modules/vinext/dist/server/static-file-cache.js";

function optionValue(longName, shortName) {
  const longIndex = process.argv.indexOf(longName);
  if (longIndex >= 0) return process.argv[longIndex + 1];
  const shortIndex = process.argv.indexOf(shortName);
  if (shortIndex >= 0) return process.argv[shortIndex + 1];
  return undefined;
}

// vinext 0.0.50 stores Windows walk results with "\" separators while HTTP
// lookups always use "/". Normalize the cache at its boundary so production
// assets are served on Windows exactly as they are on Linux/Sites.
if (process.platform === "win32") {
  const createCache = StaticFileCache.create.bind(StaticFileCache);
  StaticFileCache.create = async (clientDirectory) => {
    const cache = await createCache(clientDirectory);
    for (const [key, entry] of [...cache.entries]) {
      const normalized = key.replaceAll("\\", "/");
      if (normalized !== key) {
        cache.entries.delete(key);
        cache.entries.set(normalized, entry);
      }
    }
    return cache;
  };
}

const port = Number(optionValue("--port", "-p") ?? process.env.PORT ?? 3000);
const host = optionValue("--hostname", "-H") ?? "0.0.0.0";

if (!Number.isInteger(port) || port < 1 || port > 65_535) {
  throw new Error(`Invalid dashboard port: ${port}`);
}

await startProdServer({
  port,
  host,
  outDir: path.resolve(process.cwd(), "dist"),
});
