import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";

async function render() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request("http://localhost/", { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("server-renders the XPDE shadow terminal", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);
  const html = await response.text();
  assert.match(html, /<title>XPDE — GOLDm# Shadow Terminal<\/title>/i);
  assert.match(html, /GOLDm#/);
  assert.match(html, /SHADOW MODE/);
  assert.match(html, /DEMO DATA/);
  assert.match(html, /FORECAST/);
  assert.match(html, /Auto-trading nonaktif/);
  assert.match(html, /Probabilitas naik 57\.0%/);
  assert.match(html, /Probabilitas turun 43\.0%/);
  assert.match(html, /Origin forecast peluang arah · H3/);
  assert.match(html, /Sisa reward dari harga entry tidak memadai/);
  assert.match(html, /Offline holdout coverage/);
  assert.match(html, /Live coverage · fully-settled predictions/);
  assert.match(html, /Origin forecast P\(TP-first\)/);
  assert.match(html, /Tidak dikondisikan ulang terhadap current entry/);
  assert.match(html, /Settlement completeness/);
  assert.match(html, /Current runtime session · direction/);
  assert.match(html, /Retry realtime/);
  assert.match(
    html,
    /First-actionable TP dalam horizon · .*SCALPER.* · 200 proposal instances/,
  );
  assert.match(html, /Decision support only/);
  assert.doesNotMatch(html, /codex-preview|react-loading-skeleton/i);
});

test("keeps the server and client hydration fixture deterministic", async () => {
  const source = await readFile(
    new URL("../app/page.tsx", import.meta.url),
    "utf8",
  );
  const fixtureStart = source.indexOf("function buildDemoState");
  const fixtureEnd = source.indexOf("function money");
  assert.ok(fixtureStart >= 0 && fixtureEnd > fixtureStart);
  const fixture = source.slice(fixtureStart, fixtureEnd);
  assert.doesNotMatch(fixture, /Date\.now\(\)|new Date\(\)\.toISOString\(\)/);
  assert.match(fixture, /DEMO_REFERENCE_MS/);
  assert.match(source, /formatPrice\(state\.snapshot\.bid, priceDigits\)/);
  assert.match(source, /state\.snapshot\.symbol_spec\.digits/);
});
