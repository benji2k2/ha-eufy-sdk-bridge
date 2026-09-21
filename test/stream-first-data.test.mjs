// /stream must not answer before the feed delivers. openReadable() resolves when the P2P session is
// REQUESTED, not when it delivers; on a battery camera that wake takes ~10-20s, and a consumer handed an
// empty 200 dies with "Invalid data found when processing input" and arms the failure backoff — which
// then blocks the retry that would have worked. Drives the real route against a feed that starts late.
import { Readable } from "node:stream";
import { test } from "node:test";
import assert from "node:assert/strict";

import { loadConfig } from "../src/config.mjs";
import { createState } from "../src/state.mjs";

// The go2rtc consumer probe would fire real HTTP at :1984; the route reads this into a module const at
// load, so it must be set BEFORE the import — hence the dynamic import below.
process.env.BRIDGE_STREAM_CONSUMER_LOG_MS = "0";
const { createHttpHandler } = await import("../src/http-routes.mjs");

const FRAME = Buffer.from([0x00, 0x00, 0x00, 0x01, 0x65, 0x88]); // Annex-B start code + payload

/** A feed that stays silent for `delayMs` (negative = forever), then emits two chunks. */
function lateFeed(delayMs) {
  const feed = new Readable({ read() {} });
  if (delayMs >= 0) {
    const t = setTimeout(() => {
      feed.push(FRAME);
      feed.push(Buffer.from("SECOND"));
    }, delayMs);
    t.unref?.();
  }
  return feed;
}

function setup({ delayMs = 0, env = {} } = {}) {
  const config = loadConfig({ EUFY_EMAIL: "x@y.z", EUFY_PASSWORD: "pw", ...env });
  const state = createState();
  state.flags.ready = true;
  const noted = { opened: 0, failed: 0 };
  const ctx = {
    ...config,
    state,
    eventLog: () => {},
    broadcast: () => {},
    noteStreamOpened: () => void (noted.opened += 1),
    noteStreamFailure: () => void (noted.failed += 1),
    streamBackoffMs: () => 0,
    // The seam the route offers instead of streams.mjs' per-camera client cache: no login, no network.
    streamClientFor: async () => ({
      getDevice: async () => ({ camera: () => ({ openReadable: async () => lateFeed(delayMs) }) }),
    }),
    eufy: { async getDevice() { throw new Error("the stream route must not use ctx.eufy"); } },
  };
  return { handler: createHttpHandler(ctx), noted };
}

/** Run GET /stream/CAM1; capture the head, when it was written, and every byte written after it. */
async function pull(handler) {
  const out = { chunks: [] };
  const res = {
    writeHead(code, headers) { out.code = code; out.headers = headers; out.headAt = Date.now(); },
    write(c) { out.chunks.push(Buffer.from(c)); return true; },
    end(body) { if (body) out.chunks.push(Buffer.from(body)); },
    on() {}, once() {}, emit() {}, removeListener() {}, off() {}, destroy() {},
  };
  await handler({ url: "/stream/CAM1", headers: { host: "localhost" }, on() {} }, res);
  return out;
}

test("stream: holds the response until the feed delivers its first chunk", async () => {
  const { handler, noted } = setup({ delayMs: 120 });
  const started = Date.now();
  const out = await pull(handler);
  assert.equal(out.code, 200);
  assert.ok(out.headAt - started >= 100, "answered before the first frame arrived");
  assert.deepEqual(out.chunks[0], FRAME); // the peeked chunk is written, not dropped
  assert.equal(noted.opened, 1); // delivering counts as reachable
  assert.equal(noted.failed, 0);
});

test("stream: a camera that never wakes fails instead of serving an empty body", async () => {
  const { handler, noted } = setup({ delayMs: -1, env: { STREAM_FIRST_DATA_MS: "60" } });
  const out = await pull(handler);
  assert.equal(out.code, 502);
  assert.match(JSON.parse(out.chunks.join("")).error, /no video data within 60ms/);
  assert.equal(noted.opened, 0); // must NOT count as a successful open
  assert.equal(noted.failed, 1); // backoff armed, as before
});

test("stream: STREAM_FIRST_DATA_MS=0 answers immediately, as it always did", async () => {
  const { handler, noted } = setup({ delayMs: 500, env: { STREAM_FIRST_DATA_MS: "0" } });
  const started = Date.now();
  const out = await pull(handler);
  assert.equal(out.code, 200);
  assert.ok(out.headAt - started < 100, "waited even though the wait is disabled");
  assert.equal(noted.opened, 1);
});
