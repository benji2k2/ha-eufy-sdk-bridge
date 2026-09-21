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
  const feeds = []; // every feed the route opened, so a test can see whether it was released
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
      getDevice: async () => ({
        camera: () => ({
          openReadable: async () => {
            const feed = lateFeed(delayMs);
            feeds.push(feed);
            return feed;
          },
        }),
      }),
    }),
    eufy: { async getDevice() { throw new Error("the stream route must not use ctx.eufy"); } },
  };
  return { handler: createHttpHandler(ctx), noted, feeds };
}

/**
 * Run GET /stream/CAM1; capture the head, when it was written, and every byte written after it.
 * `leaveAfterMs` makes the requester hang up that long into the request, the way HA gives up on a
 * battery camera that is still waking.
 */
async function pull(handler, { leaveAfterMs } = {}) {
  const out = { chunks: [] };
  const res = {
    writeHead(code, headers) { out.code = code; out.headers = headers; out.headAt = Date.now(); },
    write(c) { out.chunks.push(Buffer.from(c)); return true; },
    end(body) { if (body) out.chunks.push(Buffer.from(body)); },
    on() {}, once() {}, emit() {}, removeListener() {}, off() {}, destroy() {},
  };
  const onClose = [];
  const req = {
    url: "/stream/CAM1",
    headers: { host: "localhost" },
    on(ev, fn) { if (ev === "close") onClose.push(fn); },
  };
  if (leaveAfterMs != null) setTimeout(() => onClose.forEach((fn) => fn()), leaveAfterMs);
  await handler(req, res);
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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

test("stream: a requester that leaves while the camera wakes is not answered; the session is held for a retry", async () => {
  const { handler, noted, feeds } = setup({ delayMs: 80, env: { STREAM_LINGER_MS: "60" } });
  const out = await pull(handler, { leaveAfterMs: 20 }); // gives up at 20ms, the camera delivers at 80ms
  assert.equal(out.code, undefined); // nobody left to answer
  assert.equal(noted.opened, 1); // the camera did wake: the retry must not meet the failure backoff
  assert.equal(noted.failed, 0);
  assert.equal(feeds[0].destroyed, false); // held, so a retry can join the awake camera
  await sleep(100);
  assert.equal(feeds[0].destroyed, true); // ...and released once the hold is over, never leaked
});

test("stream: STREAM_LINGER_MS=0 releases an abandoned session at once", async () => {
  const { handler, feeds } = setup({ delayMs: 40, env: { STREAM_LINGER_MS: "0" } });
  await pull(handler, { leaveAfterMs: 10 });
  await sleep(10);
  assert.equal(feeds[0].destroyed, true);
});

test("stream: a requester that stays is answered normally even with a hold configured", async () => {
  const { handler, feeds } = setup({ delayMs: 30, env: { STREAM_LINGER_MS: "60" } });
  const out = await pull(handler); // never hangs up
  assert.equal(out.code, 200);
  assert.deepEqual(out.chunks[0], FRAME);
  assert.equal(feeds[0].destroyed, false); // streaming to its requester, not on a release timer
});
