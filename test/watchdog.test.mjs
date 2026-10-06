import { test } from "node:test";
import assert from "node:assert/strict";
import { LoginStatus } from "@mega-yfue/eufy-sdk";
import { createAuth } from "../src/auth.mjs";
import { createState } from "../src/state.mjs";
import { createWatchdog } from "../src/watchdog.mjs";

function watchdogFixture() {
  const calls = [];
  const state = createState();
  state.flags.ready = true;
  const eufy = {
    pollIntervalMs: 600_000,
    async disconnect() {
      calls.push("disconnect");
    },
    async login() {
      calls.push("login");
      return { status: LoginStatus.Ok };
    },
    setPollInterval(ms) {
      calls.push(["setPollInterval", ms]);
    },
  };
  const ctx = {
    eufy,
    state,
    PUSH_STALL_MS: 5 * 60_000,
    async applyLogin(result) {
      calls.push(["applyLogin", result.status]);
    },
  };
  Object.assign(ctx, createWatchdog(ctx));
  return { calls, ctx, state };
}

test("device-event silence for more than 30 minutes does not trigger poll recovery", async (t) => {
  let now = 1_000_000;
  t.mock.method(Date, "now", () => now);
  const { calls, ctx, state } = watchdogFixture();
  state.flags.pushConnected = true;

  now += 31 * 60_000;
  await ctx.watchdogTick();

  assert.deepEqual(calls, []);
  assert.equal(state.flags.recovering, false);
});

test("a sustained explicit push disconnect still triggers recovery", async (t) => {
  let now = 1_000_000;
  t.mock.method(Date, "now", () => now);
  t.mock.method(console, "error", () => {});
  t.mock.method(console, "log", () => {});
  const { calls, ctx, state } = watchdogFixture();
  state.flags.pushConnected = false;
  state.flags.pushSince = now;

  now += 5 * 60_000;
  await ctx.watchdogTick();

  assert.deepEqual(calls, ["disconnect", "login", ["applyLogin", LoginStatus.Ok], ["setPollInterval", 600_000]]);
  assert.equal(state.flags.pushSince, now);
  assert.equal(state.flags.recovering, false);
});

test("session expiry still enters immediate re-authentication", async (t) => {
  t.mock.method(console, "error", () => {});
  const calls = [];
  const state = createState();
  state.flags.ready = true;
  const eufy = {
    async disconnect() {
      calls.push("disconnect");
    },
    async login() {
      calls.push("login");
      return { status: LoginStatus.TwoFactor, method: "email" };
    },
  };
  const ctx = {
    eufy,
    state,
    async completeBoot() {},
    broadcast(message) {
      calls.push(["broadcast", message.state]);
    },
  };
  Object.assign(ctx, createAuth(ctx));

  ctx.maybeRecoverSession();
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(state.flags.sessionLost, true);
  assert.equal(state.flags.recovering, false);
  assert.deepEqual(ctx.authStatus(), { state: "require_2fa", method: "email" });
  assert.deepEqual(calls, [["broadcast", "reauth"], "disconnect", "login", ["broadcast", "require_2fa"]]);
});
