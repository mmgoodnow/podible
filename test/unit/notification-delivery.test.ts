import { Database } from "bun:sqlite";
import { expect, mock, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { deliverNextNotification } from "../../src/notifications/delivery";
import type { NotificationFetch } from "../../src/notify";
import { BooksRepo } from "../../src/repo";
import { defaultSettings, parseSettings } from "../../src/settings";

function fixture() {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "local", providerUserId: "reader", username: "reader" });
  const outbox = repo.notifications.delivery;
  const live = defaultSettings({ notifications: {
    delivery: "live",
    email: { apiKey: "fake", from: "podible@example.test", recipients: `${user.id}=reader@example.test` },
    pushover: { enabled: true, lifecycleEnabled: true, apiToken: "fake", userKey: "owner" },
  } });
  let time = Date.parse("2026-01-01T00:00:00Z");
  const enqueue = (id: number, email = true) => repo.transaction(() => {
    outbox.enqueue(`movie:1:${id}`, { title: "Owner", message: "Ready" }, email ? [
      { id: `notice-${id}`, userId: user.id, title: "Movie", episode: null },
    ] : [], new Date(time).toISOString());
  });
  const states = (kind: "event" | "pending" | "done") => db.query<{ value_json: string }, [string]>(
    "SELECT value_json FROM app_state WHERE key GLOB ? ORDER BY key",
  ).all(`notifications:delivery:v1:${kind}:*`).map(row => JSON.parse(row.value_json));
  return { db, repo, user, outbox, live, enqueue, states, now: () => time, advance: (ms: number) => { time += ms; } };
}

test("dry-run events and legacy recovery push settings never dispatch lifecycle messages", async () => {
  const f = fixture();
  try {
    const fetcher = mock(async () => { throw new Error("unexpected provider call"); });
    f.repo.updateSettings({ ...f.live, notifications: { ...f.live.notifications, delivery: "dry-run" } });
    f.enqueue(1);
    expect(f.states("event")).toEqual([{ mode: "dry-run" }]);
    expect(f.states("pending")).toEqual([]);
    f.repo.updateSettings(f.live);
    f.enqueue(1); // Replaying the dry-run event after enabling live must not send it.
    expect(await deliverNextNotification(f.outbox, { fetcher, now: f.now })).toBeNull();

    const legacy = { notifications: { delivery: "live", pushover: { enabled: true, apiToken: "fake", userKey: "owner" } } };
    f.repo.updateSettings(parseSettings(JSON.stringify(legacy)));
    expect(f.repo.getSettings().notifications.pushover.lifecycleEnabled).toBe(false);
    f.enqueue(2);
    expect(await deliverNextNotification(f.outbox, { fetcher, now: f.now })).toBeNull();
    expect(fetcher).not.toHaveBeenCalled();
  } finally { f.db.close(); }
});

test("email retries ambiguous responses with identical payload and idempotency key, then stays terminal", async () => {
  const f = fixture();
  try {
    f.live.notifications.pushover.lifecycleEnabled = false;
    f.repo.updateSettings(f.live);
    f.enqueue(1);
    const requests: { key: string | null; body: RequestInit["body"] }[] = [];
    const fetcher: NotificationFetch = async (url, init) => {
      expect(url).toBe("https://api.resend.com/emails");
      requests.push({ key: new Headers(init?.headers).get("Idempotency-Key"), body: init?.body });
      if (requests.length === 1) throw new Error("response lost after acceptance");
      if (requests.length === 2) return Response.json({ name: "concurrent_idempotent_requests" }, { status: 409, headers: { "Retry-After": "180" } });
      return Response.json({ id: "same-provider-id" });
    };
    const tick = () => deliverNextNotification(f.outbox, { fetcher, now: f.now });
    expect(await tick()).toMatchObject({ state: "retry", at: f.now() + 30_000 });
    f.advance(29_999);
    expect(await tick()).toBeNull();
    f.advance(1);
    expect(await tick()).toMatchObject({ state: "retry", at: f.now() + 180_000 });
    f.advance(180_000);
    expect(await tick()).toEqual({ state: "accepted" });
    f.enqueue(1);
    expect(await tick()).toBeNull();
    expect(f.states("done")).toEqual([{ channel: "email", state: "accepted", attempts: 3 }]);
    expect(requests).toHaveLength(3);
    expect(requests[0]?.key).toMatch(/^podible\/[0-9a-f-]{36}$/);
    expect(requests.every(request => request.key === requests[0]?.key && request.body === requests[0]?.body)).toBe(true);
  } finally { f.db.close(); }
});

test("only email reclaims an interrupted send and stale claims cannot settle a newer attempt", async () => {
  const f = fixture();
  try {
    f.live.notifications.pushover.lifecycleEnabled = false;
    f.repo.updateSettings(f.live);
    f.enqueue(1);
    const first = f.outbox.claim(f.now())!;
    expect(first.delivery.channel).toBe("email");
    expect(f.outbox.claim(f.now())).toBeNull();
    f.advance(30_000);
    const second = f.outbox.claim(f.now())!;
    expect(second.id).toBe(first.id);
    expect(second.delivery.attempts).toBe(2);
    expect(f.outbox.settle(first, { state: "accepted" }, f.now())).toBeNull();
    expect(f.outbox.settle(second, { state: "accepted" }, f.now())).toEqual({ state: "accepted" });

    f.live.notifications.pushover.lifecycleEnabled = true;
    f.repo.updateSettings(f.live);
    f.enqueue(2, false);
    const push = f.outbox.claim(f.now())!;
    expect(push.delivery.channel).toBe("pushover");
    f.advance(30_000);
    expect(f.outbox.claim(f.now())).toBeNull();
    expect(f.states("done")).toContainEqual({ channel: "pushover", state: "unknown", attempts: 1, reason: "retry_window_closed" });
  } finally { f.db.close(); }
});

test("retry exhaustion retains uncertainty from a previously unconfirmed email", async () => {
  const f = fixture();
  try {
    f.live.notifications.pushover.lifecycleEnabled = false;
    f.repo.updateSettings(f.live);
    f.enqueue(1);
    const fetcher = mock(async () => new Response(null, { status: 503 }));
    const tick = () => deliverNextNotification(f.outbox, { fetcher, now: f.now });
    for (let attempt = 1; attempt <= 5; attempt++) {
      const outcome = await tick();
      if (outcome?.state !== "retry") throw new Error("expected retry");
      f.advance(outcome.at - f.now());
    }
    expect(await tick()).toEqual({ state: "unknown", reason: "http_503" });
    expect(await tick()).toBeNull();
    expect(fetcher).toHaveBeenCalledTimes(6);
    expect(f.states("done")).toEqual([{ channel: "email", state: "unknown", reason: "http_503", attempts: 6 }]);
  } finally { f.db.close(); }
});

test("Pushover retries explicit rate limits but not an unconfirmed send", async () => {
  const f = fixture();
  try {
    f.repo.updateSettings(f.live);
    f.enqueue(1, false);
    let calls = 0;
    const fetcher = async () => ++calls === 1
      ? new Response(null, { status: 429, headers: { "Retry-After": "60" } })
      : new Response(null, { status: 503 });
    const tick = () => deliverNextNotification(f.outbox, { fetcher, now: f.now });
    expect(await tick()).toMatchObject({ state: "retry", at: f.now() + 60_000 });
    f.advance(30_000);
    expect(await tick()).toBeNull();
    f.advance(30_000);
    expect(await tick()).toEqual({ state: "unknown", reason: "unconfirmed_push" });
    f.advance(60 * 60_000);
    expect(await tick()).toBeNull();
    expect(calls).toBe(2);
  } finally { f.db.close(); }
});

test("delivery checks current configuration, never retargets queued notices, and does not send expired email", async () => {
  const f = fixture();
  try {
    f.live.notifications.pushover.lifecycleEnabled = false;
    f.repo.updateSettings(f.live);
    f.enqueue(1);
    const fetcher = mock(async () => { throw new Error("unexpected provider call"); });
    const tick = () => deliverNextNotification(f.outbox, { fetcher, now: f.now });
    f.live.notifications.email.recipients = `${f.user.id}=new@example.test`;
    f.repo.updateSettings(f.live);
    expect(await tick()).toEqual({ state: "skipped", reason: "destination_changed" });
    f.enqueue(2);
    f.repo.updateSettings({ ...f.live, notifications: { ...f.live.notifications, delivery: "dry-run" } });
    expect(await tick()).toEqual({ state: "skipped", reason: "dry_run" });
    f.repo.updateSettings(f.live);
    f.enqueue(3);
    f.advance(12 * 60 * 60_000);
    expect(await tick()).toBeNull();
    expect(f.states("pending")).toEqual([]);
    expect(fetcher).not.toHaveBeenCalled();
  } finally { f.db.close(); }
});
