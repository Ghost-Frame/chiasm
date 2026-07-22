import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";

// ---------------------------------------------------------------------------
// GET /tasks/:id/history
// ---------------------------------------------------------------------------
// Spins up a real server (same pattern as test/auth.test.ts) and verifies the
// history endpoint returns only the task_updates rows for the requested task,
// newest first.

const TEST_PORT = 14301;
const TEST_ADMIN_KEY = "test-admin-key-" + randomBytes(16).toString("hex");
const TEST_DB_PATH = ":memory:";

let serverProcess: ReturnType<typeof import("node:child_process").spawn> | null = null;

// Sends an authenticated request to the test server and parses its JSON body.
async function api(
  method: string,
  path: string,
  opts: { body?: unknown; key?: string } = {},
): Promise<{ status: number; data: unknown }> {
  const headers: Record<string, string> = {};
  if (opts.key) headers["Authorization"] = `Bearer ${opts.key}`;
  if (opts.body) headers["Content-Type"] = "application/json";

  const res = await fetch(`http://127.0.0.1:${TEST_PORT}${path}`, {
    method,
    headers,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });

  const text = await res.text();
  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }
  return { status: res.status, data };
}

// Waits until the spawned Chiasm server accepts health checks.
async function waitForServer(timeoutMs = 10_000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`http://127.0.0.1:${TEST_PORT}/health`);
      if (res.ok) return;
    } catch { /* not ready yet */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error("Server did not start in time");
}

before(async () => {
  const { spawn } = await import("node:child_process");
  serverProcess = spawn(
    process.execPath,
    ["--experimental-strip-types", "src/server.ts"],
    {
      env: {
        ...process.env,
        PORT: String(TEST_PORT),
        HOST: "127.0.0.1",
        DB_PATH: TEST_DB_PATH,
        CHIASM_API_KEY: TEST_ADMIN_KEY,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  let stderr = "";
  serverProcess.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  serverProcess.on("exit", (code) => {
    if (code && code !== 0 && code !== null) {
      console.error("Server exited with code", code);
      if (stderr) console.error(stderr);
    }
  });

  await waitForServer();
});

after(() => {
  if (serverProcess) {
    serverProcess.kill("SIGTERM");
    serverProcess = null;
  }
});

describe("GET /tasks/:id/history", () => {
  it("returns only the requesting task's updates, newest first", async () => {
    // Task A: create + two updates -> 3 task_updates rows total.
    const created = await api("POST", "/tasks", {
      key: TEST_ADMIN_KEY,
      body: { agent: "claude-code", project: "p", title: "Task A" },
    });
    assert.equal(created.status, 201);
    const taskAId = (created.data as { id: number }).id;

    await api("PATCH", `/tasks/${taskAId}`, {
      key: TEST_ADMIN_KEY,
      body: { summary: "first update" },
    });
    await api("PATCH", `/tasks/${taskAId}`, {
      key: TEST_ADMIN_KEY,
      body: { summary: "second update" },
    });

    // Task B: an unrelated task with its own updates, must not leak into A's history.
    const createdB = await api("POST", "/tasks", {
      key: TEST_ADMIN_KEY,
      body: { agent: "claude-code", project: "p", title: "Task B" },
    });
    const taskBId = (createdB.data as { id: number }).id;
    await api("PATCH", `/tasks/${taskBId}`, {
      key: TEST_ADMIN_KEY,
      body: { summary: "task B update" },
    });

    const { status, data } = await api("GET", `/tasks/${taskAId}/history`, {
      key: TEST_ADMIN_KEY,
    });
    assert.equal(status, 200);
    const body = data as { history: Array<{ task_id: number; summary: string | null }>; count: number };

    assert.equal(body.count, 3, "task A should have 3 update rows: created + 2 patches");
    assert.equal(body.history.length, 3);
    for (const row of body.history) {
      assert.equal(row.task_id, taskAId, "history must not include another task's updates");
    }

    // Newest first: the most recent PATCH summary should be at index 0.
    assert.equal(body.history[0].summary, "second update");
    assert.equal(body.history[1].summary, "first update");
  });

  it("respects the limit query parameter", async () => {
    const created = await api("POST", "/tasks", {
      key: TEST_ADMIN_KEY,
      body: { agent: "claude-code", project: "p", title: "Task limit" },
    });
    const taskId = (created.data as { id: number }).id;
    for (let i = 0; i < 5; i++) {
      await api("PATCH", `/tasks/${taskId}`, {
        key: TEST_ADMIN_KEY,
        body: { summary: `update ${i}` },
      });
    }

    const { status, data } = await api("GET", `/tasks/${taskId}/history?limit=2`, {
      key: TEST_ADMIN_KEY,
    });
    assert.equal(status, 200);
    const body = data as { history: unknown[]; count: number };
    assert.equal(body.count, 2);
    assert.equal(body.history.length, 2);
  });

  it("returns 404 for a nonexistent task", async () => {
    const { status } = await api("GET", "/tasks/999999/history", { key: TEST_ADMIN_KEY });
    assert.equal(status, 404);
  });

  it("rejects unauthenticated requests", async () => {
    const { status } = await api("GET", "/tasks/1/history");
    assert.equal(status, 401);
  });
});
