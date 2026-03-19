import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";

// ---------------------------------------------------------------------------
// Test configuration
// ---------------------------------------------------------------------------

const TEST_PORT = 14300;
const TEST_ADMIN_KEY = "test-admin-key-" + randomBytes(16).toString("hex");
const TEST_DB_PATH = ":memory:";

// Agent keys we will provision via the admin API
const AGENTS = ["claude-code", "opencode", "gpt"] as const;
const agentKeys: Record<string, string> = {};

let serverProcess: ReturnType<typeof import("node:child_process").spawn> | null = null;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Server lifecycle
// ---------------------------------------------------------------------------

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

  // Collect stderr for debugging if needed
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

  // Provision agent keys
  for (const agent of AGENTS) {
    const { status, data } = await api("POST", "/admin/keys", {
      key: TEST_ADMIN_KEY,
      body: { agent },
    });
    assert.equal(status, 201, `Failed to create key for ${agent}`);
    agentKeys[agent] = (data as { key: string }).key;
  }
});

after(() => {
  if (serverProcess) {
    serverProcess.kill("SIGTERM");
    serverProcess = null;
  }
});

// ===========================================================================
// AUTH GATING
// ===========================================================================

describe("auth gating", () => {
  it("rejects unauthenticated GET /tasks", async () => {
    const { status, data } = await api("GET", "/tasks");
    assert.equal(status, 401);
    assert.deepEqual(data, { error: "Unauthorized" });
  });

  it("rejects unauthenticated GET /feed", async () => {
    const { status } = await api("GET", "/feed");
    assert.equal(status, 401);
  });

  it("rejects unauthenticated POST /tasks", async () => {
    const { status } = await api("POST", "/tasks", {
      body: { agent: "test", project: "test", title: "test" },
    });
    assert.equal(status, 401);
  });

  it("rejects unauthenticated GET /admin/keys", async () => {
    const { status } = await api("GET", "/admin/keys");
    assert.equal(status, 401);
  });

  it("rejects invalid bearer token", async () => {
    const { status } = await api("GET", "/tasks", { key: "bogus-key" });
    assert.equal(status, 401);
  });

  it("allows health check without auth", async () => {
    const { status, data } = await api("GET", "/health");
    assert.equal(status, 200);
    assert.equal((data as { status: string }).status, "ok");
  });

  it("allows admin key to access /tasks", async () => {
    const { status, data } = await api("GET", "/tasks", { key: TEST_ADMIN_KEY });
    assert.equal(status, 200);
    assert.ok(Array.isArray(data));
  });

  it("allows agent key to access /tasks", async () => {
    const { status, data } = await api("GET", "/tasks", {
      key: agentKeys["opencode"],
    });
    assert.equal(status, 200);
    assert.ok(Array.isArray(data));
  });
});

// ===========================================================================
// AGENT OWNERSHIP
// ===========================================================================

describe("agent ownership enforcement", () => {
  let taskId: number;

  it("agent key can create task for itself", async () => {
    const { status, data } = await api("POST", "/tasks", {
      key: agentKeys["claude-code"],
      body: { agent: "claude-code", project: "test-proj", title: "Test task" },
    });
    assert.equal(status, 201);
    taskId = (data as { id: number }).id;
    assert.ok(taskId > 0);
  });

  it("agent key cannot create task for another agent", async () => {
    const { status, data } = await api("POST", "/tasks", {
      key: agentKeys["opencode"],
      body: { agent: "claude-code", project: "test-proj", title: "Impersonation" },
    });
    assert.equal(status, 403);
    assert.ok((data as { error: string }).error.includes("cannot create"));
  });

  it("agent key can update its own task", async () => {
    const { status, data } = await api("PATCH", `/tasks/${taskId}`, {
      key: agentKeys["claude-code"],
      body: { summary: "Updated by owner" },
    });
    assert.equal(status, 200);
    assert.equal((data as { summary: string }).summary, "Updated by owner");
  });

  it("agent key cannot update another agent's task", async () => {
    const { status } = await api("PATCH", `/tasks/${taskId}`, {
      key: agentKeys["gpt"],
      body: { summary: "Hijacked" },
    });
    assert.equal(status, 403);
  });

  it("agent key cannot delete another agent's task", async () => {
    const { status } = await api("DELETE", `/tasks/${taskId}`, {
      key: agentKeys["gpt"],
    });
    assert.equal(status, 403);
  });

  it("admin key can update any task", async () => {
    const { status, data } = await api("PATCH", `/tasks/${taskId}`, {
      key: TEST_ADMIN_KEY,
      body: { summary: "Admin override" },
    });
    assert.equal(status, 200);
    assert.equal((data as { summary: string }).summary, "Admin override");
  });

  it("admin key can delete any task", async () => {
    const { status } = await api("DELETE", `/tasks/${taskId}`, {
      key: TEST_ADMIN_KEY,
    });
    assert.equal(status, 200);
  });

  it("any agent can read all tasks", async () => {
    // Create a task as claude-code
    await api("POST", "/tasks", {
      key: agentKeys["claude-code"],
      body: { agent: "claude-code", project: "p", title: "Readable by all" },
    });

    // Read as gpt
    const { status, data } = await api("GET", "/tasks", {
      key: agentKeys["gpt"],
    });
    assert.equal(status, 200);
    const tasks = data as Array<{ agent: string }>;
    assert.ok(tasks.some((t) => t.agent === "claude-code"));
  });
});

// ===========================================================================
// ADMIN ROUTES
// ===========================================================================

describe("admin route protection", () => {
  it("agent key cannot access GET /admin/keys", async () => {
    const { status, data } = await api("GET", "/admin/keys", {
      key: agentKeys["opencode"],
    });
    assert.equal(status, 403);
    assert.deepEqual(data, { error: "Admin access required" });
  });

  it("agent key cannot create agent keys", async () => {
    const { status } = await api("POST", "/admin/keys", {
      key: agentKeys["opencode"],
      body: { agent: "evil" },
    });
    assert.equal(status, 403);
  });

  it("admin key can list agent keys", async () => {
    const { status, data } = await api("GET", "/admin/keys", {
      key: TEST_ADMIN_KEY,
    });
    assert.equal(status, 200);
    assert.ok(Array.isArray(data));
    assert.ok((data as Array<unknown>).length >= AGENTS.length);
  });
});
