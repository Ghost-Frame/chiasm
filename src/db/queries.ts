import type DatabaseConstructor from "libsql";
type Database = InstanceType<typeof DatabaseConstructor>;
import { startSpan, SpanStatusCode } from "../tracing.ts";
import { emitEvent } from "../axon.ts";

export interface Task {
  id: number;
  agent: string;
  project: string;
  title: string;
  status: string;
  summary: string | null;
  expected_output: string | null;
  output_format: string;
  output: string | null;
  condition: string | null;
  guardrail_url: string | null;
  guardrail_retries: number;
  plan: string | null;
  feedback: string | null;
  created_at: string;
  updated_at: string;
}

export interface AgentKey {
  id: number;
  agent: string;
  key_hash: string;
  key_prefix: string;
  created_at: string;
  last_used_at: string | null;
  revoked: number;
}

export interface TaskUpdate {
  id: number;
  task_id: number;
  agent: string;
  status: string;
  summary: string | null;
  created_at: string;
}

export interface TaskFilters {
  agent?: string;
  project?: string;
  status?: string;
  limit?: number;
  offset?: number;
}

export function listTasks(db: Database, filters: TaskFilters = {}): Task[] {
  let query = "SELECT * FROM tasks WHERE 1=1";
  const params: Array<string | number> = [];

  if (filters.agent) { query += " AND agent = ?"; params.push(filters.agent); }
  if (filters.project) { query += " AND project = ?"; params.push(filters.project); }
  if (filters.status) { query += " AND status = ?"; params.push(filters.status); }

  query += " ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?";
  params.push(filters.limit ?? 500, filters.offset ?? 0);

  return db.prepare(query).all(...params) as Task[];
}

export function getTask(db: Database, id: number): Task | undefined {
  return db.prepare("SELECT * FROM tasks WHERE id = ?").get(id) as Task | undefined;
}

export function createTask(
  db: Database,
  data: {
    agent: string;
    project: string;
    title: string;
    summary?: string;
    expected_output?: string;
    output_format?: string;
    condition?: string;
    guardrail_url?: string;
  }
): Task {
  const span = startSpan("chiasm.createTask", { "task.agent": data.agent, "task.project": data.project });
  const run = db.transaction(() => {
    const result = db.prepare(
      `INSERT INTO tasks (agent, project, title, summary, expected_output, output_format, condition, guardrail_url)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING *`
    ).get(
      data.agent, data.project, data.title,
      data.summary ?? null,
      data.expected_output ?? null,
      data.output_format ?? "raw",
      data.condition ?? null,
      data.guardrail_url ?? null,
    ) as Task;

    db.prepare(
      "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, 'active', ?)"
    ).run(result.id, data.agent, data.summary ?? null);

    span.setAttribute("task.id", result.id);
    return result;
  });

  try {
    const result = run();
    span.setStatus({ code: SpanStatusCode.OK });
    span.end();
    emitEvent("tasks", "task.created", { agent: result.agent, project: result.project, title: result.title, task_id: result.id });
    return result;
  } catch (e: any) {
    span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
    span.recordException(e);
    span.end();
    throw e;
  }
}

export function updateTask(
  db: Database,
  id: number,
  data: { status?: string; summary?: string; plan?: string; feedback?: string }
): Task | undefined {
  const existing = getTask(db, id);
  if (!existing) return undefined;

  const status = data.status ?? existing.status;
  const summary = data.summary ?? existing.summary;
  const plan = data.plan !== undefined ? data.plan : existing.plan;
  const feedback = data.feedback !== undefined ? data.feedback : existing.feedback;

  const span = startSpan("chiasm.updateTask", { "task.id": id, "task.agent": existing.agent, "task.status": status });

  const run = db.transaction(() => {
    const result = db.prepare(
      `UPDATE tasks SET status = ?, summary = ?, plan = ?, feedback = ?, updated_at = datetime('now')
       WHERE id = ? RETURNING *`
    ).get(status, summary, plan, feedback, id) as Task;

    db.prepare(
      "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, ?, ?)"
    ).run(id, existing.agent, status, summary);

    return result;
  });

  try {
    const result = run();
    span.setStatus({ code: SpanStatusCode.OK });
    span.end();
    const eventType = result.status === "completed" ? "task.completed" : "task.updated";
    emitEvent("tasks", eventType, { agent: result.agent, project: result.project, title: result.title, task_id: result.id, status: result.status, summary: result.summary ?? undefined });
    return result;
  } catch (e: any) {
    span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
    span.recordException(e);
    span.end();
    throw e;
  }
}

export function deleteTask(db: Database, id: number): boolean {
  return db.prepare("DELETE FROM tasks WHERE id = ?").run(id).changes > 0;
}

export function submitOutput(db: Database, id: number, output: string): Task | undefined {
  const existing = getTask(db, id);
  if (!existing) return undefined;

  const result = db.prepare(
    "UPDATE tasks SET output = ?, updated_at = datetime('now') WHERE id = ? RETURNING *"
  ).get(output, id) as Task;

  db.prepare(
    "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, ?, 'Output submitted')"
  ).run(id, existing.agent, existing.status);

  emitEvent("tasks", "task.output", { agent: existing.agent, project: existing.project, title: existing.title, task_id: id });

  if (existing.guardrail_url) {
    runGuardrail(db, result);
  }

  return result;
}

function runGuardrail(db: Database, task: Task) {
  fetch(task.guardrail_url!, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      task_id: task.id,
      title: task.title,
      expected_output: task.expected_output,
      output: task.output,
      output_format: task.output_format,
    }),
    signal: AbortSignal.timeout(15000),
  })
    .then(async (res) => {
      if (!res.ok) throw new Error(`Guardrail HTTP ${res.status}`);
      const data = await res.json() as { valid: boolean; feedback?: string };
      const retries = task.guardrail_retries + 1;

      if (data.valid) {
        db.prepare(
          "UPDATE tasks SET status = 'completed', guardrail_retries = ?, updated_at = datetime('now') WHERE id = ?"
        ).run(retries, task.id);
        db.prepare(
          "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, 'completed', 'Guardrail passed')"
        ).run(task.id, task.agent);
        emitEvent("tasks", "task.completed", { agent: task.agent, project: task.project, title: task.title, task_id: task.id });
      } else {
        const fb = data.feedback ?? "Guardrail rejected output";
        db.prepare(
          "UPDATE tasks SET status = 'active', feedback = ?, guardrail_retries = ?, updated_at = datetime('now') WHERE id = ?"
        ).run(fb, retries, task.id);
        db.prepare(
          "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, 'active', ?)"
        ).run(task.id, task.agent, `Guardrail failed (attempt ${retries}): ${fb}`);
      }
    })
    .catch((err: any) => {
      db.prepare(
        "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, ?, ?)"
      ).run(task.id, task.agent, task.status, `Guardrail error: ${err.message}`);
    });
}

export function submitFeedback(db: Database, id: number, feedback: string): Task | undefined {
  const existing = getTask(db, id);
  if (!existing) return undefined;

  const result = db.prepare(
    "UPDATE tasks SET feedback = ?, status = 'active', updated_at = datetime('now') WHERE id = ? RETURNING *"
  ).get(feedback, id) as Task;

  db.prepare(
    "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, 'active', ?)"
  ).run(id, existing.agent, `Human feedback: ${feedback.slice(0, 100)}`);

  emitEvent("tasks", "task.feedback", { agent: existing.agent, project: existing.project, title: existing.title, task_id: id, feedback });

  return result;
}

export function getFeed(
  db: Database,
  limit: number = 50,
  offset: number = 0
): (TaskUpdate & { project: string; title: string })[] {
  return db.prepare(`
    SELECT tu.*, COALESCE(t.project, 'deleted') as project, COALESCE(t.title, 'deleted') as title
    FROM task_updates tu
    LEFT JOIN tasks t ON tu.task_id = t.id
    ORDER BY tu.created_at DESC, tu.id DESC
    LIMIT ? OFFSET ?
  `).all(limit, offset) as (TaskUpdate & { project: string; title: string })[];
}

// ============================================================================
// AGENT KEY MANAGEMENT
// ============================================================================

export function lookupAgentKey(db: Database, keyHash: string): AgentKey | undefined {
  const key = db.prepare(
    "SELECT * FROM agent_keys WHERE key_hash = ? AND revoked = 0"
  ).get(keyHash) as AgentKey | undefined;
  if (key) {
    db.prepare("UPDATE agent_keys SET last_used_at = datetime('now') WHERE id = ?").run(key.id);
  }
  return key;
}

export function createAgentKey(db: Database, agent: string, keyHash: string, keyPrefix: string): AgentKey {
  return db.prepare(
    "INSERT INTO agent_keys (agent, key_hash, key_prefix) VALUES (?, ?, ?) RETURNING *"
  ).get(agent, keyHash, keyPrefix) as AgentKey;
}

export function listAgentKeys(db: Database): Omit<AgentKey, "key_hash">[] {
  return db.prepare(
    "SELECT id, agent, key_prefix, created_at, last_used_at, revoked FROM agent_keys ORDER BY created_at DESC"
  ).all() as Omit<AgentKey, "key_hash">[];
}

export function revokeAgentKey(db: Database, id: number): boolean {
  return db.prepare("UPDATE agent_keys SET revoked = 1 WHERE id = ?").run(id).changes > 0;
}

export function pruneTaskUpdates(db: Database, maxRows: number, maxAgeDays: number) {
  if (maxAgeDays > 0) {
    db.prepare("DELETE FROM task_updates WHERE created_at < datetime('now', ?)").run(`-${maxAgeDays} days`);
  }
  if (maxRows > 0) {
    db.prepare(`
      DELETE FROM task_updates WHERE id IN (
        SELECT id FROM (SELECT id FROM task_updates ORDER BY created_at DESC, id DESC LIMIT -1 OFFSET ?)
      )
    `).run(maxRows);
  }
}
