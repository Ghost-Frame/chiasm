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
  last_heartbeat: string | null;
  heartbeat_interval: number;
  assigned: number;
  created_at: string;
  updated_at: string;
}

export interface PathClaim {
  id: number;
  task_id: number;
  agent: string;
  project: string;
  path: string;
  claimed_at: string;
  expires_at: string;
  released: number;
}

export interface PathConflict {
  claim_id: number;
  task_id: number;
  agent: string;
  path: string;
  claimed_path: string;
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

    // On completion: release claims and unblock dependents
    if (result.status === "completed" && existing.status !== "completed") {
      releaseClaims(db, id);
      const unblocked = checkAndUnblock(db, id);
      for (const t of unblocked) {
        emitEvent("tasks", "task.unblocked", { agent: t.agent, project: t.project, title: t.title, task_id: t.id });
      }
    }

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
// PATH CLAIMS
// ============================================================================

const DEFAULT_CLAIM_TTL = 1800; // 30 minutes

export function createClaims(
  db: Database,
  taskId: number,
  agent: string,
  project: string,
  paths: string[],
  ttlSeconds: number = DEFAULT_CLAIM_TTL,
): PathClaim[] {
  if (paths.length === 0) return [];
  const claims: PathClaim[] = [];
  const run = db.transaction(() => {
    for (const path of paths) {
      const claim = db.prepare(
        `INSERT INTO path_claims (task_id, agent, project, path, expires_at)
         VALUES (?, ?, ?, ?, datetime('now', '+' || ? || ' seconds')) RETURNING *`
      ).get(taskId, agent, project, path, ttlSeconds) as PathClaim;
      claims.push(claim);
    }
  });
  run();
  emitEvent("claims", "claim.created", { task_id: taskId, agent, project, paths });
  return claims;
}

export function releaseClaims(db: Database, taskId: number): number {
  const result = db.prepare(
    "UPDATE path_claims SET released = 1 WHERE task_id = ? AND released = 0"
  ).run(taskId);
  if (result.changes > 0) {
    emitEvent("claims", "claim.released", { task_id: taskId, count: result.changes });
  }
  return result.changes;
}

export function releaseClaimsByPath(db: Database, taskId: number, paths: string[]): number {
  let total = 0;
  for (const path of paths) {
    total += db.prepare(
      "UPDATE path_claims SET released = 1 WHERE task_id = ? AND path = ? AND released = 0"
    ).run(taskId, path).changes;
  }
  return total;
}

export function checkConflicts(
  db: Database,
  project: string,
  paths: string[],
  excludeTaskId?: number,
): PathConflict[] {
  const conflicts: PathConflict[] = [];
  for (const path of paths) {
    const rows = db.prepare(`
      SELECT id AS claim_id, task_id, agent, path AS claimed_path
      FROM path_claims
      WHERE project = ? AND released = 0 AND expires_at > datetime('now')
        AND (path || '/' LIKE ? || '%' OR ? LIKE path || '%')
        ${excludeTaskId != null ? "AND task_id != ?" : ""}
    `).all(
      ...(excludeTaskId != null
        ? [project, path, path, excludeTaskId]
        : [project, path, path])
    ) as { claim_id: number; task_id: number; agent: string; claimed_path: string }[];

    for (const row of rows) {
      conflicts.push({ ...row, path });
    }
  }
  return conflicts;
}

export function getClaimsForTask(db: Database, taskId: number): PathClaim[] {
  return db.prepare(
    "SELECT * FROM path_claims WHERE task_id = ? AND released = 0 AND expires_at > datetime('now') ORDER BY path"
  ).all(taskId) as PathClaim[];
}

export function getClaimsForProject(db: Database, project: string): PathClaim[] {
  return db.prepare(
    "SELECT * FROM path_claims WHERE project = ? AND released = 0 AND expires_at > datetime('now') ORDER BY path"
  ).all(project) as PathClaim[];
}

export function refreshClaimExpiry(db: Database, taskId: number, ttlSeconds: number = DEFAULT_CLAIM_TTL): number {
  return db.prepare(
    "UPDATE path_claims SET expires_at = datetime('now', '+' || ? || ' seconds') WHERE task_id = ? AND released = 0"
  ).run(ttlSeconds, taskId).changes;
}

// ============================================================================
// HEARTBEAT & STALE DETECTION
// ============================================================================

export function recordHeartbeat(db: Database, taskId: number): Task | undefined {
  const existing = getTask(db, taskId);
  if (!existing) return undefined;

  const result = db.prepare(
    "UPDATE tasks SET last_heartbeat = datetime('now'), updated_at = datetime('now') WHERE id = ? RETURNING *"
  ).get(taskId) as Task;

  // Refresh claim expiry on heartbeat
  refreshClaimExpiry(db, taskId, existing.heartbeat_interval * 2);

  return result;
}

export function markStaleTasks(db: Database, graceMultiplier: number = 2): Task[] {
  const staleTasks = db.prepare(`
    SELECT * FROM tasks
    WHERE status IN ('active', 'paused')
      AND last_heartbeat IS NOT NULL
      AND last_heartbeat < datetime('now', '-' || (heartbeat_interval * ?) || ' seconds')
  `).all(graceMultiplier) as Task[];

  for (const task of staleTasks) {
    db.prepare(
      "UPDATE tasks SET status = 'stale', updated_at = datetime('now') WHERE id = ?"
    ).run(task.id);
    db.prepare(
      "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, 'stale', 'Heartbeat timeout')"
    ).run(task.id, task.agent);
    releaseClaims(db, task.id);
  }

  return staleTasks;
}

// ============================================================================
// TASK DEPENDENCIES
// ============================================================================

export function hasCircularDependency(db: Database, taskId: number, targetId: number): boolean {
  // BFS from targetId: if we can reach taskId, adding taskId->targetId would be circular
  const visited = new Set<number>();
  const queue = [targetId];
  while (queue.length > 0) {
    const current = queue.shift()!;
    if (current === taskId) return true;
    if (visited.has(current)) continue;
    visited.add(current);
    const deps = db.prepare(
      "SELECT depends_on FROM task_dependencies WHERE task_id = ?"
    ).all(current) as { depends_on: number }[];
    for (const dep of deps) {
      queue.push(dep.depends_on);
    }
  }
  return false;
}

export function addDependencies(db: Database, taskId: number, dependsOn: number[]): void {
  const run = db.transaction(() => {
    for (const depId of dependsOn) {
      if (depId === taskId) throw new Error("Task cannot depend on itself");
      if (hasCircularDependency(db, taskId, depId)) {
        throw new Error(`Circular dependency: ${taskId} -> ${depId} creates a cycle`);
      }
      db.prepare(
        "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on) VALUES (?, ?)"
      ).run(taskId, depId);
    }
  });
  run();
}

export function removeDependency(db: Database, taskId: number, dependsOn: number): boolean {
  return db.prepare(
    "DELETE FROM task_dependencies WHERE task_id = ? AND depends_on = ?"
  ).run(taskId, dependsOn).changes > 0;
}

export function getDependencies(
  db: Database,
  taskId: number,
): { depends_on: number; status: string; title: string; agent: string }[] {
  return db.prepare(`
    SELECT td.depends_on, t.status, t.title, t.agent
    FROM task_dependencies td
    JOIN tasks t ON td.depends_on = t.id
    WHERE td.task_id = ?
    ORDER BY td.depends_on
  `).all(taskId) as { depends_on: number; status: string; title: string; agent: string }[];
}

export function getDependents(db: Database, taskId: number): number[] {
  return (db.prepare(
    "SELECT task_id FROM task_dependencies WHERE depends_on = ?"
  ).all(taskId) as { task_id: number }[]).map(r => r.task_id);
}

export function checkAndUnblock(db: Database, completedTaskId: number): Task[] {
  const dependents = getDependents(db, completedTaskId);
  const unblocked: Task[] = [];

  for (const depTaskId of dependents) {
    const deps = getDependencies(db, depTaskId);
    const allMet = deps.every(d => d.status === "completed");
    if (allMet) {
      const task = db.prepare(
        "UPDATE tasks SET status = 'active', updated_at = datetime('now') WHERE id = ? AND status = 'blocked' RETURNING *"
      ).get(depTaskId) as Task | undefined;
      if (task) {
        db.prepare(
          "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, 'active', 'Dependencies met, auto-unblocked')"
        ).run(task.id, task.agent);
        unblocked.push(task);
      }
    }
  }

  return unblocked;
}

// ============================================================================
// WORK QUEUE
// ============================================================================

export function enqueueTask(
  db: Database,
  data: { project: string; title: string; summary?: string; expected_output?: string; condition?: string },
): Task {
  const result = db.prepare(
    `INSERT INTO tasks (agent, project, title, summary, expected_output, status, assigned)
     VALUES ('unassigned', ?, ?, ?, ?, 'queued', 0) RETURNING *`
  ).get(
    data.project, data.title, data.summary ?? null, data.expected_output ?? null,
  ) as Task;

  db.prepare(
    "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, 'admin', 'queued', ?)"
  ).run(result.id, data.summary ?? null);

  emitEvent("tasks", "task.queued", { project: result.project, title: result.title, task_id: result.id });
  return result;
}

export function claimNextTask(db: Database, agent: string, project?: string): Task | undefined {
  const run = db.transaction(() => {
    let query = "SELECT * FROM tasks WHERE status = 'queued' AND assigned = 0";
    const params: (string | number)[] = [];
    if (project) { query += " AND project = ?"; params.push(project); }
    query += " ORDER BY created_at ASC LIMIT 1";

    const task = db.prepare(query).get(...params) as Task | undefined;
    if (!task) return undefined;

    const result = db.prepare(
      `UPDATE tasks SET agent = ?, assigned = 1, status = 'active',
       last_heartbeat = datetime('now'), updated_at = datetime('now')
       WHERE id = ? RETURNING *`
    ).get(agent, task.id) as Task;

    db.prepare(
      "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, 'active', 'Claimed from queue')"
    ).run(result.id, agent);

    return result;
  });

  const result = run();
  if (result) {
    emitEvent("tasks", "task.claimed", { agent, project: result.project, title: result.title, task_id: result.id });
  }
  return result;
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
