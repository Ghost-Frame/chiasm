import type DatabaseConstructor from "libsql";
type Database = InstanceType<typeof DatabaseConstructor>;
import type { IncomingMessage, ServerResponse } from "node:http";
import type { AuthIdentity } from "../types.ts";
import {
  listTasks,
  createTask,
  updateTask,
  deleteTask,
  getTask,
  getFeed,
  submitOutput,
  submitFeedback,
} from "../db/queries.ts";

export interface RouteOptions {
  bodyMaxBytes: number;
  tasksDefaultLimit: number;
  tasksMaxLimit: number;
  feedDefaultLimit: number;
  feedMaxLimit: number;
  taskUpdateMaxRows: number;
  taskUpdateMaxAgeDays: number;
  // Engram URL for AI planning
  engramUrl?: string;
  engramApiKey?: string;
}

const DEFAULT_ROUTE_OPTIONS: RouteOptions = {
  bodyMaxBytes: 64 * 1024,
  tasksDefaultLimit: 500,
  tasksMaxLimit: 1000,
  feedDefaultLimit: 50,
  feedMaxLimit: 200,
  taskUpdateMaxRows: 5000,
  taskUpdateMaxAgeDays: 30,
};

const VALID_STATUSES = new Set(["active", "paused", "blocked", "completed", "blocked_on_human"]);

function json(res: ServerResponse, data: unknown, status = 200) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

function error(res: ServerResponse, message: string, status = 400) {
  json(res, { error: message }, status);
}

async function readBody(req: IncomingMessage, maxBytes: number): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let totalBytes = 0;
    let settled = false;

    const onData = (chunk: Buffer) => {
      if (settled) return;
      totalBytes += chunk.length;
      if (totalBytes > maxBytes) {
        settled = true;
        req.off("data", onData);
        req.off("end", onEnd);
        req.off("error", onError);
        req.resume();
        reject(new Error("Request body too large"));
        return;
      }
      chunks.push(chunk);
    };

    const onEnd = () => {
      if (settled) return;
      settled = true;
      if (chunks.length === 0) { resolve({}); return; }
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString());
        if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
          reject(new Error("Request body must be a JSON object"));
          return;
        }
        resolve(parsed as Record<string, unknown>);
      } catch {
        reject(new Error("Invalid JSON"));
      }
    };

    const onError = (err: Error) => { if (settled) return; settled = true; reject(err); };

    req.on("data", onData);
    req.on("end", onEnd);
    req.on("error", onError);
  });
}

function parseBoundedInt(value: string | null, fallback: number, min: number, max: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function requestErrorStatus(err: unknown): number {
  return err instanceof Error && err.message === "Request body too large" ? 413 : 400;
}

function canActOnAgent(identity: AuthIdentity, taskAgent: string): boolean {
  if (identity.role === "admin") return true;
  return identity.agent === taskAgent;
}

// Generate an execution plan via Engram LLM
async function generatePlan(task: ReturnType<typeof getTask>, opts: RouteOptions): Promise<string> {
  if (!opts.engramUrl || !task) throw new Error("Engram URL not configured");

  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (opts.engramApiKey) headers["Authorization"] = `Bearer ${opts.engramApiKey}`;

  const prompt = `Create a concise step-by-step execution plan for this task:

Title: ${task.title}
Project: ${task.project}
Agent: ${task.agent}
${task.expected_output ? `Expected output: ${task.expected_output}` : ""}
${task.summary ? `Context: ${task.summary}` : ""}

Respond with a numbered list of concrete steps. Be specific and actionable.`;

  const res = await fetch(`${opts.engramUrl}/llm`, {
    method: "POST",
    headers,
    body: JSON.stringify({ prompt, system: "You are a precise task planner. Return only a numbered list of steps." }),
    signal: AbortSignal.timeout(30000),
  });

  if (!res.ok) throw new Error(`Engram LLM HTTP ${res.status}`);
  const data = await res.json() as any;
  return data.result ?? data.text ?? data.content ?? JSON.stringify(data);
}

export function handleTaskRoutes(
  db: Database,
  req: IncomingMessage,
  res: ServerResponse,
  pathname: string,
  options: RouteOptions = DEFAULT_ROUTE_OPTIONS,
  identity?: AuthIdentity,
) {
  const url = new URL(req.url!, `http://${req.headers.host}`);
  const auth: AuthIdentity = identity ?? { role: "admin", agent: null };

  // GET /tasks
  if (pathname === "/tasks" && req.method === "GET") {
    return json(res, listTasks(db, {
      agent: url.searchParams.get("agent") ?? undefined,
      project: url.searchParams.get("project") ?? undefined,
      status: url.searchParams.get("status") ?? undefined,
      limit: parseBoundedInt(url.searchParams.get("limit"), options.tasksDefaultLimit, 1, options.tasksMaxLimit),
      offset: parseBoundedInt(url.searchParams.get("offset"), 0, 0, Number.MAX_SAFE_INTEGER),
    }));
  }

  // POST /tasks
  if (pathname === "/tasks" && req.method === "POST") {
    return readBody(req, options.bodyMaxBytes).then((body) => {
      const {
        agent, project, title, summary,
        expected_output, output_format, condition, guardrail_url,
      } = body as {
        agent?: string; project?: string; title?: string; summary?: string;
        expected_output?: string; output_format?: string; condition?: string; guardrail_url?: string;
      };

      if (!agent || !project || !title) return error(res, "agent, project, and title are required");
      if (typeof agent !== "string" || typeof project !== "string" || typeof title !== "string") {
        return error(res, "agent, project, and title must be strings");
      }
      if (!canActOnAgent(auth, agent)) {
        return error(res, `Agent key for "${auth.agent}" cannot create tasks for "${agent}"`, 403);
      }

      const task = createTask(db, { agent, project, title, summary, expected_output, output_format, condition, guardrail_url });
      return json(res, task, 201);
    }).catch((err) => error(res, err instanceof Error ? err.message : "Invalid request body", requestErrorStatus(err)));
  }

  const taskMatch = pathname.match(/^\/tasks\/(\d+)$/);
  const taskId = taskMatch ? parseInt(taskMatch[1], 10) : null;

  // GET /tasks/:id
  if (taskMatch && req.method === "GET") {
    const task = getTask(db, taskId!);
    if (!task) return error(res, "Task not found", 404);
    return json(res, task);
  }

  // PATCH /tasks/:id
  if (taskMatch && req.method === "PATCH") {
    return readBody(req, options.bodyMaxBytes).then((body) => {
      const existing = getTask(db, taskId!);
      if (!existing) return error(res, "Task not found", 404);
      if (!canActOnAgent(auth, existing.agent)) {
        return error(res, `Agent key for "${auth.agent}" cannot update tasks owned by "${existing.agent}"`, 403);
      }

      const bodyData = body as { status?: unknown; summary?: unknown; agent?: unknown; plan?: unknown; feedback?: unknown };
      if (bodyData.agent !== undefined) return error(res, "agent cannot be updated");

      if (typeof bodyData.status === "string" && !VALID_STATUSES.has(bodyData.status)) {
        return error(res, `Invalid status. Must be one of: ${[...VALID_STATUSES].join(", ")}`);
      }

      const task = updateTask(db, taskId!, bodyData as { status?: string; summary?: string; plan?: string; feedback?: string });
      if (!task) return error(res, "Task not found", 404);
      return json(res, task);
    }).catch((err) => error(res, err instanceof Error ? err.message : "Invalid request body", requestErrorStatus(err)));
  }

  // DELETE /tasks/:id
  if (taskMatch && req.method === "DELETE") {
    const existing = getTask(db, taskId!);
    if (!existing) return error(res, "Task not found", 404);
    if (!canActOnAgent(auth, existing.agent)) {
      return error(res, `Agent key for "${auth.agent}" cannot delete tasks owned by "${existing.agent}"`, 403);
    }
    if (!deleteTask(db, taskId!)) return error(res, "Task not found", 404);
    return json(res, { ok: true });
  }

  // POST /tasks/:id/output — submit task output, triggers guardrail if configured
  const outputMatch = pathname.match(/^\/tasks\/(\d+)\/output$/);
  if (outputMatch && req.method === "POST") {
    return readBody(req, options.bodyMaxBytes).then((body) => {
      const id = parseInt(outputMatch[1], 10);
      const existing = getTask(db, id);
      if (!existing) return error(res, "Task not found", 404);
      if (!canActOnAgent(auth, existing.agent)) {
        return error(res, `Agent key for "${auth.agent}" cannot submit output for tasks owned by "${existing.agent}"`, 403);
      }

      const output = body.output;
      if (output === undefined) return error(res, "output is required");
      const outputStr = typeof output === "string" ? output : JSON.stringify(output);

      const task = submitOutput(db, id, outputStr);
      return json(res, task);
    }).catch((err) => error(res, err instanceof Error ? err.message : "Invalid request body", requestErrorStatus(err)));
  }

  // POST /tasks/:id/feedback — human feedback, unblocks blocked_on_human
  const feedbackMatch = pathname.match(/^\/tasks\/(\d+)\/feedback$/);
  if (feedbackMatch && req.method === "POST") {
    return readBody(req, options.bodyMaxBytes).then((body) => {
      const id = parseInt(feedbackMatch[1], 10);
      const existing = getTask(db, id);
      if (!existing) return error(res, "Task not found", 404);

      const feedback = body.feedback;
      if (!feedback || typeof feedback !== "string") return error(res, "feedback string is required");

      const task = submitFeedback(db, id, feedback);
      return json(res, task);
    }).catch((err) => error(res, err instanceof Error ? err.message : "Invalid request body", requestErrorStatus(err)));
  }

  // POST /tasks/:id/plan — generate execution plan via Engram LLM
  const planMatch = pathname.match(/^\/tasks\/(\d+)\/plan$/);
  if (planMatch && req.method === "POST") {
    const id = parseInt(planMatch[1], 10);
    const existing = getTask(db, id);
    if (!existing) return error(res, "Task not found", 404);
    if (!canActOnAgent(auth, existing.agent)) {
      return error(res, `Agent key for "${auth.agent}" cannot plan tasks owned by "${existing.agent}"`, 403);
    }

    return generatePlan(existing, options)
      .then((plan) => {
        const task = updateTask(db, id, { plan });
        return json(res, task);
      })
      .catch((err: any) => error(res, `Plan generation failed: ${err.message}`));
  }

  // GET /feed
  if (pathname === "/feed" && req.method === "GET") {
    return json(res, getFeed(db,
      parseBoundedInt(url.searchParams.get("limit"), options.feedDefaultLimit, 1, options.feedMaxLimit),
      parseBoundedInt(url.searchParams.get("offset"), 0, 0, Number.MAX_SAFE_INTEGER),
    ));
  }

  error(res, "Not found", 404);
}
