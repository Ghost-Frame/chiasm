import type DatabaseConstructor from "libsql";
type Database = InstanceType<typeof DatabaseConstructor>;
import type { IncomingMessage, ServerResponse } from "node:http";
import {
  listTasks,
  createTask,
  updateTask,
  deleteTask,
  getTask,
  getFeed,
} from "../db/queries.ts";

export interface RouteOptions {
  bodyMaxBytes: number;
  tasksDefaultLimit: number;
  tasksMaxLimit: number;
  feedDefaultLimit: number;
  feedMaxLimit: number;
  taskUpdateMaxRows: number;
  taskUpdateMaxAgeDays: number;
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

const VALID_STATUSES = new Set(["active", "paused", "blocked", "completed"]);

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

      if (chunks.length === 0) {
        resolve({});
        return;
      }

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

    const onError = (err: Error) => {
      if (settled) return;
      settled = true;
      reject(err);
    };

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

export function handleTaskRoutes(
  db: Database,
  req: IncomingMessage,
  res: ServerResponse,
  pathname: string,
  options: RouteOptions = DEFAULT_ROUTE_OPTIONS
) {
  const url = new URL(req.url!, `http://${req.headers.host}`);

  // GET /tasks - list tasks with optional filters
  if (pathname === "/tasks" && req.method === "GET") {
    const filters = {
      agent: url.searchParams.get("agent") ?? undefined,
      project: url.searchParams.get("project") ?? undefined,
      status: url.searchParams.get("status") ?? undefined,
      limit: parseBoundedInt(url.searchParams.get("limit"), options.tasksDefaultLimit, 1, options.tasksMaxLimit),
      offset: parseBoundedInt(url.searchParams.get("offset"), 0, 0, Number.MAX_SAFE_INTEGER),
    };
    return json(res, listTasks(db, filters));
  }

  // POST /tasks - create a new task
  if (pathname === "/tasks" && req.method === "POST") {
    return readBody(req, options.bodyMaxBytes).then((body) => {
      const { agent, project, title, summary } = body as {
        agent?: string;
        project?: string;
        title?: string;
        summary?: string;
      };

      if (!agent || !project || !title) {
        return error(res, "agent, project, and title are required");
      }
      if (typeof agent !== "string" || typeof project !== "string" || typeof title !== "string") {
        return error(res, "agent, project, and title must be strings");
      }
      if (summary !== undefined && typeof summary !== "string") {
        return error(res, "summary must be a string");
      }

      const task = createTask(db, { agent, project, title, summary });
      return json(res, task, 201);
    }).catch((err) => error(res, err instanceof Error ? err.message : "Invalid request body", requestErrorStatus(err)));
  }

  // GET /tasks/:id - get a single task
  const taskMatch = pathname.match(/^\/tasks\/(\d+)$/);
  if (taskMatch && req.method === "GET") {
    const task = getTask(db, parseInt(taskMatch[1], 10));
    if (!task) return error(res, "Task not found", 404);
    return json(res, task);
  }

  // PATCH /tasks/:id - update a task
  if (taskMatch && req.method === "PATCH") {
    return readBody(req, options.bodyMaxBytes).then((body) => {
      const bodyData = body as { status?: unknown; summary?: unknown; agent?: unknown };
      if (bodyData.agent !== undefined) {
        return error(res, "agent cannot be updated", 400);
      }

      if (
        (bodyData.status !== undefined && typeof bodyData.status !== "string") ||
        (bodyData.summary !== undefined && typeof bodyData.summary !== "string")
      ) {
        return error(res, "status and summary must be strings if provided");
      }

      if (typeof bodyData.status === "string" && !VALID_STATUSES.has(bodyData.status)) {
        return error(res, `Invalid status. Must be one of: ${[...VALID_STATUSES].join(", ")}`);
      }

      const task = updateTask(db, parseInt(taskMatch[1], 10), bodyData as { status?: string; summary?: string });
      if (!task) return error(res, "Task not found", 404);
      return json(res, task);
    }).catch((err) => error(res, err instanceof Error ? err.message : "Invalid request body", requestErrorStatus(err)));
  }

  // DELETE /tasks/:id - delete a task
  if (taskMatch && req.method === "DELETE") {
    const deleted = deleteTask(db, parseInt(taskMatch[1], 10));
    if (!deleted) return error(res, "Task not found", 404);
    return json(res, { ok: true });
  }

  // GET /feed - activity feed
  if (pathname === "/feed" && req.method === "GET") {
    const limit = parseBoundedInt(url.searchParams.get("limit"), options.feedDefaultLimit, 1, options.feedMaxLimit);
    const offset = parseBoundedInt(url.searchParams.get("offset"), 0, 0, Number.MAX_SAFE_INTEGER);
    return json(res, getFeed(db, limit, offset));
  }

  error(res, "Not found", 404);
}
