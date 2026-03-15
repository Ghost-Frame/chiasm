import type DatabaseConstructor from "libsql";
type Database = InstanceType<typeof DatabaseConstructor>;
import type { IncomingMessage, ServerResponse } from "node:http";
import { listTasks, createTask, updateTask, deleteTask, getTask, getFeed } from "../db/queries.ts";

function json(res: ServerResponse, data: unknown, status = 200) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

function error(res: ServerResponse, message: string, status = 400) {
  json(res, { error: message }, status);
}

async function readBody(req: IncomingMessage): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString()));
      } catch {
        reject(new Error("Invalid JSON"));
      }
    });
    req.on("error", reject);
  });
}

export function handleTaskRoutes(db: Database, req: IncomingMessage, res: ServerResponse, pathname: string) {
  const url = new URL(req.url!, `http://${req.headers.host}`);

  // GET /tasks - list tasks with optional filters
  if (pathname === "/tasks" && req.method === "GET") {
    const filters = {
      agent: url.searchParams.get("agent") ?? undefined,
      project: url.searchParams.get("project") ?? undefined,
      status: url.searchParams.get("status") ?? undefined,
    };
    return json(res, listTasks(db, filters));
  }

  // POST /tasks - create a new task
  if (pathname === "/tasks" && req.method === "POST") {
    return readBody(req).then((body) => {
      const { agent, project, title, summary } = body as {
        agent?: string;
        project?: string;
        title?: string;
        summary?: string;
      };

      if (!agent || !project || !title) {
        return error(res, "agent, project, and title are required");
      }

      const task = createTask(db, { agent, project, title, summary });
      return json(res, task, 201);
    }).catch(() => error(res, "Invalid request body"));
  }

  // GET /tasks/:id - get a single task
  const taskMatch = pathname.match(/^\/tasks\/(\d+)$/);
  if (taskMatch && req.method === "GET") {
    const task = getTask(db, parseInt(taskMatch[1]));
    if (!task) return error(res, "Task not found", 404);
    return json(res, task);
  }

  // PATCH /tasks/:id - update a task
  if (taskMatch && req.method === "PATCH") {
    return readBody(req).then((body) => {
      const task = updateTask(db, parseInt(taskMatch[1]), body as { status?: string; summary?: string; agent?: string });
      if (!task) return error(res, "Task not found", 404);
      return json(res, task);
    }).catch(() => error(res, "Invalid request body"));
  }

  // DELETE /tasks/:id - delete a task
  if (taskMatch && req.method === "DELETE") {
    const deleted = deleteTask(db, parseInt(taskMatch[1]));
    if (!deleted) return error(res, "Task not found", 404);
    return json(res, { ok: true });
  }

  // GET /feed - activity feed
  if (pathname === "/feed" && req.method === "GET") {
    const limit = parseInt(url.searchParams.get("limit") ?? "50");
    return json(res, getFeed(db, limit));
  }

  error(res, "Not found", 404);
}
