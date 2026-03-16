import { createServer, type ServerResponse } from "node:http";
import { createReadStream, existsSync } from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import { stat } from "node:fs/promises";
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { initDb } from "./db/schema.ts";
import { pruneTaskUpdates, lookupAgentKey, createAgentKey, listAgentKeys, revokeAgentKey } from "./db/queries.ts";
import { handleTaskRoutes } from "./routes/tasks.ts";

const DB_PATH = process.env.DB_PATH ?? "./chiasm.db";
const ADMIN_KEY = process.env.CHIASM_API_KEY ?? process.env.API_KEY;
const CORS_ALLOW_ORIGIN = process.env.CORS_ALLOW_ORIGIN;

function envInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

const PORT = envInt(process.env.PORT, 4300);
const BODY_MAX_BYTES = envInt(process.env.BODY_MAX_BYTES, 64 * 1024);
const TASKS_DEFAULT_LIMIT = envInt(process.env.TASKS_DEFAULT_LIMIT, 500);
const TASKS_MAX_LIMIT = envInt(process.env.TASKS_MAX_LIMIT, 1000);
const FEED_DEFAULT_LIMIT = envInt(process.env.FEED_DEFAULT_LIMIT, 50);
const FEED_MAX_LIMIT = envInt(process.env.FEED_MAX_LIMIT, 200);
const TASK_UPDATE_MAX_ROWS = envInt(process.env.TASK_UPDATE_MAX_ROWS, 5000);
const TASK_UPDATE_MAX_AGE_DAYS = envInt(process.env.TASK_UPDATE_MAX_AGE_DAYS, 30);

const FRONTEND_BUILD_DIR = resolve(fileURLToPath(new URL("../frontend/build/", import.meta.url)));
const FRONTEND_INDEX_FILE = resolve(FRONTEND_BUILD_DIR, "index.html");
const HAS_FRONTEND_BUILD = existsSync(FRONTEND_INDEX_FILE);

const MIME_TYPES: Record<string, string> = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".jpg": "image/jpeg",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml; charset=utf-8",
  ".txt": "text/plain; charset=utf-8",
  ".webp": "image/webp",
};

const db = initDb(DB_PATH);
pruneTaskUpdates(db, TASK_UPDATE_MAX_ROWS, TASK_UPDATE_MAX_AGE_DAYS);
setInterval(() => {
  pruneTaskUpdates(db, TASK_UPDATE_MAX_ROWS, TASK_UPDATE_MAX_AGE_DAYS);
}, 5 * 60 * 1000).unref();

// ============================================================================
// AUTH - per-agent keys with admin escalation
// ============================================================================

import type { AuthIdentity } from "./types.ts";
export type { AuthIdentity } from "./types.ts";

function hashKey(key: string): string {
  return createHash("sha256").update(key).digest("hex");
}

function resolveAuth(authHeader: string | undefined): AuthIdentity | null {
  // No auth configured = open access
  if (!ADMIN_KEY) return { role: "admin", agent: null };

  if (!authHeader?.startsWith("Bearer ")) return null;
  const token = authHeader.slice(7);

  // Check admin key first
  if (token === ADMIN_KEY) return { role: "admin", agent: null };

  // Check per-agent keys
  const keyRecord = lookupAgentKey(db, hashKey(token));
  if (keyRecord) return { role: "agent", agent: keyRecord.agent };

  return null;
}

// ============================================================================
// ROUTING HELPERS
// ============================================================================

function isApiRequest(pathname: string): boolean {
  return pathname === "/health" || pathname === "/tasks" || pathname === "/feed"
    || /^\/tasks\/\d+$/.test(pathname) || pathname.startsWith("/admin/");
}

function applyCors(reqOrigin: string | undefined, res: ServerResponse) {
  if (!CORS_ALLOW_ORIGIN) return;
  if (CORS_ALLOW_ORIGIN === "*" || reqOrigin === CORS_ALLOW_ORIGIN) {
    res.setHeader("Access-Control-Allow-Origin", CORS_ALLOW_ORIGIN === "*" ? "*" : reqOrigin ?? CORS_ALLOW_ORIGIN);
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.setHeader("Vary", "Origin");
  }
}

function jsonResponse(res: ServerResponse, data: unknown, status = 200) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

async function readJsonBody(req: import("node:http").IncomingMessage, maxBytes: number): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    let settled = false;
    const onData = (chunk: Buffer) => {
      if (settled) return;
      total += chunk.length;
      if (total > maxBytes) { settled = true; req.off("data", onData); req.off("end", onEnd); req.resume(); reject(new Error("Request body too large")); return; }
      chunks.push(chunk);
    };
    const onEnd = () => {
      if (settled) return;
      settled = true;
      if (chunks.length === 0) { resolve({}); return; }
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString());
        if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) { reject(new Error("Request body must be a JSON object")); return; }
        resolve(parsed as Record<string, unknown>);
      } catch { reject(new Error("Invalid JSON")); }
    };
    const onError = (err: Error) => { if (!settled) { settled = true; reject(err); } };
    req.on("data", onData);
    req.on("end", onEnd);
    req.on("error", onError);
  });
}

// ============================================================================
// ADMIN ROUTES - key management (admin-only)
// ============================================================================

async function handleAdminRoutes(
  req: import("node:http").IncomingMessage,
  res: ServerResponse,
  pathname: string,
  identity: AuthIdentity,
) {
  if (identity.role !== "admin") {
    return jsonResponse(res, { error: "Admin access required" }, 403);
  }

  // POST /admin/keys - create agent key
  if (pathname === "/admin/keys" && req.method === "POST") {
    try {
      const body = await readJsonBody(req, BODY_MAX_BYTES);
      const agent = body.agent;
      if (!agent || typeof agent !== "string") {
        return jsonResponse(res, { error: "agent (string) required" }, 400);
      }
      const rawKey = "mc_" + randomBytes(32).toString("hex");
      const record = createAgentKey(db, agent, hashKey(rawKey), rawKey.slice(0, 11));
      return jsonResponse(res, {
        id: record.id,
        agent: record.agent,
        key: rawKey,
        prefix: record.key_prefix,
        created_at: record.created_at,
        warning: "Store this key now. It cannot be retrieved again.",
      }, 201);
    } catch (err) {
      return jsonResponse(res, { error: err instanceof Error ? err.message : "Bad request" }, 400);
    }
  }

  // GET /admin/keys - list keys (no secrets)
  if (pathname === "/admin/keys" && req.method === "GET") {
    return jsonResponse(res, listAgentKeys(db));
  }

  // DELETE /admin/keys/:id - revoke key
  const keyMatch = pathname.match(/^\/admin\/keys\/(\d+)$/);
  if (keyMatch && req.method === "DELETE") {
    const revoked = revokeAgentKey(db, parseInt(keyMatch[1], 10));
    if (!revoked) return jsonResponse(res, { error: "Key not found" }, 404);
    return jsonResponse(res, { ok: true, revoked: true });
  }

  return jsonResponse(res, { error: "Not found" }, 404);
}

// ============================================================================
// STATIC FILE SERVING
// ============================================================================

function sendFile(res: ServerResponse, filePath: string, method: string) {
  const contentType = MIME_TYPES[extname(filePath).toLowerCase()] ?? "application/octet-stream";
  res.writeHead(200, { "Content-Type": contentType });
  if (method === "HEAD") { res.end(); return; }
  const stream = createReadStream(filePath);
  stream.on("error", () => { if (!res.writableEnded) res.end(); });
  stream.pipe(res);
}

async function serveFrontend(pathname: string, method: string, res: ServerResponse) {
  if (!HAS_FRONTEND_BUILD || (method !== "GET" && method !== "HEAD")) return false;
  const decodedPathname = decodeURIComponent(pathname);
  const requestedPath = decodedPathname === "/" ? "/index.html" : decodedPathname;
  const resolvedPath = resolve(FRONTEND_BUILD_DIR, `.${requestedPath}`);
  const buildDirWithSep = FRONTEND_BUILD_DIR.endsWith(sep) ? FRONTEND_BUILD_DIR : FRONTEND_BUILD_DIR + sep;
  if (resolvedPath !== FRONTEND_BUILD_DIR && !resolvedPath.startsWith(buildDirWithSep)) return false;
  try {
    const fileStat = await stat(resolvedPath);
    if (fileStat.isFile()) { sendFile(res, resolvedPath, method); return true; }
  } catch { /* fall through */ }
  if (extname(pathname)) return false;
  sendFile(res, FRONTEND_INDEX_FILE, method);
  return true;
}

// ============================================================================
// HTTP SERVER
// ============================================================================

const server = createServer(async (req, res) => {
  applyCors(req.headers.origin, res);

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  try {
    const url = new URL(req.url!, `http://${req.headers.host}`);
    const pathname = url.pathname;

    // Health check - always open
    if (pathname === "/health" && req.method === "GET") {
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ status: "ok", version: "0.2.0" }));
    }

    // Auth gate for all API requests
    if (isApiRequest(pathname)) {
      const identity = resolveAuth(req.headers.authorization);
      if (!identity) {
        res.writeHead(401, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: "Unauthorized" }));
      }

      // Admin routes
      if (pathname.startsWith("/admin/")) {
        return handleAdminRoutes(req, res, pathname, identity);
      }

      // Task routes - pass identity for agent enforcement
      handleTaskRoutes(db, req, res, pathname, {
        bodyMaxBytes: BODY_MAX_BYTES,
        tasksDefaultLimit: TASKS_DEFAULT_LIMIT,
        tasksMaxLimit: TASKS_MAX_LIMIT,
        feedDefaultLimit: FEED_DEFAULT_LIMIT,
        feedMaxLimit: FEED_MAX_LIMIT,
        taskUpdateMaxRows: TASK_UPDATE_MAX_ROWS,
        taskUpdateMaxAgeDays: TASK_UPDATE_MAX_AGE_DAYS,
      }, identity);
      return;
    }

    if (await serveFrontend(pathname, req.method ?? "GET", res)) return;

    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Not found" }));
  } catch (err) {
    console.error("Unhandled request error:", err);
    if (!res.headersSent) {
      res.writeHead(500, { "Content-Type": "application/json" });
    }
    res.end(JSON.stringify({ error: "Internal server error" }));
  }
});

server.listen(PORT, () => {
  console.log(`Chiasm running on http://localhost:${PORT}`);
  console.log(`Database: ${DB_PATH}`);
  console.log(`Auth: ${ADMIN_KEY ? "enabled (admin + per-agent keys)" : "disabled (set CHIASM_API_KEY to enable)"}`);
  console.log(`CORS: ${CORS_ALLOW_ORIGIN ?? "disabled (same-origin only)"}`);
  console.log(`Frontend: ${HAS_FRONTEND_BUILD ? `serving ${FRONTEND_BUILD_DIR}` : "build not found"}`);
});
