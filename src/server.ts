import { createServer, type ServerResponse } from "node:http";
import { createReadStream, existsSync } from "node:fs";
import { stat } from "node:fs/promises";
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { initDb } from "./db/schema.ts";
import { pruneTaskUpdates } from "./db/queries.ts";
import { handleTaskRoutes } from "./routes/tasks.ts";

const DB_PATH = process.env.DB_PATH ?? "./mission-control.db";
const API_KEY = process.env.MISSION_CONTROL_API_KEY ?? process.env.API_KEY;
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

function isApiRequest(pathname: string): boolean {
  return pathname === "/health" || pathname === "/tasks" || pathname === "/feed" || /^\/tasks\/\d+$/.test(pathname);
}

function applyCors(reqOrigin: string | undefined, res: ServerResponse) {
  if (!CORS_ALLOW_ORIGIN) {
    return;
  }

  if (CORS_ALLOW_ORIGIN === "*" || reqOrigin === CORS_ALLOW_ORIGIN) {
    res.setHeader("Access-Control-Allow-Origin", CORS_ALLOW_ORIGIN === "*" ? "*" : reqOrigin ?? CORS_ALLOW_ORIGIN);
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.setHeader("Vary", "Origin");
  }
}

function isAuthorized(authHeader: string | undefined): boolean {
  if (!API_KEY) {
    return true;
  }

  return authHeader === `Bearer ${API_KEY}`;
}

function sendFile(res: ServerResponse, filePath: string, method: string) {
  const contentType = MIME_TYPES[extname(filePath).toLowerCase()] ?? "application/octet-stream";
  res.writeHead(200, { "Content-Type": contentType });

  if (method === "HEAD") {
    res.end();
    return;
  }

  const stream = createReadStream(filePath);
  stream.on("error", () => {
    if (!res.writableEnded) res.end();
  });
  stream.pipe(res);
}

async function serveFrontend(pathname: string, method: string, res: ServerResponse) {
  if (!HAS_FRONTEND_BUILD || (method !== "GET" && method !== "HEAD")) {
    return false;
  }

  const decodedPathname = decodeURIComponent(pathname);
  const requestedPath = decodedPathname === "/" ? "/index.html" : decodedPathname;
  const resolvedPath = resolve(FRONTEND_BUILD_DIR, `.${requestedPath}`);

  const buildDirWithSep = FRONTEND_BUILD_DIR.endsWith(sep) ? FRONTEND_BUILD_DIR : FRONTEND_BUILD_DIR + sep;
  if (resolvedPath !== FRONTEND_BUILD_DIR && !resolvedPath.startsWith(buildDirWithSep)) {
    return false;
  }

  try {
    const fileStat = await stat(resolvedPath);
    if (fileStat.isFile()) {
      sendFile(res, resolvedPath, method);
      return true;
    }
  } catch {
    // Fall through to SPA fallback.
  }

  if (extname(pathname)) {
    return false;
  }

  sendFile(res, FRONTEND_INDEX_FILE, method);
  return true;
}

const server = createServer(async (req, res) => {
  applyCors(req.headers.origin, res);

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  try {
    const url = new URL(req.url!, `http://${req.headers.host}`);
    const pathname = url.pathname;

    if (pathname === "/health" && req.method === "GET") {
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ status: "ok", version: "0.1.0" }));
    }

    if (isApiRequest(pathname) && API_KEY) {
      if (!isAuthorized(req.headers.authorization)) {
        res.writeHead(401, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: "Unauthorized" }));
      }
    }

    if (isApiRequest(pathname)) {
      handleTaskRoutes(db, req, res, pathname, {
        bodyMaxBytes: BODY_MAX_BYTES,
        tasksDefaultLimit: TASKS_DEFAULT_LIMIT,
        tasksMaxLimit: TASKS_MAX_LIMIT,
        feedDefaultLimit: FEED_DEFAULT_LIMIT,
        feedMaxLimit: FEED_MAX_LIMIT,
        taskUpdateMaxRows: TASK_UPDATE_MAX_ROWS,
        taskUpdateMaxAgeDays: TASK_UPDATE_MAX_AGE_DAYS,
      });
      return;
    }

    if (await serveFrontend(pathname, req.method ?? "GET", res)) {
      return;
    }

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
  console.log(`Mission Control running on http://localhost:${PORT}`);
  console.log(`Database: ${DB_PATH}`);
  console.log(`Auth: ${API_KEY ? "enabled" : "disabled (set API_KEY to enable)"}`);
  console.log(`CORS: ${CORS_ALLOW_ORIGIN ?? "disabled (same-origin only)"}`);
  console.log(`Frontend: ${HAS_FRONTEND_BUILD ? `serving ${FRONTEND_BUILD_DIR}` : "build not found"}`);
});
