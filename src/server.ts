import { createServer } from "node:http";
import { initDb } from "./db/schema.ts";
import { handleTaskRoutes } from "./routes/tasks.ts";

const PORT = parseInt(process.env.PORT ?? "4300");
const DB_PATH = process.env.DB_PATH ?? "./mission-control.db";
const API_KEY = process.env.API_KEY;

const db = initDb(DB_PATH);

const server = createServer((req, res) => {
  // CORS headers
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  // API key auth (optional - only enforced if API_KEY is set)
  if (API_KEY) {
    const auth = req.headers.authorization;
    if (auth !== `Bearer ${API_KEY}`) {
      res.writeHead(401, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "Unauthorized" }));
    }
  }

  const url = new URL(req.url!, `http://${req.headers.host}`);
  const pathname = url.pathname;

  // Health check
  if (pathname === "/health" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ status: "ok", version: "0.1.0" }));
  }

  // Task and feed routes
  handleTaskRoutes(db, req, res, pathname);
});

server.listen(PORT, () => {
  console.log(`Mission Control running on http://localhost:${PORT}`);
  console.log(`Database: ${DB_PATH}`);
  console.log(`Auth: ${API_KEY ? "enabled" : "disabled (set API_KEY to enable)"}`);
});
