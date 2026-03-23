import Database from "libsql";

export function initDb(dbPath: string): Database {
  const db = new Database(dbPath);

  db.exec("PRAGMA foreign_keys = ON");

  db.exec(`
    CREATE TABLE IF NOT EXISTS tasks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      agent TEXT NOT NULL,
      project TEXT NOT NULL,
      title TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      summary TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS task_updates (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
      agent TEXT NOT NULL,
      status TEXT NOT NULL,
      summary TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
    CREATE INDEX IF NOT EXISTS idx_tasks_agent ON tasks(agent);
    CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(project);
    CREATE INDEX IF NOT EXISTS idx_task_updates_task_id ON task_updates(task_id);

    CREATE TABLE IF NOT EXISTS agent_keys (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      agent TEXT NOT NULL,
      key_hash TEXT NOT NULL UNIQUE,
      key_prefix TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      last_used_at TEXT,
      revoked INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_agent_keys_hash ON agent_keys(key_hash);

    CREATE TABLE IF NOT EXISTS path_claims (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
      agent TEXT NOT NULL,
      project TEXT NOT NULL,
      path TEXT NOT NULL,
      claimed_at TEXT NOT NULL DEFAULT (datetime('now')),
      expires_at TEXT NOT NULL,
      released INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_path_claims_project_path ON path_claims(project, path);
    CREATE INDEX IF NOT EXISTS idx_path_claims_task_id ON path_claims(task_id);
    CREATE INDEX IF NOT EXISTS idx_path_claims_expires ON path_claims(expires_at);

    CREATE TABLE IF NOT EXISTS task_dependencies (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
      depends_on INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(task_id, depends_on)
    );
    CREATE INDEX IF NOT EXISTS idx_task_deps_task ON task_dependencies(task_id);
    CREATE INDEX IF NOT EXISTS idx_task_deps_depends ON task_dependencies(depends_on);
  `);

  // Migrations: add new columns if they don't exist yet
  const migrations = [
    "ALTER TABLE tasks ADD COLUMN expected_output TEXT",
    "ALTER TABLE tasks ADD COLUMN output_format TEXT NOT NULL DEFAULT 'raw'",
    "ALTER TABLE tasks ADD COLUMN output TEXT",
    "ALTER TABLE tasks ADD COLUMN condition TEXT",
    "ALTER TABLE tasks ADD COLUMN guardrail_url TEXT",
    "ALTER TABLE tasks ADD COLUMN guardrail_retries INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE tasks ADD COLUMN plan TEXT",
    "ALTER TABLE tasks ADD COLUMN feedback TEXT",
    // Coordination features
    "ALTER TABLE tasks ADD COLUMN last_heartbeat TEXT",
    "ALTER TABLE tasks ADD COLUMN heartbeat_interval INTEGER NOT NULL DEFAULT 300",
    "ALTER TABLE tasks ADD COLUMN assigned INTEGER NOT NULL DEFAULT 1",
  ];
  for (const stmt of migrations) {
    try { db.exec(stmt); } catch { /* column already exists */ }
  }

  return db;
}
