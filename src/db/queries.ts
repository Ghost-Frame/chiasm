import type DatabaseConstructor from "libsql";
type Database = InstanceType<typeof DatabaseConstructor>;

export interface Task {
  id: number;
  agent: string;
  project: string;
  title: string;
  status: string;
  summary: string | null;
  created_at: string;
  updated_at: string;
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

  if (filters.agent) {
    query += " AND agent = ?";
    params.push(filters.agent);
  }
  if (filters.project) {
    query += " AND project = ?";
    params.push(filters.project);
  }
  if (filters.status) {
    query += " AND status = ?";
    params.push(filters.status);
  }

  query += " ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?";
  params.push(filters.limit ?? 500, filters.offset ?? 0);

  return db.prepare(query).all(...params) as Task[];
}

export function getTask(db: Database, id: number): Task | undefined {
  return db.prepare("SELECT * FROM tasks WHERE id = ?").get(id) as Task | undefined;
}

export function createTask(
  db: Database,
  data: { agent: string; project: string; title: string; summary?: string }
): Task {
  const run = db.transaction(() => {
    const result = db.prepare(
      "INSERT INTO tasks (agent, project, title, summary) VALUES (?, ?, ?, ?) RETURNING *"
    ).get(data.agent, data.project, data.title, data.summary ?? null) as Task;

    // Log the creation in the feed
    db.prepare(
      "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, 'active', ?)"
    ).run(result.id, data.agent, data.summary ?? null);

    return result;
  });

  return run();
}

export function updateTask(
  db: Database,
  id: number,
  data: { status?: string; summary?: string }
): Task | undefined {
  const existing = getTask(db, id);
  if (!existing) return undefined;

  const status = data.status ?? existing.status;
  const summary = data.summary ?? existing.summary;

  const run = db.transaction(() => {
    const result = db.prepare(
      "UPDATE tasks SET status = ?, summary = ?, updated_at = datetime('now') WHERE id = ? RETURNING *"
    ).get(status, summary, id) as Task;

    // Log the update in the feed
    db.prepare(
      "INSERT INTO task_updates (task_id, agent, status, summary) VALUES (?, ?, ?, ?)"
    ).run(id, existing.agent, status, summary);

    return result;
  });

  return run();
}

export function deleteTask(db: Database, id: number): boolean {
  const result = db.prepare("DELETE FROM tasks WHERE id = ?").run(id);
  return result.changes > 0;
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

export function pruneTaskUpdates(db: Database, maxRows: number, maxAgeDays: number) {
  if (maxAgeDays > 0) {
    db.prepare("DELETE FROM task_updates WHERE created_at < datetime('now', ?)").run(`-${maxAgeDays} days`);
  }

  if (maxRows > 0) {
    db.prepare(`
      DELETE FROM task_updates
      WHERE id IN (
        SELECT id FROM (
          SELECT id
          FROM task_updates
          ORDER BY created_at DESC, id DESC
          LIMIT -1 OFFSET ?
        )
      )
    `).run(maxRows);
  }
}
