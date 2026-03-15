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

export interface FeedEntry {
  id: number;
  task_id: number;
  agent: string;
  status: string;
  summary: string | null;
  created_at: string;
  project: string;
  title: string;
}

const BASE = "";

export async function fetchTasks(filters?: {
  agent?: string;
  project?: string;
  status?: string;
}): Promise<Task[]> {
  const params = new URLSearchParams();
  if (filters?.agent) params.set("agent", filters.agent);
  if (filters?.project) params.set("project", filters.project);
  if (filters?.status) params.set("status", filters.status);
  const query = params.toString();
  const res = await fetch(`${BASE}/tasks${query ? `?${query}` : ""}`);
  return res.json();
}

export async function fetchFeed(limit = 50): Promise<FeedEntry[]> {
  const res = await fetch(`${BASE}/feed?limit=${limit}`);
  return res.json();
}

export async function createTask(data: {
  agent: string;
  project: string;
  title: string;
  summary?: string;
}): Promise<Task> {
  const res = await fetch(`${BASE}/tasks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  return res.json();
}

export async function updateTask(
  id: number,
  data: { status?: string; summary?: string }
): Promise<Task> {
  const res = await fetch(`${BASE}/tasks/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  return res.json();
}

export async function deleteTask(id: number): Promise<void> {
  await fetch(`${BASE}/tasks/${id}`, { method: "DELETE" });
}
