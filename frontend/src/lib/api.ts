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
const API_KEY_STORAGE_KEY = "chiasm-api-key";

function authHeaders(init?: HeadersInit): HeadersInit {
  const headers = new Headers(init);
  const apiKey = getApiKey();
  if (apiKey) {
    headers.set("Authorization", `Bearer ${apiKey}`);
  }
  return headers;
}

export function getApiKey(): string {
  if (typeof window === "undefined") return "";
  return window.sessionStorage.getItem(API_KEY_STORAGE_KEY) ?? "";
}

export function setApiKey(apiKey: string) {
  if (typeof window === "undefined") return;
  window.sessionStorage.setItem(API_KEY_STORAGE_KEY, apiKey.trim());
}

export function clearApiKey() {
  if (typeof window === "undefined") return;
  window.sessionStorage.removeItem(API_KEY_STORAGE_KEY);
}

export function isUnauthorizedError(err: unknown): boolean {
  return err instanceof Error && err.message.startsWith("401:");
}

async function apiFetch<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: authHeaders(init?.headers),
  });
  if (!res.ok) {
    const body = await res.text();
    let message: string;
    try {
      message = JSON.parse(body).error ?? body;
    } catch {
      message = body;
    }
    throw new Error(`${res.status}: ${message}`);
  }
  return res.json();
}

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
  return apiFetch<Task[]>(`${BASE}/tasks${query ? `?${query}` : ""}`);
}

export async function fetchFeed(limit = 50): Promise<FeedEntry[]> {
  return apiFetch<FeedEntry[]>(`${BASE}/feed?limit=${limit}`);
}

export async function createTask(data: {
  agent: string;
  project: string;
  title: string;
  summary?: string;
}): Promise<Task> {
  return apiFetch<Task>(`${BASE}/tasks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
}

export async function updateTask(
  id: number,
  data: { status?: string; summary?: string }
): Promise<Task> {
  return apiFetch<Task>(`${BASE}/tasks/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
}

export async function deleteTask(id: number): Promise<void> {
  const res = await fetch(`${BASE}/tasks/${id}`, {
    method: "DELETE",
    headers: authHeaders(),
  });
  if (!res.ok) {
    const body = await res.text();
    let message: string;
    try {
      message = JSON.parse(body).error ?? body;
    } catch {
      message = body;
    }
    throw new Error(`${res.status}: ${message}`);
  }
}
