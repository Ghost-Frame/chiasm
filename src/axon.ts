// Fire-and-forget Axon event emission
const AXON_URL = process.env.AXON_URL || "";
const AXON_API_KEY = process.env.AXON_API_KEY || "";

export function emitEvent(channel: string, type: string, payload: Record<string, unknown>): void {
  if (!AXON_URL) return;
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (AXON_API_KEY) headers["Authorization"] = `Bearer ${AXON_API_KEY}`;
  fetch(`${AXON_URL}/publish`, {
    method: "POST",
    headers,
    body: JSON.stringify({ channel, type, source: "chiasm", payload }),
    signal: AbortSignal.timeout(5000),
  }).catch(() => {});
}
