export interface AuthIdentity {
  role: "admin" | "agent";
  agent: string | null;
}
