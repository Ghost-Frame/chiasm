import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { initDb } from "../src/db/schema.ts";
import { createTask, markStaleTasks, getTask } from "../src/db/queries.ts";

// ---------------------------------------------------------------------------
// markStaleTasks: heartbeat-overdue and never-heartbeated idle detection
// ---------------------------------------------------------------------------
// These exercise src/db/queries.ts directly against an in-memory libsql db
// (no HTTP server needed) so we can backdate last_heartbeat/updated_at with
// raw SQL, mirroring the Rust reference tests in
// kleos-lib/src/services/chiasm/heartbeat.rs.

describe("markStaleTasks", () => {
  it("stales a task whose heartbeat is overdue", () => {
    const db = initDb(":memory:");
    const task = createTask(db, { agent: "a", project: "p", title: "overdue-hb" });

    // heartbeat_interval defaults to 300s. Backdate last_heartbeat well past
    // 300 * 2 = 600s so it is clearly overdue.
    db.prepare(
      "UPDATE tasks SET last_heartbeat = datetime('now', '-1200 seconds') WHERE id = ?"
    ).run(task.id);

    // Note: markStaleTasks returns the pre-update row snapshot (selected
    // before the UPDATE runs), so its `status` field is still the old value.
    // The persisted status change is verified via getTask below.
    const stale = markStaleTasks(db, 2, 3600);
    assert.equal(stale.length, 1);
    assert.equal(stale[0].id, task.id);
    assert.equal(getTask(db, task.id)!.status, "stale");
  });

  it("stales a never-heartbeated task once it goes idle past the threshold", () => {
    const db = initDb(":memory:");
    const task = createTask(db, { agent: "a", project: "p", title: "idle-no-hb" });
    assert.equal(task.last_heartbeat, null);

    // Backdate updated_at to 2 hours ago; leave last_heartbeat NULL. With a
    // 1-hour idle window (3600s), this task must be staled.
    db.prepare(
      "UPDATE tasks SET updated_at = datetime('now', '-7200 seconds') WHERE id = ?"
    ).run(task.id);

    const stale = markStaleTasks(db, 2, 3600);
    assert.equal(stale.length, 1, "idle no-heartbeat task should be staled");
    assert.equal(stale[0].id, task.id);
    assert.equal(getTask(db, task.id)!.status, "stale");
  });

  it("does NOT stale a never-heartbeated task that was updated recently", () => {
    const db = initDb(":memory:");
    const task = createTask(db, { agent: "a", project: "p", title: "fresh-no-hb" });
    assert.equal(task.last_heartbeat, null);

    // updated_at is "now" (set by createTask). Well within the 3600s idle window.
    const stale = markStaleTasks(db, 2, 3600);
    assert.equal(stale.length, 0, "a just-created no-heartbeat task must not be staled");
  });
});
