# Chiasm

A real-time dashboard for tracking what your AI agents are doing across projects. Built for developers running multiple AI coding assistants (Claude Code, GPT, Gemini, etc.) simultaneously who need one screen to see all activity.

![TypeScript](https://img.shields.io/badge/TypeScript-blue) ![Svelte](https://img.shields.io/badge/Svelte-orange) ![SQLite](https://img.shields.io/badge/SQLite-lightblue)

## Features

**Kanban board** with four lanes: Live, Standby, Alert, Complete
**Activity feed** showing a real-time log of all agent status changes
**Drag-and-drop** to move tasks between statuses
**Agent color coding** for quick visual identification (Claude, GPT, Gemini, Forge, etc.)
**Flip cards** to see task details
**Live polling** with toast notifications when new tasks arrive
**Wallpaper shuffle** with parallax backgrounds and dynamic theme colors
**Auth with per-agent keys** and admin escalation for secured deployments
**Filter by agent or project**

## Quick Start

```bash
npm install
npm run dev
```

The server starts on `http://localhost:4300`. The frontend dev server proxies API requests to the backend.

For the dashboard UI:

```bash
cd frontend
npm install
npm run dev
```

## Configuration

Copy `.env.example` to `.env` and edit as needed:

```
PORT=4300
HOST=0.0.0.0
DB_PATH=./chiasm.db
CHIASM_API_KEY=your-admin-key-here
```

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | `4300` |
| `HOST` | Bind address | `0.0.0.0` |
| `DB_PATH` | SQLite database path (created automatically) | `./chiasm.db` |
| `CHIASM_API_KEY` | **Required.** Admin API key for full access | - |
| `CHIASM_AUTH` | Set to `disabled` to explicitly run without auth | - |
| `CORS_ALLOW_ORIGIN` | Allowed CORS origin (`*` or specific origin) | disabled |

The server **refuses to start** unless `CHIASM_API_KEY` is set or `CHIASM_AUTH=disabled` is explicitly configured. This prevents accidentally running with auth off.

## Authentication

Chiasm uses a two-tier auth model:

### Admin key

Set via `CHIASM_API_KEY`. Grants full access to all endpoints including `/admin/*`.

```bash
curl -X GET http://localhost:4300/tasks \
  -H "Authorization: Bearer your-admin-key-here"
```

### Per-agent keys

Created via the admin API. Each key is scoped to a specific agent and can only create/update/delete tasks owned by that agent. All agents can read all tasks and the feed.

```bash
# Create an agent key (admin only)
curl -X POST http://localhost:4300/admin/keys \
  -H "Authorization: Bearer your-admin-key-here" \
  -H "Content-Type: application/json" \
  -d '{"agent": "claude-code"}'

# Response includes the key (store it, it cannot be retrieved again)
# {"id":1,"agent":"claude-code","key":"mc_...","prefix":"mc_da2bffc0","warning":"..."}

# List agent keys (admin only, no secrets shown)
curl -X GET http://localhost:4300/admin/keys \
  -H "Authorization: Bearer your-admin-key-here"

# Revoke an agent key (admin only)
curl -X DELETE http://localhost:4300/admin/keys/1 \
  -H "Authorization: Bearer your-admin-key-here"
```

## API

All endpoints accept and return JSON. All endpoints except `/health` require authentication.

### Tasks

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET` | `/tasks` | any key | List tasks (query: `agent`, `project`, `status`, `limit`, `offset`) |
| `POST` | `/tasks` | agent's own or admin | Create a task |
| `GET` | `/tasks/:id` | any key | Get a single task |
| `PATCH` | `/tasks/:id` | agent's own or admin | Update a task |
| `DELETE` | `/tasks/:id` | agent's own or admin | Delete a task |

### Feed

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET` | `/feed?limit=50` | any key | Activity log of all status changes |

### Admin

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `POST` | `/admin/keys` | admin only | Create an agent key |
| `GET` | `/admin/keys` | admin only | List agent keys (no secrets) |
| `DELETE` | `/admin/keys/:id` | admin only | Revoke an agent key |

### Health

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET` | `/health` | none | Health check (always open) |

## Agent Integration

Any AI agent or script can check in via the REST API.

### Lifecycle

**On session start** -- create a task:

```bash
curl -s http://localhost:4300/tasks -X POST \
  -H "Authorization: Bearer $AGENT_KEY" \
  -H "Content-Type: application/json" \
  -d '{"agent": "claude-code", "project": "my-app", "title": "Refactoring auth module"}'
```

Save the returned `id` for updates.

**During work** -- update status and summary:

```bash
curl -s http://localhost:4300/tasks/1 -X PATCH \
  -H "Authorization: Bearer $AGENT_KEY" \
  -H "Content-Type: application/json" \
  -d '{"status": "active", "summary": "Halfway through, tests passing"}'
```

**On session end** -- mark completed:

```bash
curl -s http://localhost:4300/tasks/1 -X PATCH \
  -H "Authorization: Bearer $AGENT_KEY" \
  -H "Content-Type: application/json" \
  -d '{"status": "completed", "summary": "Auth module refactored, all tests green"}'
```

### Task statuses

| Status | Dashboard column | Meaning |
|--------|-----------------|---------|
| `active` | LIVE | Agent is actively working |
| `paused` | STANDBY | Work paused or idle |
| `blocked` | ALERT | Waiting on something |
| `completed` | COMPLETE | Done |

### Agent source tags

| Agent | Tag |
|-------|-----|
| Claude Code CLI | `claude-code` |
| OpenCode | `opencode` |
| GPT | `gpt` |
| Gemini | `gemini` |
| Forge | `forge` |
| Synapse | `synapse` |

The dashboard auto-assigns colors per agent and updates every 15 seconds.

## Tech Stack

**Backend**: Node.js + TypeScript (zero frameworks, raw `node:http`)
**Database**: SQLite via LibSQL
**Frontend**: SvelteKit
**Runtime**: Node.js with `--experimental-strip-types` (no build step for backend)

## License

Elastic License 2.0 -- see [LICENSE](LICENSE).
