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
**Optional API key auth** for secured deployments
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
DB_PATH=./mission-control.db
API_KEY=your-api-key-here
```

`PORT` Server port (default: 4300)
`DB_PATH` SQLite database path (created automatically on first run)
`API_KEY` Optional. If set, all requests require `Authorization: Bearer <key>`

## API

All endpoints accept and return JSON.

### Tasks

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/tasks` | List tasks (optional query params: `agent`, `project`, `status`) |
| `POST` | `/tasks` | Create a task |
| `GET` | `/tasks/:id` | Get a single task |
| `PATCH` | `/tasks/:id` | Update a task |
| `DELETE` | `/tasks/:id` | Delete a task |

### Feed

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/feed?limit=50` | Activity log of all status changes |

### Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |

### Examples

Create a task:

```bash
curl -X POST http://localhost:4300/tasks \
  -H "Content-Type: application/json" \
  -d '{"agent": "claude-code", "project": "my-app", "title": "Refactoring auth module"}'
```

Update status:

```bash
curl -X PATCH http://localhost:4300/tasks/1 \
  -H "Content-Type: application/json" \
  -d '{"status": "completed", "summary": "Done, all tests passing"}'
```

With auth enabled:

```bash
curl -X GET http://localhost:4300/tasks \
  -H "Authorization: Bearer your-api-key-here"
```

## Agent Integration

Any AI agent or script can check in via the REST API. Example agent source tags:

### Lifecycle

**On session start** - create a task:

```bash
curl -s http://127.0.0.1:4300/tasks -X POST \
  -H "Content-Type: application/json" \
  -d '{"agent": "claude-code", "project": "my-app", "title": "Refactoring auth module"}'
```

Save the returned `id` for updates.

**During work** - update status and summary as things change:

```bash
curl -s http://127.0.0.1:4300/tasks/1 -X PATCH \
  -H "Content-Type: application/json" \
  -d '{"status": "active", "summary": "Halfway through, tests passing"}'
```

**On session end** - mark completed:

```bash
curl -s http://127.0.0.1:4300/tasks/1 -X PATCH \
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
 `claude-code` Claude Code CLI
`opencode` OpenCode
`gpt` GPT sessions
`gemini` Gemini sessions
`forge` Forge editor
`synapse` Synapse agent

The dashboard auto-assigns colors per agent and updates every 15 seconds.

## Tech Stack

**Backend**: Node.js + TypeScript (zero frameworks, raw `node:http`)
**Database**: SQLite via LibSQL
**Frontend**: SvelteKit
**Runtime**: Node.js with `--experimental-strip-types` (no build step for backend)

## License

Elastic License 2.0
