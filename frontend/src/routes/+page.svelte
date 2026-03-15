<script lang="ts">
  import { onMount, tick } from "svelte";
  import * as missionApi from "$lib/api";
  import type { Task, FeedEntry } from "$lib/api";

  let tasks = $state<Task[]>([]);
  let feed = $state<FeedEntry[]>([]);
  let view = $state<"board" | "feed">("board");
  let filterAgent = $state("");
  let filterProject = $state("");
  let loading = $state(true);
  let clock = $state("");
  let errorMsg = $state("");
  let authRequired = $state(false);
  let authKeyInput = $state("");
  let authSubmitting = $state(false);
  let allKnownAgents = $state<string[]>([]);
  let allKnownProjects = $state<string[]>([]);

  // ---- NOTIFICATIONS ----
  interface Notification {
    id: number;
    agent: string;
    project: string;
    title: string;
    visible: boolean;
  }
  let notifications = $state<Notification[]>([]);
  let notifCounter = 0;
  let knownTaskIds = new Set<number>();

  function showNotification(task: Task) {
    const notif: Notification = {
      id: notifCounter++,
      agent: task.agent,
      project: task.project,
      title: task.title,
      visible: false,
    };
    notifications = [...notifications, notif];
    requestAnimationFrame(() => {
      notifications = notifications.map(n => n.id === notif.id ? { ...n, visible: true } : n);
    });
    setTimeout(() => {
      notifications = notifications.map(n => n.id === notif.id ? { ...n, visible: false } : n);
      setTimeout(() => {
        notifications = notifications.filter(n => n.id !== notif.id);
      }, 500);
    }, 4000);
  }

  // ---- CONFETTI ----
  function spawnConfetti(element: HTMLElement) {
    const rect = element.getBoundingClientRect();
    const cx = rect.left + rect.width / 2;
    const cy = rect.top + rect.height / 2;
    const colors = ["var(--accent)", "var(--active)", "#e0a040", "#c84040", "#88b068", "#e8e0d4"];

    for (let i = 0; i < 24; i++) {
      const particle = document.createElement("div");
      particle.className = "confetti-particle";
      const angle = (Math.PI * 2 * i) / 24 + (Math.random() - 0.5) * 0.5;
      const velocity = 60 + Math.random() * 80;
      const dx = Math.cos(angle) * velocity;
      const dy = Math.sin(angle) * velocity;
      const color = colors[Math.floor(Math.random() * colors.length)];
      const size = 3 + Math.random() * 4;

      particle.style.cssText = `
        position: fixed; left: ${cx}px; top: ${cy}px;
        width: ${size}px; height: ${size}px;
        background: ${color}; border-radius: ${Math.random() > 0.5 ? '50%' : '1px'};
        pointer-events: none; z-index: 10000;
        --dx: ${dx}px; --dy: ${dy}px;
        animation: confetti-burst 0.8s ease-out forwards;
      `;
      document.body.appendChild(particle);
      setTimeout(() => particle.remove(), 900);
    }
  }

  // ---- CARD FLIP ----
  let flippedCard = $state<number | null>(null);

  function toggleFlip(taskId: number) {
    flippedCard = flippedCard === taskId ? null : taskId;
  }

  // ---- DRAG AND DROP ----
  let draggedTask = $state<Task | null>(null);
  let dragOverStatus = $state<string | null>(null);

  function onDragStart(e: DragEvent, task: Task) {
    draggedTask = task;
    if (e.dataTransfer) {
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData("text/plain", String(task.id));
    }
    requestAnimationFrame(() => {
      const el = e.target as HTMLElement;
      el.classList.add("dragging");
    });
  }

  function onDragEnd(e: DragEvent) {
    draggedTask = null;
    dragOverStatus = null;
    (e.target as HTMLElement).classList.remove("dragging");
  }

  function onDragOver(e: DragEvent, status: string) {
    e.preventDefault();
    if (e.dataTransfer) e.dataTransfer.dropEffect = "move";
    dragOverStatus = status;
  }

  function onDragLeave(e: DragEvent, status: string) {
    const related = e.relatedTarget as HTMLElement | null;
    const column = (e.currentTarget as HTMLElement);
    if (!related || !column.contains(related)) {
      if (dragOverStatus === status) dragOverStatus = null;
    }
  }

  async function onDrop(e: DragEvent, status: string) {
    e.preventDefault();
    dragOverStatus = null;
    if (!draggedTask || draggedTask.status === status) return;

    const wasCompleted = status === "completed" && draggedTask.status !== "completed";
    const droppedId = draggedTask.id;
    await missionApi.updateTask(draggedTask.id, { status });
    await loadData();

    if (wasCompleted) {
      await tick();
      const card = document.querySelector(`[data-task-id="${droppedId}"]`) as HTMLElement;
      if (card) spawnConfetti(card);
    }
    draggedTask = null;
  }

  // ---- WALLPAPER & THEME ----
  interface WallpaperTheme {
    src: string;
    accent: string;
    accentDim: string;
    accentGlow: string;
    active: string;
    activeGlow: string;
  }

  const WALLPAPERS: WallpaperTheme[] = [
    {
      src: "/wallpapers/rain-city-01.jpg",
      accent: "#d4a030", accentDim: "#a07820", accentGlow: "#f0c040",
      active: "#d4a030", activeGlow: "rgba(212, 160, 48, 0.25)",
    },
    {
      src: "/wallpapers/rain-city-02.jpg",
      accent: "#c84040", accentDim: "#982828", accentGlow: "#e05050",
      active: "#c84040", activeGlow: "rgba(200, 64, 64, 0.25)",
    },
    {
      src: "/wallpapers/rain-city-05.jpg",
      accent: "#e0a040", accentDim: "#b07820", accentGlow: "#f0b850",
      active: "#e0a040", activeGlow: "rgba(224, 160, 64, 0.25)",
    },
    {
      src: "/wallpapers/dark-scenery-04.jpg",
      accent: "#e07830", accentDim: "#b05a18", accentGlow: "#f09048",
      active: "#e07830", activeGlow: "rgba(224, 120, 48, 0.25)",
    },
    {
      src: "/wallpapers/dark-scenery-05.jpg",
      accent: "#88b068", accentDim: "#607840", accentGlow: "#a0c880",
      active: "#88b068", activeGlow: "rgba(136, 176, 104, 0.25)",
    },
    {
      src: "/wallpapers/dark-scenery-08.jpg",
      accent: "#b0a080", accentDim: "#887858", accentGlow: "#c8b898",
      active: "#b0a080", activeGlow: "rgba(176, 160, 128, 0.25)",
    },
  ];

  let currentWallpaper = $state(0);

  function applyTheme(theme: WallpaperTheme) {
    const root = document.documentElement;
    root.style.setProperty("--accent", theme.accent);
    root.style.setProperty("--accent-dim", theme.accentDim);
    root.style.setProperty("--accent-glow", theme.accentGlow);
    root.style.setProperty("--active", theme.active);
    root.style.setProperty("--active-glow", theme.activeGlow);
  }

  function randomWallpaper() {
    let next: number;
    do { next = Math.floor(Math.random() * WALLPAPERS.length); } while (next === currentWallpaper && WALLPAPERS.length > 1);
    currentWallpaper = next;
    const theme = WALLPAPERS[currentWallpaper];
    const el = document.getElementById("wallpaper-bg");
    if (el) {
      el.style.opacity = "0";
      setTimeout(() => {
        el.style.backgroundImage = `url(${theme.src})`;
        applyTheme(theme);
        el.style.opacity = "1";
      }, 400);
    }
  }

  // ---- PARALLAX ----
  function onMouseMove(e: MouseEvent) {
    const el = document.getElementById("wallpaper-bg");
    if (!el) return;
    const x = (e.clientX / window.innerWidth - 0.5) * -20;
    const y = (e.clientY / window.innerHeight - 0.5) * -12;
    el.style.transform = `scale(1.08) translate(${x}px, ${y}px)`;
  }

  // ---- DATA ----
  const AGENT_COLORS: Record<string, string> = {
    "claude-code": "var(--agent-claude)",
    opencode: "var(--agent-opencode)",
    gpt: "var(--agent-gpt)",
    gemini: "var(--agent-gemini)",
    forge: "var(--agent-forge)",
    synapse: "var(--agent-synapse)",
  };

  const STATUS_LABELS: Record<string, string> = {
    active: "LIVE",
    paused: "STANDBY",
    blocked: "ALERT",
    completed: "COMPLETE",
  };

  function agentColor(agent: string): string {
    return AGENT_COLORS[agent] ?? "var(--text-dim)";
  }

  function timeAgo(dateStr: string): string {
    const date = new Date(dateStr + "Z");
    const now = new Date();
    const diff = Math.floor((now.getTime() - date.getTime()) / 1000);
    if (diff < 60) return "just now";
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
  }

  function updateClock() {
    const now = new Date();
    const offsetMin = -now.getTimezoneOffset();
    const sign = offsetMin >= 0 ? "+" : "-";
    const absH = Math.floor(Math.abs(offsetMin) / 60);
    const absM = Math.abs(offsetMin) % 60;
    const tz = absM > 0 ? `${absH}:${String(absM).padStart(2, "0")}` : String(absH);
    clock = now.toLocaleTimeString("en-US", { hour12: false }) + " UTC" + sign + tz;
  }

  async function loadData() {
    const filters: { agent?: string; project?: string } = {};
    if (filterAgent) filters.agent = filterAgent;
    if (filterProject) filters.project = filterProject;

    try {
      const [t, f] = await Promise.all([missionApi.fetchTasks(filters), missionApi.fetchFeed()]);

      // Arrival notifications
      if (!loading) {
        for (const task of t) {
          if (!knownTaskIds.has(task.id)) {
            showNotification(task);
          }
        }
      }
      knownTaskIds = new Set(t.map(task => task.id));

      tasks = t;
      feed = f;
      authRequired = false;
      errorMsg = "";

      // Accumulate all known agents/projects for filter dropdowns
      const agents = new Set(allKnownAgents);
      const projects = new Set(allKnownProjects);
      for (const task of t) {
        agents.add(task.agent);
        projects.add(task.project);
      }
      allKnownAgents = [...agents].sort();
      allKnownProjects = [...projects].sort();
    } catch (err) {
      if (missionApi.isUnauthorizedError(err)) {
        authRequired = true;
        errorMsg = "Mission Control API key required";
        return;
      }
      errorMsg = err instanceof Error ? err.message : "Failed to load data";
    } finally {
      loading = false;
    }
  }

  async function submitApiKey() {
    authSubmitting = true;
    try {
      missionApi.setApiKey(authKeyInput);
      loading = true;
      await loadData();
      if (!authRequired) {
        authKeyInput = "";
      }
    } finally {
      authSubmitting = false;
    }
  }

  function lockDashboard() {
    missionApi.clearApiKey();
    authRequired = true;
    errorMsg = "Mission Control API key required";
  }

  async function cycleStatus(task: Task, e?: MouseEvent) {
    const cycle = ["active", "paused", "blocked", "completed"];
    const nextStatus = cycle[(cycle.indexOf(task.status) + 1) % cycle.length];
    const wasCompleted = nextStatus === "completed";

    try {
      await missionApi.updateTask(task.id, { status: nextStatus });
      await loadData();

      if (wasCompleted && e) {
        const card = (e.target as HTMLElement).closest(".card") as HTMLElement;
        if (card) spawnConfetti(card);
      }
    } catch (err) {
      errorMsg = err instanceof Error ? err.message : "Failed to update task";
    }
  }

  async function removeTask(id: number) {
    try {
      await missionApi.deleteTask(id);
      await loadData();
    } catch (err) {
      errorMsg = err instanceof Error ? err.message : "Failed to delete task";
    }
  }

  function tasksByStatus(status: string): Task[] {
    return tasks.filter((t) => t.status === status);
  }

  function uniqueAgents(): string[] {
    return [...new Set(tasks.map((t) => t.agent))].sort();
  }

  function uniqueProjects(): string[] {
    return [...new Set(tasks.map((t) => t.project))].sort();
  }

  function totalByStatus(status: string): number {
    return tasks.filter(t => t.status === status).length;
  }

  onMount(() => {
    currentWallpaper = Math.floor(Math.random() * WALLPAPERS.length);
    const theme = WALLPAPERS[currentWallpaper];
    const el = document.getElementById("wallpaper-bg");
    if (el) {
      el.style.backgroundImage = `url(${theme.src})`;
      applyTheme(theme);
    }

    window.addEventListener("mousemove", onMouseMove);
    authKeyInput = missionApi.getApiKey();
    loadData();
    updateClock();
    const dataInterval = setInterval(loadData, 15000);
    const clockInterval = setInterval(updateClock, 1000);
    return () => {
      clearInterval(dataInterval);
      clearInterval(clockInterval);
      window.removeEventListener("mousemove", onMouseMove);
    };
  });
</script>

<!-- Notifications -->
{#each notifications as notif, i (notif.id)}
  <div class="notification" class:visible={notif.visible} style="top: {1.5 + i * 3.5}rem">
    <div class="notif-flash"></div>
    <div class="notif-content">
      <span class="notif-label">INCOMING</span>
      <span class="notif-agent" style="--agent-color: {agentColor(notif.agent)}">{notif.agent}</span>
      <span class="notif-text">{notif.project} / {notif.title}</span>
    </div>
  </div>
{/each}

<div class="app">
  <header>
    <div class="header-left">
      <div class="logo-mark">&#9670;</div>
      <div class="header-text">
        <h1>MISSION CONTROL</h1>
        <div class="header-meta">
          <span class="meta-item">SYS:ONLINE</span>
          <span class="meta-sep">//</span>
          <span class="meta-item">{tasks.length} OPS TRACKED</span>
          <span class="meta-sep">//</span>
          <span class="meta-item clock">{clock}</span>
          <span class="meta-sep">//</span>
          <button class="wallpaper-btn" onclick={randomWallpaper} title="Shuffle wallpaper">SHUFFLE BG</button>
          {#if authRequired || missionApi.getApiKey()}
            <span class="meta-sep">//</span>
            <button class="wallpaper-btn" onclick={lockDashboard} title="Clear API key">LOCK</button>
          {/if}
        </div>
      </div>
    </div>

    <div class="header-right">
      <div class="stat-pills">
        <div class="stat-pill live">
          <span class="stat-dot"></span>
          {totalByStatus("active")} LIVE
        </div>
        <div class="stat-pill standby">{totalByStatus("paused")} STDBY</div>
        <div class="stat-pill alert">{totalByStatus("blocked")} ALERT</div>
        <div class="stat-pill done">{totalByStatus("completed")} DONE</div>
      </div>
    </div>
  </header>

  <div class="controls">
    <div class="filters">
      <select bind:value={filterAgent} onchange={loadData}>
        <option value="">ALL AGENTS</option>
        {#each allKnownAgents as agent}
          <option value={agent}>{agent.toUpperCase()}</option>
        {/each}
      </select>
      <select bind:value={filterProject} onchange={loadData}>
        <option value="">ALL PROJECTS</option>
        {#each allKnownProjects as project}
          <option value={project}>{project.toUpperCase()}</option>
        {/each}
      </select>
    </div>
    <div class="view-toggle">
      <button class:active={view === "board"} onclick={() => (view = "board")}>
        <span class="toggle-icon">&#9638;</span> BOARD
      </button>
      <button class:active={view === "feed"} onclick={() => (view = "feed")}>
        <span class="toggle-icon">&#9776;</span> FEED
      </button>
    </div>
  </div>

  {#if errorMsg}
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div class="error-banner" onclick={() => errorMsg = ""}>
      <span class="error-icon">&#9888;</span> {errorMsg}
      <span class="error-dismiss">&times;</span>
    </div>
  {/if}

  {#if authRequired}
    <div class="auth-overlay">
      <form class="auth-panel" onsubmit={(e) => { e.preventDefault(); submitApiKey(); }}>
        <div class="auth-kicker">RESTRICTED</div>
        <h2>Mission Control API Key Required</h2>
        <p>Enter the server API key to unlock task and feed data.</p>
        <input
          type="password"
          bind:value={authKeyInput}
          placeholder="API key"
          autocomplete="current-password"
          disabled={authSubmitting}
        />
        <button type="submit" disabled={authSubmitting || !authKeyInput.trim()}>
          {authSubmitting ? "VERIFYING..." : "UNLOCK"}
        </button>
      </form>
    </div>
  {/if}

  {#if loading}
    <div class="loading">ESTABLISHING UPLINK...</div>
  {:else if view === "board"}
    <div class="board">
      {#each ["active", "paused", "blocked", "completed"] as status}
        {@const statusTasks = tasksByStatus(status)}
        <div
          class="column"
          class:drag-over={dragOverStatus === status}
          data-status={status}
          ondragover={(e) => onDragOver(e, status)}
          ondragleave={(e) => onDragLeave(e, status)}
          ondrop={(e) => onDrop(e, status)}
        >
          <div class="column-header">
            <span class="status-indicator" data-status={status}></span>
            <span class="column-title">{STATUS_LABELS[status]}</span>
            <span class="column-count">{statusTasks.length}</span>
          </div>

          <div class="column-cards">
            {#if statusTasks.length === 0}
              <div class="empty-column">NO OPERATIONS</div>
            {:else}
              {#each statusTasks as task, i (task.id)}
                {@const isFlipped = flippedCard === task.id}
                <div
                  class="breathing-card"
                  class:is-flipped={isFlipped}
                  data-task-id={task.id}
                  style="--float-delay: {(i * 0.7) + (["active","paused","blocked","completed"].indexOf(status) * 0.3)}s; --breathe-delay: {(i * 1.1) + (["active","paused","blocked","completed"].indexOf(status) * 0.5)}s"
                >
                  <div class="breathing-inner">
                    <!-- Front -->
                    <div
                      class="breathing-front card"
                      data-status={status}
                      draggable="true"
                      ondragstart={(e) => onDragStart(e, task)}
                      ondragend={onDragEnd}
                      onclick={() => toggleFlip(task.id)}
                    >
                      <div class="card-top">
                        <span class="agent-tag" style="--agent-color: {agentColor(task.agent)}">{task.agent}</span>
                        <span class="card-time">{timeAgo(task.updated_at)}</span>
                      </div>
                      <div class="card-project">{task.project}</div>
                      <div class="card-title">{task.title}</div>
                      {#if task.summary}
                        <div class="card-summary">{task.summary}</div>
                      {/if}
                      <div class="card-actions">
                        <button class="btn-action btn-cycle" onclick={(e) => { e.stopPropagation(); cycleStatus(task, e); }} title="Cycle status">&#8635;</button>
                        <button class="btn-action btn-delete" onclick={(e) => { e.stopPropagation(); removeTask(task.id); }} title="Remove">&times;</button>
                      </div>
                    </div>
                    <!-- Back -->
                    <div
                      class="breathing-back card card-back"
                      data-status={status}
                      onclick={() => toggleFlip(task.id)}
                    >
                      <div class="card-back-header">
                        <span class="agent-tag" style="--agent-color: {agentColor(task.agent)}">{task.agent}</span>
                        <button class="btn-action btn-flip" onclick={(e) => { e.stopPropagation(); toggleFlip(task.id); }} title="Close">&#10005;</button>
                      </div>
                      <div class="card-back-detail">
                        <div class="detail-row"><span class="detail-label">PROJECT</span><span>{task.project}</span></div>
                        <div class="detail-row"><span class="detail-label">TASK</span><span>{task.title}</span></div>
                        <div class="detail-row"><span class="detail-label">STATUS</span><span class="detail-status" data-status={task.status}>{STATUS_LABELS[task.status]}</span></div>
                        <div class="detail-row"><span class="detail-label">CREATED</span><span>{new Date(task.created_at + "Z").toLocaleString()}</span></div>
                        <div class="detail-row"><span class="detail-label">UPDATED</span><span>{timeAgo(task.updated_at)}</span></div>
                        {#if task.summary}
                          <div class="detail-row detail-summary"><span class="detail-label">NOTES</span><span>{task.summary}</span></div>
                        {/if}
                      </div>
                    </div>
                  </div>
                </div>
              {/each}
            {/if}
          </div>
        </div>
      {/each}
    </div>
  {:else}
    <div class="feed-list">
      {#each feed as entry, i (entry.id)}
        <div class="feed-entry" style="animation-delay: {i * 40}ms">
          <span class="feed-timestamp">{timeAgo(entry.created_at)}</span>
          <span class="feed-pipe">|</span>
          <span class="agent-tag" style="--agent-color: {agentColor(entry.agent)}">{entry.agent}</span>
          <span class="feed-arrow">&#9654;</span>
          <span class="feed-status" data-status={entry.status}>
            {STATUS_LABELS[entry.status] ?? entry.status.toUpperCase()}
          </span>
          <span class="feed-target">
            {entry.project}<span class="feed-sep">/</span>{entry.title}
          </span>
          {#if entry.summary}
            <span class="feed-note">// {entry.summary}</span>
          {/if}
        </div>
      {/each}
      {#if feed.length === 0}
        <div class="empty-feed">NO TRANSMISSIONS</div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .app {
    max-width: 1500px;
    margin: 0 auto;
    padding: 1.5rem 2rem;
  }

  /* ---- NOTIFICATIONS ---- */
  .notification {
    position: fixed;
    right: -400px;
    z-index: 10000;
    background: rgba(12, 11, 10, 0.85);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border: 1px solid var(--accent-dim);
    border-radius: 8px;
    padding: 0.6rem 1rem;
    min-width: 280px;
    max-width: 380px;
    transition: right 0.5s cubic-bezier(0.16, 1, 0.3, 1);
    overflow: hidden;
  }

  .notification.visible { right: 1.5rem; }

  .notif-flash {
    position: absolute;
    inset: 0;
    background: var(--accent);
    opacity: 0;
    animation: notif-flash 0.6s ease-out;
  }

  .notif-content {
    position: relative;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
  }

  .notif-label {
    font-family: var(--font-display);
    font-size: 0.65rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    color: var(--accent);
  }

  .notif-agent {
    font-size: 0.55rem;
    font-weight: 700;
    padding: 0.1rem 0.4rem;
    border-radius: 2px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--bg);
    background: var(--agent-color);
  }

  .notif-text { font-size: 0.7rem; color: var(--text); }

  @keyframes notif-flash {
    0% { opacity: 0.4; }
    100% { opacity: 0; }
  }

  /* ---- CONFETTI ---- */
  :global(.confetti-particle) {
    animation: confetti-burst 0.8s ease-out forwards;
  }

  @keyframes confetti-burst {
    0% { transform: translate(0, 0) rotate(0deg) scale(1); opacity: 1; }
    100% { transform: translate(var(--dx), var(--dy)) rotate(720deg) scale(0); opacity: 0; }
  }

  /* ---- HEADER ---- */
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1.25rem;
    padding: 1rem 1.25rem;
    border: 1px solid rgba(255, 255, 255, 0.06);
    border-radius: 10px;
    background: rgba(12, 11, 10, 0.55);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    flex-wrap: wrap;
    gap: 1rem;
  }

  .header-left { display: flex; align-items: center; gap: 0.75rem; }

  .logo-mark {
    font-size: 1.8rem;
    color: var(--accent);
    animation: flicker 8s infinite;
  }

  h1 {
    font-family: var(--font-display);
    font-size: 1.6rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    color: var(--accent);
  }

  .header-meta {
    display: flex;
    gap: 0.5rem;
    font-size: 0.65rem;
    color: var(--text-dim);
    letter-spacing: 0.06em;
    margin-top: 0.1rem;
    align-items: center;
  }

  .meta-sep { color: var(--border-glow); }
  .clock { color: var(--active); }

  .wallpaper-btn {
    font-size: 0.6rem;
    letter-spacing: 0.08em;
    color: var(--text-dim);
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    border: 1px solid rgba(255, 255, 255, 0.06);
    transition: all 0.2s;
    cursor: pointer;
  }

  .wallpaper-btn:hover {
    color: var(--accent);
    border-color: var(--accent-dim);
    background: rgba(212, 160, 48, 0.1);
  }

  .stat-pills { display: flex; gap: 0.5rem; }

  .stat-pill {
    font-size: 0.65rem;
    font-weight: 600;
    padding: 0.3rem 0.6rem;
    border-radius: 4px;
    letter-spacing: 0.06em;
    background: rgba(12, 11, 10, 0.5);
    border: 1px solid rgba(255, 255, 255, 0.06);
  }

  .stat-pill.live { color: var(--active); border-color: rgba(212, 160, 48, 0.3); }
  .stat-pill.standby { color: var(--paused); border-color: rgba(138, 122, 96, 0.25); }
  .stat-pill.alert { color: var(--blocked); border-color: rgba(192, 66, 42, 0.25); }
  .stat-pill.done { color: var(--completed); }

  .stat-dot {
    display: inline-block;
    width: 6px; height: 6px;
    border-radius: 50%;
    background: var(--active);
    margin-right: 0.2rem;
    animation: glow-pulse 2s ease-in-out infinite;
  }

  /* ---- CONTROLS ---- */
  .controls {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1.25rem;
    gap: 1rem;
    flex-wrap: wrap;
  }

  .filters { display: flex; gap: 0.5rem; }

  select {
    background: rgba(12, 11, 10, 0.6);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    color: var(--text);
    border: 1px solid rgba(255, 255, 255, 0.06);
    border-radius: 4px;
    padding: 0.4rem 0.7rem;
    font-family: var(--font-mono);
    font-size: 0.7rem;
    letter-spacing: 0.04em;
  }

  select:focus { outline: none; border-color: var(--accent-dim); }

  .view-toggle {
    display: flex;
    border: 1px solid rgba(255, 255, 255, 0.06);
    border-radius: 4px;
    overflow: hidden;
    background: rgba(12, 11, 10, 0.5);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
  }

  .view-toggle button {
    padding: 0.4rem 0.8rem;
    font-size: 0.7rem;
    letter-spacing: 0.06em;
    transition: all 0.15s;
    border-right: 1px solid var(--border);
  }

  .view-toggle button:last-child { border-right: none; }
  .view-toggle button:hover { background: var(--surface-hover); }
  .view-toggle button.active { background: var(--accent-dim); color: var(--bg); }
  .toggle-icon { margin-right: 0.3rem; }

  /* ---- BOARD ---- */
  .board {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.75rem;
    align-items: start;
  }

  .column {
    background: rgba(12, 11, 10, 0.5);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border: 1px solid rgba(255, 255, 255, 0.06);
    border-radius: 8px;
    overflow: hidden;
    transition: border-color 0.3s, box-shadow 0.3s;
  }

  .column[data-status="active"] { border-color: rgba(212, 160, 48, 0.25); }
  .column[data-status="blocked"] { border-color: rgba(192, 66, 42, 0.2); }

  .column.drag-over {
    border-color: var(--accent) !important;
    box-shadow: 0 0 20px var(--active-glow), inset 0 0 20px rgba(212, 160, 48, 0.05);
  }

  .column-header {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.6rem 0.75rem;
    border-bottom: 1px solid var(--border);
    font-family: var(--font-display);
    transition: background 0.3s;
  }

  .column.drag-over .column-header { background: rgba(212, 160, 48, 0.08); }

  .status-indicator { width: 8px; height: 8px; border-radius: 2px; flex-shrink: 0; }

  .status-indicator[data-status="active"] {
    background: var(--active);
    box-shadow: 0 0 6px var(--active-glow);
    animation: glow-pulse 2s ease-in-out infinite;
  }
  .status-indicator[data-status="paused"] { background: var(--paused); box-shadow: 0 0 4px var(--paused-glow); }
  .status-indicator[data-status="blocked"] { background: var(--blocked); animation: alert-pulse 1.5s ease-in-out infinite; }
  .status-indicator[data-status="completed"] { background: var(--completed); }

  .column-title { font-size: 0.8rem; font-weight: 700; letter-spacing: 0.1em; }

  .column-count {
    margin-left: auto;
    font-size: 0.65rem;
    font-family: var(--font-mono);
    color: var(--text-dim);
    background: rgba(12, 11, 10, 0.5);
    padding: 0.1rem 0.4rem;
    border-radius: 3px;
    border: 1px solid rgba(255, 255, 255, 0.06);
  }


  .column-cards {
    padding: 0.5rem;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    min-height: 80px;
    transition: min-height 0.3s;
  }

  .column.drag-over .column-cards { min-height: 120px; }

  /* ---- BREATHING CARDS ---- */
  .breathing-card {
    animation: mc-float 6s ease-in-out infinite;
    animation-delay: var(--float-delay, 0s);
  }

  .breathing-card:hover {
    animation-play-state: paused;
  }

  .breathing-inner {
    position: relative;
  }

  .breathing-card.is-flipped .breathing-front { display: none; }
  .breathing-card:not(.is-flipped) .breathing-back { display: none; }

  .breathing-card.is-flipped .breathing-back {
    position: relative;
    transform: none;
    animation: flip-in 0.35s ease-out;
  }

  .breathing-front {
    border-radius: 6px;
    transition: border-color 0.3s, box-shadow 0.3s;
    animation: mc-breathe 4s ease-in-out infinite;
    animation-delay: var(--breathe-delay, 0s);
    cursor: pointer;
  }

  .breathing-card:hover .breathing-front {
    animation-play-state: paused;
    border-color: rgba(212, 160, 48, 0.35) !important;
    box-shadow: 0 6px 20px rgba(0, 0, 0, 0.4),
                0 0 18px var(--active-glow);
  }

  .breathing-back {
    border-radius: 6px;
    cursor: pointer;
    overflow-y: auto;
  }

  @keyframes flip-in {
    0% { opacity: 0; transform: scale(0.95); }
    100% { opacity: 1; transform: scale(1); }
  }

  @keyframes mc-float {
    0%, 100% { transform: translateY(0); }
    50% { transform: translateY(-6px); }
  }

  @keyframes mc-breathe {
    0%, 100% {
      box-shadow: 0 0 8px var(--active-glow);
      border-color: rgba(255, 255, 255, 0.05);
    }
    50% {
      box-shadow: 0 0 20px var(--active-glow),
                  0 0 8px rgba(212, 160, 48, 0.1);
      border-color: rgba(212, 160, 48, 0.3);
    }
  }

  /* ---- CARDS ---- */
  .card {
    background: rgba(21, 20, 19, 0.6);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    border: 1px solid rgba(255, 255, 255, 0.05);
    border-radius: 6px;
    padding: 0.65rem;
    transition: border-color 0.2s, background 0.2s, box-shadow 0.2s;
  }

  .card[data-status="active"] { border-left: 2px solid var(--active); }
  .card[data-status="blocked"] { border-left: 2px solid var(--blocked); }
  .card[data-status="paused"] { border-left: 2px solid var(--paused); }
  .card[data-status="completed"] { border-left: 2px solid var(--completed); opacity: 0.6; }

  :global(.card.dragging) { opacity: 0.4; transform: scale(0.95); }

  .card-top {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.4rem;
  }

  .agent-tag {
    font-size: 0.55rem;
    font-weight: 700;
    padding: 0.12rem 0.45rem;
    border-radius: 2px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--bg);
    background: var(--agent-color);
  }

  .card-time { font-size: 0.6rem; color: var(--text-dim); }

  .card-project {
    font-size: 0.65rem;
    color: var(--accent);
    font-weight: 500;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    margin-bottom: 0.15rem;
  }

  .card-title {
    font-size: 0.78rem;
    font-weight: 500;
    margin-bottom: 0.25rem;
    line-height: 1.3;
  }

  .card-summary {
    font-size: 0.65rem;
    color: var(--text-dim);
    line-height: 1.4;
    padding-left: 0.4rem;
    border-left: 1px solid var(--border);
  }

  .card-actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.2rem;
    margin-top: 0.4rem;
    opacity: 0;
    transition: opacity 0.15s;
  }

  .card:hover .card-actions { opacity: 1; }

  .btn-action {
    font-size: 0.85rem;
    padding: 0.1rem 0.35rem;
    border-radius: 3px;
    color: var(--text-dim);
  }

  .btn-flip:hover, .btn-cycle:hover {
    background: var(--surface-hover);
    color: var(--accent);
  }

  .btn-delete:hover {
    background: rgba(224, 64, 64, 0.15);
    color: var(--blocked);
  }

  /* Card back */
  .card-back-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.5rem;
  }

  .card-back-detail {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }

  .detail-row {
    display: flex;
    gap: 0.5rem;
    font-size: 0.65rem;
    line-height: 1.3;
  }

  .detail-label {
    color: var(--text-dim);
    font-size: 0.55rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    min-width: 55px;
    flex-shrink: 0;
  }

  .detail-status[data-status="active"] { color: var(--active); }
  .detail-status[data-status="paused"] { color: var(--paused); }
  .detail-status[data-status="blocked"] { color: var(--blocked); }
  .detail-status[data-status="completed"] { color: var(--completed); }

  .detail-summary span { color: var(--text-dim); font-style: italic; }

  .empty-column {
    text-align: center;
    color: var(--text-dim);
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    padding: 2rem 0;
  }

  /* ---- FEED ---- */
  .feed-list { display: flex; flex-direction: column; gap: 2px; }

  .feed-entry {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0.75rem;
    background: rgba(12, 11, 10, 0.5);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.05);
    border-radius: 6px;
    font-size: 0.72rem;
    animation: slide-in 0.25s ease-out both;
    flex-wrap: wrap;
  }

  .feed-entry:hover {
    border-color: rgba(212, 160, 48, 0.2);
    background: rgba(21, 20, 19, 0.65);
  }

  .feed-timestamp { color: var(--text-dim); font-size: 0.6rem; min-width: 55px; }
  .feed-pipe { color: var(--border-glow); }
  .feed-arrow { color: var(--text-dim); font-size: 0.5rem; }

  .feed-status { font-weight: 700; font-size: 0.65rem; letter-spacing: 0.06em; }

  .feed-status[data-status="active"] { color: var(--active); }
  .feed-status[data-status="paused"] { color: var(--paused); }
  .feed-status[data-status="blocked"] { color: var(--blocked); }
  .feed-status[data-status="completed"] { color: var(--completed); }

  .feed-target { color: var(--text); }
  .feed-sep { color: var(--text-dim); margin: 0 0.1rem; }
  .feed-note { color: var(--text-dim); font-style: italic; }

  .empty-feed {
    text-align: center;
    color: var(--text-dim);
    padding: 3rem;
    letter-spacing: 0.1em;
  }

  .loading {
    text-align: center;
    color: var(--accent);
    padding: 3rem;
    letter-spacing: 0.15em;
    font-family: var(--font-display);
    font-size: 1rem;
    animation: flicker 3s infinite;
  }

  /* ---- ERROR BANNER ---- */
  .error-banner {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.6rem 1rem;
    margin-bottom: 1rem;
    background: rgba(192, 66, 42, 0.15);
    border: 1px solid rgba(192, 66, 42, 0.4);
    border-radius: 6px;
    color: #e05040;
    font-size: 0.72rem;
    cursor: pointer;
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
  }

  .error-banner:hover { background: rgba(192, 66, 42, 0.25); }
  .error-icon { font-size: 1rem; }
  .error-dismiss { margin-left: auto; font-size: 1rem; opacity: 0.6; }
  .error-dismiss:hover { opacity: 1; }

  /* ---- RESPONSIVE ---- */
  @media (max-width: 1000px) {
    .board { grid-template-columns: repeat(2, 1fr); }
  }

  @media (max-width: 550px) {
    .board { grid-template-columns: 1fr; }
    .stat-pills { flex-wrap: wrap; }
  }
</style>
