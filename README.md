# Loom

Loom runs agent sessions, called threads, on your own machine, and schedules work for them.

- It starts Claude Code and Codex agents, each in its own git worktree, and owns their whole
  life: queue, sleep and wake, crash recovery, stop and settle.
- Any agent can start, delegate to and message other threads through Loom's MCP tools.
- Its scheduler runs one-off reminders, intervals and wall-clock schedules.
- Everything goes through one small API on a Unix socket, so Thread Voice, the software
  factory and the `loom` CLI are just clients.

Status: design only. See `docs/design.md`.
