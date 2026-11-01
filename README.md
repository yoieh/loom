# Loom

Loom runs agent sessions, called threads, on your own machine, and schedules work for them.

- It starts Claude Code and Codex agents, each in its own git worktree, and owns their whole
  life: queue, sleep and wake, crash recovery, stop and settle.
- Any agent can start, delegate to and message other threads through Loom's MCP tools.
- Its scheduler runs one-off reminders, intervals and wall-clock schedules, and schedules that
  fire on events, such as a signal or a thread that settled.
- Everything goes through one small API on a Unix socket, so Thread Voice, the software
  factory and the `loom` CLI are just clients.

Status: `loom serve` and the `loom` CLI run Claude and Codex threads from a terminal, and also
threads in the T3 app once Loom is paired with it. The scheduler runs schedules too, which the
`loom schedules` commands add, list, pause and remove.
The API, the CLI and the Python client are documented in [`docs/api.md`](docs/api.md); the
design is in `docs/design.md`.

Try it, with the server in one terminal and the rest in another (the project is a git checkout):

```sh
uv sync --all-packages
uv run loom serve
uv run loom projects add "My project" ~/src/my-project
```

Then `uv run loom threads launch my-project "First task" "Say hello."` starts a thread, and
`uv run loom threads events --follow` shows what it does.
Add `--runner codex` to the launch to start a Codex thread instead.
With T3 running, `uv run loom t3 pair` once, then `--runner t3` on the launch, starts the thread
in T3.

`uv run loom install` gives Claude Code and Codex agents Loom's tools.

## License

Loom is under the [Functional Source License, Version 1.1, MIT Future License](LICENSE.md)
(FSL-1.1-MIT): you may use, change and share it for any purpose except offering a competing
commercial product or service. Each release becomes MIT two years after it is published.
