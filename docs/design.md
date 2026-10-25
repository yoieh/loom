# Loom: design

Loom runs agent sessions, called **threads**, and schedules work for them. Thread Voice, the
software factory, a terminal and the agents themselves are its clients.

Status: **proposed** 2026-10-23. The user asked for it by voice, through Bertil, in Thread
Voice's topic "Egen orkestrator och scheduler för threads i Thread Voice". It began as Thread
Voice's §78 (`docs/design-orchestrator.md` there) and moved here the same day, when the user
chose to build it as its own project and connect Thread Voice last: "vi kan både koppla på
våran fabrik men även att Threadvoice då egentligen bara blir en klient". Sections cited as
§73.x are Thread Voice's, copied into `docs/thread-voice-73.md`. Other § numbers below 73 are
Thread Voice's and matter only to that client.

## §1 Why Loom

- **One owner of every thread's life.** Start, queue, sleep, wake, crash, restart, stop and
  settle live in one place, for every kind of agent, instead of in each caller.
- **Any agent can start sessions as easily as the user.** That is the heart of Loom (the user,
  2026-10-23). Every agent gets tools to launch, delegate, message and schedule (§9).
- **Its own scheduler**, so reminders and recurring work don't need T3 (§7).
- **One API** clean enough that the factory, Thread Voice and the CLI are just clients (§8).
- **Modular.** Small packages around one contracts package; nothing depends on one big package
  (§3).
- **One environment.** Agents run on the one machine that runs Loom: the user's computer now,
  the home server later. Running threads on several machines is not covered.

T3 Code (`github.com/pingdotgg/t3code`) does much of this inside its app. Loom copies what it
does well and avoids what hurts it (§12). T3 stays usable: a T3 runner (§6) drives T3 threads
through T3's own tools.

## §2 Words

- **Thread**: one agent session. It has an id (a uuid; for Claude it is the agent's session
  uuid, §73.4), a runner (`claude`, `codex` or `t3`), a project, a workspace (§5), a title, an
  optional parent thread and an optional `origin` (the client or schedule that started it), an
  audience (`person` or `agent`, §15), and a state (§4). Codex picks its own thread id
  (`thread.started`), so its runner keeps that id in `<data_dir>/codex/<loom thread id>`, a file
  named by Loom's thread id that holds Codex's. T3 picks its own too, kept the same way in `<data_dir>/t3/`
  (§6.1).
- **Turn**: one message, or several that steer it, up to the agent's `result`.
- **Task**: a thread another thread started with `delegate`, whose result goes back to its
  parent.
- **Schedule**: a stored trigger that posts into a thread, launches a new one, or only notifies
  clients (§7).
- **Request**: something an agent waits on a person for, such as a permission prompt (§8).
- **Client**: anything that calls the API: Thread Voice, the factory, the CLI, `loom-mcp`.

## §3 Packages

One uv workspace, `packages/<name>/`, with a package per part. Each depends only on what its
row lists. Python 3.12, stdlib only unless an issue names a dependency.

| Package | Holds | Depends on |
|---|---|---|
| `loom-contracts` | the `v1` calls, events, thread and run states, workspace strategies, schedule triggers (dataclasses and `TypedDict`s), and the `Runner` and `RunnerHost` protocols. No logic, no I/O | stdlib |
| `loom-core` | the orchestrator: states, slots, queue, sleep, crashes, restart, delegation, projects, workspaces, the store and the API service layer (§4, §5, §8) | contracts |
| `loom-scheduler` | a generic tick that sources register on, schedule math (next run, missed runs, overlap), the `schedules` table. It acts only through the service layer's calls, never by importing `loom-core` internals | contracts |
| `loom-runner-claude`, `loom-runner-codex`, `loom-runner-t3` | one runner each (§6) | contracts |
| `loom-client` | the socket client, a stdlib-only Python API and the `loom` CLI (§8) | contracts |
| `loom-mcp` | the stdio MCP server that gives agents Loom's tools (§9) | client |
| `loom-service` | `loom serve`: wires core, scheduler and runners together and serves the API, as T3's `apps/server` does | core, scheduler, runners |

Rules, each checked by a packaging test:

- A package imports only the packages its row lists. A runner never imports `loom-core`; it
  gets paths, settings, the environment and secrets from the `RunnerHost` the core hands it
  (T3's `ProviderHost`).
- Behaviour is picked by a runner's capabilities (`steer`, `interrupt`, `resume`,
  `permission_requests`, …), never by its name.
- Every client goes through the same service layer. A handler decodes the input, calls one
  service method and maps its errors (T3's `ThreadManagementService`).
- No module over about 1,500 lines.
- Each runner has replay tests: a recorded stream (the spike's lines in §73.13 are the start)
  is played into it, and the events it emits are checked. No real agent in unit tests.

## §4 The orchestrator

`loom-core` is the only code that starts, wakes, stops or settles a thread.

- **States.** `queued` (waiting for a slot) → `starting` → `running` (a turn is on) → `idle`
  (process alive, no turn) → `sleeping` (process ended, resumable) → `settled`. Any state can
  go to `failed`, with the reason. A `failed` thread can only be settled (`threads.settle`),
  which keeps the reason; it is never resumed or restarted. `waiting` (a request is open, §8)
  is a sub-state of `running`. A T3 thread maps T3's states onto the same names.
- **No limit on threads.** Any number of threads and tasks can exist, at any depth. The only
  thing rationed is memory: each live local process takes about 275 MiB (§73.13 item 7).
  `agents_max` defaults to the machine's available memory at start, less 2 GiB, divided by
  350 MiB (about 60 on the user's 24 GB machine, about 15 on an 8 GB home server); the config
  key overrides it.
- **When memory is short**, the longest-idle `idle` thread is put to sleep first (stdin closed,
  §73.13 row 8); a sleeping thread costs nothing and resumes in about 0.6 s. Only when every
  live process is in a turn does a start wait, `queued`, in order: a person's own words first,
  then agents' starts, then schedules; within a source, oldest first. Nothing is refused.
- **Idle.** An `idle` thread with no turn for `idle_after` seconds (default 1200) goes to
  `sleeping`; the next message resumes it.
- **Crashes.** A process that exits during a turn without a `result` makes the turn `failed`
  and the thread `sleeping`. Loom resumes it and posts "Your last turn was cut off; continue
  where you were." At most 3 such continuations in 5 minutes (as T3's
  `RestartContinuation.ts`); then the thread is `failed` and a `thread.failed` event tells the
  clients. No restart loops. An exit after the turn's `result` (the thread is `idle`), or after
  Loom closed the process itself, is not a crash; the thread only goes to `sleeping`.
- **Restart.** On shutdown every local process is ended (close stdin, SIGTERM after 5 s). On
  start, every thread that was `running` or `idle` is `sleeping`; a turn that was cut off is
  resumed as under Crashes. Queued starts stay queued. Restarting a client touches no agent.
- **Command ids.** Every call that changes something carries an id; a retry with the same id is
  a no-op that returns the first answer. A launch's steps derive their ids from the launch's
  (`<id>:message`), so a launch half done before a crash finishes once.
- **One transaction per change.** New state rows, events and the command receipt are written in
  one SQLite transaction; a side effect (start a process, send a turn) runs after it, from that
  record.
- **One lane per thread.** Effects on a thread run one at a time, in order, under a per-thread
  lock; different threads run in parallel, on one asyncio loop.
- **Dispatch modes** on `threads.send`: `steer` (into the running turn, §73.13 row 2; the
  default), `queue` (after it), `interrupt` (stop it, then send). A task's result back to its
  parent jumps ahead of the parent's other queued messages. A client shows a post as waiting
  to go in ("queued", which is not the thread state `queued`) until its `message.entered` event,
  and as entered after it; a steered post enters mid-turn and shares the running turn's
  `result` (§73.13 row 2).
- **Stop is intent first.** Stop is recorded and shown at once; the runner's interrupt and the
  final `stopped` state follow as events. Stop on a thread stops everything it started, all the
  way down. An interrupted turn leaves its thread `idle`; when the turn has not ended 5 s after
  the interrupt (`stop_grace`), or the runner can't interrupt, the process is closed and the
  thread is `sleeping`, as is a thread stopped before it started. A stopped child's result is
  not posted to its parent. A thread that is being stopped can't delegate or launch (`conflict`),
  so nothing it starts escapes the stop.
- **Events.** Each change of state, each turn's text and tool lines, each message that entered
  the conversation (`message.entered`), each request and each `result` is an event with a
  growing id, kept 30 days. The state tables are the truth; events are a log for clients to
  follow, not something state is rebuilt from.

## §5 Projects and workspaces

- **Projects** (from §73.5): `id`, `title`, `workspace_root`, `permission_mode`, `created_at`.
  `loom project add <title> <path> [--mode <m>]`, `set-mode`, `remove` and `list` manage them.
  Modes are T3's four (`approval-required`, `auto-accept-edits`, `auto`, `full-access`),
  mapped to each agent's own flags as §73.13 rows 9 and 15 say. A project with no mode uses
  the config's `permission_mode`, default `auto`; the unset case never becomes `full-access`.
  A project's id is the slug of its title: NFKD-normalised, cut to ASCII and lowercased, each
  run of other characters than `a-z0-9` turned into `-`, `-` stripped from both ends, at most 40
  characters, and `-2`, `-3`, … added (within the 40) when the id is taken.
- **Workspaces**, the way T3's `workspaceStrategy` names them:
  - `root`: the project's main checkout;
  - `worktree`: a new worktree on a new branch (`loom/<thread-id>` by default) under
    `<data_dir>/worktrees/<project-id>/<thread-id>`, from a `base` branch (default the
    project's current branch);
  - `existing_worktree`: a path and branch already made, for example a parent's. The path must
    be the `workspace_path` of a thread of the same project, in any state; any other path is
    refused as `invalid`.
- A launch defaults to `worktree`. A delegated task defaults to its parent's workspace, since
  most delegations read or review the parent's work; it asks for `worktree` when it will write
  in parallel.

## §6 Runners

```python
class Runner(Protocol):
    name: str                                         # 'claude', 'codex', 't3'
    capabilities: frozenset[str]                      # 'steer', 'interrupt', 'resume', ...
    async def start(self, thread: ThreadSpec, host: RunnerHost) -> RunnerSession: ...

class RunnerSession(Protocol):
    events: AsyncIterator[RunnerEvent]                # turn.started, entered, text, tool, request, result, exited
    async def send(self, message: str, mode: DispatchMode) -> None: ...
    async def respond(self, request_id: str, answer: RequestAnswer) -> None: ...
    async def interrupt(self) -> None: ...
    async def close(self) -> None: ...                # stdin closed, SIGTERM after 5 s
```

- **`loom-runner-claude`**: one `claude -p --input-format stream-json --output-format
  stream-json --verbose --replay-user-messages --permission-prompt-tool stdio` process per
  live thread, `--session-id <uuid>` on the first start and `--resume <uuid>` (in its place)
  after, as §73.4 with every correction in §73.13's table (rows 1–12, 18). The thread id is the
  session uuid.
- **`loom-runner-codex`**: `codex exec --json` per turn and `codex -C <worktree> -s <sandbox>
  exec resume <codex thread id> --json -` after, as §73.4.1 with §73.13 rows 13–17. Its capabilities
  are `interrupt` and `resume`: Codex never asks, so it has no `permission_requests`, and `exec`
  takes no message mid-turn, so it has no `steer`. It reports a message as entered when its
  turn starts, so the message's `message.entered` comes right after `turn.started`. It keeps
  Codex's thread id (from `thread.started`) in `<data_dir>/codex/<loom thread id>`, a file
  named by Loom's thread id that holds Codex's (directory 0700, file 0600), to resume after a
  sleep or a restart.
- **`loom-runner-t3`**: drives a T3 thread through T3's own API when T3 is installed and
  paired, so a project can keep its threads in T3. Built last; its contract is §6.1.
- **Environment.** Each agent gets an allowlisted environment (§73.13 row 12) plus
  `LOOM_THREAD=<thread id>`, `LOOM_SOCKET`, the provider settings and the agent token from a
  0600 file in Loom's data dir (§73.13 row 11). For Claude, the host keys are the settings
  `claude_bin` (default `claude`), `claude_config_dir` (→ `CLAUDE_CONFIG_DIR`) and
  `agent_base_url` (→ `ANTHROPIC_BASE_URL`), and the secret `agent-token` (→
  `ANTHROPIC_AUTH_TOKEN`, with `ANTHROPIC_API_KEY` set empty). For Codex, the host keys are
  the settings `codex_bin` (default `codex`; the directory holding the `node` its shebang needs
  is appended to `PATH`) and `codex_home` (→ `CODEX_HOME`), and the secret `agent-token` (→
  `CLIPROXY_API_KEY`, for Codex only). Arguments are lists, never a shell string; a message
  never goes in argv.

### §6.1 The T3 runner

Written 2026-10-25 with the user, through Bertil, so that LM-10 has a contract to build
against. It rests on Thread Voice, which already drives T3 as an outside client: its
`docs/design-stage5.md` §53 (cited here as TV §53) holds the spike that paired with T3 0.1.0
and the client built from it. What TV §53 observed is marked *seen*; what comes only from
T3's tool descriptions is marked *schema*, and LM-10 checks it against T3 first.

The user decided two things by voice that day: Loom pairs with full access, as Thread Voice
does; and, since T3's API can't answer a permission prompt, a T3 thread's prompts are
answered in T3's own app. That makes the T3 runner the way for a project to stay in T3, not
the main path: "då känns det också som att vi kanske inte kommer använda så mycket av T3.
Utan vi kommer använda mer av vår egna lösning". Loom's own runners are the main path.

- **Transport.** T3's MCP endpoint over HTTP: `POST {origin}/mcp`, JSON-RPC 2.0, MCP protocol
  `2025-06-18`, every answer `application/json` (*seen*; an SSE body is accepted too, in case
  T3 changes). The origin is loopback, `http://127.0.0.1:<port>`, and T3 picks the port; the
  runner reads it from `<t3_home>/userdata/server-runtime.json` (`origin`). One `initialize`
  and `notifications/initialized` per runner, whose `Mcp-Session-Id` goes on every later call;
  T3 answers 400 (`-32600`) without it and 404 for an unknown one, and then the runner
  initializes again and retries once (*seen*). The client is stdlib `http.client`, run with
  `asyncio.to_thread`, with a 10 s timeout per call. Loom only connects out to loopback and
  still binds nothing (§11).
- **The origin check.** Before every call the runner reads `server-runtime.json` and sends
  nothing unless it names the origin the credential was paired with, so the token never goes
  to another address (TV §53 Security). The check is made again on the next call.
- **Logging in.** Each call carries `Authorization: Bearer <access_token>`. The token is an
  MCP client session: 30 days, no refresh (*seen*). A 401 (`invalid_mcp_credential`) means not
  paired; the user pairs again.
- **Pairing**, once, with `loom t3 pair`, which runs `python -m loom_runner_t3 pair` as
  `loom serve` runs `loom-service` (§8). It is T3's MCP OAuth, approved headlessly with no
  browser, as Thread Voice's `pair-t3` does (TV §53, item 1):
  1. register `client_name` "Loom" at `/oauth/mcp/register`, with one loopback redirect URI
     (`token_endpoint_auth_method: none`). Nothing ever listens on it: Loom reads the redirect
     from T3's answer and never follows it, so it binds no port to pick one. It tries
     `http://127.0.0.1/loom/callback` first; if T3 wants a port, a fixed one is written in;
  2. PKCE `S256`: `GET /oauth/mcp/authorize`, then `POST /oauth/mcp/approval`;
  3. run `t3 auth pairing create` with the four full-access scopes (`orchestration:read`,
     `orchestration:operate`, `settings:write`, `source-control:write`), `--ttl 5m`,
     `--label "Loom pairing"` and `--json`. The `t3` CLI is the setting `t3_bin`, by default
     `<t3_home>/runtime/versions/<serverVersion>/t3`; its output, which holds the code, is
     never shown;
  4. `POST /oauth/mcp/decision` with the code (`access: full-access`); check that the
     `redirectTo` it answers has the same `state`, `iss` and redirect URI;
  5. exchange the code at `/oauth/mcp/token` within 60 s, and find the new session's id in
     `t3 auth session list --json` by its label.

  It prints "Paired as 'Loom' (full access, 30 days). Remove it with `loom t3 unpair` or in T3
  Settings → Connections." Pairing again revokes the old session. `loom t3 unpair` revokes
  the stored session (`t3 auth session revoke <id>`) and deletes the file; `loom t3 status`
  says whether Loom is paired and how many days are left, and warns under 7.
- **The secret** is `<data_dir>/t3-client.json`, mode 0600: `{origin, access_token,
  expires_at, session_id, client_id, scope}`. The runner gets it as `RunnerHost.secret
  ('t3-client')`, which the host reads with the same checks as `agent-token`. It never goes
  into a log, argv, an event or an error text; the runner replaces the token with
  `[redacted]` in every reason, as §11 says for `loom-runner-claude`.
- **Settings.** `t3_home` (default `~/.t3`) and `t3_bin` (above), through `RunnerHost.setting`.
- **Access.** The pairing asks for full access, T3's ceiling, so Loom can start a T3 thread in
  a project of any mode. Every launch passes `runtimeMode` as the thread's `permission_mode`,
  so the ceiling never widens a thread (§11); T3 honours a lower mode (*seen*).
- **Thread ids.** T3 picks its own (`mcp:<uuid>`, *seen*). As for Codex, the runner keeps it in
  `<data_dir>/t3/<loom thread id>` (directory 0700, file 0600).
- **Projects and workspaces.** A Loom project uses the T3 project whose `workspaceRoot` is the
  same path, found with `t3_project_list`. If there is none, the start fails with "no T3
  project at <path>"; Loom never creates T3 projects. Loom makes the workspace itself (§5) and
  passes `{type: "root"}` for a project's root or `{type: "existing_worktree", worktreePath,
  branch}` for a worktree it made, which is a git worktree of the same repo, as T3 requires
  (*schema*).
- **Model.** With no `model`, the runner passes T3's `defaultModelSelection` from
  `<t3_home>/userdata/settings.json`, read-only, since a launch with none fails in a project
  with no default (*seen*). A `model` of the form `<instance>/<model>` sets both; a bare model
  keeps the default's instance.
- **Start.** A new thread is `t3_thread_launch {projectId, title, workspaceStrategy,
  runtimeMode, modelSelection}` with no message, so it starts idle (*schema*), and its id is
  stored at once; the first message then goes through `send`. A launch has no retry key, so it
  is never retried by itself: a crash between the launch and storing the id leaves at most one
  empty, idle T3 thread. `resume` reads the stored id and checks it with `t3_thread_read`; a
  thread that is gone or archived in T3 fails the start.
- **Send and interrupt.** `t3_thread_send {threadId, message, mode, clientRequestId}`, with
  `steer` → `steer`, `queue` → `queue` and `interrupt` → `restart`; the `clientRequestId` is
  derived from the Loom command id (§4), so a retry sends once (*schema*; TV §53 found no retry
  key on 0.1.0). `interrupt` is `t3_thread_interrupt {threadId, clientRequestId}`.
- **Events.** T3's MCP sends no stream, so a session polls `t3_thread_read {view: "activity",
  afterPosition}`: every second while the thread is `running` or has a queued run, and every
  15 s while it is idle, so a turn the user starts in T3's own app is seen too. A run that
  starts is `turn.started`; a `user_message` item is `entered`; a completed assistant item is
  `text` (T3 shows it only when complete, *seen*); a tool item is `tool`; a run that ends is
  `result`, completed, failed or stopped. The exact item and run state names come from a
  stream LM-10 records from a real T3 thread first, which also becomes the replay tests (§3).
- **Capabilities:** `steer`, `interrupt` and `resume`, not `permission_requests`.
  `t3_pending_request_respond` "cannot approve a permission request" (*schema*), so a T3
  thread's permission prompts and questions are answered in T3, by the user, as today. While
  one is open (`pendingRequestCount`) the thread stays `running`.
- **Sleep and close.** T3 owns the agent's process. `close` only stops polling and emits
  `exited`; it never stops, settles or archives the T3 thread. Stop (§4) uses `interrupt`.
- **Errors** are one line, `t3: <code>: <message>`, cut to 200 characters, from T3's `isError`
  results (*seen*). A 401 is "t3: not paired (run loom t3 pair)"; a failed origin check is
  "t3: T3 is not running at the paired address". At start they are start errors; during a
  turn they end it `failed`. A missing or expired `t3-client` secret fails the start with the
  same not-paired text, and Loom without T3 works as before.
- **Tests** never reach a real T3: the MCP client takes its HTTP call as an argument, so tests
  pass a fake, and pairing runs against a fake `t3` CLI. The recorded stream above, with
  tokens and ids replaced, is played into a session as the other runners' replay tests are.

## §7 The scheduler

- **Triggers**, in the shape of T3's `schedule_task`:
  - `{"type":"at","at":"2026-10-24T09:00"}`: once;
  - `{"type":"interval","everyMs":3600000}`: at least 60 s. The first run is due `everyMs`
    after the schedule is added, and each next one `everyMs` after the last run or skip, as
    T3's `Schedule.ts`;
  - `{"type":"fixed_time","timeOfDay":"09:07","weekdays":[1,2,3,4,5]}`: wall-clock, in the
    machine's local time zone. `weekdays` are 0 to 6 with 0 Sunday, as T3's, and missing or
    empty means every day;
  - `{"type":"webhook"}`: later, once Loom can be reached from outside (with Thread Voice's
    relay, its §74). It then needs a per-schedule secret;
  - `{"type":"event","topic":...}`: once for each matching event, with its loop guards (§15
    Event triggers). The first three are clock triggers.
- **Actions**: `post` (into a thread, which wakes it), `launch` (a new thread with the prompt,
  T3's `bindToCurrentThread: false`; it can carry a concurrency key with `on_busy`, and
  `settle: "on_result"`, which it passes on to `threads.launch`, §15), or `notify` (only a
  `schedule.fired` event with the text and who set it; Thread Voice speaks it as that
  persona). The default is `post` into the thread that made it. A reminder is `notify`.
- **The loop.** A generic tick (T3's `Scheduler.ts`) that sources register on; the schedules
  source keeps a heap by next run and wakes at the first one due, but at least every 30 s, so
  a clock jump (WSL after sleep, a laptop lid) is noticed. The schedules are rows in a
  `schedules` table, and each run is a row in `schedule_runs` with its time and outcome.
- **Missed runs**, as T3's `Schedule.ts`, except for reminders: a missed `at` runs once on start
  however late, and says it is late; a missed `interval` runs once; a `fixed_time` run more
  than 10 minutes late is skipped and logged. Never a burst of catch-up runs. A run is late
  when it starts more than 60 s after it was due: a late `post` or `launch` begins with
  `This run is late: it was due at <due>.`, and `schedule.fired` carries `late`.
- **Overlap.** For a clock trigger, a `post` or `launch` run whose previous run's turn is still
  going is skipped and logged, not queued. "Still going" means the thread the schedule's last
  fired run posted into or launched is `queued`, `starting` or `running`, so a turn someone else
  started in a posted thread counts too. Posts use dispatch mode `queue`.

## §8 The API

- **Calls** (`v1`): `threads.launch`, `threads.delegate`, `threads.send`, `threads.list`,
  `threads.get`, `threads.stop`, `threads.settle`, `threads.update` (a thread's audience, §15),
  `threads.events` (a stream from a given event id, so a client that reconnects misses nothing),
  `requests.respond`, `signals.emit` (§15), `schedules.add`, `schedules.list`, `schedules.pause`,
  `schedules.remove`, `projects.*`. Each call and event is documented, with examples, in `docs/api.md`.
- **Transport.** `loom serve` listens on `$XDG_RUNTIME_DIR/loom/api.sock` (user-only, mode
  0600), JSON lines. A local client needs no token and nothing listens on a port. A remote
  client waits for a relay and is not covered.
- **Requests.** A permission prompt is a `request.opened` event, answered with
  `requests.respond`; the first answer wins and the rest see `request.closed`. By default
  (`requests: "any"`) any caller may answer. A launch can say `requests: "caller"`, so only the
  launcher of the thread's top-level thread answers: that Loom thread, or, for a launch from
  outside Loom, the user's outside clients, which Loom does not tell apart (the factory and
  Thread Voice both count as the user); a schedule's launch has no launcher, so nobody does. A
  delegating agent never answers its child's requests under `caller`, and a thread never
  answers its own, under either. These rules guard against an agent's own calls; they are not a
  sandbox (`docs/api.md`, Who may answer a request). With no answer in
  `request_timeout` (default 600 s), Loom denies, since the agent would otherwise wait forever
  (§73.13 row 5); a request still open when its turn ends is closed as denied. Telling one
  outside client from another (so the factory alone answers its workers) is later work, and can
  be added to `v1`.
- **`loom-client`**: a stdlib-only Python API for every call, and the `loom` CLI with one
  command per call and `--json` output; `loom threads events --follow` prints the stream as
  JSON lines, so a client in any language can use Loom by running the CLI.
- **Stability.** A `v1` call or event field is only ever added to, never changed or removed. A
  breaking change is `v2`, served next to `v1`.

## §9 Tools for agents: `loom-mcp`

Starting a session is one tool call for any agent. `loom-mcp` is a stdio MCP server that
`loom install` registers with Claude Code (user scope) and Codex (`codex mcp add`, with
`env_vars = ["LOOM_THREAD", "LOOM_SOCKET", "XDG_RUNTIME_DIR"]`, §73.13 row 13, because Codex
passes an MCP server only its table's `env` and `env_vars`, §73.13 item 9). It knows its thread
from `LOOM_THREAD` when that is set and not empty, else from `CLAUDE_CODE_SESSION_ID` when
`threads.get` finds that id. An agent Loom didn't start (a terminal, T3) still gets the tools
and is recorded as an outside thread: v1 has no call that registers it, so each `launch_thread`
records it in the new thread's `origin`, `outside:<CLAUDE_CODE_SESSION_ID>` or else `outside` (a
Loom thread's is `thread:<id>`). An outside thread has no tasks, so it can't `delegate` or use
`task_status` and `task_cancel`.

| Tool | Does | Like T3's |
|---|---|---|
| `launch_thread {title, message, project_id?, workspace?, runner?, model?, audience?, concurrency_key?, on_busy?, settle?}` | a new top-level thread | `t3_thread_launch` |
| `delegate {task, workspace?, runner?, model?, concurrency_key?, on_busy?}` → `task_id` | a child thread; its last `result` is posted back to the parent | `delegate_task` |
| `task_status {task_id}`, `task_cancel {task_id}` | read or stop a child | same |
| `thread_send {thread_id, message, mode?}` | a message into a thread | `t3_thread_send` |
| `threads {project_id?}` | threads and their states | `t3_thread_list` |
| `emit {topic, payload, project_id?, key?, cause?}` | a signal (§15) | none |
| `schedule {trigger, text, action?, thread_id?, project_id?, title?, concurrency_key?, on_busy?, settle?}`, `schedules {}`, `unschedule {id}` | §7: a clock or an event trigger (§7, §15); `concurrency_key`, `on_busy` and `settle` go with a launch | `schedule_task` and friends |

- No depth or count limits: a task can delegate, and a thread can have any number of tasks.
  Every thread records where it came from, and Stop reaches all the way down (§4).
- The tools act in registered projects only.
- No tool adds a project, changes a project's mode or takes a permission mode. A thread a tool
  starts gets its project's mode (§11).

## §10 Clients

- **Thread Voice** keeps everything about people: voice, calls, asks, push, its pages,
  personas and household members. Everything about agents goes through `loom-client`: it
  launches its threads with `audience: "person"`, sends through the API and follows
  `threads.events` with `audience: "person"` (§15) for reply progress, the agent log and settled
  state; it answers `request.opened` by voice and speaks `schedule.fired`.
  Agents keep `thread-voice-mcp` for `speak` and `ask`, beside `loom-mcp`. Its pages can show a
  threads board and a schedules list from the same events. That work is Thread Voice's, once
  Loom's `v1` API exists.
- **The factory** launches its workers with `threads.launch` (a `worktree` workspace, a runner,
  a model, a prompt, `requests: "caller"`), follows `threads.events` until the `result`, or
  stops them. Slots, sleep and crash recovery come with it. Moving the factory onto Loom is the
  factory's own work.
- **The CLI** (§8) is a client too, so everything works from a terminal.

## §11 Security

- Loom starts a thread only from a client's call or a stored schedule; always in a registered
  project's root or a worktree Loom made for it.
- The socket and every store and secret file are mode 0600, under the user's runtime and data
  dirs. Nothing binds to a network port.
- A thread an agent starts, and a schedule it makes, gets the target project's permission mode,
  never the caller's if that is wider (T3's `DispatchModeLimit.ts`). So an agent can't start a
  `full-access` thread from an `approval-required` one.
- Only a client answers a request, never the thread that asked; Loom never allows anything on
  its own, and denies on timeout.
- Agent-written text in events is data: clients show it as text, capped at 2,000 characters per
  entry.
- A runner's error texts and turn failure reasons never hold the environment or the agent token.
  `loom-runner-claude` sends `claude`'s stderr to `/dev/null`, builds a start error from the
  binary and the OS error only, replaces the token with `[redacted]`, and cuts each such text to
  500 characters.
- `interval` is at least 60 s, so a mistake can't start a thread every second.
- Pairing with T3 (§6.1) makes Loom a full-access T3 client. Its token is the `t3-client`
  secret, sent only to the loopback origin it was paired with; a thread Loom starts in T3 gets
  its project's mode, never the ceiling; and the user can revoke it with `loom t3 unpair` or in
  T3 Settings → Connections.
- Webhooks (later) get T3's checks: a per-schedule random secret compared in constant time,
  optional HMAC-SHA256 signatures, a rate limit and a cap on queued deliveries.

## §12 What T3's source teaches

Read 2026-10-23, commit `9a391eb3`.

**Copied:** a contracts package everything depends on; a host interface so plug-ins never import
the app; capabilities instead of names; one service layer for every transport; idempotent
command ids; one transaction per change; one effect lane per thread; dispatch modes; stop as
intent; delegated results ahead of the queue; a generic scheduler tick; replay tests per
provider.

**Avoided:**

- **Full event sourcing.** T3 rebuilds state from an event log that must stay decodable forever,
  which piles up compatibility code. Here state tables are the truth and events are a pruned log.
- **God files.** T3's `Orchestrator.ts` is 12.4k lines and its Claude adapter 10.6k; here no
  module goes over about 1,500 lines.
- **Drivers inside the app.** T3's Claude and Codex drivers still live in its server; here every
  runner is its own package from the start.
- **Leaky helpers.** A helper two packages need goes into the lower package they share, never a
  sideways import.
- **Race code.** Much of T3's size is recovery from races between its many workers. One lane per
  thread on one asyncio loop keeps most of those races from existing.

## §13 Build order

Each row is one issue (`LM-<n>`) in the factory's tracker.

| # | What | After |
|---|---|---|
| 1 | The uv workspace, `loom-contracts` and the packaging test for §3's rules | – |
| 2 | `loom-core`: store, states, command ids, lanes, events, projects, workspaces (§4, §5), with a fake runner | 1 |
| 3 | `loom-runner-claude` with replay tests from §73.13 (§6) | 1 |
| 4 | `loom-service` and `loom-client`: `loom serve`, the socket, the CLI, `docs/api.md` (§8). After this, Loom runs Claude threads from a terminal | 2, 3 |
| 5 | Slots, sleep, crashes and restart (§4) | 4 |
| 6 | `loom-scheduler` (§7) | 4 |
| 7 | `loom-mcp` and `loom install` (§9) | 4, 6 |
| 8 | `loom-runner-codex` (§6) | 4 |
| 9 | Requests: permission prompts, `requests.respond`, timeouts (§8) | 4 |
| 10 | `loom-runner-t3` (§6) | 4 |
| LM-39 | Signals, event triggers and cause chains (§15) | 6 |
| LM-40 | Concurrency keys and settle on result (§15) | 5 |
| LM-41 | Threads carry an audience, person or agent (§15) | 4 |

From LM-39 on, a row's number is its issue key.

Thread Voice becoming a client is Thread Voice's issue, after 4, 6 and 9.

## §14 Defaults taken (change any of them)

1. Own repo, local only, never pushed until the user says so.
2. No limit on threads or tasks; only memory decides how many processes are live at once.
3. Missed runs: reminders and intervals once, wall-clock runs skipped after 10 minutes.
4. Tools and trigger shapes copy T3's, so skills and habits carry over.
5. The API is JSON lines on a Unix socket, with a CLI and a Python client.
6. State tables are the truth; events are a 30-day log.
7. "Loom" and the `loom-*` names are working names, cheap to change before issue 1.

## §15 Event-driven agents

Proposed 2026-10-24. The user wants to step in as rarely as possible: every agent thread gets one
clear goal, does it and is gone, and events start threads, not clocks. Bertil (the factory
manager) and one project manager per project are how the factory uses this. Those roles are the
factory's prompts and rules (Factory Manager's `orchestrator/agents.md`), not Loom's; Loom gives
only the parts below.

- **Signals.** `signals.emit {topic, project_id?, key?, payload, cause?}` puts an outside event
  into Loom as a `signal` event, so the factory can say "run done" or "PR opened". A signal is an
  event with no thread; one an agent thread emits records that thread as its `by`. The command id
  rule (§4) holds, so a retry emits once. Agents get it as the `loom-mcp` tool `emit`.
- **Event triggers.** A schedule trigger `{"type":"event","topic":"factory.run.status",
  "project_id":"glantfolk","match":{"to":"done"}}` fires its action (§7) for each matching
  `signal`, `thread.settled`, `thread.failed`, `request.opened` or `schedule.fired`. A prompt
  can use `{{payload.<field>}}` and `{{event.id}}`. Clock triggers stay for what really is about
  time, such as one daily check that no event was missed. `hops_max` and `rate_max` (below) are
  optional trigger fields that may only lower the defaults, so an agent's schedule can't loosen
  them. Each matching event runs: the overlap rule (§7) is for clock triggers, and an event run is
  never late.
- **Cause chains.** Every launch, send and signal records `cause` (the event behind it) and
  `hops` (its cause's hops + 1). A trigger skips an event at `hops_max` (default 8) and an event
  from a thread it launched itself, or its own `schedule.fired`, and fires at most `rate_max`
  times an hour (default 30), so agents that start agents can't loop. Each skip is a
  `trigger.skipped` event, and the first rate skip in an hour also sends one `notify`. The rules:
  - An event's hops are its `data.hops` when that is an integer (a `signal`, a `schedule.fired`
    of an event run), else its thread's hops at the time they are read, else 0.
  - A call with `cause: E` gets `hops(E) + 1`; from an agent thread, at least that thread's hops,
    so an agent can't lower its count by naming an old event.
  - An agent's call without `cause` records its thread's `cause` and `hops`; a person's, an
    unchecked caller's or a clock's has none and 0.
  - A schedule's call ignores the thread that made the schedule.
  - A delegated child gets its parent's `cause` and `hops`, or the calling thread's when those are
    more.
  - A send stores them on its message, and one with more hops than its thread raises the
    thread's `cause` and `hops` to its own; a coalescing launch that joins a waiting thread does
    the same to it.
- **Concurrency keys.** A launch with `concurrency_key` (such as `pm:glantfolk`) waits while
  another thread holds the key (`on_busy: "queue"`), or joins the one waiting launch for it
  (`"coalesce"`), so one project manager works on a project at a time and a burst of events
  becomes one thread. A thread holds its key until it settles, fails or is stopped, so a
  stopped or failed thread does not hold it; then the next waiting launch starts, oldest first.
  `coalesce` joins only a waiting launch of the same project and parent. A joining launch's
  `title`, `workspace`, `runner`, `model`, `requests`, `origin`, `audience` and `settle` are
  checked but not used: the waiting thread keeps its own.
- **One goal, then settled.** A launch with `settle: "on_result"` settles on its first
  `completed` or `failed` result instead of going idle. A `stopped` result, or any result while
  a stop is in progress, does not settle it: the thread goes idle, and the `completed` or
  `failed` result of a later message settles it. Its `thread.settled` event carries the result,
  so the next step can trigger on it.
- **Audience.** A thread is `person` (a person talks with it) or `agent` (the default). Thread
  Voice lists only `person` threads; the CLI sees all. An agent thread that needs a person asks
  through `thread-voice-mcp` as now, or is changed to `person` with `threads.update`. An event
  with no thread reaches a `person` follower of `threads.events` only when it is a
  `schedule.fired`, so a `person` follower gets no `signal` or `trigger.skipped`; an `agent`
  follower gets every event with no thread.
- **Missed events.** Events are stored (§4), and the scheduler keeps a cursor per schedule, in
  the `schedules` table, so after a restart each matching event since the cursor fires once. The
  cursor starts at the newest event when the schedule is added.
