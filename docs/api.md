# Loom API, `v1`

This page is for anyone who writes a Loom client: Thread Voice, the factory, `loom-mcp`, or a
script in any language. It documents the socket protocol `loom serve` speaks, every `v1` call and
event, `loom serve` itself, the `loom` CLI and the Python API. The design behind it is
`docs/design.md` §8; this page says what the code does today.

Served now: seventeen calls, the `threads.*`, `projects.*` and `schedules.*` ones. Only
`requests.respond` waits: it comes with LM-9, and the server answers it `invalid` until then.

## Start here

Start a server, then send it one JSON line and read one JSON line back:

```sh
uv sync --all-packages
uv run loom serve
```

In another terminal:

```sh
printf '%s\n' '{"call": "projects.list"}' | socat - UNIX-CONNECT:$XDG_RUNTIME_DIR/loom/api.sock
```

```json
{"ok":true,"result":[]}
```

The same from the CLI is `uv run loom projects list`, and from Python
`loom_client.Client().projects_list()`.

In the examples below, the first JSON line is the request and the next one the answer, as they
pass on the socket. Thread and schedule ids such as `"3f2a…"` and the paths are shortened;
everything else comes from a real run. The command ids `c1`, `c2`, … give the order the changes
ran in; each read shows the state at its point in that run, and `projects.remove` succeeded once
both threads were settled. The `schedules.*` examples, and those of `threads.update` and the
`audience` filters, come from later runs of their own.

## Transport

### The socket

- `loom serve` listens on `$XDG_RUNTIME_DIR/loom/api.sock`, or on the path its `--socket`
  option gives. The server never reads `LOOM_SOCKET`.
- A client finds the socket in `LOOM_SOCKET` when it is set and not empty, else at
  `$XDG_RUNTIME_DIR/loom/api.sock`. Every agent Loom starts has `LOOM_SOCKET` set.
- The socket file has mode 0600. Loom makes a missing socket directory with mode 0700.
- A local client needs no token. Nothing listens on a network port.
- Only processes of the user who runs Loom may talk to it (see [Callers](#callers)).

### Framing

UTF-8 JSON, one object per line, each line ending in `\n`. A connection carries any number of
requests. The server answers each with exactly one line, in order, before it reads the next.
Answers are compact and ASCII-only (other characters come as `\uXXXX` escapes), so parse them as
JSON instead of comparing bytes.

### Request

```json
{"v": "v1", "call": "projects.list", "params": {}}
```

- `v`: the API version. Leaving it out means `"v1"`.
- `call`: the call name, such as `"threads.launch"`.
- `params`: the call's parameters. Leaving it out means `{}`. The server ignores keys it does not
  know.

### Answer

```json
{"ok":true,"result":[]}
```

```json
{"ok":false,"error":{"code":"not_found","message":"no thread with id 'nope'"}}
```

`ok` is `true` with a `result`, or `false` with an `error` that has a `code` and a `message`.
Let your client act on `code`; `message` is for people and may change.

### Versions

`v1` is only ever added to: a new call, a new optional parameter or a new field in a result or
event. No `v1` field is changed or removed (§8 Stability). So ignore fields you don't know. A
breaking change will be `v2`, served next to `v1`.

### Error codes

| Code | When |
|---|---|
| `invalid` | The request is malformed, a parameter is missing or wrong, the call is unknown or not served yet, or the line is over the cap. |
| `not_found` | The thread or project the request names does not exist. |
| `conflict` | The request clashes with the current state: the thread has ended, the project still has a live thread, the path is already a project's root, or the command id was used for another call. |
| `forbidden` | The caller may not make this call (see [Callers](#callers)). |
| `failed` | Something broke inside Loom. The message is `internal error`; the traceback goes only to the server's stderr. |

### Bad requests

A bad request gets an `invalid` answer and the connection stays open, so the next line is read
as usual. The server checks in this order, and the messages are:

| Problem | Message |
|---|---|
| The line is not UTF-8 | `request is not UTF-8` |
| The line is not JSON (nesting too deep included) | `request is not JSON` |
| The JSON value is not an object | `request must be an object` |
| `v` is null, not a string or not `"v1"` | `v must be "v1"` |
| `call` is missing, null or not a string | `call must be a string` |
| `call` is not a `v1` call | `unknown call: <call>` |
| `call` is a `v1` call that is not served yet | `<call> is not served yet` |
| `params` is null or not an object | `params must be an object` |

```json
{"v": "v2", "call": "projects.list"}
{"ok":false,"error":{"code":"invalid","message":"v must be \"v1\""}}
```

```json
{"call": "requests.respond", "params": {}}
{"ok":false,"error":{"code":"invalid","message":"requests.respond is not served yet"}}
```

### Line cap

A request line may hold at most 1,048,576 bytes before its `\n`. A line of exactly 1,048,576
bytes is read and answered. Once 1,048,577 bytes have arrived without a `\n`, the server answers
this line and closes the connection without reading further:

```json
{"ok":false,"error":{"code":"invalid","message":"request line is over 1048576 bytes"}}
```

### Command ids and safe retries

Every call that changes something takes an `id`: a string of 1 to 200 characters that you make
up, for example a uuid4. It is required; without it the answer is `invalid`, `id must be a
string of 1 to 200 characters`. The CLI and the Python API make one for you.

- Sending the same call again with the same `id` changes nothing and returns the first answer
  word for word, even if the thread has moved on since (§4 Command ids). So when a connection
  drops before the answer arrives, send the request again with the same `id`.
- Using an `id` again for a different call is a `conflict`, `id was already used for <call>`.
- The calls that take an `id` are `threads.launch`, `threads.delegate`, `threads.send`,
  `threads.stop`, `threads.settle`, `threads.update`, `projects.add`, `projects.set_mode`,
  `projects.remove`, `schedules.add`, `schedules.pause` and `schedules.remove` (and
  `requests.respond`, once served).
- Loom's scheduler uses ids of the form `schedule:<schedule id>:<due>` for the calls its runs make
  (see [How schedules run](#how-schedules-run)), so don't choose ids that begin with `schedule:`.

```json
{"v": "v1", "call": "threads.stop", "params": {"id": "c3", "thread_id": "3f2a…"}}
{"ok":false,"error":{"code":"conflict","message":"id was already used for threads.launch"}}
```

## Callers

### Only the same user

When a client connects, the server reads the peer's process id and user id from the socket
(`SO_PEERCRED`). A peer that runs as another user gets one `forbidden` line, `the peer runs as
another user`, and the connection is closed. When the server cannot read the peer's credentials
at all, it answers `forbidden`, `cannot tell who the peer is`, and closes the connection the same
way. The client sees either one as the answer to its first request.

### Agents may not add projects or change a project's mode

`projects.add` and `projects.set_mode` answer `forbidden` when the request comes from a thread
Loom started, from a schedule (see [Calls a schedule makes](#calls-a-schedule-makes)), or when
the server cannot tell who sent it:

```json
{"v": "v1", "call": "projects.add", "params": {"id": "c13", "title": "Other", "path": "/tmp"}}
{"ok":false,"error":{"code":"forbidden","message":"agent threads may not add projects"}}
```

```json
{"v": "v1", "call": "projects.set_mode", "params": {"id": "c14", "project_id": "demo-repo", "permission_mode": "full-access"}}
{"ok":false,"error":{"code":"forbidden","message":"agent threads may not change a project's mode"}}
```

**Why.** A thread runs with its project's permission mode (§11). If an agent could change that
mode, it could widen its own project to `full-access` and then launch a thread with it (§9, §11).
The other calls stay open to agents: a thread an agent launches or delegates gets the project's
mode, never a wider one.

**How the server tells.** Every agent Loom starts has `LOOM_THREAD=<thread id>` in its
environment, and so does every process the agent starts unless it unsets it. The server takes
the peer's process id and walks up its parent chain in `/proc`, at most 64 processes.

- When the server itself is on that chain, the peer runs under Loom. Each process between the
  peer and the server is checked, so a child that unset `LOOM_THREAD` is still caught by its
  parent.
- When the server is not on the chain, only the peer itself is checked.
- A process counts as an agent when its starting environment (`/proc/<pid>/environ`) holds a
  non-empty `LOOM_THREAD`. A `loom` CLI run with `LOOM_THREAD` set counts too.
- When the peer's `/proc` entries can't be read, the server can't tell, and refuses these two
  calls.

**Limits.** This guards against an agent's own requests. It is not a sandbox.

- A process running as the same user can get round it: for example one that leaves Loom's process
  tree and unsets `LOOM_THREAD`, or one that writes Loom's store directly.
- Agents Loom did not start, such as a T3 thread or Claude Code in a terminal, are not seen as
  agents. They count as the user.
- `projects.remove` is not refused: it only removes the project's entry, and Loom refuses it
  while the project has a live thread.

### Calls a schedule makes

A schedule's maker is the caller of `schedules.add`, told apart as above: an agent thread, whose
id becomes the schedule's `by`, or a person (`by` null). When the schedule runs, the scheduler
makes its calls (`threads.send` or `threads.launch`, and the `threads.get` that checks for
[overlap](#how-schedules-run)) through the same service layer as a client's. Each call carries
the schedule's maker and the schedule's id:

- A schedule an agent made runs as that agent, so it can do no more than the agent could.
- A call that carries a schedule id never counts as a person, even for a schedule a person made.
  So `projects.add` and `projects.set_mode` refuse it (§11), and a schedule can't widen a
  project's mode.
- The checks `schedules.add` makes on its `thread_id` and `project_id` run as the caller itself.

Any caller may list, pause or remove any schedule.

## Results

Most calls answer with a thread, a project or a schedule.

### Thread

| Field | Type | |
|---|---|---|
| `id` | string | The thread id, a uuid. It is also the agent's session id. |
| `title` | string | |
| `runner` | string | `claude` (`codex` and `t3` come later). |
| `project_id` | string | |
| `workspace_path` | string | Where the agent works. |
| `branch` | string or null | The git branch. For a `root` workspace it is filled in when the thread starts. |
| `model` | string or null | null means the agent's default. |
| `permission_mode` | string | The mode the thread runs with (see [Permission modes](#permission-modes)). |
| `state` | string | `queued`, `starting`, `running`, `idle`, `sleeping`, `settled` or `failed` (§4 States). |
| `waiting` | bool | `true` while a running turn waits for an answer to a request. |
| `reason` | string or null | `stopping` while a stop is in progress, or why the thread failed. |
| `parent_id` | string or null | The thread that delegated this one. |
| `origin` | string or null | Who started it, as `threads.launch` was told. A thread a schedule launched has `schedule:<schedule id>`. |
| `audience` | string | `person` when a person talks with it, else `agent`, the default (§15 Audience). See [`threads.update`](#threadsupdate). |
| `created_at` | string | ISO 8601, UTC. |

### Project

| Field | Type | |
|---|---|---|
| `id` | string | The slug of the title, at most 40 characters, with `-2`, `-3`, … when taken (§5). |
| `title` | string | |
| `workspace_root` | string | The project's main checkout, an absolute path. |
| `permission_mode` | string or null | null means the server's default. |
| `created_at` | string | ISO 8601, UTC. |

### Schedule

| Field | Type | |
|---|---|---|
| `id` | string | The schedule id, a uuid. |
| `trigger` | object | The trigger as `schedules.add` was given it, with only its own keys (see [Triggers](#how-schedules-run)). |
| `action` | string | `post`, `launch` or `notify`. |
| `text` | string | What it posts, launches with or notifies. |
| `thread_id` | string or null | The thread a `post` goes into, or the thread a `notify` is about. null for a `launch`, and for a `notify` with no thread. |
| `project_id` | string or null | The project a `launch` starts its threads in; for a schedule with a thread, the thread's project. null for a `notify` with neither. |
| `title` | string or null | null means none was given; a `launch` then takes its thread's title from the text. |
| `by` | string or null | The agent thread that made it; null when a person did (see [Calls a schedule makes](#calls-a-schedule-makes)). |
| `paused` | bool | `true` once `schedules.pause` stopped it. |
| `next_run` | string or null | When it is next due, ISO 8601, UTC. null when it is paused, and once an `at` has had its one run. |
| `created_at` | string | ISO 8601, UTC. |

### Permission modes

`approval-required`, `auto-accept-edits`, `auto` and `full-access`, from narrowest to widest
(§5). A thread gets its project's mode when it is made. A project with no mode uses the
`permission_mode` key of `loom serve`'s config (default `auto`); a default of `full-access` is
used as `auto`, so only a project that names `full-access` gets it. Each time a thread's agent
starts or resumes, it takes the narrower of its own mode and its project's current one. So
narrowing a project narrows its threads the next time they start, while widening it never widens
threads that exist.

### Workspaces

The `workspace` parameter of `threads.launch` and `threads.delegate` is one of (§5):

- `{"strategy": "root"}`: the project's main checkout.
- `{"strategy": "worktree", "branch": "...", "base": "..."}`: a new git worktree at
  `<data_dir>/worktrees/<project id>/<thread id>`, on a new branch. `branch` defaults to
  `loom/<thread id>` and `base` to the project's current branch. The project's root must be the
  top of a git repo.
- `{"strategy": "existing_worktree", "path": "...", "branch": "..."}`: a checkout that already
  exists, either the project's root or a worktree Loom made for this project, on `branch`. Both
  keys are required.

A bad shape answers `invalid`. A problem found while making the worktree, after the answer (the
root is not a git repo, the branch exists already, the base is not a commit), makes the thread
`failed`, with a `thread.failed` event that gives the reason.

## Calls

| Call | Served | Changes something |
|---|---|---|
| [`threads.launch`](#threadslaunch) | yes | yes |
| [`threads.delegate`](#threadsdelegate) | yes | yes |
| [`threads.send`](#threadssend) | yes | yes |
| [`threads.list`](#threadslist) | yes | no |
| [`threads.get`](#threadsget) | yes | no |
| [`threads.stop`](#threadsstop) | yes | yes |
| [`threads.settle`](#threadssettle) | yes | yes |
| [`threads.update`](#threadsupdate) | yes | yes |
| [`threads.events`](#threadsevents) | yes | no |
| [`projects.add`](#projectsadd) | yes | yes |
| [`projects.set_mode`](#projectsset_mode) | yes | yes |
| [`projects.remove`](#projectsremove) | yes | yes |
| [`projects.list`](#projectslist) | yes | no |
| [`requests.respond`](#requestsrespond) | LM-9 | yes |
| [`schedules.add`](#schedulesadd) | yes | yes |
| [`schedules.list`](#scheduleslist) | yes | no |
| [`schedules.pause`](#schedulespause) | yes | yes |
| [`schedules.remove`](#schedulesremove) | yes | yes |

Any call can also answer `invalid` for a malformed request and `failed` for an internal error;
the lists below give only the errors particular to each call.

### `threads.launch`

Starts a new top-level thread in a project with a first message.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `project_id` | string, required | |
| `title` | string, required | Not empty. |
| `message` | string, required | The first message. Not empty. |
| `workspace` | object | Default `{"strategy": "worktree"}`. See [Workspaces](#workspaces). |
| `runner` | string | Default `"claude"`, the only runner served now; `codex` or `t3` answers `invalid`, `runner codex is not available`. |
| `model` | string | Not empty. Default: the agent's own. |
| `requests` | string | `"any"` (default) or `"caller"`: who may answer the thread's permission prompts (§8 Requests). Stored now, used once `requests.respond` is served. |
| `origin` | string | Who started the thread, for clients to show. |
| `audience` | string | `"agent"` (default) or `"person"`: who talks with the thread (§15 Audience). |

**Result:** the new thread, in state `queued`. Loom starts it after it answers; follow
`threads.events` to see the turn.

**Audience.** A launch without `audience` is `agent`, whoever calls: a person, an agent or a
schedule. Loom never guesses `person` from the caller, so a client whose threads a person talks
with, such as Thread Voice, sends `"audience": "person"`. A schedule's `launch` and a delegated
child are always `agent`. The audience only tells clients who the thread is for; it changes no
permission, and any caller may launch a `person` thread.

A start is never refused for lack of room. At most `agents_max` agent processes are live at once
(see [`config.toml`](#configtoml)). A start that finds no free slot first puts the thread that
has been `idle` longest to sleep (its process is closed and it goes to `sleeping`), then starts.
Only when every live process is in a turn does the start wait, `queued`, in §4's order: a
person's own words first, then agents' starts, then schedules, and the oldest first within each.
A thread `idle` for `idle_after` seconds goes to `sleeping` too; the next message resumes it.

**Errors:** `not_found` when the project does not exist; `invalid` for a missing or wrong
parameter, such as an `audience` that is null (`audience must not be null`) or not `person` or
`agent` (`audience must be one of person, agent`).

```json
{"v": "v1", "call": "threads.launch", "params": {"id": "c3", "project_id": "demo-repo", "title": "Fix the login test", "message": "Make the login test pass.", "workspace": {"strategy": "root"}}}
{"ok":true,"result":{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":null,"model":null,"permission_mode":"auto-accept-edits","state":"queued","waiting":false,"reason":null,"parent_id":null,"origin":null,"audience":"agent","created_at":"2026-10-24T18:47:12.958+00:00"}}
```

A thread a person talks with, from a later run:

```json
{"v": "v1", "call": "threads.launch", "params": {"id": "c3", "project_id": "demo-repo", "title": "Plan the week", "message": "What is on this week?", "workspace": {"strategy": "root"}, "audience": "person"}}
{"ok":true,"result":{"id":"31d4…","title":"Plan the week","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":null,"model":null,"permission_mode":"auto","state":"queued","waiting":false,"reason":null,"parent_id":null,"origin":null,"audience":"person","created_at":"2026-10-24T23:48:37.421+00:00"}}
```

### `threads.delegate`

Starts a child thread for a task. When one of the child's turns completes or fails, Loom posts
its result to the parent as a message, ahead of the parent's other queued messages:
`Task <id> (<title>) completed.` (or `failed: <reason>.`), then a blank line and the child's text.
Nothing is posted while the child or the parent is being stopped, or once the parent has ended.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `parent_id` | string, required | |
| `task` | string, required | The child's first message. Not empty. |
| `workspace` | object | Default: the parent's workspace (its root, or its worktree as `existing_worktree`). |
| `runner` | string | Default: the parent's runner. |
| `model` | string | Default: the parent's model. |

The child's title is the first line of `task`, cut at 80 characters. It runs in the parent's
project with the project's mode, and it inherits the parent's `requests`. Its audience is always
`agent`, whatever the parent's is.

**Result:** the child thread, in state `queued`. It starts as under
[`threads.launch`](#threadslaunch), as an agent's start: it may sleep the longest-idle thread or
wait `queued`, and is never refused.

**Errors:** `not_found` when the parent does not exist; `conflict` when the parent is settled,
failed or being stopped.

```json
{"v": "v1", "call": "threads.delegate", "params": {"id": "c5", "parent_id": "3f2a…", "task": "Review the login test fix."}}
{"ok":true,"result":{"id":"b836…","title":"Review the login test fix.","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":null,"model":null,"permission_mode":"auto-accept-edits","state":"queued","waiting":false,"reason":null,"parent_id":"3f2a…","origin":null,"audience":"agent","created_at":"2026-10-24T18:47:29.240+00:00"}}
```

### `threads.send`

Sends a message to a thread. An `idle` thread starts a turn with it, and a `sleeping` one is
resumed first. Resuming takes a slot as a start does under [`threads.launch`](#threadslaunch):
with none free, the longest-idle thread is put to sleep, or the thread waits `queued` until a
slot is free. The send is never refused for that. A message from an agent's thread ranks as an
agent's start, and a second message to a `queued` thread keeps its place in the queue and takes
the better rank. A message sent while its thread is being stopped is kept until the stop is
done; then, and after a restart, it ranks by its sender.

Loom keeps a message until it has handed it to the agent. When the agent process exits during a
turn, without the turn's result, the turn fails (`turn.result` with reason `exited`) and Loom
resumes the thread with the message `Your last turn was cut off; continue where you were.`,
ahead of its other messages, which follow in their usual order. That happens at most 3 times in
5 minutes; the next such exit makes the thread `failed` with the reason
`exited mid-turn after 3 continuations in 5 minutes`, and a delegated thread's parent gets that
failed result. An exit after the turn's result is not a crash: the thread only goes to
`sleeping`.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `thread_id` | string, required | |
| `message` | string, required | Not empty. |
| `mode` | string | What happens when a turn is running (§4 Dispatch modes): `steer` (default) adds it to the running turn, `queue` sends it after the turn, `interrupt` stops the turn and then sends it. |

**Result:** the thread as it was when the message was stored. The delivery happens after the
answer.

**Errors:** `not_found`; `conflict` when the thread is settled or failed.

```json
{"v": "v1", "call": "threads.send", "params": {"id": "c4", "thread_id": "3f2a…", "message": "Also run the linter.", "mode": "queue"}}
{"ok":true,"result":{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"idle","waiting":false,"reason":null,"parent_id":null,"origin":null,"audience":"agent","created_at":"2026-10-24T18:47:12.958+00:00"}}
```

### `threads.list`

Lists threads, oldest first. Settled and failed threads are listed too.

| Param | Type | |
|---|---|---|
| `project_id` | string | Only this project's threads. An unknown project gives `[]`. |
| `audience` | string | `"person"` or `"agent"`: only threads with this audience now. |

A thread is listed when it matches every filter given. With no filter, every thread is listed.

**Result:** a list of threads.

**Errors:** `invalid` when `audience` is null (`audience must not be null`) or not `person` or
`agent` (`audience must be one of person, agent`).

```json
{"v": "v1", "call": "threads.list", "params": {"project_id": "demo-repo"}}
{"ok":true,"result":[{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"idle","waiting":false,"reason":null,"parent_id":null,"origin":null,"audience":"agent","created_at":"2026-10-24T18:47:12.958+00:00"}]}
```

```json
{"v": "v1", "call": "threads.list", "params": {"audience": "person"}}
{"ok":true,"result":[{"id":"31d4…","title":"Plan the week","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"master","model":null,"permission_mode":"auto","state":"idle","waiting":false,"reason":null,"parent_id":null,"origin":null,"audience":"person","created_at":"2026-10-24T23:48:37.421+00:00"}]}
```

### `threads.get`

| Param | Type | |
|---|---|---|
| `thread_id` | string, required | |

**Result:** the thread. **Errors:** `not_found`.

```json
{"v": "v1", "call": "threads.get", "params": {"thread_id": "3f2a…"}}
{"ok":true,"result":{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"idle","waiting":false,"reason":null,"parent_id":null,"origin":null,"audience":"agent","created_at":"2026-10-24T18:47:12.958+00:00"}}
```

```json
{"v": "v1", "call": "threads.get", "params": {"thread_id": "nope"}}
{"ok":false,"error":{"code":"not_found","message":"no thread with id 'nope'"}}
```

### `threads.stop`

Stops a thread and every live thread it started, all the way down (§4 Stop). Their queued
messages are dropped. A running turn is interrupted; when it has not ended 5 seconds later, the
agent process is closed, the turn ends `stopped` and the thread goes to `sleeping`. An interrupted
turn leaves the thread `idle`. A stopped thread can take messages again afterwards.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `thread_id` | string, required | |

**Result:** the thread. While the stop is in progress its `reason` is `stopping`; a
`thread.state` event without the reason follows when it is done. A settled or failed thread is
returned as it is.

**Errors:** `not_found`.

```json
{"v": "v1", "call": "threads.stop", "params": {"id": "c6", "thread_id": "3f2a…"}}
{"ok":true,"result":{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"idle","waiting":false,"reason":"stopping","parent_id":null,"origin":null,"audience":"agent","created_at":"2026-10-24T18:47:12.958+00:00"}}
```

### `threads.settle`

Marks a thread done: it goes to `settled`, its queued messages are dropped and its agent process
is closed. A settled thread takes no more messages. Settling a settled thread again returns it
unchanged.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `thread_id` | string, required | |

**Result:** the thread, in state `settled`.

**Errors:** `not_found`; `conflict` for a failed thread (`thread <id> cannot go from failed to
settled`).

```json
{"v": "v1", "call": "threads.settle", "params": {"id": "c7", "thread_id": "b836…"}}
{"ok":true,"result":{"id":"b836…","title":"Review the login test fix.","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"settled","waiting":false,"reason":null,"parent_id":"3f2a…","origin":null,"audience":"agent","created_at":"2026-10-24T18:47:29.240+00:00"}}
```

### `threads.update`

Changes a thread's audience (§15 Audience), for example to `person` when an agent thread needs a
person to talk with it. A change writes one [`thread.updated`](#threadupdated) event. Updating a
thread to the audience it already has changes nothing and writes no event; the answer is the
thread as it is. It works in any state, `settled` and `failed` included, and touches no turn,
process or permission mode. Any caller may update any thread.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `thread_id` | string, required | |
| `audience` | string, required | `"person"` or `"agent"`. |

**Result:** the thread after the update.

**Errors:** `not_found` (`no thread with id '<thread_id>'`); `invalid` when `thread_id` is
missing or null (`thread_id is required`) or not a string (`thread_id must be a string`), or when
`audience` is missing or null (`audience is required`) or not `person` or `agent` (`audience must
be one of person, agent`).

```json
{"v": "v1", "call": "threads.update", "params": {"id": "c4", "thread_id": "0844…", "audience": "person"}}
{"ok":true,"result":{"id":"0844…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"master","model":null,"permission_mode":"auto","state":"idle","waiting":false,"reason":null,"parent_id":null,"origin":null,"audience":"person","created_at":"2026-10-24T23:48:37.309+00:00"}}
```

```json
{"v": "v1", "call": "threads.update", "params": {"id": "e5", "thread_id": "0844…"}}
{"ok":false,"error":{"code":"invalid","message":"audience is required"}}
```

### `threads.events`

Reads the event log: a page of stored events, or a stream that goes on with new ones. See
[Events](#events) for each event's fields.

| Param | Type | |
|---|---|---|
| `after` | int | Only events with a larger id. Default `0`, the start. |
| `thread_id` | string | Only this thread's events. |
| `audience` | string | `"person"` or `"agent"`: only events of threads with this audience, as below. |
| `follow` | bool | Default `false`. `true` opens a stream. |

An event is kept when it matches every filter given. With no filter, every event is kept.

**Without `follow`**, the result is a list of at most 1,000 events after `after`, oldest first.
To read everything, ask again with `after` set to the last id you got, until a page holds fewer
than 1,000 events.

```json
{"v": "v1", "call": "threads.events", "params": {"after": 6, "thread_id": "3f2a…"}}
{"ok":true,"result":[{"id":7,"type":"turn.result","at":"2026-10-24T18:47:13.017+00:00","thread_id":"3f2a…","data":{"state":"completed","text":"Make the login test pass."}},{"id":8,"type":"thread.state","at":"2026-10-24T18:47:13.017+00:00","thread_id":"3f2a…","data":{"state":"idle","waiting":false}}]}
```

**With `follow: true`**, the answer is the ack `{"follow": true}`. The server then writes each
event as its own line `{"event": <event>}`, in id order: first every stored event after `after`,
then each new one as Loom records it. The stream ends when the client closes the connection or
the server stops. The server reads no more requests on that connection, so use another
connection for other calls.

```json
{"v": "v1", "call": "threads.events", "params": {"after": 6, "thread_id": "3f2a…", "follow": true}}
{"ok":true,"result":{"follow":true}}
{"event":{"id":7,"type":"turn.result","at":"2026-10-24T18:47:13.017+00:00","thread_id":"3f2a…","data":{"state":"completed","text":"Make the login test pass."}}}
{"event":{"id":8,"type":"thread.state","at":"2026-10-24T18:47:13.017+00:00","thread_id":"3f2a…","data":{"state":"idle","waiting":false}}}
{"event":{"id":9,"type":"thread.state","at":"2026-10-24T18:47:22.559+00:00","thread_id":"3f2a…","data":{"state":"running","waiting":false}}}
```

**Resuming.** Remember the id of the last event you handled. When the connection drops, follow
again with `after` set to that id: you get every event after it, with nothing missed or repeated.
Events are kept 30 days.

**The `audience` filter.** With `audience: A`, an event is kept when any of these holds:

- its thread has audience `A` now;
- it is a [`thread.updated`](#threadupdated) event whose `previous_audience` is `A`, so a
  follower sees a thread leave its audience;
- its `thread_id` is null, such as the `schedule.fired` event of a `notify` with no thread.

The filter reads each thread's audience at the time of the read, not the one it had when the
event was written. So once a thread becomes `person`, a reader with `audience: "person"` that
reads from `after: 0` gets the thread's whole history, and a follower gets its events from then
on. When it goes back to `agent`, a follower gets that `thread.updated` event and none of the
thread's after it, and a reader from 0 then gets, of its history, only the `thread.updated`
events that took it out of `person`.

A follower of `person` threads: the thread `0844…`, already `person`, takes a turn, is changed
back to `agent` and takes another turn (events 25 to 30, not sent), and then a `notify` schedule
with no thread fires:

```json
{"v": "v1", "call": "threads.events", "params": {"after": 17, "audience": "person", "follow": true}}
{"ok":true,"result":{"follow":true}}
{"event":{"id":18,"type":"thread.state","at":"2026-10-24T23:49:04.196+00:00","thread_id":"0844…","data":{"state":"running","waiting":false}}}
{"event":{"id":19,"type":"turn.started","at":"2026-10-24T23:49:04.215+00:00","thread_id":"0844…","data":{}}}
{"event":{"id":20,"type":"turn.tool","at":"2026-10-24T23:49:04.239+00:00","thread_id":"0844…","data":{"tool_use_id":"toolu_fake_2","name":"Bash","input":{"command":"true","description":"Fake tool call 2"}}}}
{"event":{"id":21,"type":"turn.text","at":"2026-10-24T23:49:04.258+00:00","thread_id":"0844…","data":{"text":"Also run the linter."}}}
{"event":{"id":22,"type":"turn.result","at":"2026-10-24T23:49:04.279+00:00","thread_id":"0844…","data":{"state":"completed","text":"Also run the linter."}}}
{"event":{"id":23,"type":"thread.state","at":"2026-10-24T23:49:04.279+00:00","thread_id":"0844…","data":{"state":"idle","waiting":false}}}
{"event":{"id":24,"type":"thread.updated","at":"2026-10-24T23:49:05.220+00:00","thread_id":"0844…","data":{"audience":"agent","previous_audience":"person"}}}
{"event":{"id":31,"type":"schedule.fired","at":"2026-10-24T23:49:06.312+00:00","thread_id":null,"data":{"schedule_id":"cba9…","action":"notify","text":"Stand-up in five minutes.","late":true}}}
```

**Errors:** `invalid` when `after` is not an integer of 0 or more (`after must be an integer of 0
or more`), `follow` is not a bool (`follow must be true or false`), or `audience` is null
(`audience must not be null`) or not `person` or `agent` (`audience must be one of person,
agent`). An error answers the follow request with one error line, and no stream starts.

### `projects.add`

Registers a directory as a project, so threads can run in it.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `title` | string, required | Must hold a letter or digit; the project's id is its slug. |
| `path` | string, required | An absolute path to an existing directory, usually a git checkout. Loom stores it resolved. |
| `permission_mode` | string | One of the [permission modes](#permission-modes). Default: none, so the server's default applies. |

**Result:** the new project.

**Errors:** `invalid` (`path must be absolute`, `path must name an existing directory`, `title
must have a letter or digit`, a bad mode); `conflict` when another project already has this path
as its root; `forbidden` for an agent (see [Callers](#callers)).

```json
{"v": "v1", "call": "projects.add", "params": {"id": "c1", "title": "Demo repo", "path": "/home/me/src/demo"}}
{"ok":true,"result":{"id":"demo-repo","title":"Demo repo","workspace_root":"/home/me/src/demo","permission_mode":null,"created_at":"2026-10-24T18:47:12.927+00:00"}}
```

### `projects.set_mode`

Changes a project's permission mode. See [Permission modes](#permission-modes) for how it reaches
threads that already exist.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `project_id` | string, required | |
| `permission_mode` | string, required | One of the [permission modes](#permission-modes). |

**Result:** the project.

**Errors:** `not_found`; `invalid` for a bad mode; `forbidden` for an agent (see
[Callers](#callers)).

```json
{"v": "v1", "call": "projects.set_mode", "params": {"id": "c2", "project_id": "demo-repo", "permission_mode": "auto-accept-edits"}}
{"ok":true,"result":{"id":"demo-repo","title":"Demo repo","workspace_root":"/home/me/src/demo","permission_mode":"auto-accept-edits","created_at":"2026-10-24T18:47:12.927+00:00"}}
```

### `projects.remove`

Removes a project's entry. It deletes no files, worktrees or threads.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `project_id` | string, required | |

**Result:** the removed project.

**Errors:** `not_found`; `conflict` while the project has a live thread (any state but `settled`
or `failed`): settle its threads first.

```json
{"v": "v1", "call": "projects.remove", "params": {"id": "c8", "project_id": "demo-repo"}}
{"ok":false,"error":{"code":"conflict","message":"the project still has a live thread"}}
```

```json
{"v": "v1", "call": "projects.remove", "params": {"id": "c10", "project_id": "demo-repo"}}
{"ok":true,"result":{"id":"demo-repo","title":"Demo repo","workspace_root":"/home/me/src/demo","permission_mode":"auto-accept-edits","created_at":"2026-10-24T18:47:12.927+00:00"}}
```

### `projects.list`

Lists projects, oldest first. It takes no params.

**Result:** a list of projects.

```json
{"v": "v1", "call": "projects.list"}
{"ok":true,"result":[{"id":"demo-repo","title":"Demo repo","workspace_root":"/home/me/src/demo","permission_mode":null,"created_at":"2026-10-24T18:47:12.927+00:00"}]}
```

### How schedules run

A schedule runs a text on a trigger, with one of three actions (§7). The scheduler starts and
stops with `loom serve`. It wakes when the first schedule is due, and at least every 30 s, so it
notices a jump of the clock, such as a laptop waking from sleep.

**Triggers.** `trigger.type` is one of these three:

| Trigger | Due | Checks |
|---|---|---|
| `{"type": "at", "at": "2026-10-25T09:00"}` | Once, at `at`. A time in the past is due at once. | `at` is an ISO 8601 date and time. Without an offset it is in the machine's local time zone. |
| `{"type": "interval", "everyMs": 3600000}` | First `everyMs` after the schedule is added, then `everyMs` after each run or skip. | `everyMs` is an integer from 60000 (60 s) to 31536000000 (365 days). |
| `{"type": "fixed_time", "timeOfDay": "9:00", "weekdays": [1, 2, 3, 4, 5]}` | At `timeOfDay` on each listed day, in the machine's local time zone. | `timeOfDay` is `H:MM` or `HH:MM`, from `0:00` to `23:59`. `weekdays` are 0 to 6, 0 is Sunday and 6 is Saturday; missing or `[]` means every day. |

`{"type": "webhook"}` is part of `v1`, but `schedules.add` refuses it with `invalid` until Loom
can be reached from outside (§7). An interval is timed from when its last run ran or was
skipped, not from when it was due, so it never catches up with a burst.

**Actions.** `action` is one of these three:

- `post` (the default): sends the text into the schedule's thread with `threads.send` and
  `mode: "queue"`, so it waits for a running turn instead of steering it. Without `thread_id`,
  the thread is the caller's own, so an agent's schedule posts into the agent's thread. A person
  must give `thread_id`.
- `launch`: starts a new thread in `project_id` with `threads.launch`. The text is its first
  message. Its title is the schedule's `title`, or else the text's first non-empty line, cut at
  80 characters. Its `origin` is `schedule:<schedule id>`. It gets the default workspace (a new
  worktree) and the project's mode. `launch` needs `project_id` and takes no `thread_id`.
- `notify`: makes no call. The run only sends a [`schedule.fired`](#schedulefired) event, for a
  client such as Thread Voice to speak. Its thread is `thread_id`, or the caller's own, or none.

**Late runs.** A run is late when it starts more than 60 s after it was due; exactly 60 s is on
time. A late `post` or `launch` sends its text after a first line and a blank line:

```text
This run is late: it was due at 2026-10-24T22:20:00.000+00:00.

Look at the login test again.
```

Every late run, `notify` too, has `late: true` in its `schedule.fired` event.

**Missed runs.** A run is missed when Loom was not running at its time, or the machine slept.
The first pass after the server starts, or after the clock jumps, runs each schedule at most
once, never a burst:

- An `at` runs once, however late, and says it is late.
- An `interval` runs once, and its next run is `everyMs` after that.
- A `fixed_time` run more than 10 minutes late is skipped and logged (`skipped_late`), and the
  schedule goes on at its next time. One exactly 10 minutes late still runs, late.

**Overlap.** Before a `post` or `launch` run, the scheduler gets the thread the schedule's last
fired run posted into or launched. When that thread is `queued`, `starting` or `running`, the run
is skipped and logged (`skipped_busy`), not queued. For a `post`, a turn someone else started in
the thread counts too. `notify` runs are never skipped this way.

**Runs.** Each run is a row in the `schedule_runs` table of `loom.db`: when it was due, when it
ran, whether it was late, the thread it posted into or launched, and its outcome. Rows older than
30 days are deleted each time the server starts, and removing a schedule deletes its rows. No `v1`
call reads them.

| Outcome | When |
|---|---|
| `fired` | The run posted, launched or notified. Only these send `schedule.fired`. |
| `skipped_late` | A `fixed_time` run more than 10 minutes late. |
| `skipped_busy` | A `post` or `launch` whose last thread is still going (see Overlap). |
| `failed` | The call failed, for example because the thread was settled. The row keeps the error message, cut at 500 characters. The run is not tried again; the schedule goes on at its next time. |

**No run twice.** A `post` or `launch` run calls with the command id `schedule:<schedule
id>:<due>`, with `<due>` in ISO 8601, UTC. When Loom stops after the call and before the run's
row is written, the run is tried again with the same id, so the server returns the first answer
instead of posting or launching again (see [Command ids](#command-ids-and-safe-retries)). A
restart never repeats a run.

**Pausing.** A paused schedule stays paused: `v1` has no call to resume it (§8). To run it again,
remove it and add it again.

### `schedules.add`

Adds a schedule (§7). See [How schedules run](#how-schedules-run) for the triggers and actions.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `trigger` | object, required | An `at`, `interval` or `fixed_time` trigger. |
| `text` | string, required | Not empty. What it posts, launches with or notifies. |
| `action` | string | `post` (default), `launch` or `notify`. |
| `thread_id` | string | For `post`, the thread to post into; for `notify`, the thread it is about. Default: the caller's own thread. Not with `launch`. |
| `project_id` | string | Required for `launch`. With a thread, it must be the thread's project, which is the default. |
| `title` | string | Not empty. For `launch`, the title of each thread it starts. |

A param given as null is refused. The thread and the project must exist when the schedule is
added.

**Result:** the new [schedule](#schedule). Its `by` is the caller's thread, or null for a person,
and its `next_run` is its first due time. A schedule due already, such as an `at` in the past,
runs right after the answer.

**Errors:**

| Problem | Code | Message |
|---|---|---|
| A missing or bad `id` | `invalid` | `id must be a string of 1 to 200 characters` |
| No `trigger` | `invalid` | `trigger is required` |
| `trigger` is not an object | `invalid` | `trigger must be an object` |
| No `trigger.type` | `invalid` | `trigger.type is required` |
| A `webhook` trigger | `invalid` | `webhook triggers are not served yet` |
| Another `trigger.type` | `invalid` | `trigger.type must be one of at, interval, fixed_time` |
| No `at`, `everyMs` or `timeOfDay` for its type | `invalid` | `trigger.<field> is required` |
| A bad `at` | `invalid` | `trigger.at must be an ISO 8601 date and time` |
| A bad `everyMs` | `invalid` | `trigger.everyMs must be an integer from 60000 to 31536000000` |
| A bad `timeOfDay` | `invalid` | `trigger.timeOfDay must be H:MM or HH:MM, from 0:00 to 23:59` |
| `weekdays` is null | `invalid` | `trigger.weekdays must not be null` |
| A bad `weekdays` | `invalid` | `trigger.weekdays must be a list of integers from 0 (Sunday) to 6 (Saturday)` |
| No `text` | `invalid` | `text is required` |
| An empty or non-string `text` | `invalid` | `text must be a non-empty string` |
| A bad `action` | `invalid` | `action must be one of post, launch, notify` |
| `action`, `thread_id`, `project_id` or `title` is null | `invalid` | `<field> must not be null` |
| `thread_id` or `project_id` is not a string | `invalid` | `<field> must be a string` |
| An empty or non-string `title` | `invalid` | `title must be a non-empty string` |
| A `post` from a person without `thread_id` | `invalid` | `thread_id is required to post from outside a thread` |
| A `launch` with `thread_id` | `invalid` | `thread_id does not go with launch` |
| A `launch` without `project_id` | `invalid` | `project_id is required to launch` |
| `project_id` is not the thread's | `invalid` | `project_id is not the thread's project` |
| The thread does not exist | `not_found` | `no thread with id '<thread_id>'` |
| The project does not exist | `not_found` | `no project with id '<project_id>'` |
| The `id` was used for another call | `conflict` | `id was already used for <call>` |

```json
{"v": "v1", "call": "schedules.add", "params": {"id": "c3", "trigger": {"type": "interval", "everyMs": 3600000}, "text": "Run the tests and report what fails.", "thread_id": "8a63…"}}
{"ok":true,"result":{"id":"2cbf…","trigger":{"type":"interval","everyMs":3600000},"action":"post","text":"Run the tests and report what fails.","thread_id":"8a63…","project_id":"demo-repo","title":null,"by":null,"paused":false,"next_run":"2026-10-24T23:31:13.320+00:00","created_at":"2026-10-24T22:31:13.320+00:00"}}
```

Added just after midnight on Sunday in Stockholm, a weekday stand-up at 9:00 is next due on
Monday at 08:00 UTC, because the clocks go back an hour that night:

```json
{"v": "v1", "call": "schedules.add", "params": {"id": "c4", "trigger": {"type": "fixed_time", "timeOfDay": "9:00", "weekdays": [1, 2, 3, 4, 5]}, "text": "Write the stand-up notes.", "action": "launch", "project_id": "demo-repo", "title": "Stand-up notes"}}
{"ok":true,"result":{"id":"0a5f…","trigger":{"type":"fixed_time","timeOfDay":"9:00","weekdays":[1,2,3,4,5]},"action":"launch","text":"Write the stand-up notes.","thread_id":null,"project_id":"demo-repo","title":"Stand-up notes","by":null,"paused":false,"next_run":"2026-10-26T08:00:00.000+00:00","created_at":"2026-10-24T22:31:13.342+00:00"}}
```

```json
{"v": "v1", "call": "schedules.add", "params": {"id": "e11", "trigger": {"type": "interval", "everyMs": 3600000}, "text": "x"}}
{"ok":false,"error":{"code":"invalid","message":"thread_id is required to post from outside a thread"}}
```

### `schedules.list`

Lists schedules, oldest first. Paused schedules and `at` schedules that have run are listed too,
until they are removed.

| Param | Type | |
|---|---|---|
| `thread_id` | string | Only schedules with this `thread_id`. |
| `project_id` | string | Only schedules with this `project_id`. An unknown one gives `[]`. |

**Result:** a list of [schedules](#schedule).

**Errors:** `invalid` when a filter is null (`thread_id must not be null`) or not a string
(`thread_id must be a string`).

```json
{"v": "v1", "call": "schedules.list", "params": {"project_id": "demo"}}
{"ok":true,"result":[{"id":"403b…","trigger":{"type":"fixed_time","timeOfDay":"9:00","weekdays":[1,2,3,4,5]},"action":"launch","text":"Write the stand-up notes.","thread_id":null,"project_id":"demo","title":"Stand-up notes","by":null,"paused":false,"next_run":"2026-10-26T08:00:00.000+00:00","created_at":"2026-10-24T22:33:10.399+00:00"}]}
```

### `schedules.pause`

Stops a schedule: it runs no more, and keeps its row and its runs. There is no resume in `v1`; to
run it again, remove it and add it again. Pausing a paused schedule changes nothing.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `schedule_id` | string, required | |

**Result:** the [schedule](#schedule), with `paused: true` and `next_run: null`.

**Errors:** `not_found` (`no schedule with id '<schedule_id>'`); `invalid` when `schedule_id` is
missing (`schedule_id is required`) or not a string (`schedule_id must be a string`).

```json
{"v": "v1", "call": "schedules.pause", "params": {"id": "c8", "schedule_id": "2cbf…"}}
{"ok":true,"result":{"id":"2cbf…","trigger":{"type":"interval","everyMs":3600000},"action":"post","text":"Run the tests and report what fails.","thread_id":"8a63…","project_id":"demo-repo","title":null,"by":null,"paused":true,"next_run":null,"created_at":"2026-10-24T22:31:13.320+00:00"}}
```

### `schedules.remove`

Deletes a schedule and its runs. The `schedule.fired` events it sent stay in the event log.

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `schedule_id` | string, required | |

**Result:** the [schedule](#schedule) as it was before it was removed.

**Errors:** `not_found` (`no schedule with id '<schedule_id>'`); `invalid` when `schedule_id` is
missing (`schedule_id is required`) or not a string (`schedule_id must be a string`).

```json
{"v": "v1", "call": "schedules.remove", "params": {"id": "c9", "schedule_id": "2cbf…"}}
{"ok":true,"result":{"id":"2cbf…","trigger":{"type":"interval","everyMs":3600000},"action":"post","text":"Run the tests and report what fails.","thread_id":"8a63…","project_id":"demo-repo","title":null,"by":null,"paused":true,"next_run":null,"created_at":"2026-10-24T22:31:13.320+00:00"}}
```

```json
{"v": "v1", "call": "schedules.remove", "params": {"id": "c10", "schedule_id": "2cbf…"}}
{"ok":false,"error":{"code":"not_found","message":"no schedule with id '2cbf…'"}}
```

### Not served yet

This call is part of `v1` and its params are fixed in `loom_contracts/calls.py`, but the server
answers it `invalid`, `requests.respond is not served yet`, until its issue is done. The Python
API already has a method for it; the CLI has no command for it.

#### `requests.respond`

Not served yet (LM-9). Answers a permission prompt (a `request.opened` event); the first answer
wins (§8 Requests).

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `request_id` | string, required | From the `request.opened` event. |
| `decision` | string, required | `allow` or `deny`. |
| `message` | string | A note for the agent. |

## Events

Every change of a thread's state or audience, every turn's text and tool calls, every turn's
result and every schedule run that fired is an event, with an id that only grows. Read them with
[`threads.events`](#threadsevents). The state in `threads.get` is the truth; events are a log to
follow, kept 30 days (§4 Events).

```json
{"id": 5, "type": "turn.tool", "at": "2026-10-24T18:47:13.004+00:00", "thread_id": "3f2a…", "data": {"tool_use_id": "toolu_fake_1", "name": "Bash", "input": {"command": "true", "description": "Fake tool call 1"}}}
```

| Field | Type | |
|---|---|---|
| `id` | int | Grows by one or more with each event. |
| `type` | string | One of the ten types below. |
| `at` | string | ISO 8601, UTC, with milliseconds. |
| `thread_id` | string or null | |
| `data` | object | Depends on `type`. |

A turn in the log runs `thread.state` (`running`), `turn.started`, any number of `turn.tool` and
`turn.text`, `turn.result`, then `thread.state` (`idle`). Text in events is written by the agent:
show it as text, never as markup or terminal codes (§11). A client should skip event types it does
not know.

### `thread.state`

The thread's state changed, or its `waiting` flag or `reason` did.

| Field | Type | |
|---|---|---|
| `state` | string | The new state. |
| `waiting` | bool | |
| `reason` | string | Only when there is one: `stopping`, or why it failed. |

```json
{"id": 29, "type": "thread.state", "at": "2026-10-24T18:47:37.072+00:00", "thread_id": "3f2a…", "data": {"state": "idle", "waiting": false, "reason": "stopping"}}
```

### `thread.failed`

The thread failed; it comes right after the `thread.state` event with state `failed`. A thread
whose agent process keeps exiting mid-turn fails with the reason
`exited mid-turn after 3 continuations in 5 minutes` (see [`threads.send`](#threadssend)).

| Field | Type | |
|---|---|---|
| `reason` | string | |

```json
{"id": 37, "type": "thread.failed", "at": "2026-10-24T18:53:55.140+00:00", "thread_id": "5b7e…", "data": {"reason": "the project's root is not the top of a git repo"}}
```

### `thread.updated`

The thread's audience changed, by [`threads.update`](#threadsupdate). An update to the audience
the thread already has sends none.

| Field | Type | |
|---|---|---|
| `audience` | string | The new audience, `person` or `agent`. |
| `previous_audience` | string | The audience it had before. |

Both fields are always there.

```json
{"id": 17, "type": "thread.updated", "at": "2026-10-24T23:48:49.036+00:00", "thread_id": "0844…", "data": {"audience": "person", "previous_audience": "agent"}}
```

### `turn.started`

The agent started a turn. Its `data` is `{}`.

```json
{"id": 4, "type": "turn.started", "at": "2026-10-24T18:47:12.997+00:00", "thread_id": "3f2a…", "data": {}}
```

### `turn.text`

Text the agent wrote.

| Field | Type | |
|---|---|---|
| `text` | string | |

```json
{"id": 6, "type": "turn.text", "at": "2026-10-24T18:47:13.010+00:00", "thread_id": "3f2a…", "data": {"text": "Make the login test pass."}}
```

### `turn.tool`

The agent called a tool.

| Field | Type | |
|---|---|---|
| `tool_use_id` | string | The agent's own id for this tool call. |
| `name` | string | The tool, such as `Bash`. |
| `input` | object | The tool's input, as the agent sent it. |

```json
{"id": 5, "type": "turn.tool", "at": "2026-10-24T18:47:13.004+00:00", "thread_id": "3f2a…", "data": {"tool_use_id": "toolu_fake_1", "name": "Bash", "input": {"command": "true", "description": "Fake tool call 1"}}}
```

### `turn.result`

The turn ended.

| Field | Type | |
|---|---|---|
| `state` | string | `completed`, `failed` or `stopped`. |
| `text` | string | The agent's final text, when it gave one. |
| `reason` | string | When there is one: `exited` when the agent process ended during the turn, `restart` when `loom serve` stopped during the turn, `stop` when a stop cut the turn. |

```json
{"id": 7, "type": "turn.result", "at": "2026-10-24T18:47:13.017+00:00", "thread_id": "3f2a…", "data": {"state": "completed", "text": "Make the login test pass."}}
```

### `request.opened`

The agent asks for permission to run a tool, and the thread is `waiting`. Nothing can answer it
until `requests.respond` is served (LM-9), so for now a thread that asks waits until it is
stopped.

| Field | Type | |
|---|---|---|
| `request_id` | string | |
| `tool_name` | string | |
| `input` | object | |
| `description` | string | Only when the agent gave one. |

```json
{"id": 12, "type": "request.opened", "at": "2026-10-24T19:02:41.120+00:00", "thread_id": "3f2a…", "data": {"request_id": "req-1", "tool_name": "Bash", "input": {"command": "rm -rf build"}, "description": "Remove the build directory"}}
```

### `request.closed`

A request was answered. Not sent yet (LM-9).

| Field | Type | |
|---|---|---|
| `request_id` | string | |
| `decision` | string | `allow` or `deny`. |

```json
{"id": 13, "type": "request.closed", "at": "2026-10-24T19:02:55.004+00:00", "thread_id": "3f2a…", "data": {"request_id": "req-1", "decision": "allow"}}
```

### `schedule.fired`

A schedule's run fired: it posted, launched or notified (see
[How schedules run](#how-schedules-run)). It is sent once for each fired run, and never for a
run that was skipped or failed.

The event's `thread_id` is the thread the run posted into or launched. For a `notify` it is the
schedule's `thread_id`, or null when the schedule has none.

| Field | Type | |
|---|---|---|
| `schedule_id` | string | |
| `action` | string | `post`, `launch` or `notify`. |
| `text` | string | The schedule's text, without the late line. |
| `late` | bool | Always there. `true` when the run started more than 60 s after it was due (§7). |
| `by` | string | The agent thread that made the schedule. Left out when a person made it. |

An agent's reminder, added six minutes after its `at`, so it ran at once and late:

```json
{"id": 9, "type": "schedule.fired", "at": "2026-10-24T22:31:20.775+00:00", "thread_id": "8a63…", "data": {"schedule_id": "9370…", "action": "notify", "text": "Stand-up in five minutes.", "late": true, "by": "8a63…"}}
```

A person's `launch`, on time, names the thread it started:

```json
{"id": 18, "type": "schedule.fired", "at": "2026-10-24T22:32:00.039+00:00", "thread_id": "7865…", "data": {"schedule_id": "f842…", "action": "launch", "text": "Check the nightly build.\nSay what broke.", "late": false}}
```

## `loom serve`

```sh
loom serve [--socket PATH] [--data-dir DIR] [--config FILE]
python -m loom_service serve [--socket PATH] [--data-dir DIR] [--config FILE]
```

Both forms are the same server: `loom serve` replaces itself with `python -m loom_service serve`
(same interpreter, same options), and exits 2 with `loom: loom-service is not installed` when the
package is missing. `loom --socket PATH serve` passes `--socket` on too.

| Option | Default |
|---|---|
| `--socket` | `$XDG_RUNTIME_DIR/loom/api.sock`. With `XDG_RUNTIME_DIR` unset or empty and no `--socket`, it exits 2. |
| `--data-dir` | `$XDG_DATA_HOME/loom`, else `~/.local/share/loom`. Mode 0700. |
| `--config` | `$XDG_CONFIG_HOME/loom/config.toml`, else `~/.config/loom/config.toml`. A missing file means all defaults. |

The data dir holds the store `loom.db` (mode 0600), the `worktrees/` Loom makes, and the
`agent-token` file you put there. `loom.db` also holds the schedules and their runs (the
`schedules` and `schedule_runs` tables). The scheduler starts with the server, once it listens,
and stops when the server stops. Runs missed while the server was down follow the
[missed-run rules](#how-schedules-run) on the first pass after it starts.

### `config.toml`

Every key is optional. `agents_max` and `idle_after` are whole numbers of 1 or more; every
other value is a string.

| Key | Default | |
|---|---|---|
| `claude_bin` | `claude` | The Claude Code program to run. |
| `claude_config_dir` | none | Given to the agent as `CLAUDE_CONFIG_DIR`. |
| `agent_base_url` | none | Given to the agent as `ANTHROPIC_BASE_URL`. |
| `permission_mode` | `auto` | The mode for projects with none; one of the [permission modes](#permission-modes). |
| `agents_max` | The available memory at start, less 2 GiB, divided by 350 MiB, at least 1 | How many agent processes may be live at once (§4). |
| `idle_after` | `1200` | Seconds a thread may stay `idle` before it goes to `sleeping`. Loom checks every 30 s. |
| `codex_bin` | `codex` | The Codex program to run. When its directory holds `node`, that directory goes on the end of the agent's `PATH`. |
| `codex_home` | none | Given to Codex as `CODEX_HOME`. |

```toml
claude_bin = "/usr/local/bin/claude"
permission_mode = "auto-accept-edits"
agents_max = 8
idle_after = 1800
```

An unknown key, a string key whose value is not a string, a bad mode or a TOML error stops the
server with exit 2, naming the file and the problem. So does an `agents_max` or `idle_after`
that is not a whole number of 1 or more (a string, a float, `true`, 0 or less):
`<path>: agents_max must be a whole number of 1 or more`.

### `agent-token`

`<data_dir>/agent-token` holds the agent's API token. Loom reads it each time it starts an agent
and gives it to Claude only as `ANTHROPIC_AUTH_TOKEN` in its environment (with `ANTHROPIC_API_KEY`
set empty). The file must be a regular file with mode 0600 (no group or other bits), at most
64 KiB, in UTF-8; the space around the token is dropped. When the file is missing or empty the
agent starts without a token. When it breaks a rule, the server writes one line
`loom: warning: not using <path>: <why>` to stderr, never the token, and starts the agent without
it.

Loom gives the same token to Codex only as `CLIPROXY_API_KEY`, and never gives Codex the
`ANTHROPIC_*` variables.

### The agent's environment

Each agent gets only `HOME`, `LANG`, `LOGNAME`, `PATH`, `SHELL`, `USER` and `XDG_RUNTIME_DIR` from
the server's environment, those that are set, plus:

- `LOOM_SOCKET`: the server's socket path, so the agent can call Loom.
- `LOOM_THREAD`: the agent's own thread id. A `LOOM_THREAD` in the server's own environment is
  never passed on.
- The Claude settings above (§6 Environment). A message never goes in the agent's arguments.
- For a Codex thread: `CODEX_HOME` from `codex_home` and `CLIPROXY_API_KEY` from the token, with
  `codex_bin`'s directory appended to `PATH` when it holds `node`.

### Codex threads

The runner value `codex` is served by `threads.launch` and `threads.delegate` (`--runner codex`
in the CLI). This replaces the older notes that `codex` comes later and that it answers
`runner codex is not available`; `t3` still answers `invalid`, `runner t3 is not available`.

A Codex thread's `id` is Loom's uuid, not Codex's session id. Codex picks its own thread id (its
`thread.started` line), which Loom keeps in `<data_dir>/codex/<thread id>` (the folder mode 0700,
each file 0600) to resume the thread after it sleeps or the server restarts. The data dir's
`codex/` folder holds these records.

Codex runs in the sandbox of the thread's permission mode:

| Mode | Sandbox |
|---|---|
| `approval-required` | `read-only` |
| `auto-accept-edits` | `workspace-write` |
| `auto` | `workspace-write` |
| `full-access` | `danger-full-access` |

Codex never asks, so its threads open no requests. A `steer` message to a running Codex thread
waits for the turn to end. `interrupt` ends the turn's process and leaves the thread `idle`.

### Output and exit codes

- When it listens, it writes `loom: agents_max <n>` (the configured value, or the one it
  computed from the available memory) and then `loom: serving on <socket path>` to stderr.
- On SIGINT or SIGTERM (Ctrl-C) it stops taking connections, ends follow streams, stops the
  scheduler, closes every agent, removes the socket file, writes `loom: stopped` and exits 0.
  Threads keep their state in the store. On the next start, threads that were `running` or
  `idle` are `sleeping`; a turn that was cut off fails (`turn.result` with reason `restart`) and
  resumes with the continuation message, as under [`threads.send`](#threadssend); queued starts
  stay queued and then start. These starts, the resumes included, take slots in the queue's
  order, and only as many run at once as `agents_max` allows. A stop that was in progress ends
  there.
- A failure to start exits 2 with one line `loom: <reason>` on stderr: no socket path, a bad
  config, another server already answering on the socket, a path that exists and is not a
  socket, or a store whose schedules can't be opened (`cannot open the schedules in <data dir>:
  <error>`). A socket file nobody answers on is left over from an earlier run, and is replaced.
  With no `agents_max` in the config and no way to read the available memory (`MemAvailable`
  in `/proc/meminfo`, else `sysconf`), it exits 2 with
  `loom: cannot read the available memory; set agents_max in the config` and leaves no socket
  file.

```text
$ loom serve
loom: no socket path: XDG_RUNTIME_DIR is not set, so pass --socket
```

## The `loom` CLI

`loom` is a client like any other: one command per served call. It comes with `loom-client`
(`uv run loom` in this repo, or `python -m loom_client`).

### Global options

They go before the command.

- `--socket PATH`: the socket. Default `LOOM_SOCKET` when set and not empty, else
  `$XDG_RUNTIME_DIR/loom/api.sock`.
- `--json`: print the call's `result` as one line of JSON. `threads events` prints one JSON event
  per line instead.

### Commands

| Command | Call |
|---|---|
| `loom serve [--socket PATH] [--data-dir DIR] [--config FILE]` | starts the server, see [`loom serve`](#loom-serve) |
| `loom threads launch <project_id> <title> <message> [--workspace root\|worktree\|existing_worktree] [--branch B] [--base B] [--path P] [--runner R] [--model M] [--requests caller\|any] [--origin O] [--audience person\|agent] [--id ID]` | `threads.launch` |
| `loom threads delegate <parent_id> <task> [--workspace ...] [--branch B] [--base B] [--path P] [--runner R] [--model M] [--id ID]` | `threads.delegate` |
| `loom threads send <thread_id> <message> [--mode steer\|queue\|interrupt] [--id ID]` | `threads.send` |
| `loom threads list [--project P] [--audience person\|agent]` | `threads.list` |
| `loom threads get <thread_id>` | `threads.get` |
| `loom threads stop <thread_id> [--id ID]` | `threads.stop` |
| `loom threads settle <thread_id> [--id ID]` | `threads.settle` |
| `loom threads update <thread_id> --audience person\|agent [--id ID]` | `threads.update` |
| `loom threads events [--after N] [--thread T] [--audience person\|agent] [--follow]` | `threads.events` |
| `loom projects add <title> <path> [--mode M] [--id ID]` | `projects.add` |
| `loom projects set-mode <project_id> <mode> [--id ID]` | `projects.set_mode` |
| `loom projects remove <project_id> [--id ID]` | `projects.remove` |
| `loom projects list` | `projects.list` |
| `loom schedules add <text> (--at DATETIME \| --every SECONDS \| --time H:MM [--weekdays LIST]) [--action post\|launch\|notify] [--thread T] [--project P] [--title T] [--id ID]` | `schedules.add` |
| `loom schedules list [--thread T] [--project P]` | `schedules.list` |
| `loom schedules pause <schedule_id> [--id ID]` | `schedules.pause` |
| `loom schedules remove <schedule_id> [--id ID]` | `schedules.remove` |

- `loom project ...` is the same as `loom projects ...`, and `loom schedule ...` as
  `loom schedules ...`.
- `--branch` and `--base` go with `--workspace worktree`, `--path` and `--branch` with
  `--workspace existing_worktree`; any other mix is a usage error.
- `--audience` sends `audience`: on `threads launch` the new thread's audience, on `threads list`
  and `threads events` a filter (see [`threads.events`](#threadsevents) for which events it
  keeps). Without it, nothing is sent. `threads update` requires it.
- `projects add` makes a relative path absolute from the current directory.
- `schedules add` takes exactly one trigger option, else it is a usage error:
  - `--at DATETIME` sends `{"type": "at", "at": DATETIME}`, with the time as given.
  - `--every SECONDS`, a whole number of 1 or more, sends
    `{"type": "interval", "everyMs": SECONDS * 1000}`.
  - `--time H:MM` sends `{"type": "fixed_time", "timeOfDay": H:MM}`. With `--weekdays LIST`,
    comma-separated whole numbers with 0 for Sunday (as `1,2,3,4,5` for Monday to Friday), it
    also sends `"weekdays"`. `--weekdays` without `--time` is a usage error.

  The server checks the values (the 60 s floor, weekdays 0 to 6, the form of the time) and
  answers `invalid`, exit 1. `--action`, `--thread`, `--project` and `--title` send `action`,
  `thread_id`, `project_id` and `title`; one not given is not sent.
- A `<message>`, `<task>` or `<text>` of `-` is read from stdin to the end. Prefer it in scripts
  and agents: a message given as an argument shows in the `loom` process's command line, which
  other users on the machine can read.
- `--id` sets the command id, to retry a call safely (see
  [Command ids](#command-ids-and-safe-retries)). Without it, each run sends a new one.
- There are no `requests` commands yet (LM-9).

### Examples

Each example ran against a real server; the output is the human form, without `--json`.

```text
$ loom projects add Demo .
demo  default  /home/me/src/demo  Demo
$ loom projects set-mode demo auto-accept-edits
demo  auto-accept-edits  /home/me/src/demo  Demo
$ loom projects list
demo  auto-accept-edits  /home/me/src/demo  Demo
$ loom threads launch demo "Fix the login test" "Make the login test pass." --workspace root
be2a…  queued  demo  Fix the login test
$ loom threads list --project demo
be2a…  idle  demo  Fix the login test
$ loom threads get be2a…
id: be2a…
title: Fix the login test
runner: claude
project_id: demo
workspace_path: /home/me/src/demo
branch: main
model: -
permission_mode: auto-accept-edits
state: idle
waiting: false
reason: -
parent_id: -
origin: -
created_at: 2026-10-24T18:54:07.454+00:00
$ echo "Also run the linter." | loom threads send be2a… - --mode queue
be2a…  idle  demo  Fix the login test
$ loom threads events --thread be2a… --after 46
47 2026-10-24T18:54:08.678+00:00 be2a… turn.started
48 2026-10-24T18:54:08.682+00:00 be2a… turn.tool Bash {"command": "true", "description": "Fake tool call 2"} toolu_fake_2
49 2026-10-24T18:54:08.687+00:00 be2a… turn.text Also run the linter.\n
50 2026-10-24T18:54:08.692+00:00 be2a… turn.result completed Also run the linter.\n
51 2026-10-24T18:54:08.692+00:00 be2a… thread.state idle
$ loom threads delegate be2a… "Review the fix."
bd48…  queued  demo  Review the fix.
$ loom threads stop be2a…
be2a…  idle  demo  Fix the login test  stopping
$ loom threads settle bd48…
bd48…  settled  demo  Review the fix.
$ loom projects remove demo
loom: conflict: the project still has a live thread
$ loom threads settle be2a…
be2a…  settled  demo  Fix the login test
$ loom projects remove demo
demo  auto-accept-edits  /home/me/src/demo  Demo
```

Schedules, in another run with a project `demo`, at 00:33 on a Sunday in Stockholm (22:33 UTC
on Saturday). The `--at` time was five minutes in the past, so that reminder ran at once, late:

```text
$ loom threads launch demo "Fix the login test" "Make the login test pass." --workspace root
f6c4…  queued  demo  Fix the login test
$ loom schedules add "Run the tests and report what fails." --every 3600 --thread f6c4…
bc65…  2026-10-24T23:33:10.311+00:00  post  every 3600s  f6c4…  Run the tests and report what fails.
$ loom schedules add "Write the stand-up notes." --time 9:00 --weekdays 1,2,3,4,5 --action launch --project demo --title "Stand-up notes"
403b…  2026-10-26T08:00:00.000+00:00  launch  9:00 on 1,2,3,4,5  demo  Stand-up notes
$ loom schedules add "Stand-up in five minutes." --at 2026-10-25T00:28 --action notify
e6c4…  2026-10-24T22:28:00.000+00:00  notify  at 2026-10-25T00:28  -  Stand-up in five minutes.
$ loom schedules list
bc65…  2026-10-24T23:33:10.311+00:00  post  every 3600s  f6c4…  Run the tests and report what fails.
403b…  2026-10-26T08:00:00.000+00:00  launch  9:00 on 1,2,3,4,5  demo  Stand-up notes
e6c4…  done  notify  at 2026-10-25T00:28  -  Stand-up in five minutes.
$ loom threads events --after 8
9 2026-10-24T22:33:10.492+00:00 - schedule.fired e6c4… notify late Stand-up in five minutes.
$ loom schedules pause bc65…
bc65…  paused  post  every 3600s  f6c4…  Run the tests and report what fails.
$ loom schedules remove bc65…
bc65…  paused  post  every 3600s  f6c4…  Run the tests and report what fails.
$ loom schedules add "Ping." --every 30 --action notify
loom: invalid: trigger.everyMs must be an integer from 60000 to 31536000000
$ loom schedules add "Ping." --every 60
loom: invalid: thread_id is required to post from outside a thread
```

Audiences, in another run with a project `demo-repo`, an agent thread `0844…` and a thread
`31d4…` launched with `"audience": "person"`. Event 31 is a `notify` with no thread, so every
filter keeps it:

```text
$ loom threads list
0844…  idle  demo-repo  Fix the login test
31d4…  idle  demo-repo  Plan the week
$ loom threads list --audience person
31d4…  idle  demo-repo  Plan the week
$ loom threads update 0844… --audience person
0844…  idle  demo-repo  Fix the login test
$ loom threads list --audience person
0844…  idle  demo-repo  Fix the login test
31d4…  idle  demo-repo  Plan the week
$ loom threads events --audience person --after 30
31 2026-10-24T23:49:06.312+00:00 - schedule.fired cba9… notify late Stand-up in five minutes.
32 2026-10-24T23:49:20.137+00:00 0844… thread.updated person
$ loom threads update 0844…
usage: loom threads update [-h] --audience {person,agent} [--id ID] thread_id
loom threads update: error: the following arguments are required: --audience
```

### Following events

`loom threads events` without `--follow` prints every event after `--after` (default 0), asking
page after page until a page holds fewer than 1,000, and exits. With `--follow` it goes on
printing new events until you stop it with Ctrl-C (exit 130) or the server goes away. Then it
exits 3 and says where to resume:

```text
$ loom threads events --follow --after 80
81 2026-10-24T18:48:32.639+00:00 1abc… turn.result completed Make the login test pass.
82 2026-10-24T18:48:32.639+00:00 1abc… thread.state idle
loom: lost the connection to Loom at /run/user/1000/loom/api.sock after event 82; resume with --after 82
```

Run it again with `--after 82` to go on with nothing missed or repeated. With `--json`, each line
is the event object, so any language can follow Loom by reading the CLI's stdout:

```json
{"id": 51, "type": "turn.tool", "at": "2026-10-24T18:48:12.644+00:00", "thread_id": "9a03…", "data": {"tool_use_id": "toolu_fake_3", "name": "Bash", "input": {"command": "true", "description": "Fake tool call 3"}}}
```

### Output

- With `--json`, stdout gets the result as one line of JSON, or one line per event.
- Without it, a short human form, with two spaces between fields. A thread is one line: id,
  state (and `waiting`), project, title, and the reason when there is one. `threads get` prints
  one `field: value` line per field. A project is one line: id, mode (or `default`), root and
  title. A schedule is one line: id, state (`paused` when paused, else its `next_run`, else
  `done`), action, trigger (`at <at>`, `every <seconds>s`, `<timeOfDay>` or
  `<timeOfDay> on <weekdays>`), the thread or else the project it targets, and its title, else
  its text. An event is one line: id, time, thread (or `-`), type and its data fields, with single
  spaces. A null prints as `-`.
- Agent-written text is shown as text: each control character (newline and tab too, DEL and the
  C1 range) prints as an escape such as `\n`, `\t` or `\x1b`, so each event is one line. Each text
  is cut at 2,000 characters and then marked `…`; `--json` prints it whole.

### Exit codes

| Code | When |
|---|---|
| 0 | Done. |
| 1 | The API answered `ok: false`. stderr gets `loom: <code>: <message>`; with `--json`, stdout also gets `{"error": {"code": ..., "message": ...}}`. |
| 2 | A usage error: an unknown command, a missing argument, an option outside its choices. |
| 3 | Loom can't be reached, the connection dropped or the answer broke the protocol. stderr gets `loom: <what happened>`; a lost follow adds `resume with --after <last event id>`, and a write command whose connection was lost after it reached Loom adds `retry with --id <the id it sent>`. |
| 130 | Ctrl-C, without a traceback. |

```text
$ loom --json threads get nope
{"error": {"code": "not_found", "message": "no thread with id 'nope'"}}
loom: not_found: no thread with id 'nope'
$ loom --socket /tmp/nothing.sock threads list
loom: cannot reach Loom at /tmp/nothing.sock: No such file or directory
```

## The Python API

`loom_client` is a stdlib-only client, in the `loom-client` package (it depends only on
`loom-contracts`).

```python
import os

from loom_client import Client, LoomApiError, LoomConnectionError

client = Client()
project = client.projects_add('Demo', os.getcwd())
thread = client.threads_launch(project['id'], 'Fix the login test', 'Make the login test pass.',
                               workspace={'strategy': 'root'})
try:
    for event in client.follow_events(thread_id=thread['id']):
        print(event['id'], event['type'], event['data'])
        if event['type'] == 'turn.result':
            break
except LoomConnectionError as error:
    print('lost; resume with after =', error.last_event_id)

try:
    client.threads_get('nope')
except LoomApiError as error:
    print(error.code, error.message)
```

```text
75 thread.state {'state': 'queued', 'waiting': False}
76 thread.state {'state': 'starting', 'waiting': False}
77 thread.state {'state': 'running', 'waiting': False}
78 turn.started {}
79 turn.tool {'tool_use_id': 'toolu_fake_1', 'name': 'Bash', 'input': {'command': 'true', 'description': 'Fake tool call 1'}}
80 turn.text {'text': 'Make the login test pass.'}
81 turn.result {'state': 'completed', 'text': 'Make the login test pass.'}
not_found no thread with id 'nope'
```

### `Client`

- `default_socket() -> str`: `LOOM_SOCKET` when set and not empty, else
  `$XDG_RUNTIME_DIR/loom/api.sock`; with neither it raises `LoomConnectionError`.
- `Client(socket_path=None, timeout=30.0)`: `None` means `default_socket()`. Each call opens one
  connection, sends one request and reads one answer; `timeout` is in seconds.
- `Client.call(name, params) -> object`: the raw call. It returns `result` and raises
  `LoomApiError` on `ok: false`.
- One method per `v1` call, named after it with `_` for `.`. Required params are positional; the
  rest are keywords, and a keyword left `None` is not sent:
  - `threads_launch(project_id, title, message, *, workspace=None, runner=None, model=None, requests=None, origin=None, audience=None, id=None)`
  - `threads_delegate(parent_id, task, *, workspace=None, runner=None, model=None, id=None)`
  - `threads_send(thread_id, message, *, mode=None, id=None)`
  - `threads_list(project_id=None, *, audience=None)`, `threads_get(thread_id)`
  - `threads_stop(thread_id, *, id=None)`, `threads_settle(thread_id, *, id=None)`
  - `threads_update(thread_id, audience, *, id=None)`
  - `threads_events(after=0, thread_id=None, *, audience=None)`: one page
  - `projects_add(title, path, *, permission_mode=None, id=None)`
  - `projects_set_mode(project_id, permission_mode, *, id=None)`
  - `projects_remove(project_id, *, id=None)`, `projects_list()`
  - `schedules_add(trigger, text, *, action=None, thread_id=None, project_id=None, title=None, id=None)`
  - `schedules_list(thread_id=None, project_id=None)`
  - `schedules_pause(schedule_id, *, id=None)`, `schedules_remove(schedule_id, *, id=None)`
  - Not served yet: `requests_respond(request_id, decision, *, message=None, id=None)`.
- A call that changes something sends a new uuid4 `id` when `id` is `None`. Pass the same `id`
  again to retry it safely.
- `Client.follow_events(after=0, thread_id=None, *, audience=None) -> Iterator[Event]`: yields
  every event after `after`, then each new one, until you stop iterating. When the connection is
  lost it raises `LoomConnectionError` whose `last_event_id` is the id of the last event it
  yielded (or `after` when none was); follow again from there.

### Errors

| Class | When |
|---|---|
| `LoomApiError` | The answer was `ok: false`. It has `.code` and `.message`. |
| `LoomConnectionError` | The socket can't be reached, or the connection dropped. It has `.message` and `.last_event_id` (an int after a lost follow, else `None`). |
| `LoomProtocolError` | A subclass of `LoomConnectionError`: the answer broke this protocol, for example a line that is not JSON. |
