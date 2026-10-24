# Loom API, `v1`

This page is for anyone who writes a Loom client: Thread Voice, the factory, `loom-mcp`, or a
script in any language. It documents the socket protocol `loom serve` speaks, every `v1` call and
event, `loom serve` itself, the `loom` CLI and the Python API. The design behind it is
`docs/design.md` §8; this page says what the code does today.

Served now: the twelve `threads.*` and `projects.*` calls. `requests.respond` comes with LM-9 and
the `schedules.*` calls with LM-6; the server answers them `invalid` until then.

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
pass on the socket. Thread ids such as `"3f2a…"` and the paths are shortened; everything else
comes from a real run. The command ids `c1`, `c2`, … give the order the changes ran in; each
read shows the state at its point in that run, and `projects.remove` succeeded once both threads
were settled.

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
{"call": "schedules.add", "params": {}}
{"ok":false,"error":{"code":"invalid","message":"schedules.add is not served yet"}}
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
  `threads.stop`, `threads.settle`, `projects.add`, `projects.set_mode` and `projects.remove`
  (and `requests.respond` and the `schedules.*` changes, once served).

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
Loom started, or when the server cannot tell who sent it:

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

## Results

Most calls answer with a thread or a project.

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
| `origin` | string or null | Who started it, as `threads.launch` was told. |
| `created_at` | string | ISO 8601, UTC. |

`permission_mode` is in every thread result, though `ThreadInfo` in
`loom_contracts/calls.py` does not list it yet.

### Project

| Field | Type | |
|---|---|---|
| `id` | string | The slug of the title, at most 40 characters, with `-2`, `-3`, … when taken (§5). |
| `title` | string | |
| `workspace_root` | string | The project's main checkout, an absolute path. |
| `permission_mode` | string or null | null means the server's default. |
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
| [`threads.events`](#threadsevents) | yes | no |
| [`projects.add`](#projectsadd) | yes | yes |
| [`projects.set_mode`](#projectsset_mode) | yes | yes |
| [`projects.remove`](#projectsremove) | yes | yes |
| [`projects.list`](#projectslist) | yes | no |
| [`requests.respond`](#requestsrespond) | LM-9 | yes |
| [`schedules.add`](#schedulesadd) | LM-6 | yes |
| [`schedules.list`](#scheduleslist) | LM-6 | no |
| [`schedules.pause`](#schedulespause) | LM-6 | yes |
| [`schedules.remove`](#schedulesremove) | LM-6 | yes |

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

**Result:** the new thread, in state `queued`. Loom starts it after it answers; follow
`threads.events` to see the turn.

**Errors:** `not_found` when the project does not exist; `invalid` for a missing or wrong
parameter.

```json
{"v": "v1", "call": "threads.launch", "params": {"id": "c3", "project_id": "demo-repo", "title": "Fix the login test", "message": "Make the login test pass.", "workspace": {"strategy": "root"}}}
{"ok":true,"result":{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":null,"model":null,"permission_mode":"auto-accept-edits","state":"queued","waiting":false,"reason":null,"parent_id":null,"origin":null,"created_at":"2026-10-24T18:47:12.958+00:00"}}
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
project with the project's mode, and it inherits the parent's `requests`.

**Result:** the child thread, in state `queued`.

**Errors:** `not_found` when the parent does not exist; `conflict` when the parent is settled,
failed or being stopped.

```json
{"v": "v1", "call": "threads.delegate", "params": {"id": "c5", "parent_id": "3f2a…", "task": "Review the login test fix."}}
{"ok":true,"result":{"id":"b836…","title":"Review the login test fix.","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":null,"model":null,"permission_mode":"auto-accept-edits","state":"queued","waiting":false,"reason":null,"parent_id":"3f2a…","origin":null,"created_at":"2026-10-24T18:47:29.240+00:00"}}
```

### `threads.send`

Sends a message to a thread. An `idle` thread starts a turn with it, and a `sleeping` one is
resumed first.

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
{"ok":true,"result":{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"idle","waiting":false,"reason":null,"parent_id":null,"origin":null,"created_at":"2026-10-24T18:47:12.958+00:00"}}
```

### `threads.list`

Lists threads, oldest first. Settled and failed threads are listed too.

| Param | Type | |
|---|---|---|
| `project_id` | string | Only this project's threads. An unknown project gives `[]`. |

**Result:** a list of threads.

```json
{"v": "v1", "call": "threads.list", "params": {"project_id": "demo-repo"}}
{"ok":true,"result":[{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"idle","waiting":false,"reason":null,"parent_id":null,"origin":null,"created_at":"2026-10-24T18:47:12.958+00:00"}]}
```

### `threads.get`

| Param | Type | |
|---|---|---|
| `thread_id` | string, required | |

**Result:** the thread. **Errors:** `not_found`.

```json
{"v": "v1", "call": "threads.get", "params": {"thread_id": "3f2a…"}}
{"ok":true,"result":{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"idle","waiting":false,"reason":null,"parent_id":null,"origin":null,"created_at":"2026-10-24T18:47:12.958+00:00"}}
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
{"ok":true,"result":{"id":"3f2a…","title":"Fix the login test","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"idle","waiting":false,"reason":"stopping","parent_id":null,"origin":null,"created_at":"2026-10-24T18:47:12.958+00:00"}}
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
{"ok":true,"result":{"id":"b836…","title":"Review the login test fix.","runner":"claude","project_id":"demo-repo","workspace_path":"/home/me/src/demo","branch":"main","model":null,"permission_mode":"auto-accept-edits","state":"settled","waiting":false,"reason":null,"parent_id":"3f2a…","origin":null,"created_at":"2026-10-24T18:47:29.240+00:00"}}
```

### `threads.events`

Reads the event log: a page of stored events, or a stream that goes on with new ones. See
[Events](#events) for each event's fields.

| Param | Type | |
|---|---|---|
| `after` | int | Only events with a larger id. Default `0`, the start. |
| `thread_id` | string | Only this thread's events. |
| `follow` | bool | Default `false`. `true` opens a stream. |

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

**Errors:** `invalid` when `after` is not an integer of 0 or more (`after must be an integer of 0
or more`) or `follow` is not a bool (`follow must be true or false`). An error answers the follow
request with one error line, and no stream starts.

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

### Not served yet

These calls are part of `v1` and their params are fixed in `loom_contracts/calls.py`, but the
server answers each one `invalid`, `<call> is not served yet`, until its issue is done. The Python
API already has a method for each; the CLI has no command for them.

#### `requests.respond`

Not served yet (LM-9). Answers a permission prompt (a `request.opened` event); the first answer
wins (§8 Requests).

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `request_id` | string, required | From the `request.opened` event. |
| `decision` | string, required | `allow` or `deny`. |
| `message` | string | A note for the agent. |

#### `schedules.add`

Not served yet (LM-6). Adds a schedule (§7).

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `trigger` | object, required | `{"type": "at", "at": "..."}`, `{"type": "interval", "everyMs": N}` (at least 60000), `{"type": "fixed_time", "timeOfDay": "HH:MM", "weekdays": [...]}` or `{"type": "webhook"}`. |
| `text` | string, required | What to post, launch with or notify. |
| `action` | string | `post`, `launch` or `notify`. |
| `thread_id` | string | |
| `project_id` | string | |
| `title` | string | |

#### `schedules.list`

Not served yet (LM-6).

| Param | Type | |
|---|---|---|
| `thread_id` | string | |
| `project_id` | string | |

#### `schedules.pause`

Not served yet (LM-6).

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `schedule_id` | string, required | |

#### `schedules.remove`

Not served yet (LM-6).

| Param | Type | |
|---|---|---|
| `id` | string, required | Command id. |
| `schedule_id` | string, required | |

## Events

Every change of a thread's state, every turn's text and tool calls, and every turn's result is an
event, with an id that only grows. Read them with [`threads.events`](#threadsevents). The state
in `threads.get` is the truth; events are a log to follow, kept 30 days (§4 Events).

```json
{"id": 5, "type": "turn.tool", "at": "2026-10-24T18:47:13.004+00:00", "thread_id": "3f2a…", "data": {"tool_use_id": "toolu_fake_1", "name": "Bash", "input": {"command": "true", "description": "Fake tool call 1"}}}
```

| Field | Type | |
|---|---|---|
| `id` | int | Grows by one or more with each event. |
| `type` | string | One of the nine types below. |
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

The thread failed; it comes right after the `thread.state` event with state `failed`.

| Field | Type | |
|---|---|---|
| `reason` | string | |

```json
{"id": 37, "type": "thread.failed", "at": "2026-10-24T18:53:55.140+00:00", "thread_id": "5b7e…", "data": {"reason": "the project's root is not the top of a git repo"}}
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
| `reason` | string | When there is one: `exited` when the agent process ended during the turn, `stop` when a stop cut the turn. |

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

A schedule ran. Not sent yet (LM-6).

| Field | Type | |
|---|---|---|
| `schedule_id` | string | |
| `action` | string | `post`, `launch` or `notify`. |
| `text` | string | |
| `by` | string | Who set the schedule, when known. |
| `late` | bool | `true` when a missed run ran late (§7). |

```json
{"id": 40, "type": "schedule.fired", "at": "2026-10-25T09:00:00.012+00:00", "thread_id": null, "data": {"schedule_id": "s-1", "action": "notify", "text": "Stand-up in five minutes.", "by": "bertil"}}
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
`agent-token` file you put there.

### `config.toml`

Every key is optional and every value is a string.

| Key | Default | |
|---|---|---|
| `claude_bin` | `claude` | The Claude Code program to run. |
| `claude_config_dir` | none | Given to the agent as `CLAUDE_CONFIG_DIR`. |
| `agent_base_url` | none | Given to the agent as `ANTHROPIC_BASE_URL`. |
| `permission_mode` | `auto` | The mode for projects with none; one of the [permission modes](#permission-modes). |

```toml
claude_bin = "/usr/local/bin/claude"
permission_mode = "auto-accept-edits"
```

An unknown key, a value that is not a string, a bad mode or a TOML error stops the server with
exit 2, naming the file and the problem.

### `agent-token`

`<data_dir>/agent-token` holds the agent's API token. Loom reads it each time it starts an agent
and gives it to Claude only as `ANTHROPIC_AUTH_TOKEN` in its environment (with `ANTHROPIC_API_KEY`
set empty). The file must be a regular file with mode 0600 (no group or other bits), at most
64 KiB, in UTF-8; the space around the token is dropped. When the file is missing or empty the
agent starts without a token. When it breaks a rule, the server writes one line
`loom: warning: not using <path>: <why>` to stderr, never the token, and starts the agent without
it.

### The agent's environment

Each agent gets only `HOME`, `LANG`, `LOGNAME`, `PATH`, `SHELL`, `USER` and `XDG_RUNTIME_DIR` from
the server's environment, those that are set, plus:

- `LOOM_SOCKET`: the server's socket path, so the agent can call Loom.
- `LOOM_THREAD`: the agent's own thread id. A `LOOM_THREAD` in the server's own environment is
  never passed on.
- The Claude settings above (§6 Environment). A message never goes in the agent's arguments.

### Output and exit codes

- When it listens, it writes `loom: serving on <socket path>` to stderr.
- On SIGINT or SIGTERM (Ctrl-C) it stops taking connections, ends follow streams, closes every
  agent, removes the socket file, writes `loom: stopped` and exits 0.
- A failure to start exits 2 with one line `loom: <reason>` on stderr: no socket path, a bad
  config, another server already answering on the socket, or a path that exists and is not a
  socket. A socket file nobody answers on is left over from an earlier run, and is replaced.

```text
$ loom serve
loom: no socket path: XDG_RUNTIME_DIR is not set, so pass --socket
```

Threads that were `running` or `idle` when the server stopped are not resumed on the next start
yet; that comes with LM-5.

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
| `loom threads launch <project_id> <title> <message> [--workspace root\|worktree\|existing_worktree] [--branch B] [--base B] [--path P] [--runner R] [--model M] [--requests caller\|any] [--origin O] [--id ID]` | `threads.launch` |
| `loom threads delegate <parent_id> <task> [--workspace ...] [--branch B] [--base B] [--path P] [--runner R] [--model M] [--id ID]` | `threads.delegate` |
| `loom threads send <thread_id> <message> [--mode steer\|queue\|interrupt] [--id ID]` | `threads.send` |
| `loom threads list [--project P]` | `threads.list` |
| `loom threads get <thread_id>` | `threads.get` |
| `loom threads stop <thread_id> [--id ID]` | `threads.stop` |
| `loom threads settle <thread_id> [--id ID]` | `threads.settle` |
| `loom threads events [--after N] [--thread T] [--follow]` | `threads.events` |
| `loom projects add <title> <path> [--mode M] [--id ID]` | `projects.add` |
| `loom projects set-mode <project_id> <mode> [--id ID]` | `projects.set_mode` |
| `loom projects remove <project_id> [--id ID]` | `projects.remove` |
| `loom projects list` | `projects.list` |

- `loom project ...` is the same as `loom projects ...`.
- `--branch` and `--base` go with `--workspace worktree`, `--path` and `--branch` with
  `--workspace existing_worktree`; any other mix is a usage error.
- `projects add` makes a relative path absolute from the current directory.
- A `<message>` or `<task>` of `-` is read from stdin to the end. Prefer it in scripts and agents:
  a message given as an argument shows in the `loom` process's command line, which other users on
  the machine can read.
- `--id` sets the command id, to retry a call safely (see
  [Command ids](#command-ids-and-safe-retries)). Without it, each run sends a new one.
- There are no `schedules` or `requests` commands yet (LM-6, LM-9).

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
  title. An event is one line: id, time, thread (or `-`), type and its data fields, with single
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
| 3 | Loom can't be reached, the connection dropped or the answer broke the protocol. stderr gets `loom: <what happened>`; a lost follow adds `resume with --after <last event id>`. |
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
  - `threads_launch(project_id, title, message, *, workspace=None, runner=None, model=None, requests=None, origin=None, id=None)`
  - `threads_delegate(parent_id, task, *, workspace=None, runner=None, model=None, id=None)`
  - `threads_send(thread_id, message, *, mode=None, id=None)`
  - `threads_list(project_id=None)`, `threads_get(thread_id)`
  - `threads_stop(thread_id, *, id=None)`, `threads_settle(thread_id, *, id=None)`
  - `threads_events(after=0, thread_id=None)`: one page
  - `projects_add(title, path, *, permission_mode=None, id=None)`
  - `projects_set_mode(project_id, permission_mode, *, id=None)`
  - `projects_remove(project_id, *, id=None)`, `projects_list()`
  - Not served yet: `requests_respond(request_id, decision, *, message=None, id=None)`,
    `schedules_add(trigger, text, *, action=None, thread_id=None, project_id=None, title=None, id=None)`,
    `schedules_list(thread_id=None, project_id=None)`, `schedules_pause(schedule_id, *, id=None)`,
    `schedules_remove(schedule_id, *, id=None)`.
- A call that changes something sends a new uuid4 `id` when `id` is `None`. Pass the same `id`
  again to retry it safely.
- `Client.follow_events(after=0, thread_id=None) -> Iterator[Event]`: yields every event after
  `after`, then each new one, until you stop iterating. When the connection is lost it raises
  `LoomConnectionError` whose `last_event_id` is the id of the last event it yielded (or `after`
  when none was); follow again from there.

### Errors

| Class | When |
|---|---|
| `LoomApiError` | The answer was `ok: false`. It has `.code` and `.message`. |
| `LoomConnectionError` | The socket can't be reached, or the connection dropped. It has `.message` and `.last_event_id` (an int after a lost follow, else `None`). |
| `LoomProtocolError` | A subclass of `LoomConnectionError`: the answer broke this protocol, for example a line that is not JSON. |
