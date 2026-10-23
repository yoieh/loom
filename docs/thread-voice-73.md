# Loom: what it inherits from Thread Voice §73

Copied on 2026-10-23 from Thread Voice's `docs/design-no-t3.md` (main at the time), where
Thread Voice planned to run its own agents. Loom now does that instead (`docs/design.md` §1).
The sections keep their §73 numbers, and `docs/design.md` cites them so. Where they say "the
daemon", read "Loom's service"; where they name Thread Voice files (`server.py`,
`progress.py`, `thread-voice-mcp`, …), they describe the client side, which stays in Thread
Voice. `docs/design.md` wins where the two disagree.

### §73.4 The local host

- **One Claude Code process per active topic.** `agents.py` starts

  ```
  claude -p --input-format stream-json --output-format stream-json --verbose
         --session-id <uuid> --model <m> --permission-mode <mode>
         --permission-prompts host --disallowed-tools AskUserQuestion
  ```

  with `claude_bin` and the same environment T3 gives its Claude today (`CLAUDE_CONFIG_DIR`,
  the proxy settings), in the topic's worktree. Arguments are a list, never a shell string. The
  user's words go in on stdin as stream-json user messages, never on the command line.
- **The session id is the thread id.** The daemon picks the uuid, so `CLAUDE_CODE_SESSION_ID`
  in `thread-voice-mcp` matches the thread with no lookup.
- **Turns.** A message sent while the agent is idle starts a turn; one sent while it works is
  queued and written when the turn's `result` event arrives (steering is spike A's question).
  The stream's events drive `run_status`: written → `queued`, first `assistant` event →
  `running`, a permission request → `waiting`, `result` → `completed` or `failed`. **Changes
  §45**: `progress.py` gets these statuses from the host, and `needs_t3` becomes `needs_you`.
- **Idle and resume.** A process with no turn for `idle_after` seconds (existing key) is ended;
  the next message starts `claude -p --resume <uuid>` with the same flags. A daemon restart ends
  every process and resumes each on its next message. At most `local_agents_max` (new key,
  default 4) processes run at once; later starts wait in order.
- **Permission mode.** The project's `permission_mode` (§73.5), else `topic_runtime_mode`,
  default `auto`: the agent does what it can on its own, and only what Claude still holds for a
  yes reaches the user.
- **Questions and approvals.** `AskUserQuestion` is turned off, and the skills already tell
  agents to use `ask`, so §72 isn't needed for local threads. A permission request arrives on
  the stream (`--permission-prompts host`); the daemon turns it into a pinned `ask` with Allow
  and Deny, read aloud like any ask, and writes the answer back. No answer within the ask's
  timeout means deny.
- **Stop.** `stop` sends the stream's interrupt and, after 5 s, ends the process.
- **Settle.** Settled state, activity and titles live in the daemon's own store. Settling ends
  the process.

### §73.4.1 Codex on the local host

- The user wants to keep running Codex agents, as T3 allows today. The local host runs them
  too: a project or persona sets `agent` to `claude` (default) or `codex`.
- A Codex turn runs `codex exec --json -C <worktree> -m <model> -s <sandbox> <message>`, and
  later turns `codex exec resume <session-id> --json …`, one process per turn; the message goes
  on stdin (`-`), never in argv. Turn state comes from the JSON events, as in §73.4.
- `install` registers `thread-voice-mcp` with Codex as well (`codex mcp add`), so Codex agents
  get `speak` and `ask`. That fixes stage 5's gap where Codex threads couldn't use Thread Voice
  at all (`docs/design-stage5.md`, Codex lacks the tools).
- **Identity** (**changes §8**): Codex sets no `CLAUDE_CODE_SESSION_ID`, so the daemon passes
  `THREAD_VOICE_THREAD=<thread id>` in every agent's environment, and `thread-voice-mcp` sends
  it before falling back to `CLAUDE_CODE_SESSION_ID`.
- Codex's approvals follow its sandbox mode; what Codex asks interactively in `exec` (if
  anything) is spike A's question (9).

### §73.9 Security

- The daemon starts agents only from the user's own words (a call, `start_topic`, the dial
  sheet) and only in a registered project's root or its topic worktree.
- The process gets the daemon's environment minus Thread Voice's own secrets (the push keys,
  the T3 token).
- Permission requests are answered only by the user, through an ask; the daemon never allows
  anything on its own. `topic_runtime_mode` maps to `--permission-mode` and is checked against
  Claude's list.
- The agent log is agent-written text: shown with `textContent`, capped at 2,000 characters
  per entry, never run as markup.
- The `projects`, `reminders` and log tables live in the existing store (mode 0600).

### §73.13 Spike results

Run by job 233 on 2026-10-22 with Claude Code 2.1.301 (`/home/yoieh/.local/bin/claude`) and
codex-cli 0.163.0 (T3's `codex-proxy` binary), against a fake daemon on a scratch socket, in a
throwaway git repo. Claude used `claude-opus-5-5` and Codex `gpt-6.1-sol`, both through the
local proxy, so start times include the proxy's latency. Each case ran once, and the `auto`
classifier's choices may differ from run to run. The agent environment was built from an
allowlist: `HOME LANG LOGNAME PATH SHELL USER XDG_RUNTIME_DIR`, T3's `claude-proxy` variables
for Claude (`CODEX_HOME` and `CLIPROXY_API_KEY` for Codex), `THREAD_VOICE_RUNTIME_DIR` and
`THREAD_VOICE_THREAD`. The evidence is in `factory/jobs/233-no-t3-spike/build.md` and
`spike-raw.md`.

**1. Turns, and a message while a turn runs.**

- One process ran three turns ("Remember the word heron", "Reply with the word you
  remembered" → `heron`, "Reply with OK"). It kept the same pid, and each `result` carried the
  `--session-id` uuid as `session_id`.
- Claude writes nothing to stdout until the first user message arrives. No `system`/`init`
  came in 10 s of waiting; it came 0.05 s after the write. Then an `init` comes again at the
  start of *every* turn. `thread-voice-mcp` starts at spawn anyway: its hello came 0.37 s after
  spawn (wall 1792672242.481 → 1792672242.849 in `turns`), before any message.
- A message written while a turn runs **steers** that turn. It was written 5 s into a
  `sleep 20` Bash call. Claude finished the call, and its next model call saw both messages.
  There was one `result`, `"first\n\nsecond"`, with `num_turns: 2`, and no second result came
  in the next 60 s. The same happened with `--replay-user-messages`, which echoes each user
  message as `{"type":"user",…,"isReplay":true}` when it enters the conversation, not when it
  is read: the mid-turn message 15.6 s after its write, right after the tool's `tool_result`
  and 2.8 s before the `result`. Results also carry `queued_turn_count` and `result_index`
  (both 0 here), and `init` lists the capabilities `interrupt_cancel_queued_v1` and
  `interrupt_send_now_v1`. The spike did not try these.

**2. Interrupt and stop.**

- `{"type":"control_request","request_id":"<id>","request":{"subtype":"interrupt"}}` works
  without an `initialize` first. Its `control_response` (`"subtype":"success"`,
  `"response":{"still_queued":[]}`) came within 5 ms. Then came a `user` event,
  `[Request interrupted by user for tool use]` (or `[Request interrupted by user]` between
  model calls), and a `result` with `"subtype":"error_during_execution"`, `"is_error":true`,
  `"result":null` and `terminal_reason` `aborted_tools` (or `aborted_streaming`), 7–17 ms after
  the interrupt.
- A foreground Bash child is killed: no `sleep` was left 2 s after the result. A *background*
  Bash task survives the interrupt and dies only when the process ends, which reports it as a
  `system`/`task_notification` with `"status":"stopped"`. Claude Code itself refuses a
  foreground `sleep 25` or `sleep 60` (`Blocked: standalone sleep …`), and the model then runs
  it in the background. So the case as written tested a background task, and a `sleep 20`
  rerun tested a foreground one.
- The next message after an interrupt runs a normal turn in the same process.
- SIGTERM ended the process in 0.52–0.67 s with exit 143 (four runs), and its
  `thread-voice-mcp` child was gone 5 s later every time.

**3. Exit and resume.**

- Closing stdin while idle ends the process with exit 0 in 0.62 s.
- A new process with `--resume <uuid>` in place of `--session-id <uuid>` (the flags otherwise
  the same) reported the uuid in `init`, answered `otter`, and kept the single
  `<uuid>.jsonl` transcript.
- `--session-id <uuid> --resume <uuid>` together is refused at once with exit 1:
  `Error: --session-id can only be used with --continue or --resume if --fork-session is also
  specified.`

**4. The session id in the MCP child.** A `-p` Claude passes its own environment to stdio MCP
servers and adds `CLAUDE_CODE_SESSION_ID`. In every Claude case, including the `--resume`
process, the hello's `session_id` and the child's `CLAUDE_CODE_SESSION_ID` equalled the case
uuid, and `THREAD_VOICE_THREAD` and `THREAD_VOICE_RUNTIME_DIR` arrived unchanged, though the
spike's own environment for Claude had no `CLAUDE_CODE_SESSION_ID`. The user-scope
`thread-voice` entry in `$CLAUDE_CONFIG_DIR/.claude.json` is enough; no `--mcp-config` is
needed. The child is expected to inherit the proxy token too, as it does under T3 today; the
spike logged only the four names above, so it didn't see this. Claude passes the same
environment to every stdio MCP server the user has registered, not only `thread-voice-mcp`, so
the allowlist of row 12 still reaches those servers. That is no change from T3, and job C's doc
says so.

**5. Stream shapes and permission requests.** Cut examples (the full lines are in
`spike-raw.md`):

```
stdin   {"type":"user","message":{"role":"user","content":"Reply with OK"}}
init    {"type":"system","subtype":"init","cwd":"…/repo","session_id":"77f465be-…","tools":["Task","Bash",…],
         "mcp_servers":[{"name":"thread-voice","status":"connected","source":"user"}],"model":"claude-opus-5-5",
         "permissionMode":"auto","apiKeySource":"none","claude_code_version":"2.1.301",
         "messaging_socket_path":"/run/user/1000/cc-socks/<pid>.sock",…}
text    {"type":"assistant","message":{"model":"claude-opus-5-5","id":"msg_…","role":"assistant",
         "content":[{"type":"text","text":"heron"}],…},"parent_tool_use_id":null,"session_id":"…","uuid":"…"}
tool    {"type":"assistant","message":{…,"content":[{"type":"tool_use","id":"toolu_016V…","name":"Bash",
         "input":{"command":"echo spike-233 && …","description":"…"},…}]},…}
result  {"type":"user","message":{"role":"user","content":[{"tool_use_id":"toolu_016V…","type":"tool_result",
         "content":"spike-233\n…","is_error":false}]},"tool_use_result":{"stdout":"…","stderr":"","interrupted":false,…},…}
ok      {"type":"result","subtype":"success","is_error":false,"result":"OK","num_turns":1,"session_id":"…",
         "terminal_reason":"completed","permission_denials":[],"duration_ms":…,"total_cost_usd":…,"usage":{…},…}
bad     {"type":"result","subtype":"success","is_error":true,"result":"API Error: 400 unknown provider for model
         spike-233-no-such-model","terminal_reason":"api_error",…}
stopped {"type":"result","subtype":"error_during_execution","is_error":true,"result":null,
         "errors":["[ede_diagnostic] …"],"terminal_reason":"aborted_tools",…}
ask     {"type":"control_request","request_id":"2038e810-…","request":{"subtype":"can_use_tool","tool_name":"Bash",
         "display_name":"Bash","input":{"command":"echo spike-233 > a.txt","description":"…"},"description":"…",
         "permission_suggestions":[…],"blocked_path":"…/repo/a.txt","tool_use_id":"toolu_01PL…"}}
allow   {"type":"control_response","response":{"subtype":"success","request_id":"2038e810-…",
         "response":{"behavior":"allow","updatedInput":{…the request's input…}}}}
deny    {"type":"control_response","response":{"subtype":"success","request_id":"ac794f47-…",
         "response":{"behavior":"deny","message":"Denied by the spike-233 test."}}}
```

- For `run_status`, job C reads: `type`; `assistant` (`message.content[].type`: `text`,
  `tool_use` with `name` and `input`, `thinking`); `control_request` with
  `request.subtype == "can_use_tool"`, plus `request_id`, `tool_name`, `input` and
  `description`; `result` with `is_error`, `result`, `terminal_reason`, `session_id` and
  `permission_denials`; and `user` with `isReplay` when the replay flag is on. The bad model's
  `result` has `"subtype":"success"` with `"is_error":true`, so success is `is_error`, not
  `subtype`. Its process exited 1 when stdin closed.
- Under `--permission-mode auto`, none of (a) a `Write` to `.claude/settings.local.json`,
  (b) a `git push` to a local bare repo, or (c) an `rm -rf` of a folder in the repo asked
  anything. All three just ran.
- Under `--permission-mode manual` with §73.4's `--permission-prompts host`, **no request
  reached the stream**, even after an `initialize` control request. Each was denied on the
  spot, with a `system`/`permission_denied` event, an `is_error` `tool_result` and an entry in
  the result's `permission_denials`. With `--permission-prompt-tool stdio` instead (as T3
  runs), each request arrived as the `ask` line above. Allow ran the command (`a.txt`
  appeared). Deny with a message gave the model an `is_error` `tool_result` with that message,
  and nothing ran (`b.txt` never appeared). A request left unanswered for 120 s produced no
  output at all (no timeout, no repeat), and answering it afterwards still worked.

**6. AskUserQuestion** is missing from `init`'s `tools` both with and without
`--disallowed-tools AskUserQuestion` (the same 32 tools in each). `-p` leaves it out already;
the flag is harmless.

**7. Memory and start time.** These were sampled 2 s after the `result`, while idle, in kB.

| Run | spawn → `init` | spawn → `result` | `claude` VmRSS / VmHWM | `thread-voice-mcp` VmRSS |
|---|---|---|---|---|
| start-1 | 0.52 s | 1.73 s | 260,796 / 293,968 | 20,480 |
| start-2 | 0.48 s | 2.02 s | 260,540 / 294,240 | 20,480 |
| start-3 | 0.50 s | 3.02 s | 260,700 / 293,856 | 20,480 |
| median / max | 0.50 / 0.52 s | 2.02 / 3.02 s | 260,700 / 294,240 (HWM max) | 20,480 |
| turns, after turn 3 | 0.05 s after the first write | 1.2–2.1 s per turn | 253,464 / 256,544 | 20,480 |
| `--resume` process | 0.60 s | 2.03 s | 264,592 / 301,188 | 20,480 |

An idle agent costs about 275 MiB (Claude plus `thread-voice-mcp`, 281,180 kB). The highest
`claude` peak was 339 MiB (347,128 kB), in the permission case. Four agents come to about
1.1 GiB idle and under 1.5 GiB at peak. A resumed process answers as fast as a new one. **Proposal:** `local_agents_max = 4`
fits (this machine has 24 GB; an 8 GB home server would still fit 4), and `idle_after = 1200`
fits, since a resume costs about 0.6 s.

**8. Flags.** Next to §73.4's command:

- `--permission-prompts host` gives a `-p` process no requests (item 5). `--permission-prompt-tool
  stdio` does, though the help describes `host` as "the SDK host or
  `--permission-prompt-tool`". The help's default for `--permission-prompts` is `host`, as
  §73.4 assumes.
- `--resume <uuid>` must *replace* `--session-id <uuid>`; both together exit 1 (item 3).
- `--replay-user-messages` tells the daemon when a written message entered the conversation
  (item 1).
- `--permission-mode manual` shows as `"permissionMode":"default"` in `init`.
- `--permission-mode bypassPermissions` *without* `--allow-dangerously-skip-permissions` was
  accepted (`init` shows `bypassPermissions`), and the turn ran.
- `--include-partial-messages` and `--max-turns` were not needed. `--max-turns` is not in
  2.1.301's help.

**9. Codex** (0.163.0, `CODEX_HOME` in the scratch folder, from a copy of the `t3-codex`
`config.toml`).

- `codex mcp add --env THREAD_VOICE_RUNTIME_DIR=<run> thread-voice -- <thread-voice-mcp>`
  wrote `[mcp_servers.thread-voice]` with `command` and an `[mcp_servers.thread-voice.env]`
  table, and `codex mcp list` showed it `enabled`.
- `codex exec --json -C <repo> -m gpt-6.1-sol -s read-only -`, with the message on stdin and
  stdin then closed, emitted `thread.started` (`thread_id`, a UUID), `turn.started`,
  `item.started` / `item.completed` (`agent_message` with `text`; `command_execution` with
  `command`, `aggregated_output`, `exit_code` and `status`), and `turn.completed` (`usage`).
  It then exited 0. The first event came at 0.66 s (0.14 s on a second run), the last at
  9.5 s. The tree's peak RSS was 249 MiB: `codex` 163 MiB, the node wrapper 48 MiB,
  `thread-voice-mcp` 20 MiB, `codex-code-mode-host` 20 MiB, and short-lived `git` and
  `git-remote-https`.
- `codex exec resume <thread_id> --json -`, run with `cwd=<repo>`, answered `lynx`. The
  top-level `codex -C <repo> -s read-only exec resume <thread_id> --json -` also worked, so
  `-C` and `-s` can be set on a resume that way.
- Sandbox modes, with a request to write one file in the repo and one outside it:
  `read-only` wrote neither, `workspace-write` only the one in the repo, and
  `danger-full-access` both. No mode emitted an approval event or waited for input. Each
  exited 0 in 9–13 s. A blocked write did not appear as a `command_execution` item; only the
  agent's text said it failed.
- Codex passes an MCP server only its table's `env`. The child had `THREAD_VOICE_RUNTIME_DIR`
  but no `THREAD_VOICE_THREAD` and no `CODEX_HOME`, so `thread-voice-mcp` fell back to a
  `local-<pid>-<hex>` session id. With `env_vars = ["THREAD_VOICE_THREAD"]` added to the
  table, the variable arrived (`spike-233-codex`).
- Every run started `git-remote-https`, a plugins sync into `$CODEX_HOME/.tmp/plugins`. Codex
  also writes its state databases, `sessions/`, `shell_snapshots/` and `skills/` there.
- `codex` is `#!/usr/bin/env node`.

**Differences from §73.4, §73.4.1 and §73.9, and values they leave unset.** Jobs C and I make
these changes; this section only proposes them.

| # | Observed | §73 today | Proposal | Job |
|---|---|---|---|---|
| 1 | `--permission-prompts host` delivers no request in `-p` mode, so everything that needs a yes is denied | §73.4 starts agents with it | Use `--permission-prompt-tool stdio` in its place, and answer `can_use_tool` with the `allow` / `deny` lines above | C |
| 2 | A message written mid-turn steers the running turn: one `result` answers both | §73.4 queues it until `result` ("steering is spike A's question") | Write it at once, as T3's app does, with `--replay-user-messages`. The post is `queued` until its replay, then `running`, and it shares the turn's `result`. Keeping §73.4's queue is the fallback if one result per post matters more | C |
| 3 | `init` comes only after a message, and again on every turn | unset | Don't wait for `init` before writing. Treat each `init` as the start of a turn, not of the process | C |
| 4 | Success is `is_error`; a bad model gives `subtype: success, is_error: true` | §73.4: `result` → `completed` or `failed` | `completed` when `is_error` is false. Otherwise `failed`, with `terminal_reason` (`api_error`, …) as the detail, except `aborted_*` after a Stop, which is `stopped` | C |
| 5 | An unanswered request waits forever, with no output | §73.4: no answer within the ask's timeout means deny | Holds: the daemon's timeout is the only one, so it must send the deny itself | C |
| 6 | Interrupt answers in ms. Foreground tools die, background Bash tasks don't | §73.4: interrupt, then end the process after 5 s | Interrupt. End the process (close stdin, else SIGTERM) only if no `result` comes within 5 s or a background task is still listed (`system`/`background_tasks_changed`) | C |
| 7 | `--resume` must replace `--session-id`; both exit 1 | §73.4: "the same flags" | Say so: resume drops `--session-id` | C |
| 8 | Closing stdin ends an idle process cleanly (exit 0, 0.6 s); SIGTERM exits 143 in < 1 s and takes `thread-voice-mcp` with it | unset | End idle, settled and restart-time processes by closing stdin. Use SIGTERM after 5 s if one doesn't exit | C |
| 9 | `topic_runtime_mode` values aren't Claude's | §73.9 "checked against Claude's list" | Map `approval-required` → `manual`, `auto-accept-edits` → `acceptEdits`, `auto` → `auto`, `full-access` → `bypassPermissions` plus `--allow-dangerously-skip-permissions` (2.1.301 accepted it without, but T3 passes both). A project's `permission_mode` takes the same four values | C |
| 10 | `auto` held none of a settings write, a `git push` or an `rm -rf` | §73.12 decision 2 expects "what still needs a yes" to be asked | Note that under `auto` voice asks will be rare. A project that wants a yes for pushes or deletes uses `approval-required`. Job C also states the mode a topic gets when neither the project's `permission_mode` nor `topic_runtime_mode` is set (`runtime_mode` defaults to `None`), so the unset case can't silently become `auto` or `bypassPermissions` | C (doc), E |
| 11 | The daemon has none of `CLAUDE_CONFIG_DIR`, `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN` or `ANTHROPIC_API_KEY`, and its `PATH` lacks `~/.local/bin` and node | §73.4 "the same environment T3 gives its Claude" has no source without T3 | `CLAUDE_CONFIG_DIR` from `claude_config_dir` (already a key). A new non-secret key `agent_base_url` for `ANTHROPIC_BASE_URL`. The token in a 0600 file in the data dir (for example `<data_dir>/agent-token`, set with a `thread-voice agent-token` command reading stdin, which `install` can fill from T3's `provider-env-…ANTHROPIC_AUTH_TOKEN` secret when T3 is there). It goes only into the child's environment, never into `config.toml` or argv. `ANTHROPIC_API_KEY` is set empty, as T3 does | C, D |
| 12 | An allowlist kept the builder's `CLAUDE_CODE_*` and `CLAUDECODE` out of the spike | §73.9: "the daemon's environment minus Thread Voice's own secrets" | Build the agent environment from an allowlist (`HOME LANG LOGNAME PATH SHELL USER XDG_RUNTIME_DIR`, the provider variables, `THREAD_VOICE_THREAD`), not from the daemon's environment minus secrets | C |
| 13 | `THREAD_VOICE_THREAD` reaches the MCP child under Claude, but under Codex only with `env_vars` | §73.4.1: "the daemon passes `THREAD_VOICE_THREAD` in every agent's environment" | `install` writes `env_vars = ["THREAD_VOICE_THREAD"]` into Codex's `[mcp_servers.thread-voice]`, since `codex mcp add` has no flag for it | I |
| 14 | `codex exec resume` has no `-C` or `-s`; the top-level form works | §73.4.1: `codex exec resume <session-id> --json …` | Later turns run `codex -C <worktree> -s <sandbox> exec resume <thread_id> --json -`. The session id is `thread.started`'s `thread_id` | I |
| 15 | Codex `exec` never asks; the sandbox silently blocks | §73.4.1 left this to the spike | No `waiting` state for Codex. Sandbox per mode: `approval-required` → `read-only`, `auto-accept-edits` and `auto` → `workspace-write`, `full-access` → `danger-full-access` | I |
| 16 | `codex` needs `node` on `PATH`; the daemon's `PATH` has none. Codex needs `CODEX_HOME` and `CLIPROXY_API_KEY` (the same bytes as the Claude token) | unset | A `codex_bin` key (default T3's path when present). The directory of the `node` its shebang needs goes on the end of the child's `PATH`, so the nvm `bin/` with every global npm command can't shadow system tools. It goes first only if a system `node` is too old. A `codex_home` key. The token file from row 11 passed as `CLIPROXY_API_KEY` to Codex only | I |
| 17 | Codex syncs plugins over the network (`git-remote-https`) at every start | unset | Note it for the home server; no change | I |
| 18 | About 275 MiB per idle agent; resume in about 0.6 s | §73.4 defaults | Keep `local_agents_max = 4` and `idle_after = 1200` | C |

**Already seen by the spec writer**, checked again:

- `claude --help` (2.1.301) lists every §73.4 flag, with the same `--permission-mode` choices.
  `--permission-prompts` has `host` and `none`, and its text mentions
  `--permission-prompt-tool`, which isn't listed on its own. `--max-turns` is absent, and
  `--replay-user-messages` and `--include-partial-messages` exist. **Holds** (`default:
  "host"`).
- `RUNTIME_MODES` are T3's four values, not Claude's. **Holds** (row 9).
- T3's own Claude runs without `-p`, with `--permission-prompt-tool stdio`,
  `--include-partial-messages`, `--tools default`, `--permission-mode bypassPermissions
  --allow-dangerously-skip-permissions`, `--effort high`, `--thinking adaptive`, `--add-dir`
  and `--settings`. **Holds**. The running T3 Claudes also showed `--input-format
  stream-json --output-format stream-json --verbose`, `--allowedTools mcp__t3-code__*` and
  `--thinking-display summarized`, and new threads use `--session-id=<id>`, not
  `--resume=<id>`.
- T3 gives Claude `CLAUDE_CONFIG_DIR`, `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN` and an
  empty `ANTHROPIC_API_KEY`, and the daemon has none of the four, with only
  `claude_config_dir` in `config.toml`. **Holds** (row 11).
- `thread-voice-mcp` is registered at user scope with no `env`, and gets the whole Claude
  environment. **Holds**, under `-p` too (item 4).
- A hello makes the real daemon upsert `threads` and `sessions` rows. **Holds**
  (`_agent_hello`); the spike's sessions reached only the fake daemon, and the store has no
  row for them.
- `codex` on `PATH` is 0.160.1, T3's binary 0.163.0, with provider `cliproxyapi` and
  `env_key = "CLIPROXY_API_KEY"`, and the same bytes as the Claude token. **Holds** (`cmp -s`
  exit 0).
- `codex exec resume` lacks `-C`, `--cd`, `-s` and `--sandbox`. The top level has `-s`, `-C`
  and `-a`, `codex exec` has no `-a`, and `-` reads stdin. **Holds** (row 14).
- Other observations: `-p` sessions added no project entry to `$CLAUDE_CONFIG_DIR/.claude.json`.
  Each `claude` process opens `/run/user/1000/cc-socks/<pid>.sock` and removes it on exit.
  Claude keeps background task output under `/tmp/claude-1000/<project slug>/`.
