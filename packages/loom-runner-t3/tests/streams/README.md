# Recorded T3 answers

These are the replay fixtures for `loom-runner-t3`. `docs/design.md` §6.1 cites them as
*seen*, T3 0.1.2.

- **Recorded:** 2026-10-25, against T3 Code 0.1.2 (`serverInfo.version` in
  `../answers/initialize.json`), MCP protocol `2025-06-18`, at the loopback origin
  `http://127.0.0.1:41773`.
- **How:** a throwaway stdlib script outside the repo paired with T3 as its own client,
  "Loom t3-record", by §6.1's pairing steps, with full access. It then launched three
  threads in the T3 project whose `workspaceRoot` is this repo's main checkout. Each
  thread got `runtimeMode` `approval-required` and T3's `defaultModelSelection`. Afterwards
  the script archived the threads with `t3_thread_organize {action: "archive"}`, revoked
  the session with `t3 auth session revoke`, and removed the scratch git worktree and its
  branch. The six cases used six turns.

## The files

- `streams/<case>.jsonl`: one line per `t3_thread_read {view: "activity", afterPosition}`
  answer, in poll order, about one a second. Each line is the tool result's JSON: the
  parsed `content[0].text`, which equals its `structuredContent`. The first read of a case
  uses the `afterPosition` that its steps below give. Each later read uses the last
  `nextPosition` that was not `null`; a read with no new items answers `nextPosition: null`.
  A stream with a run ends at most two reads after the read that shows the run ended.
- `answers/<name>.json`: one HTTP exchange, `{"request": {"method", "path", "body"},
  "status", "headers", "body"}`. `headers` holds only the answer's `content-type` and
  `mcp-session-id`, and `location` in `oauth-authorize`. `body` is the answer's JSON, or its
  text when it is not JSON (`""` for an empty body).
- `answers/pairing-create.json` and `answers/session-list.json`: the `t3` CLI's JSON output,
  as `{"argv": [...], "stdout": <JSON>}`.

Request headers are not kept. Every `/mcp` request sent `Accept: application/json,
text/event-stream`, `Content-Type: application/json`, `MCP-Protocol-Version: 2025-06-18`,
the access token in `Authorization`, and the `Mcp-Session-Id` from `initialize`. The
exceptions are named below. The OAuth requests sent no token. `oauth-token`'s request body
was form-encoded, and the fixture keeps it as that form text.

## The cases

Thread 1 is "Loom t3-record A (idle launch)", thread 2 "Loom t3-record B (existing
worktree)" and thread 3 "Loom t3-record C (unknown model)".

- `idle-launch`: thread 1 is launched with `workspaceStrategy {type: "root"}` and no
  message (`launch-idle`). Then come three reads with no `afterPosition`, a second apart.
- `turn`: thread 1. One read with no `afterPosition`. Then `t3_thread_send` "Reply with the
  single word ok." with mode `queue` and `clientRequestId` request id 1 (`send`). The same
  send follows at once, with the same `clientRequestId` (`send-repeat`). Then reads.
  Position 0 is the only user message.
- `interrupt`: thread 1, starting at `afterPosition` 2. One read, then a send of "Count
  slowly from 1 to 200, one number per line." (`queue`, request id 5), then three reads
  while it runs. Then `t3_thread_interrupt {threadId, clientRequestId}` with request id 2
  (`interrupt`), and at once the same call with the same id (`interrupt-repeat`). Then
  reads.
- `tool`: thread 2 is launched idle with `{type: "existing_worktree", worktreePath, branch}`
  (`launch-worktree`). Its worktree was made the way Loom makes one, with `git worktree add
  -b loom/t3-record-1 <dir> <base>` from a worktree of this repo. One read, then a send of
  "Read README.md with your read tool and reply with its first line." (`queue`, request id
  4). Then reads. T3 opened no permission request, so `pendingRequestCount` stays 0.
- `steer`: thread 2, starting at `afterPosition` 6. Positions 4 to 6 (run 2) are an earlier
  "Count slowly from 1 to 30, one number per line." turn. It ended before a steer could be
  sent, so it is in no fixture. One read, then a send of "Count slowly from 1 to 200, one
  number per line." (`queue`, request id 6). Once a read shows `running`, "Reply with the
  single word ok." is sent with mode `steer` (`send-steer`, request id 3). Then reads.
- `failed`: thread 3 is launched idle with `modelSelection {instanceId: "claude-proxy",
  model: "loom-t3-record-no-such-model"}` (`launch-unknown-model`), and T3 accepts it. One
  read, then a send of "Reply with the single word ok." (`queue`, request id 7). Then reads.
  The provider answers 400, and T3 ends the run `failed`.

## The other answers

- `initialize` and `initialized` (`notifications/initialized`), `tools-list` (`tools/list`,
  on one line) and `project-list` (`t3_project_list`).
- `read-unknown-thread`: `t3_thread_read` of a made-up thread id (thread 4).
- `no-session`: `tools/list` with the token and no `Mcp-Session-Id`.
- `unknown-session`: `tools/list` with the token and a made-up `Mcp-Session-Id`.
- `bad-token`: `tools/list` with a made-up access token and the real `Mcp-Session-Id`.
- `archive`: `t3_thread_organize {action: "archive"}` on thread 1. Threads 2 and 3 were
  archived the same way, and those calls were not recorded.
- `read-archived-thread`: `t3_thread_read` of thread 1 after it was archived, with
  `afterPosition` 8.
- `oauth-register`, `oauth-authorize`, `oauth-approval`, `oauth-decision` and `oauth-token`:
  §6.1's pairing steps 1, 2, 4 and 5. `oauth-register` registered the redirect URI with no
  port, `http://127.0.0.1/loom/callback`.
- `pairing-create`: step 3, `t3 auth pairing create`.
- `session-list`: `t3 auth session list --json` right after the token exchange.

## What was left out

- `turn` dropped 83 reads and `interrupt` 118 reads after their last kept read. Each one
  was the same as the last kept read, with no items.
- `project-list` keeps only the Loom project. The user's other projects are left out of
  both `content[0].text` and `structuredContent`.
- `session-list` keeps only the scratch client's session. The user's other sessions are
  left out.

## Placeholders

Each one replaces a real value everywhere it appeared, in every encoding (`%3A`, `%253A`
and `%25253A` forms included). So the same thread has the same placeholder in every file.
A user message's id ends in the `clientRequestId` of the send that made it.

| Placeholder | Replaces |
|---|---|
| `mcp:00000000-0000-4000-8000-00000000000<n>` | T3 thread ids: `n` 1 to 3 are threads 1 to 3, and 4 is the made-up id in `read-unknown-thread`. The same uuid sits inside run ids (`run:thread:mcp%3A<uuid>:ordinal:<n>`) and checkpoint item ids. |
| `mcp:00000000-0000-4000-8001-000000000001` | the Loom T3 project's `id` |
| `00000000-0000-4000-8002-000000000001` | the scratch client's session id: `sessionId` in `session-list`, and `client%3A<id>` inside message ids |
| `00000000-0000-4000-8003-000000000001` | the `Mcp-Session-Id` |
| `00000000-0000-4000-8004-00000000000<n>` | the `clientRequestId`s, request ids 1 to 7 above, also inside message ids (`thread-send:<id>`) |
| `00000000-0000-4000-8005-00000000000<n>` | the provider's item ids (`native-item:<id>`) |
| `00000000-0000-4000-8006-000000000001` | the pairing code's `id` in `pairing-create` |
| `toolu_replay01` | the provider's id of the tool call in `tool` |
| `replay-access-token` | the access token |
| `replay-client-id` | the registered `client_id` |
| `replay-code-verifier` | the PKCE code verifier |
| `replay-code-challenge` | the PKCE code challenge |
| `replay-state` | the OAuth `state` |
| `replay-pairing-code` | the code from `t3 auth pairing create` (`credential`) |
| `replay-authorization-code` | the authorization code in `redirectTo` |
| `/tmp/loom-replay/project` | this repo's main checkout, the Loom project's `workspaceRoot` |
| `/tmp/loom-replay/worktree` | the scratch git worktree |
| `/tmp/loom-replay/t3-home` | T3's home, `~/.t3` |

The user name appeared only inside these paths. Everything else is as T3 sent it,
timestamps included.
