# Security rules

Loom's rules for the factory's security review, adapted on 2026-10-23 from Thread Voice's.
`CLAUDE.md`'s conventions and `docs/design.md` §11 come first. A "must fix" is an item under the
gate file's `## Must fix`; a "note" goes under `## Notes` and never blocks.

- Look for: unvalidated input, injection (SQL, shell, path), authn and authz gaps, secrets in code,
  logs, argv or events, unsafe deserialization or `eval`, denial of service through unbounded work
  or memory, risky new dependencies, and insecure defaults. Judge only what the diff changes or
  exposes.
- Loom starts processes that act for the user. Any path by which a thread could start with a wider
  permission mode than its caller or project allows is a must fix.
- When the diff sends a request that tells another system to act (start an agent, send a turn,
  answer a permission request), and that request can fail ambiguously, check that the state left
  behind stops a retry from repeating the action (command ids, §4). If it doesn't, that is a must
  fix unless the plan's risks accept it.
- A file, socket or directory Loom creates without mode 0600 (0700 for directories) is a must fix.
- An action an agent can reach through `loom-mcp` that deletes user data or a worktree with
  uncommitted work, where tool-description text is the only guard, is a must fix.

See also `review.md`, which every review step follows.
