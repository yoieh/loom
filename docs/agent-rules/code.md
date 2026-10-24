# Code rules

Loom's rules for the factory's code steps: build, fix and the code review, adapted on 2026-10-23
from Thread Voice's. `CLAUDE.md`, the repo profile, comes first. A "must fix" is an item under the
gate file's `## Must fix`; a "note" goes under `## Notes` and never blocks.

## Building

- Make the smallest change that meets every acceptance criterion, and follow the repo profile's
  conventions.
- Edit `CLAUDE.md` only when the change alters a lasting fact there (the stack, the layout, a test
  command, or a convention), as the spec's Design lists it. Never edit it to record what the job
  built.
- The base is the commit the job branch was cut from: `git merge-base HEAD <feature_branch>` in a
  factory job, else `main`.
- Check that each new or changed test fails against the base's code: commit first, revert each
  source file in place with `git checkout <base> -- <file>` (or delete a file the base lacks), rerun
  the tests with `PYTHONDONTWRITEBYTECODE=1`, then restore it with `git checkout HEAD -- <file>`.
  Never use `git stash`, because other sessions share it. In `output.md`, give per acceptance
  criterion the test that verifies it and whether it fails on the base (`fails`, or
  `passes (guard)`).
- A new import between packages must match `docs/design.md` §3's table; the packaging test checks
  it.
- A runner change comes with a replay test: a recorded stream in, the events out.

## Reviewing code

- Run the test command yourself in the worktree. Don't trust `output.md` alone.
- Reproduce at least one fails-on-the-base result from `output.md` without touching the worktree,
  with `<base>` as Building defines it:
  `mkdir <tmp> && git -C <worktree> archive <base> | tar -x -C <tmp>`, copy the job's new and
  changed test files into `<tmp>`, then run
  `cd <tmp> && uv run python -m unittest packages.<name>.tests.test_<module>` and remove `<tmp>`. A
  new test that passes there, with no `passes (guard)` label in `output.md`, is a must fix. Never
  revert files in the shared worktree to do this.
- Look for: bugs and unhandled edge cases, acceptance criteria with no test or a test that doesn't
  really check them, code that doesn't match the repo's conventions, needless complexity or dead
  code, and changes outside the spec's scope.
- A `CLAUDE.md` hunk is a must fix when the spec's Design doesn't list it, or when it records what
  the job built instead of a lasting fact.
- A module over about 1,500 lines, or an import that breaks §3's dependency table, is a must fix.
- A fake that only ever returns success for an outside call (an agent process, git, T3), when the
  real call's failure would break the Goal, is a must fix that quotes the plan's risk line on that
  call, or says that there is none.
- A diff that changes a `v1` call or event field in a way other than adding to it is a must fix
  (§8 stability).
- A bound is not a tradeoff. A diff that breaks a bound stated in the spec's Goal or the plan's
  risks is a must fix that quotes that line.

See also `review.md`, which every review step follows.
