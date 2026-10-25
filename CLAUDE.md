# Loom

## Repo profile

Every factory agent reads this section first. Change this section only when a
lasting fact here changes.

- **What it is:** Loom runs agent sessions (threads) and schedules work for
  them; Thread Voice, the software factory, the `loom` CLI and agents are its
  clients. See `README.md`.
- **What is built:** see `git log` on main. Open and planned work is in the
  factory's issue tracker (see below).
- **Design:** `docs/design.md` is the source of truth; issues cite its
  sections (§). `docs/thread-voice-73.md` holds the parts of Thread Voice's
  §73 design and spike results it builds on, cited as §73.x.
- **Layout:** a uv workspace. Each package is `packages/<name>/` with its own
  `pyproject.toml` (hatchling) and its import package inside, and its tests
  in `packages/<name>/tests/test_<module>.py`. `packages/`, each
  `packages/<name>/` and its `tests/` hold an empty `__init__.py`, which the
  test command's discovery needs, and the root workspace excludes
  `packages/__pycache__`. The packages and the dependencies each may have are
  in `docs/design.md` §3; a package imports only what its row lists.
  `packages/loom-contracts/tests/test_packaging.py` holds §3's table, so a new
  package adds its row there. Factory rules for this repo are in
  `docs/agent-rules/`.
- **Language / stack:**
  - Python 3.12, managed with uv: the root `pyproject.toml` (the workspace),
    `uv.lock`, `.python-version`, and `.venv` in the repo. Run `uv sync
    --all-packages` to set up.
  - Standard library only. Add a dependency only when an issue names it; only
    such a run edits a `pyproject.toml`'s dependencies and `uv.lock`.
  - Tests use the stdlib `unittest` only (`IsolatedAsyncioTestCase` for async
    code). There are no dev dependencies.
  - Loom runs `claude`, `codex`, `t3` and `git` as child processes; they are
    not Python dependencies.
- **Test command:** `uv run python -m unittest discover -s packages -p 'test_*.py' -t .`
  (add `-v` for names); one file is
  `uv run python -m unittest packages.<name>.tests.test_<module>`. Tests use temp
  dirs and temp Unix sockets, never the real `~/.local/share/loom`,
  `~/.config/loom` or `$XDG_RUNTIME_DIR/loom`, and fakes or recorded streams
  instead of real agents. Each `test_*.py` ends with its
  `if __name__ == '__main__':` block, so new code goes above it.
- **Conventions:**
  - Python: 4-space indent (PEP 8), single quotes, type hints on public
    functions, and sparse comments that explain why.
  - Keep files small and plain; no module over about 1,500 lines.
  - Write commit messages in sentence case and the imperative mood, with no
    prefix (for example "Add the schedule heap").
  - Never bind to a network port. The API is a Unix socket with mode 0600;
    create store, secret and state files with mode 0600.
  - Child processes get argument lists, never a shell string, and an
    allowlisted environment. A message to an agent never goes in argv.
  - In `loom_contracts`, modules import each other relatively
    (`from .states import ThreadState`), because the packaging test allows
    only `__future__`, `collections`, `dataclasses`, `enum` and `typing`
    there; and a module that defines a `TypedDict` does not use
    `from __future__ import annotations`, which drops `NotRequired` on
    Python 3.12.

## Software factory

Loom is the project `loom` (issue key `LM`) in the global factory, whose
state lives in `~/.factory`. In Claude Code, `/factory LM-<n>` starts a run on
an issue and `/factory` alone lists the ready ones;
`factory issue list --project loom` lists them in a shell. A run ends on a
feature branch: merge it into main by hand, then mark the issue done
(`factory issue set LM-<n> --status done`).

The factory's steps also read this repo's rules in `docs/agent-rules/`:
`code.md` for building and code review, `security.md` for the security
review, and `review.md` for every review.

The repo's `origin` is a private backup, github.com/yoieh/loom (the user,
2026-10-25). The factory has no code host for it, so runs still end `no_pr`
and are merged into main here; after such a merge, push main
(`git push origin main`). Never push any other branch.
