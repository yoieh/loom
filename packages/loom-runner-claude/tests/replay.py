import asyncio
import dataclasses
import json
import signal
from collections.abc import Callable
from pathlib import Path

from loom_contracts.runners import PermissionMode, RunnerName, ThreadSpec

STREAMS = Path(__file__).resolve().parent / 'streams'

THREAD_ID = '77f465be-2f52-4311-84a5-e19e6d1e339b'

FAKE_PID = 4_194_404


def make_thread(**changes: object) -> ThreadSpec:
    thread = ThreadSpec(
        id=THREAD_ID,
        runner=RunnerName.CLAUDE,
        project_id='loom',
        workspace_path='/tmp/loom-replay/repo',
        title='Replay',
        permission_mode=PermissionMode.AUTO,
    )
    return dataclasses.replace(thread, **changes)


def stream_lines(case: str) -> list[bytes]:
    return [line for line in (STREAMS / f'{case}.jsonl').read_bytes().split(b'\n') if line]


def stream_turns(case: str) -> list[list[bytes]]:
    turns: list[list[bytes]] = [[]]
    for line in stream_lines(case):
        turns[-1].append(line)
        if json.loads(line).get('type') == 'result':
            turns.append([])
    return [turn for turn in turns if turn]


def changed(line: bytes, change: Callable[[dict], object]) -> bytes:
    data = json.loads(line)
    change(data)
    return json.dumps(data).encode()


class FakeHost:
    data_dir = '/tmp/loom-replay/data'

    def __init__(
        self,
        environment: dict[str, object] | None = None,
        settings: dict[str, object] | None = None,
        secrets: dict[str, object] | None = None,
    ) -> None:
        self.env = environment or {}
        self.settings = settings or {}
        self.secrets = secrets or {}

    def setting(self, key: str) -> str | None:
        return self.settings.get(key)

    def environment(self, thread: ThreadSpec) -> dict[str, str]:
        return dict(self.env)

    def secret(self, name: str) -> str | None:
        return self.secrets.get(name)


class FakeStdin:
    def __init__(self, process: 'FakeProcess') -> None:
        self.lines: list[bytes] = []
        self.error: BaseException | None = None
        self.drain_error: BaseException | None = None
        self.closed = False
        self._process = process

    def write(self, data: bytes) -> None:
        if self.error is not None:
            raise self.error
        self.lines.append(bytes(data))

    async def drain(self) -> None:
        await asyncio.sleep(0)
        if self.drain_error is not None:
            raise self.drain_error

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._process.stdin_closed()


class FakeProcess:
    def __init__(
        self,
        exit_code: int = 0,
        ignore_eof: bool = False,
        ignore_terminate: bool = False,
        grandchild: bool = False,
        pid: int = FAKE_PID,
        group: int | None = None,
        group_error: OSError | None = None,
    ) -> None:
        self.stdin = FakeStdin(self)
        self.stdout = asyncio.StreamReader()
        self.pid = pid
        self.group = pid if group is None else group
        self.group_error = group_error
        self.returncode: int | None = None
        self.exit_code = exit_code
        self.ignore_eof = ignore_eof
        self.ignore_terminate = ignore_terminate
        self.grandchild = grandchild
        self.signals: list[str] = []
        self.group_lookups: list[int] = []
        self.group_signals: list[tuple[int, int]] = []
        self._exited = asyncio.Event()

    def feed(self, *lines: bytes) -> None:
        for line in lines:
            self.stdout.feed_data(line if line.endswith(b'\n') else line + b'\n')

    def exit(self, code: int) -> None:
        if self.returncode is None:
            self.returncode = code
            if not self.grandchild:
                self.stdout.feed_eof()
            self._exited.set()

    def getpgid(self, pid: int) -> int:
        self.group_lookups.append(pid)
        if self.group_error is not None:
            raise self.group_error
        if pid != self.pid:
            raise ProcessLookupError(pid)
        return self.group

    def killpg(self, group: int, signum: int) -> None:
        self.group_signals.append((group, signum))
        if group != self.group or (self.returncode is not None and not self.grandchild):
            raise ProcessLookupError(group)
        if signum == 0 or (signum == signal.SIGTERM and self.ignore_terminate):
            return
        self.exit(-signum)
        if self.grandchild:
            self.grandchild = False
            self.stdout.feed_eof()

    def group_sent(self) -> list[int]:
        return [signum for _, signum in self.group_signals if signum != 0]

    async def wait(self) -> int:
        await self._exited.wait()
        return self.returncode

    def terminate(self) -> None:
        self.signals.append('terminate')
        if not self.ignore_terminate:
            self.exit(-signal.SIGTERM)

    def kill(self) -> None:
        self.signals.append('kill')
        self.exit(-signal.SIGKILL)

    def stdin_closed(self) -> None:
        if not self.ignore_eof:
            self.exit(self.exit_code)


class FakeSpawn:
    def __init__(self, process: FakeProcess | None = None, error: OSError | None = None) -> None:
        self.process = process
        self.error = error
        self.calls: list[tuple[list[str], str, dict[str, str], int]] = []

    async def __call__(
        self, argv: list[str], cwd: str, env: dict[str, str], limit: int,
    ) -> FakeProcess:
        self.calls.append((list(argv), cwd, dict(env), limit))
        if self.error is not None:
            raise self.error
        return self.process
