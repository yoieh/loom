import asyncio
import dataclasses
import json
import signal
from collections.abc import Callable
from pathlib import Path

from loom_contracts.runners import PermissionMode, RunnerName, ThreadSpec

STREAMS = Path(__file__).resolve().parent / 'streams'

THREAD_ID = '77f465be-2f52-4311-84a5-e19e6d1e339b'

CODEX_ID = '01a163a1-4140-78d3-b4ab-ee2d93e984ad'

WORKSPACE = '/tmp/loom-replay/repo'

FIRST_PID = 4_194_404


def make_thread(**changes: object) -> ThreadSpec:
    thread = ThreadSpec(
        id=THREAD_ID,
        runner=RunnerName.CODEX,
        project_id='loom',
        workspace_path=WORKSPACE,
        title='Replay',
        permission_mode=PermissionMode.AUTO,
    )
    return dataclasses.replace(thread, **changes)


def stream_lines(case: str) -> list[bytes]:
    return [line for line in (STREAMS / f'{case}.jsonl').read_bytes().split(b'\n') if line]


def line(data: object) -> bytes:
    return json.dumps(data, separators=(',', ':')).encode()


def changed(raw: bytes, change: Callable[[dict], object]) -> bytes:
    data = json.loads(raw)
    change(data)
    return line(data)


class FakeHost:
    def __init__(
        self,
        data_dir: str,
        environment: dict[str, object] | None = None,
        settings: dict[str, object] | None = None,
        secrets: dict[str, object] | None = None,
    ) -> None:
        self.data_dir = data_dir
        self.env = environment or {}
        self.settings = settings or {}
        self.secrets = secrets or {}
        self.asked: list[ThreadSpec] = []

    def setting(self, key: str) -> str | None:
        return self.settings.get(key)

    def environment(self, thread: ThreadSpec) -> dict[str, str]:
        self.asked.append(thread)
        return dict(self.env)

    def secret(self, name: str) -> str | None:
        return self.secrets.get(name)


class FakeStdin:
    def __init__(self, process: 'FakeProcess') -> None:
        self.data = bytearray()
        self.closed = False
        self._process = process

    def write(self, data: bytes) -> None:
        self.data += data

    async def drain(self) -> None:
        await asyncio.sleep(0)

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._process.stdin_closed()


class FakeProcess:
    def __init__(
        self,
        lines: list[bytes] | None = None,
        exit_code: int = 0,
        stay: bool = False,
        ignore_terminate: bool = False,
        grandchild: bool = False,
        group: int | None = None,
        group_error: OSError | None = None,
    ) -> None:
        self.lines = list(lines or [])
        self.exit_code = exit_code
        self.stay = stay
        self.ignore_terminate = ignore_terminate
        self.grandchild = grandchild
        self.own_group = group
        self.group_error = group_error
        self.pid = 0
        self.group = 0
        self.stdin = FakeStdin(self)
        self.stdout = asyncio.StreamReader()
        self.returncode: int | None = None
        self.signals: list[str] = []
        self.group_signals: list[tuple[int, int]] = []
        self._exited = asyncio.Event()

    def assign(self, pid: int) -> None:
        self.pid = pid
        self.group = pid if self.own_group is None else self.own_group

    def feed(self, *lines: bytes) -> None:
        for raw in lines:
            self.stdout.feed_data(raw if raw.endswith(b'\n') else raw + b'\n')

    def exit(self, code: int) -> None:
        if self.returncode is None:
            self.returncode = code
            if not self.grandchild:
                self.stdout.feed_eof()
            self._exited.set()

    def stdin_closed(self) -> None:
        self.feed(*self.lines)
        if not self.stay:
            self.exit(self.exit_code)

    def killpg(self, group: int, signum: int) -> None:
        self.group_signals.append((group, signum))
        if self.returncode is not None and not self.grandchild:
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


class FakeSpawn:
    def __init__(self, *outcomes: FakeProcess | OSError) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[list[str], str, dict[str, str], int]] = []
        self.processes: list[FakeProcess] = []
        self.lookups: list[int] = []

    @property
    def argvs(self) -> list[list[str]]:
        return [call[0] for call in self.calls]

    async def __call__(
        self, argv: list[str], cwd: str, env: dict[str, str], limit: int,
    ) -> FakeProcess:
        self.calls.append((list(argv), cwd, dict(env), limit))
        outcome = self.outcomes.pop(0) if self.outcomes else FakeProcess()
        if isinstance(outcome, OSError):
            raise outcome
        outcome.assign(FIRST_PID + len(self.processes))
        self.processes.append(outcome)
        return outcome

    def getpgid(self, pid: int) -> int:
        self.lookups.append(pid)
        for process in self.processes:
            if process.pid == pid:
                if process.group_error is not None:
                    raise process.group_error
                return process.group
        raise ProcessLookupError(pid)

    def killpg(self, group: int, signum: int) -> None:
        for process in self.processes:
            if process.group == group:
                process.killpg(group, signum)
                return
        raise ProcessLookupError(group)
