import asyncio
import contextlib
import dataclasses
import uuid
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Protocol

from loom_contracts.runners import (
    Capability,
    Decision,
    DispatchMode,
    Exited,
    PermissionRequest,
    RequestAnswer,
    RunnerEvent,
    RunnerHost,
    ThreadSpec,
    TurnResult,
    TurnStarted,
)

from .command import claude_argv, claude_env
from .reasons import safe_reason
from .stream import (
    BadRequest,
    Init,
    Parsed,
    Result,
    TaskEnded,
    TaskStarted,
    allow_line,
    deny_line,
    interrupt_line,
    parse_line,
    turn_result,
    user_line,
)

UNREADABLE_REQUEST = 'Loom could not read this permission request.'

_CHUNK = 65_536


class SessionClosed(Exception):
    pass


class RunnerStartError(Exception):
    pass


class Process(Protocol):
    stdin: asyncio.StreamWriter | None
    stdout: asyncio.StreamReader | None
    returncode: int | None

    async def wait(self) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


Spawn = Callable[[list[str], str, dict[str, str], int], Awaitable[Process]]


async def spawn_process(argv: list[str], cwd: str, env: dict[str, str], limit: int) -> Process:
    return await asyncio.create_subprocess_exec(
        *argv,
        cwd=cwd,
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        limit=limit + 2,
    )


def _start_text(claude_bin: str, error: OSError) -> str:
    why = error.strerror
    if not isinstance(why, str) or not why:
        why = type(error).__name__
    text = f'could not start {claude_bin}: {why}'
    if isinstance(error.filename, str) and error.filename != claude_bin:
        text += f': {error.filename}'
    return text


class ClaudeRunner:
    name = 'claude'
    capabilities = frozenset({
        Capability.STEER,
        Capability.INTERRUPT,
        Capability.RESUME,
        Capability.PERMISSION_REQUESTS,
    })

    def __init__(
        self,
        spawn: Spawn = spawn_process,
        close_timeout: float = 5.0,
        interrupt_timeout: float = 5.0,
        line_limit: int = 8_388_608,
    ) -> None:
        self._spawn = spawn
        self._close_timeout = close_timeout
        self._interrupt_timeout = interrupt_timeout
        self._line_limit = line_limit

    async def start(self, thread: ThreadSpec, host: RunnerHost) -> 'ClaudeSession':
        claude_bin = host.setting('claude_bin')
        if not isinstance(claude_bin, str) or not claude_bin:
            claude_bin = 'claude'
        argv = claude_argv(thread, claude_bin)
        env = claude_env(thread, host)
        token = env.get('ANTHROPIC_AUTH_TOKEN')
        secrets = (token,) if token else ()
        try:
            process = await self._spawn(argv, thread.workspace_path, env, self._line_limit)
        except OSError as error:
            text = _start_text(claude_bin, error)
            raise RunnerStartError(safe_reason(text, secrets)) from error
        return ClaudeSession(
            process,
            close_timeout=self._close_timeout,
            interrupt_timeout=self._interrupt_timeout,
            line_limit=self._line_limit,
            secrets=secrets,
        )


class ClaudeSession:
    def __init__(
        self,
        process: Process,
        close_timeout: float,
        interrupt_timeout: float,
        line_limit: int,
        *,
        secrets: tuple[str, ...] = (),
    ) -> None:
        self._process = process
        self._secrets = secrets
        self._close_timeout = close_timeout
        self._interrupt_timeout = interrupt_timeout
        self._line_limit = line_limit
        self._loop = asyncio.get_running_loop()
        self._queue: asyncio.Queue[RunnerEvent] = asyncio.Queue()
        self._write_lock = asyncio.Lock()
        self._turn_open = False
        self._turn_end: asyncio.Future[bool | None] | None = None
        self._started = False
        self._interrupted = False
        self._holding = 0
        self._pending: deque[str] = deque()
        self._requests: dict[str, dict[str, object]] = {}
        self._tasks: set[str] = set()
        self._closing = False
        self._ended = False
        self._shutdown: asyncio.Future[None] | None = None
        self.events: AsyncIterator[RunnerEvent] = self._iterate()
        self._reader = self._loop.create_task(self._run())

    async def send(self, message: str, mode: DispatchMode) -> None:
        mode = DispatchMode(mode)
        self._check_open()
        if not self._turn_open or mode is DispatchMode.STEER:
            await self._write_user(message)
            return
        if mode is DispatchMode.QUEUE:
            self._pending.append(message)
            return
        self._holding += 1
        try:
            outcome = await self._stop_turn()
        finally:
            self._holding -= 1
        if outcome is None:
            await self.close()
            raise SessionClosed('the turn did not stop in time')
        self._check_open()
        await self._write_user(message)

    async def respond(self, request_id: str, answer: RequestAnswer) -> None:
        self._check_open()
        if request_id not in self._requests:
            raise KeyError(request_id)
        decision = Decision(answer.decision)
        tool_input = self._requests.pop(request_id)
        if decision is Decision.ALLOW:
            line = allow_line(request_id, tool_input)
        else:
            line = deny_line(request_id, answer.message or 'Denied.')
        await self._write(line)

    async def interrupt(self) -> None:
        if not self._turn_open or self._is_closed():
            return
        self._holding += 1
        try:
            outcome = await self._stop_turn()
        except SessionClosed:
            outcome = None
        finally:
            self._holding -= 1
        if outcome is None or outcome:
            await self.close()
            return
        await self._write_next()

    async def close(self) -> None:
        self._closing = True
        if self._shutdown is None:
            self._shutdown = self._loop.create_task(self._end_process())
        await asyncio.shield(self._shutdown)

    def _is_closed(self) -> bool:
        return self._closing or self._ended or self._process.returncode is not None

    def _check_open(self) -> None:
        if self._is_closed():
            raise SessionClosed('the session has ended')

    async def _iterate(self) -> AsyncIterator[RunnerEvent]:
        while True:
            event = await self._queue.get()
            yield event
            if isinstance(event, Exited):
                return

    def _emit(self, event: RunnerEvent) -> None:
        self._queue.put_nowait(event)

    def _open_turn(self) -> None:
        if not self._turn_open:
            self._turn_open = True
            self._turn_end = self._loop.create_future()

    def _end_turn(self, outcome: bool | None) -> None:
        self._turn_open = False
        self._started = False
        self._interrupted = False
        self._requests.clear()
        if self._turn_end is not None and not self._turn_end.done():
            self._turn_end.set_result(outcome)
        self._turn_end = None

    async def _stop_turn(self) -> bool | None:
        turn_end = self._turn_end
        if turn_end is None:
            return False
        if not self._interrupted:
            self._interrupted = True
            await self._write(interrupt_line(str(uuid.uuid4())))
        try:
            return await asyncio.wait_for(asyncio.shield(turn_end), self._interrupt_timeout)
        except TimeoutError:
            return None

    async def _write_user(self, message: str) -> None:
        self._open_turn()
        await self._write(user_line(message))

    async def _write_next(self) -> None:
        if self._turn_open or self._holding or not self._pending or self._is_closed():
            return
        message = self._pending.popleft()
        with contextlib.suppress(SessionClosed):
            await self._write_user(message)

    async def _write(self, line: bytes) -> None:
        async with self._write_lock:
            stdin = self._process.stdin
            if stdin is None or stdin.is_closing():
                raise SessionClosed('stdin is closed')
            try:
                stdin.write(line)
                await stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as error:
                raise SessionClosed('stdin is broken') from error

    async def _run(self) -> None:
        try:
            await self._read_lines()
        finally:
            code = await self._process.wait()
            self._ended = True
            self._end_turn(None)
            self._emit(Exited(code))

    async def _read_lines(self) -> None:
        stdout = self._process.stdout
        if stdout is None:
            return
        buffer = bytearray()
        dropping = False
        while chunk := await stdout.read(_CHUNK):
            buffer += chunk
            start = 0
            while (end := buffer.find(b'\n', start)) != -1:
                if not dropping and end - start <= self._line_limit:
                    await self._handle(bytes(buffer[start:end]))
                dropping = False
                start = end + 1
            del buffer[:start]
            if len(buffer) > self._line_limit:
                buffer.clear()
                dropping = True
        if buffer and not dropping:
            await self._handle(bytes(buffer))

    async def _handle(self, line: bytes) -> None:
        for item in parse_line(line):
            await self._apply(item)

    async def _apply(self, item: Parsed) -> None:
        match item:
            case Init():
                if not self._started:
                    self._started = True
                    self._open_turn()
                    self._emit(TurnStarted())
            case PermissionRequest():
                self._requests[item.request_id] = item.input
                self._emit(item)
            case BadRequest():
                with contextlib.suppress(SessionClosed):
                    await self._write(deny_line(item.request_id, UNREADABLE_REQUEST))
            case TaskStarted():
                self._tasks.add(item.task_id)
            case TaskEnded():
                self._tasks.discard(item.task_id)
            case Result():
                self._emit(self._safe(turn_result(item, self._interrupted)))
                self._end_turn(bool(self._tasks))
                await self._write_next()
            case _:
                self._emit(item)

    def _safe(self, result: TurnResult) -> TurnResult:
        if result.reason is None:
            return result
        return dataclasses.replace(result, reason=safe_reason(result.reason, self._secrets))

    async def _end_process(self) -> None:
        process = self._process
        stdin = process.stdin
        if stdin is not None and not stdin.is_closing():
            stdin.close()
        exit_wait = self._loop.create_task(process.wait())
        for signal in (process.terminate, process.kill):
            done, _ = await asyncio.wait({exit_wait}, timeout=self._close_timeout)
            if done:
                return
            with contextlib.suppress(ProcessLookupError):
                signal()
        await exit_wait
