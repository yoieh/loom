import asyncio
import contextlib
import dataclasses
import os
import signal
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from typing import Protocol

from loom_contracts.runners import (
    Capability,
    DispatchMode,
    Exited,
    MessageEntered,
    RequestAnswer,
    RunnerEvent,
    RunnerHost,
    TextOut,
    ThreadSpec,
    ToolUse,
    TurnResult,
    TurnStarted,
)
from loom_contracts.states import TurnState

from .command import codex_argv, codex_env
from .reasons import safe_reason
from .record import RecordError, read_record, write_record
from .stream import ErrorLine, Parsed, ThreadStarted, TurnEnded, parse_line

INTERRUPTED = 'interrupted'

_CHUNK = 65_536

_GROUP_POLL = 0.02


class SessionClosed(Exception):
    pass


class RunnerStartError(Exception):
    pass


class Process(Protocol):
    pid: int
    stdin: asyncio.StreamWriter | None
    stdout: asyncio.StreamReader | None
    returncode: int | None

    async def wait(self) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


Spawn = Callable[[list[str], str, dict[str, str], int], Awaitable[Process]]

GetGroup = Callable[[int], int]

SignalGroup = Callable[[int, int], None]


class _ExitProtocol(asyncio.subprocess.SubprocessStreamProtocol):
    def __init__(self, limit: int, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(limit=limit, loop=loop)
        self.exited: asyncio.Future[None] = loop.create_future()

    def process_exited(self) -> None:
        super().process_exited()
        if not self.exited.done():
            self.exited.set_result(None)


class _CodexProcess(asyncio.subprocess.Process):
    def __init__(
        self,
        transport: asyncio.SubprocessTransport,
        protocol: _ExitProtocol,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__(transport, protocol, loop)
        self._exit = protocol.exited

    async def wait(self) -> int:
        await asyncio.shield(self._exit)
        return self.returncode


async def spawn_process(argv: list[str], cwd: str, env: dict[str, str], limit: int) -> Process:
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.subprocess_exec(
        lambda: _ExitProtocol(limit + 2, loop),
        *argv,
        cwd=cwd,
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
    )
    return _CodexProcess(transport, protocol, loop)


def _start_text(codex_bin: str, error: OSError) -> str:
    why = error.strerror
    if not isinstance(why, str) or not why:
        why = type(error).__name__
    text = f'could not start {codex_bin}: {why}'
    if isinstance(error.filename, str) and error.filename != codex_bin:
        text += f': {error.filename}'
    return text


def _child_group(process: Process, getpgid: GetGroup) -> int | None:
    try:
        group = getpgid(process.pid)
    except OSError:
        return None
    if group <= 1 or group == os.getpgrp():
        return None
    return group


@dataclasses.dataclass(eq=False)
class _Turn:
    message: bytes
    prompt: str
    spawned: asyncio.Future[None]
    process: Process | None = None
    group: int | None = None
    watch: asyncio.Task[None] | None = None
    stop: asyncio.Task[None] | None = None
    started: bool = False
    over: bool = False
    interrupted: bool = False
    text: str | None = None
    error: str | None = None
    tools: set[str] = dataclasses.field(default_factory=set)


class CodexRunner:
    name = 'codex'
    capabilities = frozenset({Capability.INTERRUPT, Capability.RESUME})

    def __init__(
        self,
        spawn: Spawn = spawn_process,
        close_timeout: float = 5.0,
        line_limit: int = 8_388_608,
        getpgid: GetGroup = os.getpgid,
        killpg: SignalGroup = os.killpg,
    ) -> None:
        self._spawn = spawn
        self._close_timeout = close_timeout
        self._line_limit = line_limit
        self._getpgid = getpgid
        self._killpg = killpg

    async def start(self, thread: ThreadSpec, host: RunnerHost) -> 'CodexSession':
        codex_bin = host.setting('codex_bin')
        if not isinstance(codex_bin, str) or not codex_bin:
            codex_bin = 'codex'
        codex_argv(thread, codex_bin, None)
        env = codex_env(thread, host, codex_bin)
        token = env.get('CLIPROXY_API_KEY')
        codex_id = None
        if thread.resume is True:
            try:
                codex_id = read_record(host.data_dir, thread.id)
            except RecordError as error:
                text = f'the codex thread record for {thread.id} is unreadable'
                raise RunnerStartError(text) from error
        return CodexSession(
            thread,
            codex_bin,
            env,
            codex_id,
            data_dir=host.data_dir,
            spawn=self._spawn,
            close_timeout=self._close_timeout,
            line_limit=self._line_limit,
            secrets=(token,) if token else (),
            getpgid=self._getpgid,
            killpg=self._killpg,
        )


class CodexSession:
    def __init__(
        self,
        thread: ThreadSpec,
        codex_bin: str,
        env: dict[str, str],
        codex_id: str | None,
        *,
        data_dir: str,
        spawn: Spawn = spawn_process,
        close_timeout: float = 5.0,
        line_limit: int = 8_388_608,
        secrets: tuple[str, ...] = (),
        getpgid: GetGroup = os.getpgid,
        killpg: SignalGroup = os.killpg,
    ) -> None:
        self._thread = thread
        self._codex_bin = codex_bin
        self._env = env
        self._codex_id = codex_id
        self._data_dir = data_dir
        self._spawn = spawn
        self._close_timeout = close_timeout
        self._line_limit = line_limit
        self._secrets = secrets
        self._getpgid = getpgid
        self._killpg = killpg
        self._loop = asyncio.get_running_loop()
        self._queue: asyncio.Queue[RunnerEvent] = asyncio.Queue()
        self._lane = asyncio.Lock()
        self._turn: _Turn | None = None
        self._last: _Turn | None = None
        self._holding = 0
        self._pending: deque[str] = deque()
        self._tasks: set[asyncio.Task] = set()
        self._closing = False
        self._ended = False
        self._shutdown: asyncio.Task[None] | None = None
        self.events: AsyncIterator[RunnerEvent] = self._iterate()

    async def send(self, message: str, mode: DispatchMode) -> None:
        mode = DispatchMode(mode)
        self._check_open()
        if self._turn is not None and mode is not DispatchMode.INTERRUPT:
            self._pending.append(message)
            return
        while self._turn is not None:
            self._holding += 1
            try:
                await self._stop()
            finally:
                self._holding -= 1
            self._check_open()
        await self._begin(message)

    async def respond(self, request_id: str, answer: RequestAnswer) -> None:
        raise KeyError(request_id)

    async def interrupt(self) -> None:
        if self._is_closed():
            return
        await self._stop()

    async def close(self) -> None:
        self._closing = True
        if self._shutdown is None:
            self._shutdown = self._loop.create_task(self._shut())
        await asyncio.shield(self._shutdown)

    def _is_closed(self) -> bool:
        return self._closing or self._ended

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
        if not self._ended:
            self._queue.put_nowait(event)

    def _finish(self, code: int | None) -> None:
        if self._ended:
            return
        self._ended = True
        self._pending.clear()
        self._queue.put_nowait(Exited(code))

    def _task(self, coroutine: Coroutine[object, object, None]) -> asyncio.Task[None]:
        task = self._loop.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _reserve(self, message: str) -> _Turn:
        turn = _Turn(message.encode('utf-8', 'replace'), message, self._loop.create_future())
        self._turn = turn
        return turn

    async def _begin(self, message: str) -> None:
        turn = self._reserve(message)
        try:
            await self._open(turn)
        finally:
            self._start_held()

    def _start_held(self) -> None:
        if self._turn is not None or self._holding or self._is_closed() or not self._pending:
            return
        turn = self._reserve(self._pending.popleft())
        self._task(self._begin_held(turn))

    async def _begin_held(self, turn: _Turn) -> None:
        try:
            await self._open(turn)
        except SessionClosed:
            pass
        except RunnerStartError as error:
            self._emit(TurnResult(TurnState.FAILED, None, str(error)))
        finally:
            self._start_held()

    async def _open(self, turn: _Turn) -> None:
        try:
            async with self._lane:
                await self._retire(self._last)
                self._check_open()
                argv = codex_argv(self._thread, self._codex_bin, self._codex_id)
                try:
                    process = await self._spawn(
                        argv, self._thread.workspace_path, self._env, self._line_limit,
                    )
                except OSError as error:
                    text = _start_text(self._codex_bin, error)
                    raise RunnerStartError(safe_reason(text, self._secrets)) from error
                turn.process = process
                turn.group = _child_group(process, self._getpgid)
                turn.watch = self._task(self._watch(turn))
                self._last = turn
        except BaseException:
            if self._turn is turn:
                self._turn = None
            raise
        finally:
            if not turn.spawned.done():
                turn.spawned.set_result(None)
        self._check_open()
        await self._feed(turn)

    async def _feed(self, turn: _Turn) -> None:
        stdin = turn.process.stdin
        if stdin is None:
            return
        with contextlib.suppress(OSError):
            stdin.write(turn.message)
            await stdin.drain()
        stdin.close()

    async def _stop(self) -> None:
        turn = self._turn
        if turn is None:
            return
        turn.interrupted = True
        await asyncio.shield(turn.spawned)
        if turn.watch is None:
            return
        await self._kill(turn)
        await asyncio.shield(turn.watch)

    async def _shut(self) -> None:
        self._pending.clear()
        turn = self._turn
        if turn is not None:
            await asyncio.shield(turn.spawned)
        last = self._last
        code = None
        if last is not None and last.watch is not None and not last.watch.done():
            if last is self._turn:
                await self._kill(last)
                await asyncio.shield(last.watch)
            else:
                await self._retire(last)
            code = last.process.returncode
        self._finish(code)

    async def _watch(self, turn: _Turn) -> None:
        reading = self._loop.create_task(self._read_lines(turn))
        try:
            code = await turn.process.wait()
            await self._end_group(turn)
            await asyncio.wait({reading}, timeout=self._close_timeout)
        finally:
            reading.cancel()
        self._exited(turn, code)

    def _exited(self, turn: _Turn, code: int) -> None:
        if turn.over or self._is_closed():
            return
        if turn.interrupted:
            self._end_turn(turn, TurnResult(TurnState.STOPPED, turn.text, INTERRUPTED))
        elif not turn.started:
            reason = turn.error or f'codex exited with code {code}'
            reason = safe_reason(reason, self._secrets)
            self._end_turn(turn, TurnResult(TurnState.FAILED, None, reason))
        else:
            turn.over = True
            if self._turn is turn:
                self._turn = None
            self._finish(code)

    def _end_turn(self, turn: _Turn, result: TurnResult) -> None:
        turn.over = True
        self._emit(result)
        if self._turn is turn:
            self._turn = None
        self._start_held()

    async def _read_lines(self, turn: _Turn) -> None:
        stdout = turn.process.stdout
        if stdout is None:
            return
        buffer = bytearray()
        dropping = False
        while chunk := await stdout.read(_CHUNK):
            buffer += chunk
            start = 0
            while (end := buffer.find(b'\n', start)) != -1:
                if not dropping and end - start <= self._line_limit:
                    self._handle(turn, bytes(buffer[start:end]))
                dropping = False
                start = end + 1
            del buffer[:start]
            if len(buffer) > self._line_limit:
                buffer.clear()
                dropping = True
        if buffer and not dropping:
            self._handle(turn, bytes(buffer))

    def _handle(self, turn: _Turn, line: bytes) -> None:
        item = parse_line(line)
        if item is not None and not turn.over and not self._ended:
            self._apply(turn, item)

    def _apply(self, turn: _Turn, item: Parsed) -> None:
        match item:
            case ThreadStarted():
                self._remember(item.thread_id)
            case TurnStarted():
                if not turn.started:
                    turn.started = True
                    self._emit(item)
                    self._emit(MessageEntered(turn.prompt))
            case TextOut():
                turn.text = item.text
                self._emit(item)
            case ToolUse():
                if item.tool_use_id not in turn.tools:
                    turn.tools.add(item.tool_use_id)
                    self._emit(item)
            case ErrorLine():
                turn.error = item.message
            case TurnEnded():
                state = TurnState.FAILED if item.failed else TurnState.COMPLETED
                reason = None if item.reason is None else safe_reason(item.reason, self._secrets)
                self._end_turn(turn, TurnResult(state, turn.text, reason))

    def _remember(self, codex_id: str) -> None:
        if codex_id == self._codex_id:
            return
        self._codex_id = codex_id
        with contextlib.suppress(OSError):
            write_record(self._data_dir, self._thread.id, codex_id)

    async def _retire(self, turn: _Turn | None) -> None:
        if turn is None or turn.watch is None or turn.watch.done():
            return
        exit_wait = self._loop.create_task(turn.process.wait())
        done, _ = await asyncio.wait({exit_wait}, timeout=self._close_timeout)
        if not done:
            exit_wait.cancel()
            await self._kill(turn)
        await asyncio.shield(turn.watch)

    async def _kill(self, turn: _Turn) -> None:
        if turn.stop is None:
            turn.stop = self._loop.create_task(self._signal_until_exit(turn))
        await asyncio.shield(turn.stop)

    async def _signal_until_exit(self, turn: _Turn) -> None:
        process = turn.process
        exit_wait = self._loop.create_task(process.wait())
        for signum in (signal.SIGTERM, signal.SIGKILL):
            if process.returncode is not None:
                break
            self._signal(turn, signum)
            done, _ = await asyncio.wait({exit_wait}, timeout=self._close_timeout)
            if done:
                break
        await exit_wait

    def _signal(self, turn: _Turn, signum: int) -> None:
        if turn.group is not None:
            self._signal_group(turn.group, signum)
            return
        stop = turn.process.terminate if signum == signal.SIGTERM else turn.process.kill
        with contextlib.suppress(ProcessLookupError):
            stop()

    def _signal_group(self, group: int, signum: int) -> bool:
        try:
            self._killpg(group, signum)
        except ProcessLookupError:
            return False
        return True

    async def _end_group(self, turn: _Turn) -> None:
        group = turn.group
        if group is None:
            return
        if not self._signal_group(group, 0) or not self._signal_group(group, signal.SIGTERM):
            return
        deadline = self._loop.time() + self._close_timeout
        while self._loop.time() < deadline:
            await asyncio.sleep(_GROUP_POLL)
            if not self._signal_group(group, 0):
                return
        self._signal_group(group, signal.SIGKILL)
