import asyncio
from collections.abc import AsyncIterator, Callable

from loom_contracts.runners import (
    DispatchMode, RequestAnswer, RunnerEvent, ThreadSpec, TurnResult,
)
from loom_contracts.states import TurnState

_END = object()


class FakeSession:
    def __init__(self, runner: 'FakeRunner', thread: ThreadSpec) -> None:
        self.runner = runner
        self.thread = thread
        self.sent: list[tuple[str, DispatchMode]] = []
        self.responses: list[tuple[str, RequestAnswer]] = []
        self.interrupts = 0
        self.closed = False
        self.fail_send: Exception | None = None
        self.fail_interrupt: Exception | None = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self.events: AsyncIterator[RunnerEvent] = self._stream()

    async def _stream(self) -> AsyncIterator[RunnerEvent]:
        while (event := await self._queue.get()) is not _END:
            yield event

    def emit(self, event: RunnerEvent) -> None:
        self._queue.put_nowait(event)

    async def send(self, message: str, mode: DispatchMode) -> None:
        if self.fail_send is not None:
            error, self.fail_send = self.fail_send, None
            raise error
        self.sent.append((message, mode))

    async def respond(self, request_id: str, answer: RequestAnswer) -> None:
        self.responses.append((request_id, answer))

    async def interrupt(self) -> None:
        self.interrupts += 1
        if self.fail_interrupt is not None:
            error, self.fail_interrupt = self.fail_interrupt, None
            raise error
        if self.runner.stop_on_interrupt:
            self.emit(TurnResult(TurnState.STOPPED, None, 'interrupted'))

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._queue.put_nowait(_END)


class FakeRunner:
    def __init__(self, name: str = 'claude',
                 capabilities: frozenset[str] = frozenset({'steer', 'interrupt', 'resume'}),
                 fail_start: Exception | None = None,
                 stop_on_interrupt: bool = True) -> None:
        self.name = name
        self.capabilities = frozenset(capabilities)
        self.fail_start = fail_start
        self.stop_on_interrupt = stop_on_interrupt
        self.starts: list[ThreadSpec] = []
        self.sessions: dict[str, FakeSession] = {}

    async def start(self, thread: ThreadSpec, host: object) -> FakeSession:
        self.starts.append(thread)
        if self.fail_start is not None:
            raise self.fail_start
        session = FakeSession(self, thread)
        self.sessions[thread.id] = session
        return session


class FakeHost:
    def __init__(self, data_dir: str, settings: dict[str, str] | None = None) -> None:
        self.data_dir = str(data_dir)
        self.settings = dict(settings or {})

    def setting(self, key: str) -> str | None:
        return self.settings.get(key)

    def environment(self, thread: ThreadSpec) -> dict[str, str]:
        return {'LOOM_THREAD': thread.id}

    def secret(self, name: str) -> str | None:
        return None


async def wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f'condition not met within {timeout} s')
        await asyncio.sleep(0.005)
