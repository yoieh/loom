import asyncio
from collections.abc import Awaitable, Callable


class Lanes:
    def __init__(self) -> None:
        self._tails: dict[str, asyncio.Future] = {}
        self._active: set[asyncio.Future] = set()

    def submit[T](self, key: str, effect: Callable[[], Awaitable[T]]) -> asyncio.Future[T]:
        task = asyncio.ensure_future(self._run(self._tails.get(key), effect))
        self._tails[key] = task
        self._active.add(task)
        task.add_done_callback(lambda done: self._finish(key, done))
        return task

    async def _run[T](self, previous: asyncio.Future | None,
                      effect: Callable[[], Awaitable[T]]) -> T:
        if previous is not None:
            await asyncio.wait([previous])
        return await effect()

    def _finish(self, key: str, task: asyncio.Future) -> None:
        self._active.discard(task)
        if self._tails.get(key) is task:
            del self._tails[key]

    async def drain(self) -> None:
        while pending := [task for task in self._active if not task.done()]:
            await asyncio.wait(pending)
