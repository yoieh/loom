import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Protocol


class Source(Protocol):
    def next_wake(self) -> datetime | None: ...

    async def run_due(self, now: datetime) -> None: ...


class Tick:
    def __init__(self, clock: Callable[[], datetime], *, max_sleep: float = 30.0) -> None:
        self.clock = clock
        self.max_sleep = max_sleep
        self._sources: list[Source] = []
        self._woken = asyncio.Event()

    def register(self, source: Source) -> None:
        self._sources.append(source)
        self.wake()

    def wake(self) -> None:
        self._woken.set()

    def delay(self) -> float:
        wakes = [wake for source in self._sources if (wake := source.next_wake()) is not None]
        if not wakes:
            return self.max_sleep
        seconds = (min(wakes) - self.clock()).total_seconds()
        return min(max(seconds, 0.0), self.max_sleep)

    async def run_once(self) -> None:
        now = self.clock()
        for source in tuple(self._sources):
            wake = source.next_wake()
            if wake is None or wake > now:
                continue
            try:
                await source.run_due(now)
            except Exception:
                pass

    async def wait(self, timeout: float) -> None:
        try:
            async with asyncio.timeout(timeout):
                await self._woken.wait()
        except TimeoutError:
            pass

    async def run(self) -> None:
        while True:
            self._woken.clear()
            await self.run_once()
            await self.wait(self.delay())
