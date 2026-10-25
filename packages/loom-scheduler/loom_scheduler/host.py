import sqlite3
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol

from loom_contracts.calls import CallName
from loom_contracts.events import EventType

CODES = frozenset({'invalid', 'not_found', 'conflict', 'failed', 'forbidden'})


class ScheduleError(Exception):
    def __init__(self, code: str, message: str) -> None:
        if code not in CODES:
            raise ValueError(f'unknown error code: {code!r}')
        super().__init__(f'{code}: {message}')
        self.code = code
        self.message = message


@dataclass(frozen=True)
class Maker:
    by: str | None
    verified: bool
    schedule_id: str | None


class SchedulerHost(Protocol):
    def transaction(self) -> AbstractContextManager[sqlite3.Connection]: ...

    def command(self, command_id: str, call: CallName,
                change: Callable[[sqlite3.Connection], object]) -> tuple[object, bool]: ...

    def append_event(self, tx: sqlite3.Connection, type: EventType, thread_id: str | None,
                     data: Mapping[str, object]) -> int: ...

    async def call(self, call: CallName, params: dict, maker: Maker) -> object: ...

    def listen(self, listener: Callable[[], None]) -> Callable[[], None]: ...

    def last_event_id(self) -> int: ...
