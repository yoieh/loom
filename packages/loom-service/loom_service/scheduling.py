import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractContextManager

from loom_core.errors import LoomError
from loom_core.service import Caller, Method, Service
from loom_core.store import Store
from loom_scheduler import Maker, ScheduleError, Scheduler


class ServiceHost:
    def __init__(self, store: Store, service: Service) -> None:
        self.store = store
        self.service = service

    def transaction(self) -> AbstractContextManager[sqlite3.Connection]:
        return self.store.transaction()

    def command(self, command_id: str, call: str,
                change: Callable[[sqlite3.Connection], object]) -> tuple[object, bool]:
        try:
            receipt = self.store.command(command_id, call, change)
        except LoomError as error:
            raise ScheduleError(error.code, error.message) from None
        return receipt.answer, receipt.replayed

    def append_event(self, tx: sqlite3.Connection, type: str, thread_id: str | None,
                     data: Mapping[str, object]) -> int:
        return self.store.append_event(tx, type, thread_id, data)

    async def call(self, call: str, params: dict, maker: Maker) -> object:
        caller = Caller(thread_id=maker.by, verified=maker.verified,
                        scheduled=maker.schedule_id is not None, schedule_id=maker.schedule_id)
        try:
            return await self.service.method(call)(params, caller)
        except LoomError as error:
            raise ScheduleError(error.code, error.message) from None

    def listen(self, listener: Callable[[], None]) -> Callable[[], None]:
        return self.store.listen(listener)

    def last_event_id(self) -> int:
        return self.store.last_event_id()


def _served(method: Callable[[object, object], Awaitable[object]]) -> Method:
    async def serve(params: object, caller: Caller) -> object:
        try:
            return await method(params, caller)
        except ScheduleError as error:
            raise LoomError(error.code, error.message) from None

    return serve


def serve_schedules(service: Service, scheduler: Scheduler) -> None:
    service.serve({call: _served(method) for call, method in scheduler.methods().items()})
