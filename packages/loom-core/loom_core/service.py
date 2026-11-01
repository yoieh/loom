import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass

from loom_contracts.calls import Audience, CallName, ProjectInfo, ThreadInfo
from loom_contracts.events import Event

from . import keys
from .errors import LoomError
from .orchestrator import Orchestrator
from .projects import Projects
from .signals import Signals
from .slots import StartSource
from .store import MAX_EVENT_ID, MAX_EVENTS, Store

SERVED: tuple[str, ...] = (
    'threads.launch', 'threads.delegate', 'threads.send', 'threads.list', 'threads.get',
    'threads.stop', 'threads.settle', 'threads.update', 'threads.events', 'projects.add',
    'projects.set_mode', 'projects.remove', 'projects.list', 'requests.respond', 'signals.emit',
)


@dataclass(frozen=True)
class Caller:
    thread_id: str | None = None
    verified: bool = True
    schedule_id: str | None = None

    @property
    def scheduled(self) -> bool:
        return self.schedule_id is not None


Method = Callable[[object, Caller], Awaitable[object]]


def _params(params: object) -> dict:
    if not isinstance(params, dict):
        raise LoomError('invalid', 'params must be an object')
    return params


def _required(params: dict, field: str) -> object:
    value = params.get(field)
    if value is None:
        raise LoomError('invalid', f'{field} is required')
    return value


def _string(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise LoomError('invalid', f'{field} must be a string')
    return value


def _audience(params: dict) -> Audience | None:
    if 'audience' not in params:
        return None
    value = params['audience']
    if value is None:
        raise LoomError('invalid', 'audience must not be null')
    if isinstance(value, str) and value in tuple(Audience):
        return Audience(value)
    raise LoomError('invalid', f'audience must be one of {", ".join(Audience)}')


def _is_person(caller: Caller) -> bool:
    if caller.scheduled:
        return False
    return caller.thread_id is None and caller.verified is True


def _identity(caller: Caller) -> str | None:
    if caller.scheduled:
        return None
    if caller.thread_id is not None:
        return f'thread:{caller.thread_id}'
    return 'person' if caller.verified is True else None


def _own_thread(caller: Caller) -> str | None:
    if caller.scheduled:
        return None
    return caller.thread_id


def _source(caller: Caller) -> StartSource:
    if caller.scheduled:
        return StartSource.SCHEDULE
    if caller.thread_id is not None or caller.verified is not True:
        return StartSource.AGENT
    return StartSource.PERSON


def _events_params(params: object) -> tuple[int, str | None, Audience | None, bool]:
    params = _params(params)
    after = params.get('after', 0)
    if (
        isinstance(after, bool) or not isinstance(after, int)
        or not 0 <= after <= MAX_EVENT_ID
    ):
        raise LoomError('invalid', 'after must be an integer of 0 or more')
    thread_id = None
    if 'thread_id' in params:
        thread_id = _string(params['thread_id'], 'thread_id')
    audience = _audience(params)
    follow = params.get('follow', False)
    if not isinstance(follow, bool):
        raise LoomError('invalid', 'follow must be true or false')
    return after, thread_id, audience, follow


class Service:
    def __init__(self, store: Store, projects: Projects, orchestrator: Orchestrator) -> None:
        self.store = store
        self.projects = projects
        self.orchestrator = orchestrator
        self.signals = Signals(store, projects)
        self._methods: dict[str, Method] = {
            'threads.launch': self._launch,
            'threads.delegate': self._delegate,
            'threads.send': self._send,
            'threads.list': self._list_threads,
            'threads.get': self._get_thread,
            'threads.stop': self._stop,
            'threads.settle': self._settle,
            'threads.update': self._update,
            'threads.events': self._events,
            'projects.add': self._add_project,
            'projects.set_mode': self._set_mode,
            'projects.remove': self._remove_project,
            'projects.list': self._list_projects,
            'requests.respond': self._respond,
            'signals.emit': self._emit,
        }

    def method(self, call: object) -> Method:
        if not isinstance(call, str):
            raise LoomError('invalid', 'call must be a string')
        try:
            CallName(call)
        except ValueError:
            raise LoomError('invalid', f'unknown call: {call}') from None
        if call not in self._methods:
            raise LoomError('invalid', f'{call} is not served yet')
        return self._methods[call]

    def serve(self, methods: Mapping[str, Method]) -> None:
        names = []
        for call in methods:
            try:
                name = str(CallName(call))
            except ValueError:
                raise ValueError(f'unknown call: {call!r}') from None
            if name in self._methods:
                raise ValueError(f'{name} is already served')
            names.append(name)
        self._methods.update(zip(names, methods.values()))

    def follow(self, params: object, caller: Caller) -> AsyncIterator[Event]:
        after, thread_id, audience, _ = _events_params(params)
        return self._follow(after, thread_id, audience)

    async def close(self) -> None:
        await self.orchestrator.close()
        self.store.close()

    async def _follow(self, after: int, thread_id: str | None,
                      audience: Audience | None) -> AsyncIterator[Event]:
        wake = asyncio.Event()
        remove = self.store.listen(wake.set)
        try:
            while True:
                wake.clear()
                events = self.store.events(after, thread_id, audience=audience)
                for event in events:
                    after = event['id']
                    yield event
                if len(events) < MAX_EVENTS:
                    await wake.wait()
        finally:
            remove()

    async def _launch(self, params: object, caller: Caller) -> ThreadInfo:
        thread_id = caller.thread_id
        if thread_id is not None and not caller.scheduled:
            if self._stopping(_params(params).get('id'), thread_id):
                raise LoomError('conflict', f'thread {thread_id} is stopping')
        return await self.orchestrator.launch(params, source=_source(caller),
                                              launcher=_identity(caller),
                                              caller_thread=_own_thread(caller))

    def _stopping(self, command_id: object, thread_id: str) -> bool:
        with self.store.transaction() as tx:
            if isinstance(command_id, str) and tx.execute(
                    'SELECT 1 FROM commands WHERE id = ?', (command_id,)).fetchone():
                return False
            row = tx.execute(
                'SELECT stop_requested FROM threads WHERE id = ?', (thread_id,)).fetchone()
        return row is not None and bool(row[0])

    async def _delegate(self, params: object, caller: Caller) -> ThreadInfo:
        return await self.orchestrator.delegate(params, source=StartSource.AGENT,
                                                caller_thread=_own_thread(caller))

    async def _send(self, params: object, caller: Caller) -> ThreadInfo:
        return await self.orchestrator.send(params, source=_source(caller),
                                            caller_thread=_own_thread(caller))

    async def _list_threads(self, params: object, caller: Caller) -> list[ThreadInfo]:
        params = _params(params)
        project_id = None
        if keys.PROJECT_KEY in params:
            project_id = _string(params[keys.PROJECT_KEY], keys.PROJECT_KEY)
        return self.orchestrator.list(project_id, _audience(params))

    async def _get_thread(self, params: object, caller: Caller) -> ThreadInfo:
        params = _params(params)
        return self.orchestrator.get(_string(_required(params, 'thread_id'), 'thread_id'))

    async def _stop(self, params: object, caller: Caller) -> ThreadInfo:
        return await self.orchestrator.stop(params)

    async def _settle(self, params: object, caller: Caller) -> ThreadInfo:
        return await self.orchestrator.settle(params)

    async def _update(self, params: object, caller: Caller) -> ThreadInfo:
        return await self.orchestrator.update(params)

    async def _events(self, params: object, caller: Caller) -> list[Event]:
        after, thread_id, audience, follow = _events_params(params)
        if follow:
            raise LoomError('invalid', 'follow needs a stream')
        return self.store.events(after, thread_id, audience=audience)

    async def _add_project(self, params: object, caller: Caller) -> ProjectInfo:
        return self.projects.add(params)

    async def _set_mode(self, params: object, caller: Caller) -> ProjectInfo:
        if not _is_person(caller):
            raise LoomError('forbidden', "agent threads may not change a project's mode")
        return self.projects.set_mode(params)

    async def _remove_project(self, params: object, caller: Caller) -> ProjectInfo:
        return self.projects.remove(params)

    async def _list_projects(self, params: object, caller: Caller) -> list[ProjectInfo]:
        _params(params)
        return self.projects.list()

    async def _respond(self, params: object, caller: Caller) -> ThreadInfo:
        return await self.orchestrator.respond(params, answerer=_identity(caller),
                                               answerer_thread=caller.thread_id)

    async def _emit(self, params: object, caller: Caller) -> Event:
        return self.signals.emit(params, caller_thread=_own_thread(caller))
