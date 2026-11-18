import asyncio
import uuid
from collections.abc import Awaitable, Callable

from loom_core.errors import LoomError
from loom_core.service import Caller, Method, Service
from loom_core.store import MAX_COMMAND_ID
from loom_core.threads import MODE_ORDER
from loom_runner_t3 import T3Error, T3NotFound, T3Threads
from loom_runner_t3.session import SEND_MODES

CALL_LIMIT = 25.0
MAX_THREAD_ID = 200
MAX_STATUSES = 10
MAX_REASON = 2000
MAX_LIST = 100
MAX_SEARCH = 50
MAX_READ = 100
VIEWS = ('messages', 'activity')
LIST = 't3_threads.list'
SEARCH = 't3_threads.search'
READ = 't3_threads.read'
SEND = 't3_threads.send'
INTERRUPT = 't3_threads.interrupt'
MODES = tuple(SEND_MODES)

CANNOT_TELL = 'cannot tell the permission mode of this thread or the T3 thread'
WIDER = 'a thread may not send to a T3 thread whose permission mode is wider than its own'

Run = Callable[[dict, Caller], Awaitable[object]]


def _invalid(message: str) -> LoomError:
    return LoomError('invalid', message)


def _required(params: dict, key: str) -> object:
    value = params.get(key)
    if value is None:
        raise _invalid(f'{key} is required')
    return value


def _string(value: object, key: str) -> str:
    if not isinstance(value, str):
        raise _invalid(f'{key} must be a string')
    return value


def _text(value: object, key: str) -> str:
    if not isinstance(value, str) or not value:
        raise _invalid(f'{key} must be a non-empty string')
    return value


def _optional_string(params: dict, key: str) -> str | None:
    return _string(params[key], key) if key in params else None


def _whole(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _limit(params: dict, most: int) -> int | None:
    if 'limit' not in params:
        return None
    value = params['limit']
    if not _whole(value) or not 1 <= value <= most:
        raise _invalid(f'limit must be an integer from 1 to {most}')
    return value


def _position(params: dict, key: str) -> int | None:
    if key not in params:
        return None
    value = params[key]
    if not _whole(value) or value < 0:
        raise _invalid(f'{key} must be an integer of 0 or more')
    return value


def _command_id(params: dict) -> str:
    value = params.get('id')
    if not isinstance(value, str) or not 1 <= len(value) <= MAX_COMMAND_ID:
        raise _invalid(f'id must be a string of 1 to {MAX_COMMAND_ID} characters')
    return value


def _thread_id(params: dict) -> str:
    value = _required(params, 'thread_id')
    if not isinstance(value, str) or not 1 <= len(value) <= MAX_THREAD_ID:
        raise _invalid(f'thread_id must be a string of 1 to {MAX_THREAD_ID} characters')
    return value


def _statuses(params: dict) -> list[str] | None:
    if 'statuses' not in params:
        return None
    value = params['statuses']
    if (not isinstance(value, list) or len(value) > MAX_STATUSES
            or not all(isinstance(item, str) for item in value)):
        raise _invalid(f'statuses must be a list of at most {MAX_STATUSES} strings')
    return value


def _view(params: dict) -> str | None:
    if 'view' not in params:
        return None
    value = params['view']
    if value not in VIEWS:
        raise _invalid(f'view must be one of {", ".join(VIEWS)}')
    return value


def _mode(params: dict) -> object:
    value = params.get('mode', MODES[0])
    for mode in MODES:
        if isinstance(value, str) and value == mode:
            return mode
    raise _invalid(f'mode must be one of {", ".join(MODES)}')


def _reason(params: dict) -> str | None:
    if 'reason' not in params:
        return None
    value = params['reason']
    if not isinstance(value, str) or len(value) > MAX_REASON:
        raise _invalid(f'reason must be a string of at most {MAX_REASON} characters')
    return value


def _request_id(call: str, thread_id: str, command_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f'loom:{call}:{thread_id}:{command_id}'))


def _is_person(caller: Caller) -> bool:
    return not caller.scheduled and caller.thread_id is None and caller.verified is True


def _rank(mode: object) -> int | None:
    if isinstance(mode, str) and mode in MODE_ORDER:
        return MODE_ORDER.index(mode)
    return None


class T3Calls:
    def __init__(self, service: Service, t3: T3Threads) -> None:
        self.service = service
        self.t3 = t3

    def _root(self, project_id: str) -> str:
        return self.service.projects.get(project_id)['workspace_root']

    async def _check_send(self, caller: Caller, thread_id: str) -> None:
        if _is_person(caller):
            return
        if caller.scheduled or caller.verified is not True or caller.thread_id is None:
            raise LoomError('forbidden', CANNOT_TELL)
        try:
            own = self.service.orchestrator.get(caller.thread_id)['permission_mode']
        except LoomError as error:
            if error.code == 'not_found':
                raise LoomError('forbidden', CANNOT_TELL) from None
            raise
        theirs = _rank(await self.t3.runtime_mode(thread_id))
        if theirs is None:
            raise LoomError('forbidden', CANNOT_TELL)
        if theirs > MODE_ORDER.index(own):
            raise LoomError('forbidden', WIDER)

    async def list(self, params: dict, caller: Caller) -> object:
        project_id = _string(_required(params, 'project_id'), 'project_id')
        statuses = _statuses(params)
        title_contains = _optional_string(params, 'title_contains')
        limit = _limit(params, MAX_LIST)
        cursor = _position(params, 'cursor')
        return await self.t3.list_threads(
            self._root(project_id), statuses=statuses, title_contains=title_contains,
            limit=limit, cursor=cursor)

    async def search(self, params: dict, caller: Caller) -> object:
        query = _text(_required(params, 'query'), 'query')
        project_id = _optional_string(params, 'project_id')
        limit = _limit(params, MAX_SEARCH)
        root = None if project_id is None else self._root(project_id)
        return await self.t3.search(query, root=root, limit=limit)

    async def read(self, params: dict, caller: Caller) -> object:
        thread_id = _thread_id(params)
        view = _view(params)
        after_position = _position(params, 'after_position')
        limit = _limit(params, MAX_READ)
        item_id = _optional_string(params, 'item_id')
        text_offset = _position(params, 'text_offset')
        return await self.t3.read(thread_id, view=view, after_position=after_position,
                                  limit=limit, item_id=item_id, text_offset=text_offset)

    async def send(self, params: dict, caller: Caller) -> object:
        command_id = _command_id(params)
        thread_id = _thread_id(params)
        message = _text(_required(params, 'message'), 'message')
        mode = _mode(params)
        await self._check_send(caller, thread_id)
        return await self.t3.send(
            thread_id, message, mode=mode,
            client_request_id=_request_id(SEND, thread_id, command_id))

    async def interrupt(self, params: dict, caller: Caller) -> object:
        command_id = _command_id(params)
        thread_id = _thread_id(params)
        reason = _reason(params)
        return await self.t3.interrupt(
            thread_id, reason=reason,
            client_request_id=_request_id(INTERRUPT, thread_id, command_id))


def _served(run: Run, limit: float) -> Method:
    async def serve(params: object, caller: Caller) -> object:
        if not isinstance(params, dict):
            raise _invalid('params must be an object')
        try:
            return await asyncio.wait_for(run(params, caller), limit)
        except TimeoutError:
            raise LoomError(
                'failed', f't3: timeout: T3 did not answer within {limit:g} s') from None
        except T3NotFound as error:
            raise LoomError('not_found', str(error)) from None
        except T3Error as error:
            raise LoomError('failed', str(error)) from None

    return serve


def serve_t3_threads(service: Service, t3: T3Threads, *, limit: float = CALL_LIMIT) -> None:
    calls = T3Calls(service, t3)
    service.serve({
        LIST: _served(calls.list, limit),
        SEARCH: _served(calls.search, limit),
        READ: _served(calls.read, limit),
        SEND: _served(calls.send, limit),
        INTERRUPT: _served(calls.interrupt, limit),
    })
