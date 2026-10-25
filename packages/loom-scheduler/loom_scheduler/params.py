from dataclasses import dataclass
from datetime import tzinfo
from enum import StrEnum

from loom_contracts.calls import MAX_CONCURRENCY_KEY, OnBusy, Settle
from loom_contracts.triggers import ScheduleAction

from .host import ScheduleError
from .timing import check_trigger

MAX_COMMAND_ID = 200
LAUNCH_OPTIONS = ('concurrency_key', 'on_busy', 'settle')


@dataclass(frozen=True)
class AddRequest:
    command_id: str
    trigger: dict
    text: str
    action: ScheduleAction
    thread_id: str | None
    project_id: str | None
    title: str | None
    concurrency_key: str | None
    on_busy: OnBusy | None
    settle: Settle | None


def _invalid(message: str) -> ScheduleError:
    return ScheduleError('invalid', message)


def _object(params: object) -> dict:
    if not isinstance(params, dict):
        raise _invalid('params must be an object')
    return params


def _required(params: dict, field: str) -> object:
    value = params.get(field)
    if value is None:
        raise _invalid(f'{field} is required')
    return value


def _not_null(params: dict, field: str) -> object:
    value = params[field]
    if value is None:
        raise _invalid(f'{field} must not be null')
    return value


def _string(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise _invalid(f'{field} must be a string')
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise _invalid(f'{field} must be a non-empty string')
    return value


def _optional_string(params: dict, field: str) -> str | None:
    if field not in params:
        return None
    return _string(_not_null(params, field), field)


def _command_id(params: dict) -> str:
    value = params.get('id')
    if not isinstance(value, str) or not 1 <= len(value) <= MAX_COMMAND_ID:
        raise _invalid(f'id must be a string of 1 to {MAX_COMMAND_ID} characters')
    return value


def _action(params: dict) -> ScheduleAction:
    if 'action' not in params:
        return ScheduleAction.POST
    value = _not_null(params, 'action')
    if isinstance(value, str):
        try:
            return ScheduleAction(value)
        except ValueError:
            pass
    raise _invalid(f'action must be one of {", ".join(ScheduleAction)}')


def _choice[E: StrEnum](params: dict, field: str, kind: type[E]) -> E:
    value = _not_null(params, field)
    if isinstance(value, str):
        try:
            return kind(value)
        except ValueError:
            pass
    raise _invalid(f'{field} must be one of {", ".join(kind)}')


def _launch_options(params: dict,
                    action: ScheduleAction) -> tuple[str | None, OnBusy | None, Settle | None]:
    given = [field for field in LAUNCH_OPTIONS if field in params]
    if given and action is not ScheduleAction.LAUNCH:
        raise _invalid(f'{given[0]} goes only with launch')
    key = None
    if 'concurrency_key' in params:
        key = _not_null(params, 'concurrency_key')
        if not isinstance(key, str) or not 1 <= len(key) <= MAX_CONCURRENCY_KEY:
            raise _invalid('concurrency_key must be a string of 1 to '
                           f'{MAX_CONCURRENCY_KEY} characters')
    on_busy = None
    if 'on_busy' in params:
        on_busy = _choice(params, 'on_busy', OnBusy)
        if key is None:
            raise _invalid('on_busy needs concurrency_key')
    elif key is not None:
        on_busy = OnBusy.QUEUE
    settle = _choice(params, 'settle', Settle) if 'settle' in params else None
    return key, on_busy, settle


def check_add(params: object, caller_thread: str | None, tz: tzinfo | None) -> AddRequest:
    params = _object(params)
    command_id = _command_id(params)
    trigger = check_trigger(params.get('trigger'), tz)
    text = _text(_required(params, 'text'), 'text')
    action = _action(params)
    thread_id = _optional_string(params, 'thread_id')
    project_id = _optional_string(params, 'project_id')
    title = None
    if 'title' in params:
        title = _text(_not_null(params, 'title'), 'title')
    key, on_busy, settle = _launch_options(params, action)
    if action is ScheduleAction.LAUNCH:
        if thread_id is not None:
            raise _invalid('thread_id does not go with launch')
        if project_id is None:
            raise _invalid('project_id is required to launch')
    elif thread_id is None:
        thread_id = caller_thread
        if thread_id is None and action is ScheduleAction.POST:
            raise _invalid('thread_id is required to post from outside a thread')
    return AddRequest(command_id, trigger, text, action, thread_id, project_id, title, key,
                      on_busy, settle)


def check_list(params: object) -> tuple[str | None, str | None]:
    params = _object(params)
    return _optional_string(params, 'thread_id'), _optional_string(params, 'project_id')


def check_target(params: object) -> tuple[str, str]:
    params = _object(params)
    command_id = _command_id(params)
    schedule_id = _string(_required(params, 'schedule_id'), 'schedule_id')
    return command_id, schedule_id
