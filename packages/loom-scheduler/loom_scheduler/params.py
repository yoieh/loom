from dataclasses import dataclass
from datetime import tzinfo

from loom_contracts.triggers import ScheduleAction

from .host import ScheduleError
from .timing import check_trigger

MAX_COMMAND_ID = 200


@dataclass(frozen=True)
class AddRequest:
    command_id: str
    trigger: dict
    text: str
    action: ScheduleAction
    thread_id: str | None
    project_id: str | None
    title: str | None


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
    if action is ScheduleAction.LAUNCH:
        if thread_id is not None:
            raise _invalid('thread_id does not go with launch')
        if project_id is None:
            raise _invalid('project_id is required to launch')
    elif thread_id is None:
        thread_id = caller_thread
        if thread_id is None and action is ScheduleAction.POST:
            raise _invalid('thread_id is required to post from outside a thread')
    return AddRequest(command_id, trigger, text, action, thread_id, project_id, title)


def check_list(params: object) -> tuple[str | None, str | None]:
    params = _object(params)
    return _optional_string(params, 'thread_id'), _optional_string(params, 'project_id')


def check_target(params: object) -> tuple[str, str]:
    params = _object(params)
    command_id = _command_id(params)
    schedule_id = _string(_required(params, 'schedule_id'), 'schedule_id')
    return command_id, schedule_id
