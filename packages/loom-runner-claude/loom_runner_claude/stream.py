import json
from dataclasses import dataclass

from loom_contracts.runners import PermissionRequest, TextOut, ToolUse, TurnResult
from loom_contracts.states import TurnState


@dataclass(frozen=True)
class Init:
    session_id: str | None


@dataclass(frozen=True)
class Result:
    is_error: bool
    text: str | None
    terminal_reason: str | None


@dataclass(frozen=True)
class BadRequest:
    request_id: str


@dataclass(frozen=True)
class TaskStarted:
    task_id: str


@dataclass(frozen=True)
class TaskEnded:
    task_id: str


Parsed = (
    TextOut | ToolUse | PermissionRequest | Init | Result | BadRequest | TaskStarted | TaskEnded
)


def _str_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _system(data: dict) -> list[Parsed]:
    subtype = data.get('subtype')
    if subtype == 'init':
        return [Init(_str_or_none(data.get('session_id')))]
    task_id = data.get('task_id')
    if not isinstance(task_id, str):
        return []
    if subtype == 'task_started':
        return [TaskStarted(task_id)] if data.get('is_backgrounded') is True else []
    if subtype == 'task_notification':
        return [TaskEnded(task_id)]
    return []


def _assistant(data: dict) -> list[Parsed]:
    message = data.get('message')
    if not isinstance(message, dict):
        return []
    content = message.get('content')
    if not isinstance(content, list):
        return []
    items: list[Parsed] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        kind = block.get('type')
        if kind == 'text':
            text = block.get('text')
            if isinstance(text, str):
                items.append(TextOut(text))
        elif kind == 'tool_use':
            tool_use_id = block.get('id')
            name = block.get('name')
            tool_input = block.get('input')
            readable = isinstance(tool_use_id, str) and isinstance(name, str)
            if readable and isinstance(tool_input, dict):
                items.append(ToolUse(tool_use_id, name, tool_input))
    return items


def _control_request(data: dict) -> list[Parsed]:
    request_id = data.get('request_id')
    request = data.get('request')
    if not isinstance(request_id, str) or not isinstance(request, dict):
        return []
    if request.get('subtype') != 'can_use_tool':
        return []
    tool_name = request.get('tool_name')
    tool_input = request.get('input')
    if not isinstance(tool_name, str) or not isinstance(tool_input, dict):
        return [BadRequest(request_id)]
    description = _str_or_none(request.get('description'))
    return [PermissionRequest(request_id, tool_name, tool_input, description)]


def _result(data: dict) -> list[Parsed]:
    return [Result(
        is_error=data.get('is_error') is not False,
        text=_str_or_none(data.get('result')),
        terminal_reason=_str_or_none(data.get('terminal_reason')),
    )]


_RULES = {
    'system': _system,
    'assistant': _assistant,
    'control_request': _control_request,
    'result': _result,
}


def parse_line(line: bytes) -> list[Parsed]:
    try:
        data = json.loads(bytes(line).decode('utf-8'))
    except (UnicodeDecodeError, ValueError, RecursionError):
        return []
    if not isinstance(data, dict):
        return []
    kind = data.get('type')
    rule = _RULES.get(kind) if isinstance(kind, str) else None
    return rule(data) if rule else []


def turn_result(result: Result, interrupted: bool) -> TurnResult:
    if not result.is_error:
        return TurnResult(TurnState.COMPLETED, result.text, None)
    reason = result.terminal_reason
    if interrupted and reason is not None and reason.startswith('aborted_'):
        return TurnResult(TurnState.STOPPED, result.text, reason)
    return TurnResult(TurnState.FAILED, result.text, reason)


def _line(obj: dict[str, object]) -> bytes:
    return json.dumps(obj, separators=(',', ':')).encode('ascii') + b'\n'


def user_line(message: str) -> bytes:
    return _line({'type': 'user', 'message': {'role': 'user', 'content': message}})


def interrupt_line(request_id: str) -> bytes:
    return _line({
        'type': 'control_request',
        'request_id': request_id,
        'request': {'subtype': 'interrupt'},
    })


def allow_line(request_id: str, updated_input: dict[str, object]) -> bytes:
    return _line({
        'type': 'control_response',
        'response': {
            'subtype': 'success',
            'request_id': request_id,
            'response': {'behavior': 'allow', 'updatedInput': updated_input},
        },
    })


def deny_line(request_id: str, message: str) -> bytes:
    return _line({
        'type': 'control_response',
        'response': {
            'subtype': 'success',
            'request_id': request_id,
            'response': {'behavior': 'deny', 'message': message},
        },
    })
