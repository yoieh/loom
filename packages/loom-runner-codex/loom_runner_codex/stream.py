import json
from dataclasses import dataclass

from loom_contracts.runners import TextOut, ToolUse, TurnStarted

from .command import is_canonical_uuid

TURN_FAILED = 'turn failed'

_SILENT_ITEMS = frozenset({'reasoning', 'todo_list', 'error'})

_DROPPED_KEYS = frozenset({'id', 'type', 'status', 'aggregated_output', 'exit_code'})


@dataclass(frozen=True)
class ThreadStarted:
    thread_id: str


@dataclass(frozen=True)
class TurnEnded:
    failed: bool
    reason: str | None


@dataclass(frozen=True)
class ErrorLine:
    message: str


Parsed = TurnStarted | TextOut | ToolUse | ThreadStarted | TurnEnded | ErrorLine


def _thread_started(data: dict) -> Parsed | None:
    thread_id = data.get('thread_id')
    return ThreadStarted(thread_id) if is_canonical_uuid(thread_id) else None


def _turn_started(data: dict) -> Parsed | None:
    return TurnStarted()


def _item(data: dict, completed: bool) -> Parsed | None:
    item = data.get('item')
    if not isinstance(item, dict):
        return None
    item_id = item.get('id')
    kind = item.get('type')
    if not isinstance(item_id, str) or not isinstance(kind, str):
        return None
    if kind == 'agent_message':
        text = item.get('text')
        return TextOut(text) if completed and isinstance(text, str) else None
    if kind in _SILENT_ITEMS:
        return None
    return ToolUse(item_id, kind, {key: value for key, value in item.items()
                                   if key not in _DROPPED_KEYS})


def _item_started(data: dict) -> Parsed | None:
    return _item(data, completed=False)


def _item_completed(data: dict) -> Parsed | None:
    return _item(data, completed=True)


def _turn_completed(data: dict) -> Parsed | None:
    return TurnEnded(failed=False, reason=None)


def _turn_failed(data: dict) -> Parsed | None:
    error = data.get('error')
    message = error.get('message') if isinstance(error, dict) else None
    return TurnEnded(failed=True, reason=message if isinstance(message, str) else TURN_FAILED)


def _error(data: dict) -> Parsed | None:
    message = data.get('message')
    return ErrorLine(message) if isinstance(message, str) else None


_RULES = {
    'thread.started': _thread_started,
    'turn.started': _turn_started,
    'item.started': _item_started,
    'item.completed': _item_completed,
    'turn.completed': _turn_completed,
    'turn.failed': _turn_failed,
    'error': _error,
}


def parse_line(line: bytes) -> Parsed | None:
    try:
        data = json.loads(bytes(line).decode('utf-8'))
    except (UnicodeDecodeError, ValueError, RecursionError):
        return None
    if not isinstance(data, dict):
        return None
    kind = data.get('type')
    rule = _RULES.get(kind) if isinstance(kind, str) else None
    return rule(data) if rule else None
