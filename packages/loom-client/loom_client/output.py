import json

from loom_contracts.calls import CallName
from loom_contracts.events import EventType

MAX_TEXT = 2000
CUT_MARK = '…'

_NAMED = {'\n': '\\n', '\t': '\\t', '\r': '\\r'}
_ESCAPES = {
    code: _NAMED.get(chr(code), f'\\x{code:02x}')
    for code in (*range(0x20), 0x7f, *range(0x80, 0xa0))
} | {code: f'\\u{code:04x}' for code in range(0xd800, 0xe000)}

EVENT_FIELDS: dict[str, tuple[str, ...]] = {
    EventType.THREAD_STATE: ('state', 'waiting', 'reason'),
    EventType.THREAD_FAILED: ('reason',),
    EventType.TURN_STARTED: (),
    EventType.TURN_TEXT: ('text',),
    EventType.TURN_TOOL: ('name', 'input', 'tool_use_id'),
    EventType.TURN_RESULT: ('state', 'text', 'reason'),
    EventType.REQUEST_OPENED: ('request_id', 'tool_name', 'description', 'input'),
    EventType.REQUEST_CLOSED: ('request_id', 'decision'),
    EventType.SCHEDULE_FIRED: ('schedule_id', 'action', 'by', 'late', 'text'),
}


def escape(text: str) -> str:
    return text.translate(_ESCAPES)


def entry(value: object) -> str:
    if value is None:
        return '-'
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    if len(text) > MAX_TEXT:
        text = text[:MAX_TEXT] + CUT_MARK
    return escape(text)


def _field(item: object, key: str) -> object:
    return item.get(key) if isinstance(item, dict) else None


def _items(result: object) -> list:
    return result if isinstance(result, list) else [result]


def thread_line(thread: object) -> str:
    if not isinstance(thread, dict):
        return entry(thread)
    state = entry(thread.get('state'))
    if thread.get('waiting') is True:
        state += ' waiting'
    parts = [entry(thread.get('id')), state, entry(thread.get('project_id')),
             entry(thread.get('title'))]
    if thread.get('reason') is not None:
        parts.append(entry(thread['reason']))
    return '  '.join(parts)


def thread_detail(thread: object) -> list[str]:
    if not isinstance(thread, dict):
        return [entry(thread)]
    return [f'{escape(str(key))}: {entry(value)}' for key, value in thread.items()]


def project_line(project: object) -> str:
    if not isinstance(project, dict):
        return entry(project)
    mode = project.get('permission_mode') or 'default'
    return '  '.join([entry(project.get('id')), entry(mode),
                      entry(project.get('workspace_root')), entry(project.get('title'))])


def event_line(event: object) -> str:
    kind = _field(event, 'type')
    data = _field(event, 'data')
    parts = [entry(_field(event, 'id')), entry(_field(event, 'at')),
             entry(_field(event, 'thread_id')), entry(kind)]
    fields = EVENT_FIELDS.get(kind) if isinstance(kind, str) else None
    if fields is None:
        if data:
            parts.append(entry(data))
        return ' '.join(parts)
    for key in fields:
        value = _field(data, key)
        if value is True:
            parts.append(key)
        elif value is not None and value is not False:
            parts.append(entry(value))
    return ' '.join(parts)


def human(call: str, result: object) -> list[str]:
    if call == CallName.THREADS_GET:
        return thread_detail(result)
    if call.startswith('projects.'):
        return [project_line(item) for item in _items(result)]
    return [thread_line(item) for item in _items(result)]
