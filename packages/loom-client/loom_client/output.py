import json

from loom_contracts.calls import CallName
from loom_contracts.events import EventType
from loom_contracts.triggers import TriggerType

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
    EventType.THREAD_UPDATED: ('audience',),
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


def _whole(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _schedule_state(schedule: dict) -> str | None:
    paused = schedule.get('paused')
    if paused is True:
        return 'paused'
    if paused is not False or 'next_run' not in schedule:
        return None
    next_run = schedule['next_run']
    if next_run is None:
        return 'done'
    return next_run if isinstance(next_run, str) else None


def _trigger_text(trigger: object) -> str | None:
    kind = _field(trigger, 'type')
    if kind == TriggerType.AT:
        at = trigger.get('at')
        return f'at {at}' if isinstance(at, str) else None
    if kind == TriggerType.INTERVAL:
        every = trigger.get('everyMs')
        if not _whole(every):
            return None
        seconds = every // 1000 if every % 1000 == 0 else every / 1000
        return f'every {seconds}s'
    if kind == TriggerType.FIXED_TIME:
        time = trigger.get('timeOfDay')
        weekdays = trigger.get('weekdays')
        if not isinstance(time, str):
            return None
        if weekdays is None or weekdays == []:
            return time
        if not isinstance(weekdays, list) or not all(_whole(day) for day in weekdays):
            return None
        return f'{time} on {",".join(str(day) for day in weekdays)}'
    return None


def schedule_line(schedule: object) -> str:
    if not isinstance(schedule, dict):
        return entry(schedule)
    target = schedule.get('thread_id')
    if target is None:
        target = schedule.get('project_id')
    return '  '.join([entry(schedule.get('id')), entry(_schedule_state(schedule)),
                      entry(schedule.get('action')), entry(_trigger_text(schedule.get('trigger'))),
                      entry(target), entry(schedule.get('title') or schedule.get('text'))])


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
    if call.startswith('schedules.'):
        return [schedule_line(item) for item in _items(result)]
    return [thread_line(item) for item in _items(result)]
