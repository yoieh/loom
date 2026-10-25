import json
import re
import sqlite3

from loom_contracts.calls import MAX_PAYLOAD, MAX_SIGNAL_KEY, MAX_TOPIC, TOPIC_PATTERN, CallName
from loom_contracts.events import Event, EventType

from . import causes, keys
from .errors import LoomError
from .projects import Projects
from .store import Store

TOPIC = re.compile(TOPIC_PATTERN)
LOOM_TYPES = frozenset(EventType)


def _params(params: object) -> dict:
    if not isinstance(params, dict):
        raise LoomError('invalid', 'params must be an object')
    return params


def _required(params: dict, field: str) -> object:
    value = params.get(field)
    if value is None:
        raise LoomError('invalid', f'{field} is required')
    return value


def _given(params: dict, field: str) -> object:
    value = params[field]
    if value is None:
        raise LoomError('invalid', f'{field} must not be null')
    return value


def _topic(value: object) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= MAX_TOPIC:
        raise LoomError('invalid', f'topic must be a string of 1 to {MAX_TOPIC} characters')
    if TOPIC.fullmatch(value) is None:
        raise LoomError('invalid', 'topic must start with a letter or digit and hold only '
                        'letters, digits, ".", "_", ":" and "-"')
    if value in LOOM_TYPES:
        raise LoomError('invalid', 'topic must not be a Loom event type')
    return value


def _payload(value: object) -> dict:
    if not isinstance(value, dict):
        raise LoomError('invalid', 'payload must be a JSON object')
    text = json.dumps(value, separators=(',', ':'), ensure_ascii=False)
    if len(text.encode('utf-8', 'surrogatepass')) > MAX_PAYLOAD:
        raise LoomError('invalid', f'payload must be at most {MAX_PAYLOAD} bytes as JSON')
    return value


def _key(value: object) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= MAX_SIGNAL_KEY:
        raise LoomError('invalid', f'key must be a string of 1 to {MAX_SIGNAL_KEY} characters')
    return value


class Signals:
    def __init__(self, store: Store, projects: Projects) -> None:
        self.store = store
        self.projects = projects

    def emit(self, params: object, *, caller_thread: str | None = None) -> Event:
        params = _params(params)
        topic = _topic(_required(params, 'topic'))
        payload = _payload(_required(params, 'payload'))
        project_id = None
        if keys.PROJECT_KEY in params:
            project_id = _given(params, keys.PROJECT_KEY)
            if not isinstance(project_id, str):
                raise LoomError('invalid', f'{keys.PROJECT_KEY} must be a string')
        key = _key(_given(params, 'key')) if 'key' in params else None
        cause = causes.cause_param(params)

        def change(tx: sqlite3.Connection) -> Event:
            if project_id is not None:
                self.projects.get(project_id)
            chain = causes.of_call(tx, cause, caller_thread)
            by = caller_thread if causes.thread_chain(tx, caller_thread) is not None else None
            data: dict[str, object] = {'topic': topic, 'payload': payload, 'hops': chain.hops}
            for field, value in ((keys.PROJECT_KEY, project_id), ('key', key),
                                 ('cause', chain.cause), ('by', by)):
                if value is not None:
                    data[field] = value
            event_id = self.store.append_event(tx, EventType.SIGNAL, None, data)
            return self.store.event(tx, event_id)

        return self.store.command(params.get('id'), CallName.SIGNALS_EMIT, change).answer
