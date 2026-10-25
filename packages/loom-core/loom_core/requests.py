import sqlite3
from datetime import datetime, timedelta

from loom_contracts.events import EventType
from loom_contracts.runners import Decision, PermissionRequest

from .clock import iso
from .errors import LoomError
from .store import Store

CALLER = 'caller'
ENDED = 'ended'
TIMEOUT = 'timeout'
MAX_MESSAGE = 2000
COULD_NOT_OPEN = 'Loom could not open this request.'


def timeout_message(seconds: int) -> str:
    return f'No answer came within {seconds} s, so Loom denied this request.'


def message_of(value: object) -> str:
    if not isinstance(value, str) or len(value) > MAX_MESSAGE:
        raise LoomError('invalid', f'message must be a string of at most {MAX_MESSAGE} characters')
    return value


def _rows(tx: sqlite3.Connection, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
    cursor = tx.cursor()
    cursor.row_factory = sqlite3.Row
    return cursor.execute(sql, args).fetchall()


def is_known(tx: sqlite3.Connection, request_id: str) -> bool:
    return tx.execute('SELECT 1 FROM permission_requests WHERE id = ?',
                      (request_id,)).fetchone() is not None


def fetch(tx: sqlite3.Connection, request_id: str) -> sqlite3.Row:
    found = _rows(tx, 'SELECT * FROM permission_requests WHERE id = ?', (request_id,))
    if not found:
        raise LoomError('not_found', f'no request with id {request_id!r}')
    return found[0]


def has_open(tx: sqlite3.Connection, thread_id: str) -> bool:
    return tx.execute(
        'SELECT 1 FROM permission_requests WHERE thread_id = ? AND closed_at IS NULL LIMIT 1',
        (thread_id,),
    ).fetchone() is not None


def expired(tx: sqlite3.Connection, now: datetime) -> list[sqlite3.Row]:
    return _rows(tx, 'SELECT * FROM permission_requests WHERE closed_at IS NULL '
                     'AND expires_at <= ? ORDER BY expires_at, id', (iso(now),))


def open_request(store: Store, tx: sqlite3.Connection, thread_id: str,
                 event: PermissionRequest, timeout: int) -> None:
    opened = store.clock()
    expires_at = iso(opened + timedelta(seconds=timeout))
    tx.execute(
        'INSERT INTO permission_requests (id, thread_id, opened_at, expires_at) '
        'VALUES (?, ?, ?, ?)', (event.request_id, thread_id, iso(opened), expires_at),
    )
    data: dict[str, object] = {'request_id': event.request_id, 'tool_name': event.tool_name,
                               'input': event.input}
    if event.description is not None:
        data['description'] = event.description
    data['expires_at'] = expires_at
    store.append_event(tx, EventType.REQUEST_OPENED, thread_id, data)


def check_answerer(thread: sqlite3.Row, answerer: str | None,
                   answerer_thread: str | None) -> None:
    if answerer_thread is not None and answerer_thread == thread['id']:
        raise LoomError('forbidden', 'a thread may not answer its own requests')
    if thread['requests'] == CALLER and (answerer is None or answerer != thread['launched_by']):
        raise LoomError('forbidden',
                        'only the caller that launched this thread may answer its requests')


def close(store: Store, tx: sqlite3.Connection, request: sqlite3.Row, decision: Decision,
          reason: str | None = None) -> None:
    tx.execute(
        'UPDATE permission_requests SET closed_at = ?, decision = ?, reason = ? WHERE id = ?',
        (iso(store.clock()), str(decision), reason, request['id']),
    )
    data: dict[str, object] = {'request_id': request['id'], 'decision': decision}
    if reason is not None:
        data['reason'] = reason
    store.append_event(tx, EventType.REQUEST_CLOSED, request['thread_id'], data)


def close_ended(store: Store, tx: sqlite3.Connection, thread_id: str) -> None:
    for request in _rows(tx, 'SELECT * FROM permission_requests WHERE thread_id = ? '
                             'AND closed_at IS NULL ORDER BY opened_at, id', (thread_id,)):
        close(store, tx, request, Decision.DENY, ENDED)
