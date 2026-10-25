import sqlite3

from loom_contracts.calls import MAX_CONCURRENCY_KEY, OnBusy

from .errors import LoomError
from .store import Store

HELD = 'held'
WAITING = 'waiting'
FREED = 'freed'
WAITING_SQL = ('SELECT id, project_id, parent_id FROM threads WHERE concurrency_key = ? '
               'AND key_state = ? ORDER BY created_at {0}, id {0} LIMIT 1')
OLDEST = WAITING_SQL.format('ASC')
NEWEST = WAITING_SQL.format('DESC')


def key_params(params: dict) -> tuple[str | None, OnBusy]:
    key = None
    if 'concurrency_key' in params:
        key = params['concurrency_key']
        if key is None:
            raise LoomError('invalid', 'concurrency_key must not be null')
        if not isinstance(key, str) or not 1 <= len(key) <= MAX_CONCURRENCY_KEY:
            raise LoomError('invalid', 'concurrency_key must be a string of 1 to '
                            f'{MAX_CONCURRENCY_KEY} characters')
    on_busy = OnBusy.QUEUE
    if 'on_busy' in params:
        value = params['on_busy']
        if value is None:
            raise LoomError('invalid', 'on_busy must not be null')
        if not isinstance(value, str) or value not in tuple(OnBusy):
            raise LoomError('invalid', f'on_busy must be one of {", ".join(OnBusy)}')
        if key is None:
            raise LoomError('invalid', 'on_busy needs concurrency_key')
        on_busy = OnBusy(value)
    return key, on_busy


def holder(tx: sqlite3.Connection, key: str) -> str | None:
    found = tx.execute('SELECT id FROM threads WHERE concurrency_key = ? AND key_state = ?',
                       (key, HELD)).fetchone()
    return None if found is None else found[0]


def _waiting(tx: sqlite3.Connection, key: str, sql: str) -> tuple[str, str, str | None] | None:
    return tx.execute(sql, (key, WAITING)).fetchone()


def claim(tx: sqlite3.Connection, key: str | None) -> str | None:
    if key is None:
        return None
    return WAITING if holder(tx, key) is not None else HELD


def join(tx: sqlite3.Connection, key: str, project_id: str, parent_id: str | None,
         text: str) -> str | None:
    found = _waiting(tx, key, NEWEST)
    if found is None:
        return None
    thread_id, waiting_project, waiting_parent = found
    if (waiting_project, waiting_parent) != (project_id, parent_id):
        raise LoomError('conflict', f'concurrency key {key} is waiting in another project or '
                        'under another parent')
    tx.execute('UPDATE messages SET text = text || ? WHERE seq = '
               '(SELECT min(seq) FROM messages WHERE thread_id = ?)', (f'\n\n{text}', thread_id))
    return thread_id


def free(store: Store, tx: sqlite3.Connection, thread_id: str) -> None:
    found = tx.execute('SELECT concurrency_key, key_state FROM threads WHERE id = ?',
                       (thread_id,)).fetchone()
    if found is None or found[1] not in (HELD, WAITING):
        return
    key, state = found
    tx.execute('UPDATE threads SET key_state = ? WHERE id = ?', (FREED, thread_id))
    if state != HELD:
        return
    following = _waiting(tx, key, OLDEST)
    if following is not None:
        tx.execute('UPDATE threads SET key_state = ? WHERE id = ?', (HELD, following[0]))
        store.hand_on(tx, following[0])
