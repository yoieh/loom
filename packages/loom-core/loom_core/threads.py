import sqlite3
from collections.abc import Mapping

from loom_contracts.calls import ThreadInfo
from loom_contracts.events import EventType
from loom_contracts.runners import PermissionMode, RunnerName
from loom_contracts.states import ThreadState

from .clock import iso
from .errors import LoomError
from .store import Store

ENDED = frozenset({ThreadState.SETTLED, ThreadState.FAILED})
LIVE = frozenset(ThreadState) - ENDED
STOPPING = 'stopping'

_MOVES = {
    ThreadState.QUEUED: {ThreadState.STARTING, ThreadState.SLEEPING},
    ThreadState.STARTING: {ThreadState.RUNNING, ThreadState.SLEEPING},
    ThreadState.RUNNING: {ThreadState.IDLE, ThreadState.SLEEPING},
    ThreadState.IDLE: {ThreadState.RUNNING, ThreadState.SLEEPING},
    ThreadState.SLEEPING: {ThreadState.STARTING},
    ThreadState.SETTLED: set(),
    ThreadState.FAILED: {ThreadState.SETTLED},
}

ALLOWED: dict[ThreadState, frozenset[ThreadState]] = {
    state: frozenset(_MOVES[state] | ENDED if state in LIVE else _MOVES[state])
    for state in ThreadState
}

MODE_ORDER = (
    PermissionMode.APPROVAL_REQUIRED, PermissionMode.AUTO_ACCEPT_EDITS, PermissionMode.AUTO,
    PermissionMode.FULL_ACCESS,
)

COLUMNS = (
    'id', 'title', 'runner', 'project_id', 'workspace_strategy', 'workspace_path', 'branch',
    'base', 'model', 'permission_mode', 'requests', 'state', 'waiting', 'reason', 'parent_id',
    'origin', 'has_session', 'stop_requested', 'created_at', 'updated_at',
)


def _rows(tx: sqlite3.Connection, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
    cursor = tx.cursor()
    cursor.row_factory = sqlite3.Row
    return cursor.execute(sql, args).fetchall()


def fetch(tx: sqlite3.Connection, thread_id: str) -> sqlite3.Row:
    found = _rows(tx, 'SELECT * FROM threads WHERE id = ?', (thread_id,))
    if not found:
        raise LoomError('not_found', f'no thread with id {thread_id!r}')
    return found[0]


def select(tx: sqlite3.Connection, project_id: str | None = None) -> list[sqlite3.Row]:
    if project_id is None:
        return _rows(tx, 'SELECT * FROM threads ORDER BY created_at, id')
    return _rows(tx, 'SELECT * FROM threads WHERE project_id = ? ORDER BY created_at, id',
                 (project_id,))


def stop_targets(tx: sqlite3.Connection, thread_id: str) -> list[sqlite3.Row]:
    return _rows(
        tx,
        'WITH RECURSIVE tree(id) AS ('
        ' SELECT id FROM threads WHERE id = ?'
        ' UNION SELECT threads.id FROM threads JOIN tree ON threads.parent_id = tree.id'
        ') SELECT threads.* FROM threads JOIN tree ON threads.id = tree.id'
        ' WHERE threads.state NOT IN (?, ?) ORDER BY threads.created_at, threads.id',
        (thread_id, *sorted(ENDED)),
    )


def request_stop(store: Store, tx: sqlite3.Connection, thread_id: str) -> None:
    row = fetch(tx, thread_id)
    tx.execute('UPDATE threads SET stop_requested = 1 WHERE id = ?', (thread_id,))
    transition(store, tx, thread_id, ThreadState(row['state']), STOPPING)


def clear_stop(store: Store, tx: sqlite3.Connection, thread_id: str) -> None:
    row = fetch(tx, thread_id)
    tx.execute('UPDATE threads SET stop_requested = 0 WHERE id = ?', (thread_id,))
    if row['state'] in LIVE and row['reason'] == STOPPING:
        transition(store, tx, thread_id, ThreadState(row['state']))


def narrower(first: PermissionMode, second: PermissionMode) -> PermissionMode:
    return min(PermissionMode(first), PermissionMode(second), key=MODE_ORDER.index)


def has_live(tx: sqlite3.Connection, project_id: str) -> bool:
    return tx.execute(
        'SELECT 1 FROM threads WHERE project_id = ? AND state NOT IN (?, ?) LIMIT 1',
        (project_id, *sorted(ENDED)),
    ).fetchone() is not None


def thread_info(row: sqlite3.Row) -> ThreadInfo:
    return {
        'id': row['id'],
        'title': row['title'],
        'runner': RunnerName(row['runner']),
        'project_id': row['project_id'],
        'workspace_path': row['workspace_path'],
        'branch': row['branch'],
        'model': row['model'],
        'permission_mode': PermissionMode(row['permission_mode']),
        'state': ThreadState(row['state']),
        'waiting': bool(row['waiting']),
        'reason': row['reason'],
        'parent_id': row['parent_id'],
        'origin': row['origin'],
        'created_at': row['created_at'],
    }


def _state_data(state: ThreadState, waiting: bool, reason: str | None) -> dict[str, object]:
    data: dict[str, object] = {'state': state, 'waiting': waiting}
    if reason is not None:
        data['reason'] = reason
    return data


def insert(store: Store, tx: sqlite3.Connection, fields: Mapping[str, object]) -> sqlite3.Row:
    now = iso(store.clock())
    values = {column: None for column in COLUMNS}
    values.update(state=ThreadState.QUEUED, waiting=0, has_session=0, stop_requested=0,
                  created_at=now, updated_at=now)
    values.update(fields)
    tx.execute(
        f'INSERT INTO threads ({", ".join(COLUMNS)}) VALUES ({", ".join("?" for _ in COLUMNS)})',
        tuple(values[column] for column in COLUMNS),
    )
    row = fetch(tx, values['id'])
    store.append_event(tx, EventType.THREAD_STATE, row['id'],
                       _state_data(ThreadState(row['state']), bool(row['waiting']), row['reason']))
    return row


def transition(store: Store, tx: sqlite3.Connection, thread_id: str, state: ThreadState,
               reason: str | None = None, *, waiting: bool | None = None) -> None:
    row = fetch(tx, thread_id)
    current = ThreadState(row['state'])
    state = ThreadState(state)
    if state not in ALLOWED[current] and (state != current or current in ENDED):
        raise LoomError('conflict', f'thread {thread_id} cannot go from {current} to {state}')
    if reason is None and row['stop_requested'] and state in LIVE:
        reason = STOPPING
    if state is not ThreadState.RUNNING:
        waiting = False
    elif waiting is None:
        waiting = bool(row['waiting'])
    tx.execute(
        'UPDATE threads SET state = ?, waiting = ?, reason = ?, updated_at = ? WHERE id = ?',
        (str(state), int(waiting), reason, iso(store.clock()), thread_id),
    )
    store.append_event(tx, EventType.THREAD_STATE, thread_id,
                       _state_data(state, waiting, reason))
    if state is ThreadState.FAILED:
        store.append_event(tx, EventType.THREAD_FAILED, thread_id,
                           {'reason': reason if reason is not None else ''})
