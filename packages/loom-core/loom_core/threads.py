import sqlite3
from collections.abc import Mapping
from datetime import timedelta

from loom_contracts.calls import Audience, Settle, ThreadInfo
from loom_contracts.events import EventType
from loom_contracts.runners import PermissionMode, RunnerName
from loom_contracts.states import ThreadState

from . import concurrency, requests
from .clock import iso
from .errors import LoomError
from .slots import StartSource
from .store import Store

ENDED = frozenset({ThreadState.SETTLED, ThreadState.FAILED})
LIVE = frozenset(ThreadState) - ENDED
STOPPING = 'stopping'
CONTINUATION = 'Your last turn was cut off; continue where you were.'
CONTINUATIONS = 3
CONTINUATION_WINDOW = timedelta(seconds=300)
CRASHED = 'exited mid-turn after 3 continuations in 5 minutes'
SETTLED_RESULT = 2000

_MOVES = {
    ThreadState.QUEUED: {ThreadState.STARTING, ThreadState.SLEEPING},
    ThreadState.STARTING: {ThreadState.RUNNING, ThreadState.SLEEPING},
    ThreadState.RUNNING: {ThreadState.IDLE, ThreadState.SLEEPING},
    ThreadState.IDLE: {ThreadState.RUNNING, ThreadState.SLEEPING},
    ThreadState.SLEEPING: {ThreadState.STARTING, ThreadState.QUEUED},
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
    'origin', 'has_session', 'stop_requested', 'created_at', 'updated_at', 'audience',
    'concurrency_key', 'settle', 'key_state', 'launched_by',
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


def select(tx: sqlite3.Connection, project_id: str | None = None,
           audience: Audience | None = None) -> list[sqlite3.Row]:
    found = {'project_id': project_id, 'audience': audience}
    where = [f'{column} = ?' for column, value in found.items() if value is not None]
    sql = 'SELECT * FROM threads'
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    return _rows(tx, sql + ' ORDER BY created_at, id',
                 tuple(str(value) for value in found.values() if value is not None))


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
    concurrency.free(store, tx, thread_id)
    transition(store, tx, thread_id, ThreadState(row['state']), STOPPING)


def clear_stop(store: Store, tx: sqlite3.Connection, thread_id: str) -> None:
    row = fetch(tx, thread_id)
    tx.execute('UPDATE threads SET stop_requested = 0 WHERE id = ?', (thread_id,))
    if row['state'] in LIVE and row['reason'] == STOPPING:
        transition(store, tx, thread_id, ThreadState(row['state']))


def add_continuation(store: Store, tx: sqlite3.Connection, thread_id: str) -> int | None:
    now = store.clock()
    tx.execute('DELETE FROM continuations WHERE thread_id = ? AND at <= ?',
               (thread_id, iso(now - CONTINUATION_WINDOW)))
    count = tx.execute('SELECT count(*) FROM continuations WHERE thread_id = ?',
                       (thread_id,)).fetchone()[0]
    if count >= CONTINUATIONS:
        return None
    tx.execute('INSERT INTO continuations (thread_id, at) VALUES (?, ?)', (thread_id, iso(now)))
    return count + 1


def add_start(tx: sqlite3.Connection, thread_id: str, source: StartSource, at: str) -> None:
    tx.execute('INSERT INTO starts (thread_id, rank, queued_at) VALUES (?, ?, ?)',
               (thread_id, int(source), at))


def rank_start(tx: sqlite3.Connection, thread_id: str, source: StartSource) -> None:
    tx.execute(
        'INSERT INTO starts (thread_id, rank, queued_at) '
        'SELECT id, min(?, ?), created_at FROM threads WHERE id = ? '
        'ON CONFLICT (thread_id) DO UPDATE SET rank = min(rank, ?)',
        (int(source), int(StartSource.AGENT), thread_id, int(source)),
    )


def start_queue(tx: sqlite3.Connection) -> list[tuple[int, str, str]]:
    return tx.execute(
        'SELECT coalesce(starts.rank, ?), coalesce(starts.queued_at, threads.created_at), '
        'threads.id FROM threads LEFT JOIN starts ON starts.thread_id = threads.id '
        'WHERE threads.state = ? AND threads.stop_requested = 0 '
        "AND coalesce(threads.key_state, '') != ? ORDER BY 1, 2, 3",
        (int(StartSource.AGENT), str(ThreadState.QUEUED), concurrency.WAITING),
    ).fetchall()


def narrower(first: PermissionMode, second: PermissionMode) -> PermissionMode:
    return min(PermissionMode(first), PermissionMode(second), key=MODE_ORDER.index)


def has_live(tx: sqlite3.Connection, project_id: str) -> bool:
    return tx.execute(
        'SELECT 1 FROM threads WHERE project_id = ? AND state NOT IN (?, ?) LIMIT 1',
        (project_id, *sorted(ENDED)),
    ).fetchone() is not None


def uses_workspace(tx: sqlite3.Connection, project_id: str, path: str) -> bool:
    return tx.execute(
        'SELECT 1 FROM threads WHERE project_id = ? AND workspace_path = ? LIMIT 1',
        (project_id, path),
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
        'audience': Audience(row['audience']),
        'concurrency_key': row['concurrency_key'],
        'settle': None if row['settle'] is None else Settle(row['settle']),
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
                  created_at=now, updated_at=now, audience=Audience.AGENT)
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
               reason: str | None = None, *, waiting: bool | None = None,
               result: str | None = None) -> None:
    row = fetch(tx, thread_id)
    current = ThreadState(row['state'])
    state = ThreadState(state)
    if state not in ALLOWED[current] and (state != current or current in ENDED):
        raise LoomError('conflict', f'thread {thread_id} cannot go from {current} to {state}')
    if reason is None and row['stop_requested'] and state in LIVE:
        reason = STOPPING
    if state is not ThreadState.RUNNING:
        waiting = False
        requests.close_ended(store, tx, thread_id)
    elif waiting is None:
        waiting = bool(row['waiting'])
    tx.execute(
        'UPDATE threads SET state = ?, waiting = ?, reason = ?, updated_at = ? WHERE id = ?',
        (str(state), int(waiting), reason, iso(store.clock()), thread_id),
    )
    if current is ThreadState.QUEUED and state is not ThreadState.QUEUED:
        tx.execute('DELETE FROM starts WHERE thread_id = ?', (thread_id,))
    store.append_event(tx, EventType.THREAD_STATE, thread_id,
                       _state_data(state, waiting, reason))
    if state is ThreadState.FAILED:
        store.append_event(tx, EventType.THREAD_FAILED, thread_id,
                           {'reason': reason if reason is not None else ''})
    elif state is ThreadState.SETTLED:
        data = {} if result is None else {'result': result[:SETTLED_RESULT]}
        store.append_event(tx, EventType.THREAD_SETTLED, thread_id, data)
    if state in ENDED:
        concurrency.free(store, tx, thread_id)
