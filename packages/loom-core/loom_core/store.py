import json
import os
import sqlite3
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from loom_contracts.calls import Audience, CallName
from loom_contracts.events import Event, EventType

from .clock import Clock, iso, utc_now
from .errors import LoomError

DB_NAME = 'loom.db'
EVENT_DAYS = 30
MAX_COMMAND_ID = 200
MAX_EVENTS = 1000
MAX_EVENT_ID = 2**63 - 1
MESSAGE_SOURCE = 'source INTEGER NOT NULL DEFAULT 1'
THREAD_AUDIENCE = "audience TEXT NOT NULL DEFAULT 'agent'"
THREAD_KEY = 'concurrency_key TEXT'
THREAD_SETTLE = 'settle TEXT'
THREAD_KEY_STATE = 'key_state TEXT'
THREAD_LAUNCHED_BY = 'launched_by TEXT'
CAUSE = 'cause INTEGER'
HOPS = 'hops INTEGER NOT NULL DEFAULT 0'
THREAD_KEY_INDEX = 'CREATE INDEX IF NOT EXISTS threads_key ON threads (concurrency_key, key_state)'
PERSON_THREADLESS = (EventType.SCHEDULE_FIRED,)

SCHEMA: list[str] = [
    '''CREATE TABLE IF NOT EXISTS commands (
        id TEXT PRIMARY KEY,
        call TEXT NOT NULL,
        answer TEXT NOT NULL,
        at TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,
        at TEXT NOT NULL,
        thread_id TEXT,
        data TEXT NOT NULL
    )''',
    'CREATE INDEX IF NOT EXISTS events_at ON events (at)',
    'CREATE INDEX IF NOT EXISTS events_thread ON events (thread_id, id)',
    '''CREATE TABLE IF NOT EXISTS projects (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        workspace_root TEXT NOT NULL UNIQUE,
        permission_mode TEXT NULL,
        created_at TEXT NOT NULL
    )''',
    f'''CREATE TABLE IF NOT EXISTS threads (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        runner TEXT NOT NULL,
        project_id TEXT NOT NULL,
        workspace_strategy TEXT NOT NULL,
        workspace_path TEXT NOT NULL,
        branch TEXT,
        base TEXT,
        model TEXT,
        permission_mode TEXT NOT NULL,
        requests TEXT NOT NULL,
        state TEXT NOT NULL,
        waiting INTEGER NOT NULL DEFAULT 0,
        reason TEXT,
        parent_id TEXT,
        origin TEXT,
        has_session INTEGER NOT NULL DEFAULT 0,
        stop_requested INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        {THREAD_AUDIENCE},
        {THREAD_KEY},
        {THREAD_SETTLE},
        {THREAD_KEY_STATE},
        {THREAD_LAUNCHED_BY},
        {CAUSE},
        {HOPS}
    )''',
    'CREATE INDEX IF NOT EXISTS threads_project ON threads (project_id, state)',
    'CREATE INDEX IF NOT EXISTS threads_parent ON threads (parent_id)',
    f'''CREATE TABLE IF NOT EXISTS messages (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_id TEXT NOT NULL,
        command_id TEXT NOT NULL,
        text TEXT NOT NULL,
        mode TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        {MESSAGE_SOURCE},
        {CAUSE},
        {HOPS}
    )''',
    'CREATE INDEX IF NOT EXISTS messages_next ON messages (thread_id, priority DESC, seq)',
    'CREATE TABLE IF NOT EXISTS continuations (thread_id TEXT NOT NULL, at TEXT NOT NULL)',
    'CREATE INDEX IF NOT EXISTS continuations_thread ON continuations (thread_id, at)',
    '''CREATE TABLE IF NOT EXISTS starts (
        thread_id TEXT PRIMARY KEY,
        rank INTEGER NOT NULL,
        queued_at TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS permission_requests (
        id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        closed_at TEXT,
        decision TEXT,
        reason TEXT
    )''',
    'CREATE INDEX IF NOT EXISTS permission_requests_open '
    'ON permission_requests (thread_id, expires_at) WHERE closed_at IS NULL',
]


@dataclass(frozen=True)
class Receipt:
    answer: Any
    replayed: bool


def _open_db_file(path: Path) -> None:
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        if stat.S_IMODE(os.fstat(fd).st_mode) != 0o600:
            os.fchmod(fd, 0o600)
    finally:
        os.close(fd)


def _make_data_dir(data_dir: Path) -> None:
    try:
        data_dir.mkdir(mode=0o700, parents=True)
    except FileExistsError:
        return
    os.chmod(data_dir, 0o700)


def _add_column(tx: sqlite3.Connection, table: str, column: str) -> None:
    columns = {row[1] for row in tx.execute(f'PRAGMA table_info({table})')}
    if column.split()[0] not in columns:
        tx.execute(f'ALTER TABLE {table} ADD COLUMN {column}')


def _check_command_id(command_id: object) -> str:
    if not isinstance(command_id, str) or not 1 <= len(command_id) <= MAX_COMMAND_ID:
        raise LoomError('invalid', f'id must be a string of 1 to {MAX_COMMAND_ID} characters')
    return command_id


def _event(row: tuple) -> Event:
    return {
        'id': row[0],
        'type': EventType(row[1]),
        'at': row[2],
        'thread_id': row[3],
        'data': json.loads(row[4]),
    }


class Store:
    def __init__(self, connection: sqlite3.Connection, clock: Clock, data_dir: Path) -> None:
        self.connection = connection
        self.clock = clock
        self.data_dir = data_dir
        self.listeners: set[Callable[[], None]] = set()
        self.on_handed: Callable[[str], None] | None = None
        self._wrote_events = False
        self._handed: list[str] = []

    @classmethod
    def open(cls, data_dir: Path, clock: Clock = utc_now) -> 'Store':
        data_dir = Path(data_dir)
        _make_data_dir(data_dir)
        path = data_dir / DB_NAME
        _open_db_file(path)
        connection = sqlite3.connect(path, isolation_level=None)
        store = cls(connection, clock, data_dir)
        try:
            connection.execute('PRAGMA foreign_keys=ON')
            with store.transaction() as tx:
                for statement in SCHEMA:
                    tx.execute(statement)
                _add_column(tx, 'threads', THREAD_AUDIENCE)
                for column in (THREAD_KEY, THREAD_SETTLE, THREAD_KEY_STATE, THREAD_LAUNCHED_BY,
                               CAUSE, HOPS):
                    _add_column(tx, 'threads', column)
                tx.execute(THREAD_KEY_INDEX)
                for column in (MESSAGE_SOURCE, CAUSE, HOPS):
                    _add_column(tx, 'messages', column)
            store.prune_events()
        except BaseException:
            connection.close()
            raise
        return store

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.connection.execute('BEGIN IMMEDIATE')
        self._wrote_events = False
        self._handed = []
        try:
            yield self.connection
            self.connection.execute('COMMIT')
        except BaseException:
            self._wrote_events = False
            self._handed = []
            if self.connection.in_transaction:
                self.connection.execute('ROLLBACK')
            raise
        handed, self._handed = self._handed, []
        if self._wrote_events:
            self._wrote_events = False
            self._notify()
        for thread_id in handed:
            self._hand(thread_id)

    def listen(self, listener: Callable[[], None]) -> Callable[[], None]:
        self.listeners.add(listener)
        return lambda: self.listeners.discard(listener)

    def hand_on(self, tx: sqlite3.Connection, thread_id: str) -> None:
        if tx.in_transaction:
            self._handed.append(thread_id)
        else:
            self._hand(thread_id)

    def _hand(self, thread_id: str) -> None:
        if self.on_handed is not None:
            self.on_handed(thread_id)

    def _notify(self) -> None:
        for listener in tuple(self.listeners):
            listener()

    def command(
        self,
        command_id: object,
        call: CallName,
        change: Callable[[sqlite3.Connection], object],
    ) -> Receipt:
        command_id = _check_command_id(command_id)
        with self.transaction() as tx:
            row = tx.execute(
                'SELECT call, answer FROM commands WHERE id = ?', (command_id,),
            ).fetchone()
            if row is not None:
                if row[0] != str(call):
                    raise LoomError('conflict', f'id was already used for {row[0]}')
                return Receipt(json.loads(row[1]), True)
            answer = json.dumps(change(tx))
            tx.execute(
                'INSERT INTO commands (id, call, answer, at) VALUES (?, ?, ?, ?)',
                (command_id, str(call), answer, iso(self.clock())),
            )
        return Receipt(json.loads(answer), False)

    def append_event(
        self,
        tx: sqlite3.Connection,
        type: EventType,
        thread_id: str | None,
        data: Mapping[str, object],
    ) -> int:
        cursor = tx.execute(
            'INSERT INTO events (type, at, thread_id, data) VALUES (?, ?, ?, ?)',
            (str(EventType(type)), iso(self.clock()), thread_id, json.dumps(dict(data))),
        )
        if tx.in_transaction:
            self._wrote_events = True
        else:
            self._notify()
        return cursor.lastrowid

    def events(
        self, after: object = 0, thread_id: object = None, limit: int = MAX_EVENTS,
        audience: object = None,
    ) -> list[Event]:
        if (
            isinstance(after, bool) or not isinstance(after, int)
            or not 0 <= after <= MAX_EVENT_ID
        ):
            raise LoomError('invalid', 'after must be an integer of 0 or more')
        if thread_id is not None and not isinstance(thread_id, str):
            raise LoomError('invalid', 'thread_id must be a string')
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise LoomError('invalid', 'limit must be an integer of 1 or more')
        if audience is not None and (
                not isinstance(audience, str) or audience not in tuple(Audience)):
            raise LoomError('invalid', f'audience must be one of {", ".join(Audience)}')
        sql = 'SELECT id, type, at, thread_id, data FROM events WHERE id > ?'
        args: list[object] = [after]
        if thread_id is not None:
            sql += ' AND thread_id = ?'
            args.append(thread_id)
        if audience is not None:
            threadless = 'thread_id IS NULL'
            if audience != Audience.AGENT:
                threadless += f' AND type IN ({", ".join("?" for _ in PERSON_THREADLESS)})'
                args.extend(str(kind) for kind in PERSON_THREADLESS)
            sql += (f' AND (({threadless})'
                    ' OR thread_id IN (SELECT id FROM threads WHERE audience = ?)'
                    " OR (type = ? AND json_extract(data, '$.previous_audience') = ?))")
            args.extend((str(audience), str(EventType.THREAD_UPDATED), str(audience)))
        sql += ' ORDER BY id LIMIT ?'
        args.append(min(limit, MAX_EVENTS))
        return [_event(row) for row in self.connection.execute(sql, args)]

    def event(self, tx: sqlite3.Connection, event_id: int) -> Event | None:
        row = tx.execute('SELECT id, type, at, thread_id, data FROM events WHERE id = ?',
                         (event_id,)).fetchone()
        return None if row is None else _event(row)

    def prune_events(self) -> int:
        cutoff = iso(self.clock() - timedelta(days=EVENT_DAYS))
        with self.transaction() as tx:
            return tx.execute('DELETE FROM events WHERE at < ?', (cutoff,)).rowcount

    def close(self) -> None:
        self.connection.close()
