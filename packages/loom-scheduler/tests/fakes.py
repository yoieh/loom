import json
import sqlite3
import tempfile
import types
import unittest
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from loom_contracts.calls import CallName
from loom_contracts.events import EventType

from loom_scheduler import Maker, ScheduleError, Scheduler

START = datetime(2026, 10, 23, 7, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self, now: datetime = START) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta: float) -> datetime:
        self.now += timedelta(**delta)
        return self.now


@dataclass
class Call:
    call: CallName
    params: dict
    maker: Maker


@dataclass
class Event:
    type: EventType
    thread_id: str | None
    data: dict


@dataclass
class FakeHost:
    path: Path
    connection: sqlite3.Connection = field(init=False)
    calls: list[Call] = field(default_factory=list)
    events: list[Event] = field(default_factory=list)
    threads: dict[str, dict] = field(default_factory=dict)
    projects: list[dict] = field(default_factory=list)
    fail: dict[CallName, BaseException] = field(default_factory=dict)
    cut_after: CallName | None = None
    launched: int = 0
    _receipts: dict[str, object] = field(default_factory=dict)
    _pending: list[Event] | None = None
    _cut: bool = False

    def __post_init__(self) -> None:
        self.connection = sqlite3.connect(self.path / 'loom.db', isolation_level=None)
        self.connection.execute('PRAGMA synchronous=OFF')
        self.connection.execute(
            'CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY, call TEXT NOT NULL, '
            'answer TEXT NOT NULL)',
        )

    def close(self) -> None:
        self.connection.close()

    def add_thread(self, thread_id: str, project_id: str = 'p1',
                   state: str = 'idle') -> dict:
        self.threads[thread_id] = {'id': thread_id, 'project_id': project_id, 'state': state}
        return self.threads[thread_id]

    def add_project(self, project_id: str) -> None:
        self.projects.append({'id': project_id, 'title': project_id})

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        if self._cut:
            self._cut = False
            raise sqlite3.OperationalError('disk I/O error')
        self.connection.execute('BEGIN IMMEDIATE')
        self._pending = []
        try:
            yield self.connection
            self.connection.execute('COMMIT')
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute('ROLLBACK')
            raise
        finally:
            pending, self._pending = self._pending, None
        self.events.extend(pending)

    def command(self, command_id: str, call: CallName,
                change: Callable[[sqlite3.Connection], object]) -> tuple[object, bool]:
        with self.transaction() as tx:
            row = tx.execute('SELECT call, answer FROM commands WHERE id = ?',
                             (command_id,)).fetchone()
            if row is not None:
                if row[0] != str(call):
                    raise ScheduleError('conflict', f'id was already used for {row[0]}')
                return json.loads(row[1]), True
            answer = json.dumps(change(tx))
            tx.execute('INSERT INTO commands (id, call, answer) VALUES (?, ?, ?)',
                       (command_id, str(call), answer))
        return json.loads(answer), False

    def append_event(self, tx: sqlite3.Connection, type: EventType, thread_id: str | None,
                     data: Mapping[str, object]) -> int:
        assert self._pending is not None, 'append_event outside a transaction'
        self._pending.append(Event(EventType(type), thread_id, dict(data)))
        return len(self.events) + len(self._pending)

    async def call(self, call: CallName, params: dict, maker: Maker) -> object:
        self.calls.append(Call(call, params, maker))
        if call in self.fail:
            raise self.fail[call]
        answer = self._answer(call, params)
        if call == self.cut_after:
            self.cut_after = None
            self._cut = True
        return json.loads(json.dumps(answer))

    def _answer(self, call: CallName, params: dict) -> object:
        if call == CallName.PROJECTS_LIST:
            return self.projects
        if call == CallName.THREADS_GET:
            return self._thread(params['thread_id'])
        if params['id'] in self._receipts:
            return self._receipts[params['id']]
        if call == CallName.THREADS_SEND:
            answer = self._thread(params['thread_id'])
        elif call == CallName.THREADS_LAUNCH:
            self.launched += 1
            answer = self.add_thread(f'launched-{self.launched}', params['project_id'],
                                     'queued')
        else:
            raise AssertionError(f'unexpected call {call}')
        self._receipts[params['id']] = answer
        return answer

    def _thread(self, thread_id: str) -> dict:
        if thread_id not in self.threads:
            raise ScheduleError('not_found', f"no thread with id '{thread_id}'")
        return self.threads[thread_id]

    def calls_of(self, call: CallName) -> list[Call]:
        return [made for made in self.calls if made.call == call]

    def rows(self, sql: str, *args: object) -> list[tuple]:
        return self.connection.execute(sql, args).fetchall()


PERSON = types.SimpleNamespace(thread_id=None, verified=True)
AGENT = types.SimpleNamespace(thread_id='t1', verified=True)
UNKNOWN = types.SimpleNamespace(thread_id=None, verified=False)
PLUS_TWO = timezone(timedelta(hours=2))


class SchedulerCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.host = FakeHost(Path(self.tmp.name))
        self.host.add_project('p1')
        self.host.add_project('p2')
        self.host.add_thread('t1', 'p1')
        self.host.add_thread('t2', 'p2')
        self.clock = FakeClock()
        self.scheduler = self.new_scheduler()

    async def asyncTearDown(self) -> None:
        await self.scheduler.close()
        self.host.close()
        self.tmp.cleanup()

    def new_scheduler(self) -> Scheduler:
        scheduler = Scheduler(self.host, clock=self.clock, tz=PLUS_TWO)
        scheduler.open()
        return scheduler

    async def call(self, name: str, params: object, caller: object = PERSON) -> object:
        return await self.scheduler.methods()[name](params, caller)

    async def refused(self, name: str, params: object, code: str = 'invalid',
                      caller: object = PERSON) -> str:
        with self.assertRaises(ScheduleError) as caught:
            await self.call(name, params, caller)
        self.assertEqual(caught.exception.code, code)
        return caught.exception.message

    async def add(self, command_id: str = 'a1', caller: object = PERSON,
                  **fields: object) -> dict:
        params = {'id': command_id, 'trigger': {'type': 'interval', 'everyMs': 60_000},
                  'text': 'check the build', 'thread_id': 't1', **fields}
        params = {key: value for key, value in params.items() if value is not ...}
        return await self.call('schedules.add', params, caller)

    async def run_pass(self, **delta: float) -> None:
        self.clock.advance(**delta)
        await self.scheduler.tick.run_once()

    def receipts(self) -> int:
        return self.host.rows('SELECT COUNT(*) FROM commands')[0][0]

    def schedule_rows(self) -> int:
        return self.host.rows('SELECT COUNT(*) FROM schedules')[0][0]

    def runs(self, schedule_id: str) -> list[tuple]:
        return self.host.rows(
            'SELECT due, at, outcome, late, thread_id, reason FROM schedule_runs '
            'WHERE schedule_id = ? ORDER BY id', schedule_id)

    def next_run(self, schedule_id: str) -> str | None:
        return self.host.rows('SELECT next_run FROM schedules WHERE id = ?', schedule_id)[0][0]
