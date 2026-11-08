import asyncio
import heapq
import json
import sqlite3
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, tzinfo
from typing import Protocol

from loom_contracts.calls import CallName, ScheduleInfo
from loom_contracts.events import EventType
from loom_contracts.states import ThreadState
from loom_contracts.triggers import ScheduleAction, TriggerType

from . import timing
from .events import EventSource
from .host import Maker, ScheduleError, SchedulerHost
from .params import AddRequest, check_add, check_list, check_target
from .table import (
    COLUMNS, FAILED, FIRED, MAX_REASON, RETRY_AFTER, SELECT, SKIPPED_BUSY, SKIPPED_LATE, create,
    launch_options, title, to_row, workspace,
)
from .tick import Tick
from .timing import iso, parse_iso

RUN_DAYS = 30
BUSY = frozenset({ThreadState.QUEUED, ThreadState.STARTING, ThreadState.RUNNING})

Method = Callable[[object, object], Awaitable[object]]


class Caller(Protocol):
    thread_id: str | None
    verified: bool


def _info(row: dict) -> ScheduleInfo:
    return {
        'id': row['id'],
        'trigger': json.loads(row['trigger']),
        'action': row['action'],
        'text': row['text'],
        'thread_id': row['thread_id'],
        'project_id': row['project_id'],
        'title': row['title'],
        'concurrency_key': row['concurrency_key'],
        'on_busy': row['on_busy'],
        'settle': row['settle'],
        'runner': row['runner'],
        'model': row['model'],
        'workspace': workspace(row),
        'by': row['by'],
        'paused': bool(row['paused']),
        'next_run': row['next_run'],
        'created_at': row['created_at'],
    }


def _fetch(tx: sqlite3.Connection, schedule_id: str) -> dict:
    row = to_row(tx.execute(f'{SELECT} WHERE id = ?', (schedule_id,)).fetchone())
    if row is None:
        raise ScheduleError('not_found', f"no schedule with id '{schedule_id}'")
    return row


def _message(text: str, due: str, late: bool) -> str:
    if not late:
        return text
    return f'This run is late: it was due at {due}.\n\n{text}'


class Scheduler:
    def __init__(self, host: SchedulerHost, *, clock: Callable[[], datetime],
                 tz: tzinfo | None = None) -> None:
        self.host = host
        self.clock = clock
        self.tz = tz
        self.tick = Tick(clock)
        self.tick.register(self)
        self.events = EventSource(host, clock, self.tick.wake)
        self.tick.register(self.events)
        self._heap: list[tuple[datetime, str]] = []
        self._next: dict[str, datetime] = {}
        self._task: asyncio.Task | None = None

    def open(self) -> None:
        cutoff = iso(self.clock() - timedelta(days=RUN_DAYS))
        with self.host.transaction() as tx:
            create(tx)
            tx.execute('DELETE FROM schedule_runs WHERE at < ?', (cutoff,))
        with self.host.transaction() as tx:
            rows = tx.execute(
                'SELECT id, next_run FROM schedules WHERE paused = 0 AND next_run IS NOT NULL',
            ).fetchall()
        self._heap.clear()
        self._next.clear()
        for schedule_id, next_run in rows:
            self._plan(schedule_id, parse_iso(next_run))
        self.events.open()
        self.tick.wake()

    def methods(self) -> dict[str, Method]:
        return {
            'schedules.add': self._add,
            'schedules.list': self._list,
            'schedules.pause': self._pause,
            'schedules.remove': self._remove,
        }

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.tick.run())

    async def close(self) -> None:
        self.events.close()
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def next_wake(self) -> datetime | None:
        while self._heap and self._next.get(self._heap[0][1]) != self._heap[0][0]:
            heapq.heappop(self._heap)
        return self._heap[0][0] if self._heap else None

    async def run_due(self, now: datetime) -> None:
        while (wake := self.next_wake()) is not None and wake <= now:
            _, schedule_id = heapq.heappop(self._heap)
            del self._next[schedule_id]
            try:
                await self._run(schedule_id, now)
            except Exception:
                self._plan(schedule_id, now + RETRY_AFTER)

    def _plan(self, schedule_id: str, at: datetime | None) -> None:
        if at is None:
            self._next.pop(schedule_id, None)
            return
        self._next[schedule_id] = at
        heapq.heappush(self._heap, (at, schedule_id))

    async def _add(self, params: object, caller: Caller) -> ScheduleInfo:
        by = caller.thread_id
        verified = caller.verified is True
        request = check_add(params, by, self.tz)
        project_id = await self._check_targets(request, Maker(by, verified, None))
        now = self.clock()
        first = timing.first_run(request.trigger, now, self.tz)
        on_events = request.trigger['type'] == TriggerType.EVENT
        stored = (None if request.workspace is None
                  else json.dumps(request.workspace, separators=(',', ':')))

        def change(tx: sqlite3.Connection) -> ScheduleInfo:
            schedule_id = str(uuid.uuid4())
            event_after = self.host.last_event_id() if on_events else None
            tx.execute(
                f'INSERT INTO schedules ({", ".join(COLUMNS)}) '
                f'VALUES ({", ".join("?" for _ in COLUMNS)})',
                (schedule_id, json.dumps(request.trigger), request.action.value, request.text,
                 request.thread_id, project_id, request.title, request.concurrency_key,
                 request.on_busy, request.settle, request.runner, request.model, stored, by,
                 int(verified), 0,
                 None if first is None else iso(first), iso(now), event_after),
            )
            return _info(_fetch(tx, schedule_id))

        answer, replayed = self.host.command(request.command_id, CallName.SCHEDULES_ADD, change)
        if not replayed and answer['next_run'] is not None:
            self._plan(answer['id'], parse_iso(answer['next_run']))
            self.tick.wake()
        if not replayed and on_events:
            self.events.watch()
        return answer

    async def _check_targets(self, request: AddRequest, maker: Maker) -> str | None:
        project_id = request.project_id
        if request.thread_id is not None:
            thread = await self.host.call(
                CallName.THREADS_GET, {'thread_id': request.thread_id}, maker,
            )
            if project_id is None:
                project_id = thread['project_id']
            elif project_id != thread['project_id']:
                raise ScheduleError('invalid', "project_id is not the thread's project")
        named = [name for name in (project_id, request.trigger.get('project_id'))
                 if name is not None]
        if named:
            projects = await self.host.call(CallName.PROJECTS_LIST, {}, maker)
            known = {project['id'] for project in projects}
            for name in named:
                if name not in known:
                    raise ScheduleError('not_found', f"no project with id '{name}'")
        return project_id

    async def _list(self, params: object, caller: Caller) -> list[ScheduleInfo]:
        thread_id, project_id = check_list(params)
        sql, args = SELECT, []
        filters = [(field, value) for field, value in
                   (('thread_id', thread_id), ('project_id', project_id)) if value is not None]
        if filters:
            sql += ' WHERE ' + ' AND '.join(f'{field} = ?' for field, _ in filters)
            args = [value for _, value in filters]
        with self.host.transaction() as tx:
            rows = tx.execute(f'{sql} ORDER BY created_at, id', args).fetchall()
        return [_info(to_row(row)) for row in rows]

    async def _pause(self, params: object, caller: Caller) -> ScheduleInfo:
        command_id, schedule_id = check_target(params)

        def change(tx: sqlite3.Connection) -> ScheduleInfo:
            _fetch(tx, schedule_id)
            tx.execute('UPDATE schedules SET paused = 1, next_run = NULL WHERE id = ?',
                       (schedule_id,))
            return _info(_fetch(tx, schedule_id))

        answer, replayed = self.host.command(command_id, CallName.SCHEDULES_PAUSE, change)
        if not replayed:
            self._plan(schedule_id, None)
        return answer

    async def _remove(self, params: object, caller: Caller) -> ScheduleInfo:
        command_id, schedule_id = check_target(params)

        def change(tx: sqlite3.Connection) -> ScheduleInfo:
            info = _info(_fetch(tx, schedule_id))
            tx.execute('DELETE FROM schedule_runs WHERE schedule_id = ?', (schedule_id,))
            tx.execute('DELETE FROM schedules WHERE id = ?', (schedule_id,))
            return info

        answer, replayed = self.host.command(command_id, CallName.SCHEDULES_REMOVE, change)
        if not replayed:
            self._plan(schedule_id, None)
        return answer

    async def _run(self, schedule_id: str, now: datetime) -> None:
        with self.host.transaction() as tx:
            row = to_row(tx.execute(f'{SELECT} WHERE id = ?', (schedule_id,)).fetchone())
        if row is None or row['paused'] or row['next_run'] is None:
            return
        due = parse_iso(row['next_run'])
        if due > now:
            self._plan(schedule_id, due)
            return
        trigger = json.loads(row['trigger'])
        late = timing.is_late(due, now)
        after = timing.run_after(trigger, now, self.tz)
        if timing.is_missed(trigger, due, now):
            outcome, thread_id, reason = SKIPPED_LATE, None, None
        else:
            outcome, thread_id, reason = await self._fire(row, iso(due), late)
        self._record(row, due, now, after, outcome, late, thread_id, reason)

    async def _fire(self, row: dict, due: str,
                    late: bool) -> tuple[str, str | None, str | None]:
        maker = Maker(row['by'], bool(row['verified']), row['id'])
        action = ScheduleAction(row['action'])
        command_id = f'schedule:{row["id"]}:{due}'
        message = _message(row['text'], due, late)
        try:
            if action is ScheduleAction.NOTIFY:
                return FIRED, None, None
            if await self._busy(row['id'], maker):
                return SKIPPED_BUSY, None, None
            if action is ScheduleAction.POST:
                await self.host.call(CallName.THREADS_SEND, {
                    'id': command_id, 'thread_id': row['thread_id'], 'message': message,
                    'mode': 'queue',
                }, maker)
                return FIRED, row['thread_id'], None
            params = {
                'id': command_id, 'title': title(row['title'], row['text']), 'message': message,
                'project_id': row['project_id'], 'origin': f'schedule:{row["id"]}',
            }
            params.update(launch_options(row))
            thread = await self.host.call(CallName.THREADS_LAUNCH, params, maker)
            return FIRED, thread['id'], None
        except ScheduleError as error:
            return FAILED, None, error.message[:MAX_REASON]
        except Exception as error:
            return FAILED, None, type(error).__name__

    async def _busy(self, schedule_id: str, maker: Maker) -> bool:
        with self.host.transaction() as tx:
            last = tx.execute(
                'SELECT thread_id FROM schedule_runs WHERE schedule_id = ? AND outcome = ? '
                'ORDER BY id DESC LIMIT 1', (schedule_id, FIRED),
            ).fetchone()
        if last is None or last[0] is None:
            return False
        try:
            thread = await self.host.call(CallName.THREADS_GET, {'thread_id': last[0]}, maker)
        except ScheduleError as error:
            if error.code == 'not_found':
                return False
            raise
        return thread['state'] in BUSY

    def _record(self, row: dict, due: datetime, now: datetime, after: datetime | None,
                outcome: str, late: bool, thread_id: str | None, reason: str | None) -> None:
        schedule_id = row['id']
        next_run = None if after is None else iso(after)
        with self.host.transaction() as tx:
            current = tx.execute(
                'SELECT paused FROM schedules WHERE id = ?', (schedule_id,),
            ).fetchone()
            if current is not None:
                tx.execute(
                    'INSERT INTO schedule_runs (schedule_id, due, at, outcome, late, thread_id, '
                    'reason) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (schedule_id, iso(due), iso(now), outcome, int(late), thread_id, reason),
                )
                tx.execute('UPDATE schedules SET next_run = ? WHERE id = ? AND paused = 0',
                           (next_run, schedule_id))
            if outcome == FIRED:
                data: dict[str, object] = {
                    'schedule_id': schedule_id, 'action': row['action'], 'text': row['text'],
                    'late': late,
                }
                if row['by'] is not None:
                    data['by'] = row['by']
                event_thread = row['thread_id'] if thread_id is None else thread_id
                self.host.append_event(tx, EventType.SCHEDULE_FIRED, event_thread, data)
        if current is not None and not current[0] and next_run is not None:
            self._plan(schedule_id, parse_iso(next_run))
