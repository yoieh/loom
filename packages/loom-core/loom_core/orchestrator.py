import asyncio
import sqlite3
import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path

from loom_contracts.calls import Audience, CallName, OnBusy, ProjectInfo, Settle, ThreadInfo
from loom_contracts.events import EventType
from loom_contracts.runners import (
    Capability, DispatchMode, Exited, PermissionMode, PermissionRequest, Runner, RunnerEvent,
    RunnerHost, RunnerName, RunnerSession, TextOut, ThreadSpec, ToolUse, TurnResult, TurnStarted,
)
from loom_contracts.states import ThreadState, TurnState
from loom_contracts.workspaces import WorkspaceStrategy

from . import concurrency, delegation, keys, threads
from .clock import iso
from .errors import LoomError
from .lanes import Lanes
from .projects import Projects
from .slots import StartSource
from .store import Store
from .threads import ENDED, LIVE, fetch, thread_info, transition
from .workspaces import WorkspacePlan, make_workspace, plan_workspace

REQUESTS = ('caller', 'any')
DEFAULT_WORKSPACE = {'strategy': WorkspaceStrategy.WORKTREE.value}
RECOVERED = frozenset({ThreadState.STARTING, ThreadState.RUNNING, ThreadState.IDLE})
SETTLING = frozenset({TurnState.COMPLETED, TurnState.FAILED})


def _params(params: object) -> dict:
    if not isinstance(params, dict):
        raise LoomError('invalid', 'params must be an object')
    return params


def _required(params: dict, field: str) -> object:
    value = params.get(field)
    if value is None:
        raise LoomError('invalid', f'{field} is required')
    return value


def _not_null(params: dict, field: str) -> object:
    value = params[field]
    if value is None:
        raise LoomError('invalid', f'{field} must not be null')
    return value


def _string(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise LoomError('invalid', f'{field} must be a string')
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise LoomError('invalid', f'{field} must be a non-empty string')
    return value


def _choice[E: StrEnum](value: object, field: str, kind: type[E]) -> E:
    if isinstance(value, str):
        try:
            return kind(value)
        except ValueError:
            pass
    raise LoomError('invalid', f'{field} must be one of {", ".join(item for item in kind)}')


def _plan(tx: sqlite3.Connection, workspace: object, project: ProjectInfo, project_id: str,
          thread_id: str, data_dir: Path) -> WorkspacePlan:
    plan = plan_workspace(workspace, project, thread_id, data_dir)
    if (plan.strategy is WorkspaceStrategy.EXISTING_WORKTREE
            and not threads.uses_workspace(tx, project_id, plan.path)):
        raise LoomError('invalid', 'path is not the workspace of a thread of this project')
    return plan


def _join(tx: sqlite3.Connection, key: str | None, on_busy: OnBusy, project_id: str,
          parent_id: str | None, text: str) -> ThreadInfo | None:
    if on_busy is not OnBusy.COALESCE or concurrency.claim(tx, key) != concurrency.WAITING:
        return None
    thread_id = concurrency.join(tx, key, project_id, parent_id, text)
    return None if thread_id is None else thread_info(fetch(tx, thread_id))


def _held(tx: sqlite3.Connection, thread_id: str) -> tuple[int, StartSource] | None:
    found = tx.execute(
        'SELECT seq, (SELECT min(source) FROM messages WHERE thread_id = ?) FROM messages '
        'WHERE thread_id = ? ORDER BY priority DESC, seq LIMIT 1', (thread_id, thread_id),
    ).fetchone()
    return None if found is None else (found[0], StartSource(found[1]))


def _reason(error: BaseException) -> str:
    if isinstance(error, LoomError):
        return error.message
    return str(error) or type(error).__name__


class Orchestrator:
    def __init__(self, store: Store, projects: Projects, runners: Mapping[str, Runner],
                 host: RunnerHost, *, stop_grace: float = 5.0, agents_max: int = 1000,
                 idle_after: int = 1200, idle_check: float = 30.0) -> None:
        self.store = store
        self.projects = projects
        self.runners = runners
        self.host = host
        self.stop_grace = stop_grace
        self.agents_max = agents_max
        self.idle_after = idle_after
        self.idle_check = idle_check
        self.lanes = Lanes()
        self._sessions: dict[str, RunnerSession] = {}
        self._pumps: dict[RunnerSession, asyncio.Task] = {}
        self._turn_ends: dict[str, asyncio.Future] = {}
        self._stop_waits: dict[str, asyncio.Task] = {}
        self._closing = False
        self._slots: set[str] = set()
        self._starting: set[str] = set()
        self._sleepers: set[str] = set()
        self._idle_since: dict[str, datetime] = {}
        self._idle_task: asyncio.Task | None = None
        store.on_handed = self._submit_start

    async def launch(self, params: object, *,
                     source: StartSource = StartSource.PERSON) -> ThreadInfo:
        params = _params(params)
        title = _text(_required(params, 'title'), 'title')
        message = _text(_required(params, 'message'), 'message')
        project_id = _string(_required(params, keys.PROJECT_KEY), keys.PROJECT_KEY)
        workspace = DEFAULT_WORKSPACE
        if 'workspace' in params:
            workspace = _not_null(params, 'workspace')
            if not isinstance(workspace, dict):
                raise LoomError('invalid', 'workspace must be an object')
        runner = RunnerName.CLAUDE
        if 'runner' in params:
            runner = _choice(_not_null(params, 'runner'), 'runner', RunnerName)
        if runner not in self.runners:
            raise LoomError('invalid', f'runner {runner} is not available')
        model = None
        if 'model' in params:
            model = _text(_not_null(params, 'model'), 'model')
        requests = 'any'
        if 'requests' in params:
            requests = _not_null(params, 'requests')
            if requests not in REQUESTS:
                raise LoomError('invalid', 'requests must be one of caller, any')
        origin = None
        if 'origin' in params:
            origin = _string(_not_null(params, 'origin'), 'origin')
        audience = Audience.AGENT
        if 'audience' in params:
            audience = _choice(_not_null(params, 'audience'), 'audience', Audience)
        key, on_busy = concurrency.key_params(params)
        settle = None
        if 'settle' in params:
            settle = _choice(_not_null(params, 'settle'), 'settle', Settle)
        command_id = params.get('id')
        start = False

        def change(tx: sqlite3.Connection) -> ThreadInfo:
            nonlocal start
            project = self.projects.get(project_id)
            mode = self.projects.effective_mode(project_id)
            thread_id = str(uuid.uuid4())
            plan = _plan(tx, workspace, project, project_id, thread_id, self.store.data_dir)
            joined = _join(tx, key, on_busy, project_id, None, message)
            if joined is not None:
                return joined
            key_state = concurrency.claim(tx, key)
            start = key_state != concurrency.WAITING
            row = threads.insert(self.store, tx, {
                'id': thread_id,
                'title': title,
                'runner': runner,
                'project_id': project_id,
                'workspace_strategy': plan.strategy,
                'workspace_path': plan.path,
                'branch': plan.branch,
                'base': plan.base,
                'model': model,
                'permission_mode': mode,
                'requests': requests,
                'origin': origin,
                'audience': audience,
                'concurrency_key': key,
                'settle': settle,
                'key_state': key_state,
            })
            threads.add_start(tx, thread_id, source, row['created_at'])
            self._add_message(tx, thread_id, f'{command_id}:message', message, DispatchMode.STEER,
                              source=source)
            return thread_info(row)

        receipt = self.store.command(command_id, CallName.THREADS_LAUNCH, change)
        if not receipt.replayed and start:
            self._submit_start(receipt.answer['id'])
        return receipt.answer

    async def delegate(self, params: object, *,
                       source: StartSource = StartSource.PERSON) -> ThreadInfo:
        params = _params(params)
        parent_id = _string(_required(params, keys.PARENT_KEY), keys.PARENT_KEY)
        task = _text(_required(params, 'task'), 'task')
        workspace = None
        if 'workspace' in params:
            workspace = _not_null(params, 'workspace')
            if not isinstance(workspace, dict):
                raise LoomError('invalid', 'workspace must be an object')
        runner = None
        if 'runner' in params:
            runner = _choice(_not_null(params, 'runner'), 'runner', RunnerName)
            if runner not in self.runners:
                raise LoomError('invalid', f'runner {runner} is not available')
        has_model = 'model' in params
        model = _text(_not_null(params, 'model'), 'model') if has_model else None
        key, on_busy = concurrency.key_params(params)
        command_id = params.get('id')
        start = False

        def change(tx: sqlite3.Connection) -> ThreadInfo:
            nonlocal start
            parent = fetch(tx, parent_id)
            if parent['state'] in ENDED:
                raise LoomError('conflict', f'thread {parent_id} is {parent["state"]}')
            if parent['stop_requested']:
                raise LoomError('conflict', f'thread {parent_id} is stopping')
            child_runner = runner or parent['runner']
            if child_runner not in self.runners:
                raise LoomError('invalid', f'runner {child_runner} is not available')
            project_id = parent['project_id']
            project = self.projects.get(project_id)
            thread_id = str(uuid.uuid4())
            chosen = workspace if workspace is not None else delegation.parent_workspace(parent)
            plan = _plan(tx, chosen, project, project_id, thread_id, self.store.data_dir)
            joined = _join(tx, key, on_busy, project_id, parent_id, task)
            if joined is not None:
                return joined
            key_state = concurrency.claim(tx, key)
            start = key_state != concurrency.WAITING
            row = threads.insert(self.store, tx, {
                'id': thread_id,
                'title': delegation.child_title(task),
                'runner': child_runner,
                'project_id': project_id,
                'workspace_strategy': plan.strategy,
                'workspace_path': plan.path,
                'branch': plan.branch,
                'base': plan.base,
                'model': model if has_model else parent['model'],
                'permission_mode': self.projects.effective_mode(project_id),
                'requests': parent['requests'],
                'parent_id': parent_id,
                'audience': Audience.AGENT,
                'concurrency_key': key,
                'key_state': key_state,
            })
            threads.add_start(tx, thread_id, source, row['created_at'])
            self._add_message(tx, thread_id, f'{command_id}:message', task, DispatchMode.STEER,
                              source=source)
            return thread_info(row)

        receipt = self.store.command(command_id, CallName.THREADS_DELEGATE, change)
        if not receipt.replayed and start:
            self._submit_start(receipt.answer['id'])
        return receipt.answer

    async def send(self, params: object, *,
                   source: StartSource = StartSource.PERSON) -> ThreadInfo:
        params = _params(params)
        thread_id = _string(_required(params, 'thread_id'), 'thread_id')
        message = _text(_required(params, 'message'), 'message')
        mode = DispatchMode.STEER
        if 'mode' in params:
            mode = _choice(_not_null(params, 'mode'), 'mode', DispatchMode)
        command_id = params.get('id')
        seq = None

        def change(tx: sqlite3.Connection) -> ThreadInfo:
            nonlocal seq
            row = fetch(tx, thread_id)
            if row['state'] in ENDED:
                raise LoomError('conflict', f'thread {thread_id} is {row["state"]}')
            seq = self._add_message(tx, thread_id, command_id, message, mode, source=source)
            return thread_info(row)

        receipt = self.store.command(command_id, CallName.THREADS_SEND, change)
        if not receipt.replayed:
            self.lanes.submit(thread_id, lambda: self._deliver(thread_id, seq, source))
        return receipt.answer

    async def settle(self, params: object) -> ThreadInfo:
        params = _params(params)
        thread_id = _string(_required(params, 'thread_id'), 'thread_id')
        before = None

        def change(tx: sqlite3.Connection) -> ThreadInfo:
            nonlocal before
            row = fetch(tx, thread_id)
            if row['state'] != ThreadState.SETTLED:
                before = ThreadState(row['state'])
                reason = row['reason'] if before is ThreadState.FAILED else None
                transition(self.store, tx, thread_id, ThreadState.SETTLED, reason)
                tx.execute('DELETE FROM messages WHERE thread_id = ?', (thread_id,))
                tx.execute('DELETE FROM continuations WHERE thread_id = ?', (thread_id,))
            return thread_info(fetch(tx, thread_id))

        receipt = self.store.command(params.get('id'), CallName.THREADS_SETTLE, change)
        if not receipt.replayed and before is not None:
            self.lanes.submit(thread_id, lambda: self._end_session(thread_id, before))
        return receipt.answer

    async def update(self, params: object) -> ThreadInfo:
        params = _params(params)
        thread_id = _string(_required(params, 'thread_id'), 'thread_id')
        audience = _choice(_required(params, 'audience'), 'audience', Audience)

        def change(tx: sqlite3.Connection) -> ThreadInfo:
            previous = Audience(fetch(tx, thread_id)['audience'])
            if previous is not audience:
                tx.execute('UPDATE threads SET audience = ?, updated_at = ? WHERE id = ?',
                           (str(audience), iso(self.store.clock()), thread_id))
                self.store.append_event(tx, EventType.THREAD_UPDATED, thread_id,
                                        {'audience': audience, 'previous_audience': previous})
            return thread_info(fetch(tx, thread_id))

        return self.store.command(params.get('id'), CallName.THREADS_UPDATE, change).answer

    async def stop(self, params: object) -> ThreadInfo:
        params = _params(params)
        thread_id = _string(_required(params, 'thread_id'), 'thread_id')
        stopped: list[str] = []

        def change(tx: sqlite3.Connection) -> ThreadInfo:
            stopped.clear()
            if fetch(tx, thread_id)['state'] not in ENDED:
                for row in threads.stop_targets(tx, thread_id):
                    tx.execute('DELETE FROM messages WHERE thread_id = ?', (row['id'],))
                    if not row['stop_requested']:
                        threads.request_stop(self.store, tx, row['id'])
                        stopped.append(row['id'])
            return thread_info(fetch(tx, thread_id))

        receipt = self.store.command(params.get('id'), CallName.THREADS_STOP, change)
        if not receipt.replayed:
            for target in stopped:
                self.lanes.submit(target, lambda target=target: self._stop_turn(target))
        return receipt.answer

    def get(self, thread_id: str) -> ThreadInfo:
        return thread_info(fetch(self.store.connection, _string(thread_id, 'thread_id')))

    async def drain(self) -> None:
        await self.lanes.drain()
        while self._stop_waits:
            await asyncio.wait(list(self._stop_waits.values()))
            await self.lanes.drain()

    async def close(self) -> None:
        self._closing = True
        if self._idle_task is not None:
            self._idle_task.cancel()
            await asyncio.gather(self._idle_task, return_exceptions=True)
            self._idle_task = None
        waits = list(self._stop_waits.values())
        for wait in waits:
            wait.cancel()
        await asyncio.gather(*waits, return_exceptions=True)
        self._stop_waits.clear()
        await self.lanes.drain()
        sessions = list(self._sessions.values())
        self._sessions.clear()
        await asyncio.gather(*(self._close_session(session) for session in sessions))
        pumps = list(self._pumps.values())
        for pump in pumps:
            pump.cancel()
        await asyncio.gather(*pumps, return_exceptions=True)

    async def recover(self) -> None:
        for row in threads.select(self.store.connection):
            if row['state'] in RECOVERED or (row['stop_requested'] and row['state'] in LIVE):
                with self.store.transaction() as tx:
                    self._recover(tx, fetch(tx, row['id']))
        starts: list[tuple[int, str, str, int | None]] = [
            (rank, at, thread_id, None)
            for rank, at, thread_id in threads.start_queue(self.store.connection)]
        now = iso(self.store.clock())
        for row in threads.select(self.store.connection):
            found = _held(self.store.connection, row['id'])
            if (row['state'] in LIVE and row['state'] != ThreadState.QUEUED
                    and found is not None):
                seq, source = found
                starts.append((source, now, row['id'], seq))
        for rank, _, thread_id, seq in sorted(starts):
            if seq is None:
                self._submit_start(thread_id)
            else:
                self.lanes.submit(thread_id, lambda thread_id=thread_id, seq=seq, rank=rank:
                                  self._deliver(thread_id, seq, StartSource(rank)))
        if self._idle_task is None and not self._closing:
            self._idle_task = asyncio.get_running_loop().create_task(
                self._check_idle(), name='loom-idle')

    async def sleep_idle(self) -> None:
        cutoff = self.store.clock() - timedelta(seconds=self.idle_after)
        due = [thread_id for thread_id, since in self._idle_since.items()
               if since <= cutoff and self._sleepable(thread_id)]
        waits = []
        for thread_id in due:
            self._sleepers.add(thread_id)
            waits.append(self.lanes.submit(
                thread_id, lambda thread_id=thread_id: self._put_to_sleep(thread_id)))
        if waits:
            await asyncio.wait(waits)

    async def _check_idle(self) -> None:
        while True:
            await asyncio.sleep(self.idle_check)
            try:
                await self.sleep_idle()
            except Exception:
                pass

    def _recover(self, tx: sqlite3.Connection, row: sqlite3.Row) -> None:
        thread_id = row['id']
        state = ThreadState(row['state'])
        stopping = bool(row['stop_requested'])
        if state is ThreadState.RUNNING and not stopping:
            self.store.append_event(tx, EventType.TURN_RESULT, thread_id,
                                    {'state': TurnState.FAILED, 'reason': 'restart'})
            self._crash(tx, row)
            return
        if state is ThreadState.RUNNING:
            self.store.append_event(tx, EventType.TURN_RESULT, thread_id,
                                    {'state': TurnState.STOPPED, 'reason': 'stop'})
        if state in RECOVERED or (stopping and state is ThreadState.QUEUED):
            transition(self.store, tx, thread_id, ThreadState.SLEEPING)
        if stopping:
            threads.clear_stop(self.store, tx, thread_id)

    def _add_message(self, tx: sqlite3.Connection, thread_id: str, command_id: str,
                     text: str, mode: DispatchMode, priority: int = 0, *,
                     source: StartSource = StartSource.AGENT) -> int:
        cursor = tx.execute(
            'INSERT INTO messages '
            '(thread_id, command_id, text, mode, priority, created_at, source) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            (thread_id, command_id, text, mode, priority, iso(self.store.clock()), int(source)),
        )
        return cursor.lastrowid

    def _row(self, thread_id: str) -> sqlite3.Row:
        return fetch(self.store.connection, thread_id)

    def _capable(self, row: sqlite3.Row, capability: Capability) -> bool:
        return capability in self.runners[row['runner']].capabilities

    def _move(self, thread_id: str, state: ThreadState, reason: str | None = None) -> bool:
        with self.store.transaction() as tx:
            if fetch(tx, thread_id)['state'] in ENDED:
                return False
            transition(self.store, tx, thread_id, state, reason)
        return True

    async def _fail(self, thread_id: str, error: BaseException | str) -> None:
        reason = error if isinstance(error, str) else _reason(error)
        self._move(thread_id, ThreadState.FAILED, reason)
        session = self._sessions.pop(thread_id, None)
        if session is not None:
            await self._close_session(session)
        self._give_back(thread_id)

    async def _close_session(self, session: RunnerSession) -> None:
        try:
            await session.close()
        except Exception:
            pass

    def _submit_start(self, thread_id: str) -> None:
        self._starting.add(thread_id)
        self.lanes.submit(thread_id, lambda: self._start(thread_id))

    async def _start(self, thread_id: str) -> None:
        try:
            row = self._row(thread_id)
            if row['state'] != ThreadState.QUEUED or row['key_state'] == concurrency.WAITING:
                return
            if row['stop_requested']:
                self._move(thread_id, ThreadState.SLEEPING)
                return
            await self._admit(thread_id, None)
        finally:
            self._starting.discard(thread_id)
            self._check_slot(thread_id)

    async def _admit(self, thread_id: str, source: StartSource | None) -> None:
        while thread_id not in self._slots:
            if self._closing:
                return
            if len(self._slots) < self.agents_max:
                self._slots.add(thread_id)
                break
            victim = self._longest_idle(thread_id)
            if victim is None:
                if source is not None:
                    self._enqueue(thread_id, source)
                return
            freed = asyncio.get_running_loop().create_future()
            self._sleepers.add(victim)
            self.lanes.submit(victim, lambda victim=victim, freed=freed:
                              self._put_to_sleep(victim, (thread_id, freed)))
            await freed
        await self._boot(thread_id, resume=bool(self._row(thread_id)['has_session']))
        self._check_slot(thread_id)

    def _enqueue(self, thread_id: str, source: StartSource) -> None:
        with self.store.transaction() as tx:
            if fetch(tx, thread_id)['state'] == ThreadState.SLEEPING:
                transition(self.store, tx, thread_id, ThreadState.QUEUED)
                threads.add_start(tx, thread_id, source, iso(self.store.clock()))

    def _queue(self) -> list[str]:
        return [thread_id for _, _, thread_id in threads.start_queue(self.store.connection)
                if thread_id not in self._slots and thread_id not in self._starting]

    def _sleepable(self, thread_id: str) -> bool:
        if thread_id not in self._sessions or thread_id in self._sleepers:
            return False
        row = self._row(thread_id)
        return row['state'] == ThreadState.IDLE and not row['stop_requested']

    def _longest_idle(self, other_than: str | None = None) -> str | None:
        found = sorted((since, thread_id) for thread_id, since in self._idle_since.items()
                       if thread_id != other_than and self._sleepable(thread_id))
        return found[0][1] if found else None

    def _check_slot(self, thread_id: str) -> None:
        if thread_id in self._slots and thread_id not in self._sessions:
            self._give_back(thread_id)

    def _give_back(self, thread_id: str) -> None:
        self._idle_since.pop(thread_id, None)
        if thread_id not in self._slots:
            return
        self._slots.discard(thread_id)
        if self._closing:
            return
        for head in self._queue()[:max(0, self.agents_max - len(self._slots))]:
            self._slots.add(head)
            self._submit_start(head)

    def _hand_over(self, thread_id: str) -> None:
        if self._closing or self._row(thread_id)['state'] != ThreadState.IDLE:
            return
        queue = self._queue()
        if queue and self._longest_idle(queue[0]) is not None:
            self._submit_start(queue[0])

    async def _put_to_sleep(self, thread_id: str,
                            waiter: tuple[str, asyncio.Future] | None = None) -> None:
        self._sleepers.discard(thread_id)
        try:
            session = self._sessions.get(thread_id)
            row = self._row(thread_id)
            if session is None or row['state'] != ThreadState.IDLE or row['stop_requested']:
                return
            del self._sessions[thread_id]
            await self._close_session(session)
            self._move(thread_id, ThreadState.SLEEPING)
            if waiter is None:
                self._give_back(thread_id)
            else:
                self._idle_since.pop(thread_id, None)
                self._slots.discard(thread_id)
                self._slots.add(waiter[0])
        finally:
            if waiter is not None and not waiter[1].done():
                waiter[1].set_result(None)

    async def _boot(self, thread_id: str, resume: bool) -> None:
        if self._closing:
            return
        row = self._row(thread_id)
        plan = WorkspacePlan(WorkspaceStrategy(row['workspace_strategy']), row['workspace_path'],
                             row['branch'], row['base'])
        try:
            made = await make_workspace(plan, self.projects.get(row['project_id']),
                                        self.store.data_dir)
        except Exception as error:
            await self._fail(thread_id, error)
            return
        with self.store.transaction() as tx:
            row = fetch(tx, thread_id)
            if row['state'] in ENDED:
                return
            if row['stop_requested']:
                if row['state'] != ThreadState.SLEEPING:
                    transition(self.store, tx, thread_id, ThreadState.SLEEPING)
                return
            mode = threads.narrower(row['permission_mode'],
                                    self.projects.effective_mode(row['project_id']))
            tx.execute(
                'UPDATE threads SET branch = ?, base = ?, permission_mode = ? WHERE id = ?',
                (made.branch, made.base, mode, thread_id),
            )
            transition(self.store, tx, thread_id, ThreadState.STARTING)
        row = self._row(thread_id)
        spec = ThreadSpec(
            id=thread_id,
            runner=RunnerName(row['runner']),
            project_id=row['project_id'],
            workspace_path=row['workspace_path'],
            title=row['title'],
            permission_mode=PermissionMode(row['permission_mode']),
            model=row['model'],
            resume=resume,
            parent_id=row['parent_id'],
            origin=row['origin'],
        )
        try:
            session = await self.runners[row['runner']].start(spec, self.host)
        except Exception as error:
            await self._fail(thread_id, error)
            return
        with self.store.transaction() as tx:
            row = fetch(tx, thread_id)
            live = row['state'] not in ENDED and not row['stop_requested']
            if live:
                tx.execute('UPDATE threads SET has_session = 1 WHERE id = ?', (thread_id,))
            elif row['state'] not in ENDED:
                transition(self.store, tx, thread_id, ThreadState.SLEEPING)
        if not live:
            await self._close_session(session)
            return
        self._sessions[thread_id] = session
        self._pumps[session] = asyncio.ensure_future(self._pump(thread_id, session))
        await self._dispatch_next(thread_id)

    async def _dispatch_next(self, thread_id: str) -> None:
        session = self._sessions.get(thread_id)
        if session is None:
            return
        found = self.store.connection.execute(
            'SELECT seq, text FROM messages WHERE thread_id = ? '
            'ORDER BY priority DESC, seq LIMIT 1', (thread_id,),
        ).fetchone()
        if found is None:
            if self._row(thread_id)['state'] == ThreadState.STARTING:
                self._sessions.pop(thread_id, None)
                await self._close_session(session)
                self._move(thread_id, ThreadState.SLEEPING)
                self._give_back(thread_id)
            return
        try:
            await session.send(found[1], DispatchMode.QUEUE)
        except Exception as error:
            await self._fail(thread_id, error)
            return
        with self.store.transaction() as tx:
            if fetch(tx, thread_id)['state'] in ENDED:
                return
            tx.execute('DELETE FROM messages WHERE seq = ?', (found[0],))
            transition(self.store, tx, thread_id, ThreadState.RUNNING)

    async def _deliver(self, thread_id: str, seq: int, source: StartSource) -> None:
        row = self._row(thread_id)
        if row['stop_requested']:
            return
        state = ThreadState(row['state'])
        if state is ThreadState.SLEEPING:
            if row['has_session'] and not self._capable(row, Capability.RESUME):
                await self._fail(thread_id, 'runner cannot resume')
                return
            await self._admit(thread_id, source)
        elif state is ThreadState.QUEUED:
            with self.store.transaction() as tx:
                threads.rank_start(tx, thread_id, source)
        elif state is ThreadState.IDLE:
            await self._dispatch_next(thread_id)
        elif state is ThreadState.RUNNING:
            await self._while_running(row, seq)

    async def _while_running(self, row: sqlite3.Row, seq: int) -> None:
        thread_id = row['id']
        session = self._sessions.get(thread_id)
        found = self.store.connection.execute(
            'SELECT text, mode FROM messages WHERE seq = ?', (seq,),
        ).fetchone()
        if session is None or found is None:
            return
        mode = DispatchMode(found[1])
        try:
            if mode is DispatchMode.STEER and self._capable(row, Capability.STEER):
                await session.send(found[0], DispatchMode.STEER)
                with self.store.transaction() as tx:
                    tx.execute('DELETE FROM messages WHERE seq = ?', (seq,))
            elif mode is DispatchMode.INTERRUPT and self._capable(row, Capability.INTERRUPT):
                await session.interrupt()
        except Exception as error:
            await self._fail(thread_id, error)

    async def _end_session(self, thread_id: str, before: ThreadState) -> None:
        session = self._sessions.pop(thread_id, None)
        if session is None:
            return
        if before is ThreadState.RUNNING and self._capable(self._row(thread_id),
                                                           Capability.INTERRUPT):
            try:
                await session.interrupt()
            except Exception:
                pass
        await self._close_session(session)
        self._give_back(thread_id)

    async def _stop_turn(self, thread_id: str) -> None:
        row = self._row(thread_id)
        state = ThreadState(row['state'])
        session = self._sessions.get(thread_id)
        if state is ThreadState.RUNNING and session is not None:
            if self._capable(row, Capability.INTERRUPT):
                ended = asyncio.get_running_loop().create_future()
                self._turn_ends[thread_id] = ended
                try:
                    await session.interrupt()
                except Exception:
                    self._turn_ends.pop(thread_id, None)
                else:
                    self._stop_waits[thread_id] = asyncio.ensure_future(
                        self._wait_for_stop(thread_id, ended))
                    return
            await self._cut_turn(thread_id, session)
        elif state in (ThreadState.QUEUED, ThreadState.STARTING):
            self._move(thread_id, ThreadState.SLEEPING)
        await self._end_stop(thread_id)

    async def _wait_for_stop(self, thread_id: str, ended: asyncio.Future) -> None:
        try:
            await asyncio.wait_for(ended, self.stop_grace)
            late = False
        except TimeoutError:
            late = True
        finally:
            if self._turn_ends.get(thread_id) is ended:
                del self._turn_ends[thread_id]
        self.lanes.submit(thread_id, lambda: self._finish_stop(thread_id, late))
        if self._stop_waits.get(thread_id) is asyncio.current_task():
            del self._stop_waits[thread_id]

    async def _finish_stop(self, thread_id: str, late: bool) -> None:
        session = self._sessions.get(thread_id)
        if late and session is not None and self._row(thread_id)['state'] == ThreadState.RUNNING:
            await self._cut_turn(thread_id, session)
        await self._end_stop(thread_id)

    async def _cut_turn(self, thread_id: str, session: RunnerSession) -> None:
        self._sessions.pop(thread_id, None)
        await self._close_session(session)
        self._give_back(thread_id)
        with self.store.transaction() as tx:
            if fetch(tx, thread_id)['state'] in ENDED:
                return
            self.store.append_event(tx, EventType.TURN_RESULT, thread_id,
                                    {'state': TurnState.STOPPED, 'reason': 'stop'})
            transition(self.store, tx, thread_id, ThreadState.SLEEPING)

    async def _end_stop(self, thread_id: str) -> None:
        with self.store.transaction() as tx:
            threads.clear_stop(self.store, tx, thread_id)
            live = fetch(tx, thread_id)['state'] not in ENDED
            found = _held(tx, thread_id)
        if live and found is not None:
            await self._deliver(thread_id, *found)
        if live:
            self._hand_over(thread_id)

    async def _pump(self, thread_id: str, session: RunnerSession) -> None:
        try:
            async for event in session.events:
                if self._closing:
                    return
                await asyncio.wait([self.lanes.submit(
                    thread_id, lambda event=event: self._on_event(thread_id, session, event),
                )])
        except Exception:
            pass
        finally:
            self._pumps.pop(session, None)
        if not self._closing and self._sessions.get(thread_id) is session:
            self.lanes.submit(thread_id, lambda: self._on_event(thread_id, session, Exited(None)))

    async def _on_event(self, thread_id: str, session: RunnerSession,
                        event: RunnerEvent) -> None:
        current = self._sessions.get(thread_id) is session
        turn_over = False
        settled = False
        posted = None
        with self.store.transaction() as tx:
            row = fetch(tx, thread_id)
            state = ThreadState(row['state'])
            live = current and state not in ENDED
            self._log(tx, thread_id, event, state if live else None)
            if live and (isinstance(event, Exited) and state is ThreadState.RUNNING
                         and not row['stop_requested']):
                posted = self._crash(tx, row)
            elif live:
                posted = self._post_result(tx, row, event)
                settled = self._settle_on_result(tx, row, event)
                turn_over = not settled and self._apply(tx, row, event)
        if current and isinstance(event, (TurnResult, Exited)):
            ended = self._turn_ends.pop(thread_id, None)
            if ended is not None and not ended.done():
                ended.set_result(None)
        if posted is not None:
            parent_id, seq = posted
            self.lanes.submit(parent_id, lambda: self._deliver(parent_id, seq,
                                                               StartSource.AGENT))
        if not live:
            return
        if isinstance(event, Exited) or settled:
            self._sessions.pop(thread_id, None)
            await self._close_session(session)
            self._give_back(thread_id)
        elif turn_over and not row['stop_requested']:
            await self._dispatch_next(thread_id)
            self._hand_over(thread_id)

    def _post_result(self, tx: sqlite3.Connection, row: sqlite3.Row,
                     event: RunnerEvent) -> tuple[str, int] | None:
        if (not isinstance(event, TurnResult) or event.state not in delegation.POSTED
                or row['state'] != ThreadState.RUNNING):
            return None
        return self._post(tx, row, event)

    def _settle_on_result(self, tx: sqlite3.Connection, row: sqlite3.Row,
                          event: RunnerEvent) -> bool:
        if (not isinstance(event, TurnResult) or event.state not in SETTLING
                or row['settle'] != Settle.ON_RESULT or row['state'] != ThreadState.RUNNING
                or row['stop_requested']):
            return False
        thread_id = row['id']
        transition(self.store, tx, thread_id, ThreadState.SETTLED, result=event.text or '')
        tx.execute('DELETE FROM messages WHERE thread_id = ?', (thread_id,))
        tx.execute('DELETE FROM continuations WHERE thread_id = ?', (thread_id,))
        return True

    def _post(self, tx: sqlite3.Connection, row: sqlite3.Row,
              result: TurnResult) -> tuple[str, int] | None:
        if row['parent_id'] is None or row['stop_requested']:
            return None
        parent = fetch(tx, row['parent_id'])
        if parent['state'] in ENDED or parent['stop_requested']:
            return None
        seq = self._add_message(tx, parent['id'], f'{row["id"]}:result',
                                delegation.result_text(row, result), DispatchMode.QUEUE,
                                priority=1)
        return parent['id'], seq

    def _crash(self, tx: sqlite3.Connection, row: sqlite3.Row) -> tuple[str, int] | None:
        thread_id = row['id']
        count = threads.add_continuation(self.store, tx, thread_id)
        if count is None:
            transition(self.store, tx, thread_id, ThreadState.FAILED, threads.CRASHED)
            return self._post(tx, row, TurnResult(TurnState.FAILED, None, threads.CRASHED))
        seq = self._add_message(tx, thread_id, f'{thread_id}:continuation:{count}',
                                threads.CONTINUATION, DispatchMode.QUEUE, priority=2)
        transition(self.store, tx, thread_id, ThreadState.SLEEPING)
        return thread_id, seq

    def _log(self, tx: sqlite3.Connection, thread_id: str, event: RunnerEvent,
             state: ThreadState | None) -> None:
        append = self.store.append_event
        if isinstance(event, TurnStarted):
            append(tx, EventType.TURN_STARTED, thread_id, {})
        elif isinstance(event, TextOut):
            append(tx, EventType.TURN_TEXT, thread_id, {'text': event.text})
        elif isinstance(event, ToolUse):
            append(tx, EventType.TURN_TOOL, thread_id, {'tool_use_id': event.tool_use_id,
                                                        'name': event.name, 'input': event.input})
        elif isinstance(event, PermissionRequest):
            data = {'request_id': event.request_id, 'tool_name': event.tool_name,
                    'input': event.input}
            if event.description is not None:
                data['description'] = event.description
            append(tx, EventType.REQUEST_OPENED, thread_id, data)
        elif isinstance(event, TurnResult):
            data = {'state': event.state}
            if event.text is not None:
                data['text'] = event.text
            if event.reason is not None:
                data['reason'] = event.reason
            append(tx, EventType.TURN_RESULT, thread_id, data)
        elif isinstance(event, Exited) and state is ThreadState.RUNNING:
            append(tx, EventType.TURN_RESULT, thread_id,
                   {'state': TurnState.FAILED, 'reason': 'exited'})

    def _apply(self, tx: sqlite3.Connection, row: sqlite3.Row, event: RunnerEvent) -> bool:
        thread_id = row['id']
        state = ThreadState(row['state'])
        if isinstance(event, PermissionRequest) and state is ThreadState.RUNNING:
            transition(self.store, tx, thread_id, state, row['reason'], waiting=True)
        elif isinstance(event, TurnResult) and state is ThreadState.RUNNING:
            transition(self.store, tx, thread_id, ThreadState.IDLE)
            self._idle_since[thread_id] = self.store.clock()
            return True
        elif isinstance(event, Exited) and state in (ThreadState.RUNNING, ThreadState.IDLE):
            transition(self.store, tx, thread_id, ThreadState.SLEEPING)
        return False

    def list(self, project_id: str | None = None,
             audience: object = None) -> list[ThreadInfo]:
        if project_id is not None:
            _string(project_id, keys.PROJECT_KEY)
        if audience is not None:
            audience = _choice(audience, 'audience', Audience)
        return [thread_info(row)
                for row in threads.select(self.store.connection, project_id, audience)]
