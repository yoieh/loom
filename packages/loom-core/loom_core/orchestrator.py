import asyncio
import sqlite3
import uuid
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path

from loom_contracts.calls import CallName, ProjectInfo, ThreadInfo
from loom_contracts.events import EventType
from loom_contracts.runners import (
    Capability, DispatchMode, Exited, PermissionMode, PermissionRequest, Runner, RunnerEvent,
    RunnerHost, RunnerName, RunnerSession, TextOut, ThreadSpec, ToolUse, TurnResult, TurnStarted,
)
from loom_contracts.states import ThreadState, TurnState
from loom_contracts.workspaces import WorkspaceStrategy

from . import delegation, keys, threads
from .clock import iso
from .errors import LoomError
from .lanes import Lanes
from .projects import Projects
from .store import Store
from .threads import ENDED, fetch, thread_info, transition
from .workspaces import WorkspacePlan, make_workspace, plan_workspace

REQUESTS = ('caller', 'any')
DEFAULT_WORKSPACE = {'strategy': WorkspaceStrategy.WORKTREE.value}


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


def _reason(error: BaseException) -> str:
    if isinstance(error, LoomError):
        return error.message
    return str(error) or type(error).__name__


class Orchestrator:
    def __init__(self, store: Store, projects: Projects, runners: Mapping[str, Runner],
                 host: RunnerHost, *, stop_grace: float = 5.0) -> None:
        self.store = store
        self.projects = projects
        self.runners = runners
        self.host = host
        self.stop_grace = stop_grace
        self.lanes = Lanes()
        self._sessions: dict[str, RunnerSession] = {}
        self._pumps: dict[RunnerSession, asyncio.Task] = {}
        self._turn_ends: dict[str, asyncio.Future] = {}
        self._stop_waits: dict[str, asyncio.Task] = {}
        self._closing = False

    async def launch(self, params: object) -> ThreadInfo:
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
        command_id = params.get('id')

        def change(tx: sqlite3.Connection) -> ThreadInfo:
            project = self.projects.get(project_id)
            mode = self.projects.effective_mode(project_id)
            thread_id = str(uuid.uuid4())
            plan = _plan(tx, workspace, project, project_id, thread_id, self.store.data_dir)
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
            })
            self._add_message(tx, thread_id, f'{command_id}:message', message, DispatchMode.STEER)
            return thread_info(row)

        receipt = self.store.command(command_id, CallName.THREADS_LAUNCH, change)
        if not receipt.replayed:
            thread_id = receipt.answer['id']
            self.lanes.submit(thread_id, lambda: self._start(thread_id))
        return receipt.answer

    async def delegate(self, params: object) -> ThreadInfo:
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
        command_id = params.get('id')

        def change(tx: sqlite3.Connection) -> ThreadInfo:
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
            })
            self._add_message(tx, thread_id, f'{command_id}:message', task, DispatchMode.STEER)
            return thread_info(row)

        receipt = self.store.command(command_id, CallName.THREADS_DELEGATE, change)
        if not receipt.replayed:
            thread_id = receipt.answer['id']
            self.lanes.submit(thread_id, lambda: self._start(thread_id))
        return receipt.answer

    async def send(self, params: object) -> ThreadInfo:
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
            seq = self._add_message(tx, thread_id, command_id, message, mode)
            return thread_info(row)

        receipt = self.store.command(command_id, CallName.THREADS_SEND, change)
        if not receipt.replayed:
            self.lanes.submit(thread_id, lambda: self._deliver(thread_id, seq))
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
                transition(self.store, tx, thread_id, ThreadState.SETTLED)
                tx.execute('DELETE FROM messages WHERE thread_id = ?', (thread_id,))
            return thread_info(fetch(tx, thread_id))

        receipt = self.store.command(params.get('id'), CallName.THREADS_SETTLE, change)
        if not receipt.replayed and before is not None:
            self.lanes.submit(thread_id, lambda: self._end_session(thread_id, before))
        return receipt.answer

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
        waits = list(self._stop_waits.values())
        for wait in waits:
            wait.cancel()
        await asyncio.gather(*waits, return_exceptions=True)
        self._stop_waits.clear()
        await self.lanes.drain()
        sessions = list(self._sessions.values())
        self._sessions.clear()
        for session in sessions:
            await self._close_session(session)
        pumps = list(self._pumps.values())
        for pump in pumps:
            pump.cancel()
        await asyncio.gather(*pumps, return_exceptions=True)

    def _add_message(self, tx: sqlite3.Connection, thread_id: str, command_id: str,
                     text: str, mode: DispatchMode, priority: int = 0) -> int:
        cursor = tx.execute(
            'INSERT INTO messages (thread_id, command_id, text, mode, priority, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (thread_id, command_id, text, mode, priority, iso(self.store.clock())),
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

    async def _close_session(self, session: RunnerSession) -> None:
        try:
            await session.close()
        except Exception:
            pass

    async def _start(self, thread_id: str) -> None:
        row = self._row(thread_id)
        if row['state'] != ThreadState.QUEUED:
            return
        if row['stop_requested']:
            self._move(thread_id, ThreadState.SLEEPING)
            return
        await self._boot(thread_id, resume=False)

    async def _boot(self, thread_id: str, resume: bool) -> None:
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

    async def _deliver(self, thread_id: str, seq: int) -> None:
        row = self._row(thread_id)
        if row['stop_requested']:
            return
        state = ThreadState(row['state'])
        if state is ThreadState.SLEEPING:
            if row['has_session'] and not self._capable(row, Capability.RESUME):
                await self._fail(thread_id, 'runner cannot resume')
                return
            await self._boot(thread_id, resume=bool(row['has_session']))
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
            found = tx.execute(
                'SELECT seq FROM messages WHERE thread_id = ? '
                'ORDER BY priority DESC, seq LIMIT 1', (thread_id,),
            ).fetchone()
        if live and found is not None:
            await self._deliver(thread_id, found[0])

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
        posted = None
        with self.store.transaction() as tx:
            row = fetch(tx, thread_id)
            state = ThreadState(row['state'])
            live = current and state not in ENDED
            self._log(tx, thread_id, event, state if live else None)
            if live:
                posted = self._post_result(tx, row, event)
                turn_over = self._apply(tx, row, event)
        if current and isinstance(event, (TurnResult, Exited)):
            ended = self._turn_ends.pop(thread_id, None)
            if ended is not None and not ended.done():
                ended.set_result(None)
        if posted is not None:
            parent_id, seq = posted
            self.lanes.submit(parent_id, lambda: self._deliver(parent_id, seq))
        if not live:
            return
        if isinstance(event, Exited):
            self._sessions.pop(thread_id, None)
            await self._close_session(session)
        elif turn_over and not row['stop_requested']:
            await self._dispatch_next(thread_id)

    def _post_result(self, tx: sqlite3.Connection, row: sqlite3.Row,
                     event: RunnerEvent) -> tuple[str, int] | None:
        if (not isinstance(event, TurnResult) or event.state not in delegation.POSTED
                or row['state'] != ThreadState.RUNNING or row['parent_id'] is None
                or row['stop_requested']):
            return None
        parent = fetch(tx, row['parent_id'])
        if parent['state'] in ENDED or parent['stop_requested']:
            return None
        seq = self._add_message(tx, parent['id'], f'{row["id"]}:result',
                                delegation.result_text(row, event), DispatchMode.QUEUE,
                                priority=1)
        return parent['id'], seq

    def _log(self, tx: sqlite3.Connection, thread_id: str, event: RunnerEvent,
             state: ThreadState | None) -> None:
        append = self.store.append_event
        if isinstance(event, TurnStarted):
            append(tx, EventType.TURN_STARTED, thread_id, {})
        elif isinstance(event, TextOut):
            append(tx, EventType.TURN_TEXT, thread_id, {'text': event.text})
        elif isinstance(event, ToolUse):
            append(tx, EventType.TURN_TOOL, thread_id, {'name': event.name, 'input': event.input})
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
            return True
        elif isinstance(event, Exited) and state in (ThreadState.RUNNING, ThreadState.IDLE):
            transition(self.store, tx, thread_id, ThreadState.SLEEPING)
        return False

    def list(self, project_id: str | None = None) -> list[ThreadInfo]:
        if project_id is not None:
            _string(project_id, keys.PROJECT_KEY)
        return [thread_info(row) for row in threads.select(self.store.connection, project_id)]
