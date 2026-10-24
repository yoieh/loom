import os
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

from loom_contracts.events import EventType
from loom_contracts.runners import (
    DispatchMode, Exited, PermissionRequest, TextOut, ThreadSpec, ToolUse, TurnResult,
    TurnStarted,
)
from loom_contracts.states import ThreadState, TurnState

from loom_core import threads
from loom_core.errors import LoomError
from loom_core.orchestrator import Orchestrator
from loom_core.projects import Projects
from loom_core.store import DB_NAME, Store

from .fakes import FakeHost, FakeRunner, FakeSession, wait_for
from .test_workspaces import git, make_repo

QUEUE = DispatchMode.QUEUE
STEER = DispatchMode.STEER
ROOT = {'strategy': 'root'}
START = datetime(2026, 10, 24, 15, 8, 27, tzinfo=UTC)
BARE = {'project', 'parent'}


class SteppingClock:
    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> datetime:
        self.now += timedelta(milliseconds=1)
        return self.now


def done(state: TurnState = TurnState.COMPLETED, text: str | None = 'ok',
         reason: str | None = None) -> TurnResult:
    return TurnResult(state, text, reason)


class OrchestratorTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name).resolve()
        home = self.base / 'home'
        home.mkdir()
        patcher = mock.patch.dict(os.environ, {'HOME': str(home)})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.root = make_repo(self.base / 'repo')
        self.data_dir = self.base / 'data'
        self.store = Store.open(self.data_dir, SteppingClock())
        self.projects = Projects(self.store)
        self.projects.add({'id': 'p1', 'title': 'Loom', 'path': str(self.root)})
        self.runner = FakeRunner()
        self.orchestrator = self.make(self.runner)
        self.calls = 0

    async def asyncTearDown(self) -> None:
        await self.orchestrator.close()
        self.store.close()
        self.tmp.cleanup()

    def make(self, *runners: FakeRunner, projects: Projects | None = None) -> Orchestrator:
        return Orchestrator(self.store, projects or self.projects,
                            {runner.name: runner for runner in runners},
                            FakeHost(str(self.data_dir)))

    async def use(self, runner: FakeRunner) -> None:
        await self.orchestrator.close()
        self.runner = runner
        self.orchestrator = self.make(runner)

    def next_id(self) -> str:
        self.calls += 1
        return f'cmd-{self.calls}'

    def launch_params(self, **extra: object) -> dict:
        return {'id': self.next_id(), 'title': 'Work', 'message': 'Hello',
                'project_id': 'loom', 'workspace': ROOT, **extra}

    async def launch(self, **extra: object) -> dict:
        return await self.orchestrator.launch(self.launch_params(**extra))

    async def running(self, **extra: object) -> tuple[str, FakeSession]:
        info = await self.launch(**extra)
        await self.orchestrator.drain()
        self.assertEqual(self.state(info['id']), 'running')
        return info['id'], self.runner.sessions[info['id']]

    async def idle(self, **extra: object) -> tuple[str, FakeSession]:
        thread_id, session = await self.running(**extra)
        await self.finish(thread_id, session)
        self.assertEqual(self.state(thread_id), 'idle')
        return thread_id, session

    async def send(self, thread_id: str, message: str = 'more', **extra: object) -> dict:
        return await self.orchestrator.send(
            {'id': self.next_id(), 'thread_id': thread_id, 'message': message, **extra})

    async def finish(self, thread_id: str, session: FakeSession,
                     result: TurnResult | None = None) -> None:
        count = len(self.events(thread_id, EventType.TURN_RESULT))
        session.emit(result or done())
        await wait_for(lambda: len(self.events(thread_id, EventType.TURN_RESULT)) > count)
        await self.orchestrator.drain()

    def state(self, thread_id: str) -> str:
        return self.orchestrator.get(thread_id)['state']

    def events(self, thread_id: str | None = None, type: EventType | None = None) -> list[dict]:
        found = self.store.events(thread_id=thread_id)
        return [event for event in found if type is None or event['type'] == type]

    def states(self, thread_id: str) -> list[str]:
        return [event['data']['state'] for event in self.events(thread_id, EventType.THREAD_STATE)]

    def pending(self, thread_id: str) -> list[tuple[str, str]]:
        return self.store.connection.execute(
            'SELECT text, mode FROM messages WHERE thread_id = ? ORDER BY seq', (thread_id,),
        ).fetchall()

    def counts(self) -> tuple[int, ...]:
        return tuple(
            self.store.connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
            for table in ('threads', 'messages', 'commands', 'events')
        )

    async def assert_error(self, code: str, call, params: object,
                           field: str | None = None) -> None:
        before = self.counts()
        with self.assertRaises(LoomError) as caught:
            await call(params)
        self.assertEqual(caught.exception.code, code)
        if field:
            self.assertIn(field, caught.exception.message)
        await self.orchestrator.drain()
        self.assertEqual(self.counts(), before)


class LaunchTest(OrchestratorTest):
    async def test_launch_in_a_git_project_makes_a_worktree_and_runs(self) -> None:
        params = self.launch_params()
        del params['workspace']
        info = await self.orchestrator.launch(params)
        thread_id = info['id']
        await self.orchestrator.drain()
        path = self.data_dir.resolve() / 'worktrees' / 'loom' / thread_id
        self.assertEqual(info['workspace_path'], str(path))
        self.assertEqual(info['branch'], f'loom/{thread_id}')
        self.assertEqual(git(path, 'symbolic-ref', '--short', 'HEAD'), f'loom/{thread_id}')
        self.assertEqual(len(self.runner.starts), 1)
        spec = self.runner.starts[0]
        self.assertEqual((spec.id, spec.workspace_path, spec.permission_mode, spec.resume),
                         (thread_id, str(path), 'auto', False))
        self.assertEqual(spec.project_id, 'loom')
        self.assertEqual(self.runner.sessions[thread_id].sent, [('Hello', QUEUE)])
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual(self.states(thread_id), ['queued', 'starting', 'running'])
        row = threads.fetch(self.store.connection, thread_id)
        self.assertEqual((row['base'], row['has_session'], row['workspace_strategy']),
                         ('main', 1, 'worktree'))
        self.assertEqual(self.pending(thread_id), [])

    async def test_launch_is_recorded_in_one_transaction_before_the_start(self) -> None:
        seen_by_other: list[tuple] = []
        db = self.data_dir / DB_NAME

        class CheckingRunner(FakeRunner):
            async def start(self, thread: ThreadSpec, host: object) -> FakeSession:
                other = sqlite3.connect(db)
                try:
                    seen_by_other.append((
                        other.execute('SELECT id FROM threads').fetchall(),
                        other.execute('SELECT id, call FROM commands WHERE id = ?',
                                      ('c1',)).fetchall(),
                    ))
                finally:
                    other.close()
                return await super().start(thread, host)

        await self.use(CheckingRunner())
        statements: list[str] = []
        self.store.connection.set_trace_callback(statements.append)
        info = await self.orchestrator.launch(self.launch_params(id='c1'))
        self.store.connection.set_trace_callback(None)
        self.assertEqual([s for s in statements if s.strip().upper() == 'BEGIN IMMEDIATE'],
                         ['BEGIN IMMEDIATE'])
        self.assertEqual([s for s in statements if s.strip().upper() == 'COMMIT'], ['COMMIT'])
        self.assertEqual(info['state'], 'queued')
        self.assertEqual(self.states(info['id']), ['queued'])
        self.assertEqual(
            self.store.connection.execute(
                'SELECT thread_id, command_id, text, mode, priority FROM messages').fetchall(),
            [(info['id'], 'c1:message', 'Hello', 'steer', 0)],
        )
        self.assertEqual(self.store.connection.execute(
            "SELECT id, call FROM commands WHERE call = 'threads.launch'").fetchall(),
            [('c1', 'threads.launch')])
        self.assertEqual(self.runner.starts, [])
        await self.orchestrator.drain()
        self.assertEqual(seen_by_other, [([(info['id'],)], [('c1', 'threads.launch')])])

    async def test_retry_returns_the_first_answer_and_starts_once(self) -> None:
        params = self.launch_params()
        first = await self.orchestrator.launch(dict(params))
        await self.orchestrator.drain()
        with mock.patch.object(self.orchestrator.lanes, 'submit') as submit:
            again = await self.orchestrator.launch(dict(params))
        submit.assert_not_called()
        await self.orchestrator.drain()
        self.assertEqual(again, first)
        self.assertEqual(len(self.orchestrator.list()), 1)
        self.assertEqual(len(self.runner.starts), 1)
        self.assertEqual(self.runner.sessions[first['id']].sent, [('Hello', QUEUE)])

    async def test_permission_mode_comes_from_the_project(self) -> None:
        await self.orchestrator.close()
        self.orchestrator = self.make(self.runner,
                                      projects=Projects(self.store, default_mode='full-access'))
        info = await self.launch()
        await self.orchestrator.drain()
        self.assertEqual(info['permission_mode'], 'auto')
        self.assertEqual(self.orchestrator.get(info['id'])['permission_mode'], 'auto')
        self.assertEqual(self.runner.starts[-1].permission_mode, 'auto')
        self.projects.set_mode({'id': 'm1', 'project_id': 'loom',
                                'permission_mode': 'approval-required'})
        info = await self.launch()
        await self.orchestrator.drain()
        self.assertEqual(info['permission_mode'], 'approval-required')
        self.assertEqual(self.runner.starts[-1].permission_mode, 'approval-required')

    async def test_defaults_for_missing_optional_fields(self) -> None:
        params = {'id': 'c1', 'title': 'Work', 'message': 'Hello', 'project_id': 'loom'}
        info = await self.orchestrator.launch(params)
        await self.orchestrator.drain()
        row = threads.fetch(self.store.connection, info['id'])
        self.assertEqual((row['workspace_strategy'], row['runner'], row['requests'],
                          row['model'], row['origin']),
                         ('worktree', 'claude', 'any', None, None))
        self.assertEqual(self.runner.starts[0].model, None)

    async def test_given_fields_are_stored(self) -> None:
        info = await self.launch(model='opus', requests='caller', origin='thread-voice')
        await self.orchestrator.drain()
        row = threads.fetch(self.store.connection, info['id'])
        self.assertEqual((row['model'], row['requests'], row['origin']),
                         ('opus', 'caller', 'thread-voice'))
        spec = self.runner.starts[0]
        self.assertEqual((spec.model, spec.origin, spec.title, spec.parent_id),
                         ('opus', 'thread-voice', 'Work', None))
        self.assertEqual(info['branch'], None)
        self.assertEqual(self.orchestrator.get(info['id'])['branch'], 'main')

    async def test_no_info_row_or_event_has_a_bare_key(self) -> None:
        thread_id, session = await self.running()
        session.emit(ToolUse('t1', 'Read', {'path': 'x'}))
        await self.finish(thread_id, session)
        info = self.orchestrator.get(thread_id)
        self.assertEqual(BARE & set(info), set())
        self.assertEqual((info['project_id'], info['parent_id']), ('loom', None))
        row = threads.fetch(self.store.connection, thread_id)
        self.assertEqual(BARE & set(row.keys()), set())
        for event in self.events():
            self.assertEqual(BARE & set(event['data']), set())


class LaunchParamsTest(OrchestratorTest):
    async def test_null_fields_are_invalid(self) -> None:
        for field in ('title', 'message', 'project_id', 'workspace', 'runner', 'model',
                      'requests', 'origin'):
            with self.subTest(field=field):
                await self.assert_error('invalid', self.orchestrator.launch,
                                        self.launch_params(**{field: None}), field)
        self.assertEqual(self.runner.starts, [])

    async def test_missing_fields_are_invalid(self) -> None:
        for field in ('title', 'message', 'project_id'):
            params = self.launch_params()
            del params[field]
            with self.subTest(field=field):
                await self.assert_error('invalid', self.orchestrator.launch, params, field)
        params = self.launch_params(project='loom')
        del params['project_id']
        await self.assert_error('invalid', self.orchestrator.launch, params, 'project_id')
        params = self.launch_params()
        del params['id']
        await self.assert_error('invalid', self.orchestrator.launch, params, 'id')

    async def test_wrong_types_are_invalid(self) -> None:
        cases = {'title': [5, ''], 'message': ['', 5], 'project_id': [5],
                 'workspace': ['root', ['root']], 'runner': ['gpt', 5],
                 'model': [5, ''], 'requests': ['all', 5], 'origin': [5]}
        for field, values in cases.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    await self.assert_error('invalid', self.orchestrator.launch,
                                            self.launch_params(**{field: value}), field)
        await self.assert_error('invalid', self.orchestrator.launch, ['Work'], 'params')
        await self.assert_error('invalid', self.orchestrator.launch,
                                self.launch_params(workspace={'strategy': 'clone'}), 'strategy')
        self.assertEqual(self.runner.starts, [])

    async def test_unknown_project_is_not_found(self) -> None:
        await self.assert_error('not_found', self.orchestrator.launch,
                                self.launch_params(project_id='nope'))

    async def test_runner_that_is_not_registered_is_invalid(self) -> None:
        await self.assert_error('invalid', self.orchestrator.launch,
                                self.launch_params(runner='codex'), 'runner')

    async def test_another_registered_runner_is_used(self) -> None:
        codex = FakeRunner('codex')
        await self.orchestrator.close()
        self.orchestrator = self.make(self.runner, codex)
        info = await self.launch(runner='codex')
        await self.orchestrator.drain()
        self.assertEqual(info['runner'], 'codex')
        self.assertEqual((len(codex.starts), len(self.runner.starts)), (1, 0))


class EventsTest(OrchestratorTest):
    async def test_runner_events_become_events_and_states(self) -> None:
        thread_id, session = await self.running()
        session.emit(TurnStarted())
        session.emit(TextOut('Working on it'))
        session.emit(ToolUse('t1', 'Bash', {'command': 'ls'}))
        session.emit(PermissionRequest('r1', 'Bash', {'command': 'rm x'}, 'Remove x'))
        await wait_for(lambda: self.events(thread_id, EventType.REQUEST_OPENED))
        await self.orchestrator.drain()
        self.assertIs(self.orchestrator.get(thread_id)['waiting'], True)
        self.assertEqual(self.state(thread_id), 'running')
        await self.finish(thread_id, session, done(text='All done'))
        info = self.orchestrator.get(thread_id)
        self.assertEqual((info['state'], info['waiting']), ('idle', False))
        logged = [(event['type'], event['data']) for event in self.events(thread_id)
                  if event['type'] != EventType.THREAD_STATE]
        self.assertEqual(logged, [
            (EventType.TURN_STARTED, {}),
            (EventType.TURN_TEXT, {'text': 'Working on it'}),
            (EventType.TURN_TOOL, {'name': 'Bash', 'input': {'command': 'ls'}}),
            (EventType.REQUEST_OPENED, {'request_id': 'r1', 'tool_name': 'Bash',
                                        'input': {'command': 'rm x'},
                                        'description': 'Remove x'}),
            (EventType.TURN_RESULT, {'state': 'completed', 'text': 'All done'}),
        ])
        self.assertEqual(self.states(thread_id),
                         ['queued', 'starting', 'running', 'running', 'idle'])

    async def test_result_shapes(self) -> None:
        thread_id, session = await self.running()
        session.emit(PermissionRequest('r1', 'Edit', {}, None))
        await self.finish(thread_id, session, done(TurnState.FAILED, None, 'boom'))
        opened = self.events(thread_id, EventType.REQUEST_OPENED)[0]['data']
        self.assertEqual(opened, {'request_id': 'r1', 'tool_name': 'Edit', 'input': {}})
        result = self.events(thread_id, EventType.TURN_RESULT)[0]['data']
        self.assertEqual(result, {'state': 'failed', 'reason': 'boom'})
        self.assertEqual(self.state(thread_id), 'idle')

    async def test_exited_during_a_turn_fails_the_turn_and_sleeps(self) -> None:
        thread_id, session = await self.running()
        session.emit(Exited(1))
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        self.assertEqual(self.events(thread_id, EventType.TURN_RESULT)[-1]['data'],
                         {'state': 'failed', 'reason': 'exited'})
        self.assertTrue(session.closed)

    async def test_exited_while_idle_sleeps(self) -> None:
        thread_id, session = await self.idle()
        session.emit(Exited(0))
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        self.assertEqual(len(self.events(thread_id, EventType.TURN_RESULT)), 1)

    async def test_a_stream_that_ends_without_exited_sleeps(self) -> None:
        thread_id, session = await self.running()
        await session.close()
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        self.assertEqual(self.events(thread_id, EventType.TURN_RESULT)[-1]['data'],
                         {'state': 'failed', 'reason': 'exited'})


class SendTest(OrchestratorTest):
    async def test_send_without_a_mode_steers_a_running_turn(self) -> None:
        thread_id, session = await self.running()
        info = await self.send(thread_id, 'Also this')
        self.assertEqual(info['state'], 'running')
        await self.orchestrator.drain()
        self.assertEqual(session.sent, [('Hello', QUEUE), ('Also this', STEER)])
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual(self.pending(thread_id), [])

    async def test_steer_without_the_capability_waits_for_the_result(self) -> None:
        await self.use(FakeRunner(capabilities=frozenset({'interrupt', 'resume'})))
        thread_id, session = await self.running()
        await self.send(thread_id, 'Also this', mode='steer')
        await self.orchestrator.drain()
        self.assertEqual(session.sent, [('Hello', QUEUE)])
        self.assertEqual(self.pending(thread_id), [('Also this', 'steer')])
        await self.finish(thread_id, session)
        self.assertEqual(session.sent, [('Hello', QUEUE), ('Also this', QUEUE)])
        self.assertEqual(self.state(thread_id), 'running')

    async def test_queued_messages_go_one_per_turn_in_order(self) -> None:
        thread_id, session = await self.running()
        for text in ('one', 'two', 'three'):
            await self.send(thread_id, text, mode='queue')
        await self.orchestrator.drain()
        self.assertEqual(session.sent, [('Hello', QUEUE)])
        for count, text in enumerate(('one', 'two', 'three'), start=2):
            await self.finish(thread_id, session)
            self.assertEqual(session.sent[-1], (text, QUEUE))
            self.assertEqual(len(session.sent), count)
            self.assertEqual(self.state(thread_id), 'running')
        await self.finish(thread_id, session)
        self.assertEqual(self.state(thread_id), 'idle')
        self.assertEqual(len(session.sent), 4)

    async def test_interrupt_stops_the_turn_then_sends(self) -> None:
        thread_id, session = await self.running()
        await self.send(thread_id, 'Stop and do this', mode='interrupt')
        await wait_for(lambda: len(session.sent) == 2)
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 1)
        self.assertEqual(session.sent, [('Hello', QUEUE), ('Stop and do this', QUEUE)])
        self.assertEqual(self.events(thread_id, EventType.TURN_RESULT)[0]['data'],
                         {'state': 'stopped', 'reason': 'interrupted'})
        self.assertEqual(self.state(thread_id), 'running')

    async def test_interrupt_without_the_capability_waits_like_queue(self) -> None:
        await self.use(FakeRunner(capabilities=frozenset({'steer', 'resume'})))
        thread_id, session = await self.running()
        await self.send(thread_id, 'Later', mode='interrupt')
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 0)
        self.assertEqual(session.sent, [('Hello', QUEUE)])
        await self.finish(thread_id, session)
        self.assertEqual(session.sent[-1], ('Later', QUEUE))

    async def test_any_mode_to_an_idle_thread_is_a_new_turn(self) -> None:
        thread_id, session = await self.idle()
        for mode in ('steer', 'queue', 'interrupt'):
            with self.subTest(mode=mode):
                await self.send(thread_id, f'as {mode}', mode=mode)
                await self.orchestrator.drain()
                self.assertEqual(session.sent[-1], (f'as {mode}', QUEUE))
                self.assertEqual(self.state(thread_id), 'running')
                self.assertEqual(session.interrupts, 0)
                await self.finish(thread_id, session)

    async def test_send_to_a_sleeping_thread_resumes_it(self) -> None:
        thread_id, session = await self.idle()
        session.emit(Exited(0))
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        await self.send(thread_id, 'Wake up')
        await self.orchestrator.drain()
        self.assertEqual([spec.resume for spec in self.runner.starts], [False, True])
        woken = self.runner.sessions[thread_id]
        self.assertIsNot(woken, session)
        self.assertEqual(woken.sent, [('Wake up', QUEUE)])
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual(self.states(thread_id)[-3:], ['sleeping', 'starting', 'running'])

    async def wake_after_set_mode(self, mode: str) -> str:
        thread_id, session = await self.idle()
        self.projects.set_mode({'id': self.next_id(), 'project_id': 'loom',
                                'permission_mode': mode})
        session.emit(Exited(0))
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        await self.send(thread_id, 'Wake up')
        await self.orchestrator.drain()
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual([spec.resume for spec in self.runner.starts], [False, True])
        return thread_id

    async def test_a_wake_takes_a_narrowed_project_mode(self) -> None:
        thread_id = await self.wake_after_set_mode('approval-required')
        self.assertEqual([spec.permission_mode for spec in self.runner.starts],
                         ['auto', 'approval-required'])
        self.assertEqual(self.orchestrator.get(thread_id)['permission_mode'],
                         'approval-required')

    async def test_a_wake_never_takes_a_widened_project_mode(self) -> None:
        thread_id = await self.wake_after_set_mode('full-access')
        self.assertEqual([spec.permission_mode for spec in self.runner.starts],
                         ['auto', 'auto'])
        self.assertEqual(self.orchestrator.get(thread_id)['permission_mode'], 'auto')

    async def test_send_to_a_sleeping_thread_without_resume_fails_it(self) -> None:
        await self.use(FakeRunner(capabilities=frozenset({'steer', 'interrupt'})))
        thread_id, session = await self.idle()
        session.emit(Exited(0))
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        await self.send(thread_id, 'Wake up')
        await self.orchestrator.drain()
        info = self.orchestrator.get(thread_id)
        self.assertEqual((info['state'], info['reason']), ('failed', 'runner cannot resume'))
        self.assertEqual(len(self.runner.starts), 1)
        self.assertEqual(self.events(thread_id, EventType.THREAD_FAILED)[-1]['data'],
                         {'reason': 'runner cannot resume'})

    async def test_send_to_a_sleeping_thread_that_never_started_starts_it(self) -> None:
        info = await self.launch()
        with self.store.transaction() as tx:
            threads.transition(self.store, tx, info['id'], ThreadState.SLEEPING)
        await self.orchestrator.drain()
        self.assertEqual(self.runner.starts, [])
        await self.send(info['id'], 'Now')
        await self.orchestrator.drain()
        self.assertEqual([spec.resume for spec in self.runner.starts], [False])
        session = self.runner.sessions[info['id']]
        self.assertEqual(session.sent, [('Hello', QUEUE)])
        await self.finish(info['id'], session)
        self.assertEqual(session.sent[-1], ('Now', QUEUE))

    async def test_send_while_starting_waits_for_the_start(self) -> None:
        info = await self.launch()
        await self.send(info['id'], 'Second', mode='queue')
        await self.orchestrator.drain()
        session = self.runner.sessions[info['id']]
        self.assertEqual(session.sent, [('Hello', QUEUE)])
        await self.finish(info['id'], session)
        self.assertEqual(session.sent[-1], ('Second', QUEUE))

    async def test_retry_returns_the_first_answer_and_sends_once(self) -> None:
        thread_id, session = await self.running()
        params = {'id': 's1', 'thread_id': thread_id, 'message': 'Once'}
        first = await self.orchestrator.send(dict(params))
        await self.orchestrator.drain()
        await self.finish(thread_id, session)
        with mock.patch.object(self.orchestrator.lanes, 'submit') as submit:
            again = await self.orchestrator.send(dict(params))
        submit.assert_not_called()
        await self.orchestrator.drain()
        self.assertEqual(again, first)
        self.assertEqual(session.sent, [('Hello', QUEUE), ('Once', STEER)])

    async def test_send_to_a_settled_or_failed_thread_is_conflict(self) -> None:
        settled, _ = await self.running()
        await self.orchestrator.settle({'id': 'x1', 'thread_id': settled})
        await wait_for(lambda: self.events(settled, EventType.TURN_RESULT))
        await self.orchestrator.drain()
        await self.assert_error('conflict', self.orchestrator.send,
                                {'id': 'x2', 'thread_id': settled, 'message': 'Hi'})
        await self.use(FakeRunner(fail_start=RuntimeError('no binary')))
        failed = (await self.launch())['id']
        await self.orchestrator.drain()
        self.assertEqual(self.state(failed), 'failed')
        await self.assert_error('conflict', self.orchestrator.send,
                                {'id': 'x3', 'thread_id': failed, 'message': 'Hi'})


class SendParamsTest(OrchestratorTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.thread_id, _ = await self.running()

    def params(self, **extra: object) -> dict:
        return {'id': self.next_id(), 'thread_id': self.thread_id, 'message': 'Hi', **extra}

    async def test_null_fields_are_invalid(self) -> None:
        for field in ('thread_id', 'message', 'mode'):
            with self.subTest(field=field):
                await self.assert_error('invalid', self.orchestrator.send,
                                        self.params(**{field: None}), field)

    async def test_missing_fields_are_invalid(self) -> None:
        for field in ('thread_id', 'message', 'id'):
            params = self.params()
            del params[field]
            with self.subTest(field=field):
                await self.assert_error('invalid', self.orchestrator.send, params, field)

    async def test_wrong_types_are_invalid(self) -> None:
        for field, value in (('thread_id', 5), ('message', 5), ('message', ''),
                             ('mode', 'shout'), ('mode', 5)):
            with self.subTest(field=field, value=value):
                await self.assert_error('invalid', self.orchestrator.send,
                                        self.params(**{field: value}), field)
        await self.assert_error('invalid', self.orchestrator.send, 'hi', 'params')

    async def test_unknown_thread_is_not_found(self) -> None:
        await self.assert_error('not_found', self.orchestrator.send,
                                self.params(thread_id='nope'))


class FailureTest(OrchestratorTest):
    async def test_start_raising_fails_the_thread(self) -> None:
        await self.use(FakeRunner(fail_start=RuntimeError('claude is not installed')))
        info = await self.launch()
        await self.orchestrator.drain()
        got = self.orchestrator.get(info['id'])
        self.assertEqual((got['state'], got['reason']), ('failed', 'claude is not installed'))
        self.assertEqual(self.events(info['id'], EventType.THREAD_FAILED)[0]['data'],
                         {'reason': 'claude is not installed'})
        self.assertEqual(self.states(info['id']), ['queued', 'starting', 'failed'])

    async def test_send_raising_fails_the_thread_and_closes_the_session(self) -> None:
        thread_id, session = await self.running()
        session.fail_send = OSError('broken pipe')
        await self.send(thread_id, 'Steer')
        await self.orchestrator.drain()
        got = self.orchestrator.get(thread_id)
        self.assertEqual((got['state'], got['reason']), ('failed', 'broken pipe'))
        self.assertEqual(self.events(thread_id, EventType.THREAD_FAILED)[0]['data'],
                         {'reason': 'broken pipe'})
        self.assertTrue(session.closed)

    async def test_send_of_a_new_turn_raising_fails_the_thread(self) -> None:
        thread_id, session = await self.idle()
        session.fail_send = OSError('broken pipe')
        await self.send(thread_id, 'Next')
        await self.orchestrator.drain()
        self.assertEqual(self.orchestrator.get(thread_id)['reason'], 'broken pipe')
        self.assertEqual(self.state(thread_id), 'failed')
        self.assertTrue(session.closed)

    async def test_interrupt_raising_fails_the_thread(self) -> None:
        thread_id, session = await self.running()
        session.fail_interrupt = OSError('gone')
        await self.send(thread_id, 'Stop', mode='interrupt')
        await self.orchestrator.drain()
        self.assertEqual((self.state(thread_id), self.orchestrator.get(thread_id)['reason']),
                         ('failed', 'gone'))
        self.assertTrue(session.closed)

    async def test_workspace_failing_fails_the_thread(self) -> None:
        plain = self.base / 'plain'
        plain.mkdir()
        self.projects.add({'id': 'p2', 'title': 'Plain', 'path': str(plain)})
        info = await self.launch(project_id='plain', workspace={'strategy': 'worktree'})
        await self.orchestrator.drain()
        got = self.orchestrator.get(info['id'])
        self.assertEqual(got['state'], 'failed')
        self.assertEqual(got['reason'], "the project's root is not the top of a git repo")
        self.assertEqual(self.events(info['id'], EventType.THREAD_FAILED)[0]['data'],
                         {'reason': got['reason']})
        self.assertEqual(self.runner.starts, [])


class SettleTest(OrchestratorTest):
    async def test_settle_is_recorded_at_once_and_ends_the_session(self) -> None:
        thread_id, session = await self.running()
        await self.send(thread_id, 'Later', mode='queue')
        info = await self.orchestrator.settle({'id': 'z1', 'thread_id': thread_id})
        self.assertEqual(info['state'], 'settled')
        self.assertEqual(self.state(thread_id), 'settled')
        self.assertEqual(self.pending(thread_id), [])
        self.assertEqual(session.interrupts, 0)
        session.emit(TextOut('late'))
        session.emit(done(text='late result'))
        session.emit(Exited(0))
        await self.orchestrator.drain()
        await wait_for(lambda: len(self.events(thread_id, EventType.TURN_RESULT)) == 2)
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 1)
        self.assertTrue(session.closed)
        self.assertEqual(session.sent, [('Hello', QUEUE)])
        self.assertEqual(self.state(thread_id), 'settled')
        self.assertEqual(self.states(thread_id), ['queued', 'starting', 'running', 'settled'])

    async def test_settle_of_an_idle_thread_closes_without_interrupt(self) -> None:
        thread_id, session = await self.idle()
        await self.orchestrator.settle({'id': 'z1', 'thread_id': thread_id})
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 0)
        self.assertTrue(session.closed)

    async def test_settle_without_interrupt_capability_only_closes(self) -> None:
        await self.use(FakeRunner(capabilities=frozenset({'steer'})))
        thread_id, session = await self.running()
        await self.orchestrator.settle({'id': 'z1', 'thread_id': thread_id})
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 0)
        self.assertTrue(session.closed)

    async def test_settle_before_the_start_means_no_start(self) -> None:
        info = await self.launch()
        await self.orchestrator.settle({'id': 'z1', 'thread_id': info['id']})
        await self.orchestrator.drain()
        self.assertEqual(self.runner.starts, [])
        self.assertEqual(self.state(info['id']), 'settled')

    async def test_settle_twice_returns_the_info(self) -> None:
        thread_id, session = await self.running()
        first = await self.orchestrator.settle({'id': 'z1', 'thread_id': thread_id})
        self.assertEqual(await self.orchestrator.settle({'id': 'z1', 'thread_id': thread_id}),
                         first)
        second = await self.orchestrator.settle({'id': 'z2', 'thread_id': thread_id})
        self.assertEqual(second, first)
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 1)
        self.assertEqual(self.states(thread_id).count('settled'), 1)

    async def test_bad_params(self) -> None:
        thread_id, _ = await self.running()
        for value in (None, 5, 'missing'):
            params = {'id': self.next_id(), 'thread_id': value}
            if value == 'missing':
                del params['thread_id']
            with self.subTest(value=value):
                await self.assert_error('invalid', self.orchestrator.settle, params, 'thread_id')
        await self.assert_error('not_found', self.orchestrator.settle,
                                {'id': self.next_id(), 'thread_id': 'nope'})
        await self.assert_error('invalid', self.orchestrator.settle, None, 'params')
        self.assertEqual(self.state(thread_id), 'running')


class ReadTest(OrchestratorTest):
    async def test_get_of_an_unknown_thread_is_not_found(self) -> None:
        with self.assertRaises(LoomError) as caught:
            self.orchestrator.get('nope')
        self.assertEqual(caught.exception.code, 'not_found')
        with self.assertRaises(LoomError) as caught:
            self.orchestrator.get(5)
        self.assertEqual(caught.exception.code, 'invalid')

    async def test_list_is_in_creation_order_and_filters(self) -> None:
        other = make_repo(self.base / 'other')
        self.projects.add({'id': 'p2', 'title': 'Other', 'path': str(other)})
        first = await self.launch()
        second = await self.launch(project_id='other')
        third = await self.launch()
        await self.orchestrator.drain()
        listed = self.orchestrator.list()
        self.assertEqual([info['id'] for info in listed],
                         [first['id'], second['id'], third['id']])
        self.assertEqual(listed[0], self.orchestrator.get(first['id']))
        self.assertIsInstance(listed[0], dict)
        self.assertEqual([info['id'] for info in self.orchestrator.list(project_id='other')],
                         [second['id']])
        self.assertEqual(self.orchestrator.list('nope'), [])
        with self.assertRaises(LoomError) as caught:
            self.orchestrator.list(5)
        self.assertEqual(caught.exception.code, 'invalid')


class CloseTest(OrchestratorTest):
    async def test_close_ends_sessions_and_keeps_states(self) -> None:
        thread_id, session = await self.running()
        await self.orchestrator.close()
        self.assertTrue(session.closed)
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual(self.orchestrator._pumps, {})


if __name__ == '__main__':
    unittest.main()
