import asyncio
import json
import os
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

import loom_runner_t3
from loom_contracts.runners import (
    Decision,
    DispatchMode,
    Exited,
    MessageEntered,
    PermissionMode,
    RequestAnswer,
    Runner,
    RunnerSession,
    TextOut,
    ToolUse,
    TurnResult,
    TurnStarted,
)
from loom_contracts.states import TurnState
from loom_runner_t3 import T3Runner
from loom_runner_t3.credential import FIELDS
from loom_runner_t3.reasons import ANSWERED_IN_T3, NOT_PAIRED, NOT_RUNNING, T3Error
from loom_runner_t3.record import write_record

from .replay import (
    DEFAULT_MODEL,
    NOW,
    ORIGIN,
    PROJECT_ID,
    PROJECT_ROOT,
    T3_THREAD,
    T3_THREAD_2,
    THREAD_ID,
    TOKEN,
    WORKTREE,
    FakeClock,
    FakeGit,
    FakeHost,
    FakeHttp,
    FakeSleep,
    bad_token,
    credential,
    credential_text,
    from_recording,
    launch,
    lost_session,
    make_t3_home,
    make_thread,
    mode_of,
    projects,
    reads,
    recorded,
    stream,
    tool,
    tool_error,
    until,
    write_json,
)
from .test_credential import WRONG_TYPES

SETUP_ENV = {**os.environ, 'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_CONFIG_NOSYSTEM': '1'}

RUN_1 = 'run:thread:mcp%3A00000000-0000-4000-8000-000000000001:ordinal:1'

OK = 'Reply with the single word ok.'
COUNT_200 = 'Count slowly from 1 to 200, one number per line.'
README = 'Read README.md with your read tool and reply with its first line.'
FIRST_LINE = 'The first line of `README.md` is:\n\n```\n# Loom\n```'
PROVIDER_ERROR = 'API Error: 400 unknown provider for model loom-t3-record-no-such-model'


def git(cwd: Path, *args: str) -> str:
    done = subprocess.run(['git', *args], cwd=cwd, env=SETUP_ENV, check=True,
                          capture_output=True, text=True)
    return done.stdout.strip()


def make_repo(path: Path) -> Path:
    path.mkdir(parents=True)
    git(path, 'init', '-q', '-b', 'main')
    git(path, '-c', 'user.name=Test', '-c', 'user.email=test@example.com',
        'commit', '-q', '--allow-empty', '-m', 'First')
    return path.resolve()


def idle_read(thread: str = T3_THREAD, archived: bool = False, **thread_fields: object) -> dict:
    value = json.loads(json.dumps(stream('turn')[0]))
    value['thread']['threadId'] = thread
    value['thread']['archived'] = archived
    value['thread'].update(thread_fields)
    return value


def running_read(position: int | None = None, items: list | None = None,
                 status: str = 'running', active: str | None = RUN_1,
                 pending: int = 0) -> dict:
    return {
        'thread': {'threadId': T3_THREAD, 'status': status, 'activeRunId': active,
                   'latestRunId': RUN_1, 'pendingRequestCount': pending, 'archived': False},
        'recentRuns': [{'runId': RUN_1, 'status': status}],
        'items': items or [],
        'nextPosition': position,
        'hasMore': False,
    }


def user_item(position: int, text: str = OK) -> dict:
    return {'position': position, 'itemId': f'i{position}', 'runId': RUN_1,
            'type': 'user_message', 'status': 'completed', 'title': None, 'text': text}


class Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name).resolve()
        self.data_dir = self.base / 'data'
        self.data_dir.mkdir(mode=0o700)
        self.home = make_t3_home(self.base)
        self.http = FakeHttp()
        self.http.on('t3_project_list', projects())
        self.http.on('t3_thread_launch', launch())
        self.http.on('t3_thread_send', from_recording('send'))
        self.http.on('t3_thread_interrupt', from_recording('interrupt'))
        self.http.on('t3_thread_read', tool(idle_read()))
        self.sleep = FakeSleep()
        self.clock = FakeClock()
        self.git = FakeGit()
        self.ids = []
        self.host = FakeHost(str(self.data_dir), str(self.home), credential_text())
        self.runner = self.make_runner()

    def make_runner(self, **changes: object) -> T3Runner:
        def new_id() -> str:
            self.ids.append(str(uuid.uuid4()))
            return self.ids[-1]

        options = {'http': self.http, 'clock': self.clock, 'sleep': self.sleep,
                   'git': self.git, 'new_id': new_id}
        options.update(changes)
        return T3Runner(**options)

    async def start(self, **changes: object):
        session = await self.runner.start(make_thread(**changes), self.host)
        self.addAsyncCleanup(session.close)
        return session

    async def start_fails(self, text: str | None = None, **changes: object) -> str:
        with self.assertRaises(T3Error) as caught:
            await self.start(**changes)
        message = str(caught.exception)
        if text is not None:
            self.assertEqual(message, text)
        self.assertNotIn(TOKEN, message)
        return message

    async def next_event(self, session, timeout: float = 5.0):
        return await asyncio.wait_for(anext(session.events), timeout)

    async def turn(self, session) -> list:
        events = []
        while not events or not isinstance(events[-1], (TurnResult, Exited)):
            events.append(await self.next_event(session))
        return events

    async def rest(self, session) -> list:
        await session.close()
        return [event async for event in session.events]


class RunnerTest(Base):
    def test_the_runner_is_a_runner(self) -> None:
        runner = T3Runner()
        self.assertIsInstance(runner, Runner)
        self.assertIs(loom_runner_t3.T3Runner, T3Runner)
        self.assertEqual(runner.name, 't3')
        self.assertEqual(runner.capabilities, frozenset({'steer', 'interrupt', 'resume'}))

    async def test_its_session_is_a_runner_session(self) -> None:
        session = await self.start()
        self.assertIsInstance(session, RunnerSession)


class NotPairedTest(Base):
    async def check(self, secret: str | None) -> None:
        self.host.secrets['t3-client'] = secret
        await self.start_fails(NOT_PAIRED)
        self.assertEqual(self.http.calls, [])
        self.assertEqual(self.git.calls, [])

    async def test_no_secret_or_not_json(self) -> None:
        for secret in (None, '', 'not json', '[]', '{"origin": '):
            with self.subTest(secret=secret):
                await self.check(secret)

    async def test_each_null_field(self) -> None:
        for name in FIELDS:
            with self.subTest(field=name):
                await self.check(credential_text(**{name: None}))

    async def test_each_missing_field(self) -> None:
        for name in FIELDS:
            with self.subTest(field=name):
                data = credential()
                del data[name]
                await self.check(json.dumps(data))

    async def test_each_wrong_typed_field(self) -> None:
        for name, value in WRONG_TYPES.items():
            with self.subTest(field=name):
                await self.check(credential_text(**{name: value}))
        await self.check(credential_text(expires_at=True))

    async def test_expiry_boundary(self) -> None:
        self.host.secrets['t3-client'] = credential_text(expires_at=NOW)
        await self.start_fails(NOT_PAIRED)
        self.assertEqual(self.http.calls, [])
        self.clock.now = NOW - 1
        await self.start()
        self.assertIn('t3_thread_launch', self.http.keys())


class OriginTest(Base):
    async def test_a_missing_runtime_file_fails_before_any_call(self) -> None:
        (self.home / 'userdata' / 'server-runtime.json').unlink()
        await self.start_fails(NOT_RUNNING)
        self.assertEqual(self.http.calls, [])
        self.assertEqual(self.git.calls, [])

    async def test_another_origin_fails_before_any_call(self) -> None:
        for runtime in ({'origin': 'http://127.0.0.1:1'}, {'origin': None}, {}, {'origin': 5}):
            with self.subTest(runtime=runtime):
                write_json(self.home / 'userdata' / 'server-runtime.json', runtime)
                await self.start_fails(NOT_RUNNING)
                self.assertEqual(self.http.calls, [])

    async def test_t3_home_defaults_to_dot_t3(self) -> None:
        self.host.settings['t3_home'] = None
        with mock.patch.dict(os.environ, {'HOME': str(self.base / 'nohome')}):
            await self.start_fails(NOT_RUNNING)
        self.assertIn('setting:t3_home', self.host.asked)


class ProjectTest(Base):
    async def test_the_project_with_the_same_root_is_used(self) -> None:
        other = {'id': 'mcp:other', 'workspaceRoot': '/tmp/elsewhere'}
        self.http.on('t3_project_list', projects(PROJECT_ROOT, other))
        await self.start()
        self.assertEqual(self.http.sent('t3_thread_launch')[0].arguments['projectId'],
                         PROJECT_ID)

    async def test_no_project_fails_and_creates_none(self) -> None:
        self.http.on('t3_project_list', projects('/tmp/elsewhere'))
        await self.start_fails(f't3: no T3 project at {PROJECT_ROOT}')
        self.assertEqual(self.http.keys(), ['initialize', 'notifications/initialized',
                                            't3_project_list'])

    async def test_the_project_list_is_paged(self) -> None:
        first = {'projects': [{'id': 'mcp:a', 'workspaceRoot': '/tmp/a'}], 'nextCursor': 1}
        self.http.on('t3_project_list', tool(first), projects())
        await self.start()
        pages = self.http.sent('t3_project_list')
        self.assertEqual([page.arguments for page in pages], [{}, {'cursor': 1}])

    async def check_skipped(self, broken: list[dict]) -> None:
        good = {'id': 'mcp:good', 'workspaceRoot': PROJECT_ROOT}
        self.http.on('t3_project_list', tool({'projects': [*broken, good],
                                              'nextCursor': None}))
        await self.start()
        self.assertEqual(self.http.sent('t3_thread_launch')[-1].arguments['projectId'],
                         'mcp:good')
        self.http.on('t3_project_list', tool({'projects': broken, 'nextCursor': None}))
        await self.start_fails(f't3: no T3 project at {PROJECT_ROOT}')

    async def test_entries_with_a_null_field_are_skipped(self) -> None:
        await self.check_skipped([{'id': None, 'workspaceRoot': PROJECT_ROOT},
                                  {'id': 'mcp:x', 'workspaceRoot': None}])

    async def test_entries_with_a_missing_field_are_skipped(self) -> None:
        await self.check_skipped([{'workspaceRoot': PROJECT_ROOT}, {'id': 'mcp:x'}])

    async def test_entries_with_a_wrong_typed_field_are_skipped(self) -> None:
        await self.check_skipped([{'id': 7, 'workspaceRoot': PROJECT_ROOT},
                                  {'id': 'mcp:x', 'workspaceRoot': [PROJECT_ROOT]},
                                  'not an object'])


class WorkspaceTest(Base):
    def setUp(self) -> None:
        super().setUp()
        self.repo = make_repo(self.base / 'repo')
        self.http.on('t3_project_list', projects(str(self.repo)))
        self.runner = self.make_runner(git=loom_runner_t3.session.run_git)

    def strategy(self) -> dict:
        return self.http.sent('t3_thread_launch')[-1].arguments['workspaceStrategy']

    async def test_the_main_checkout_is_root(self) -> None:
        await self.start(workspace_path=str(self.repo))
        self.assertEqual(self.strategy(), {'type': 'root'})

    async def test_a_worktree_is_an_existing_worktree_with_its_branch(self) -> None:
        tree = self.base / 'worktrees' / 'one'
        git(self.repo, 'worktree', 'add', '-q', '-b', 'loom/one', str(tree), 'main')
        await self.start(workspace_path=str(tree))
        self.assertEqual(self.strategy(), {'type': 'existing_worktree',
                                           'worktreePath': str(tree.resolve()),
                                           'branch': 'loom/one'})

    async def test_a_detached_worktree_fails_and_names_the_path(self) -> None:
        tree = self.base / 'worktrees' / 'detached'
        git(self.repo, 'worktree', 'add', '-q', '--detach', str(tree), 'main')
        message = await self.start_fails(workspace_path=str(tree))
        self.assertTrue(message.startswith('t3: git: '))
        self.assertIn(str(tree), message)
        self.assertNotIn('t3_thread_launch', self.http.keys())

    async def test_a_path_outside_git_fails(self) -> None:
        outside = self.base / 'plain'
        outside.mkdir()
        await self.start_fails(f't3: git: {outside} is not a git checkout',
                               workspace_path=str(outside))

    async def test_a_missing_git_fails(self) -> None:
        async def no_git(args: list[str]) -> tuple[int, str]:
            raise FileNotFoundError(2, 'No such file or directory')

        self.runner = self.make_runner(git=no_git)
        await self.start_fails('t3: git: could not run git: No such file or directory',
                               workspace_path=str(self.repo))


class LaunchTest(Base):
    def launched(self) -> dict:
        return self.http.sent('t3_thread_launch')[-1].arguments

    async def test_the_runtime_mode_is_the_permission_mode(self) -> None:
        for mode in PermissionMode:
            with self.subTest(mode=mode):
                await self.start(permission_mode=mode)
                self.assertEqual(self.launched()['runtimeMode'], str(mode))

    async def test_a_new_thread_is_launched_once_with_no_message(self) -> None:
        session = await self.start(title='My thread')
        self.assertEqual(self.http.count('t3_thread_launch'), 1)
        self.assertEqual(self.launched(), {
            'projectId': PROJECT_ID,
            'title': 'My thread',
            'workspaceStrategy': {'type': 'root'},
            'runtimeMode': 'approval-required',
            'modelSelection': DEFAULT_MODEL,
        })
        record = self.data_dir / 't3' / THREAD_ID
        self.assertEqual(record.read_text(), f'{T3_THREAD}\n')
        self.assertEqual(mode_of(record), 0o600)
        self.assertEqual(mode_of(record.parent), 0o700)
        self.assertEqual(session.t3_id, T3_THREAD)
        self.assertNotIn('t3_thread_read', self.http.keys())

    async def test_the_launch_matches_the_recorded_requests(self) -> None:
        for name, changes, git in (
            ('launch-idle', {}, FakeGit()),
            ('launch-worktree', {'workspace_path': WORKTREE},
             FakeGit(top=WORKTREE, branch='loom/t3-record-1')),
            ('launch-unknown-model', {'model': 'claude-proxy/loom-t3-record-no-such-model'},
             FakeGit()),
        ):
            with self.subTest(name):
                self.runner = self.make_runner(git=git)
                self.http.on('t3_thread_launch', launch(name))
                await self.start(**changes)
                expected = recorded(name)['request']['body']['params']['arguments']
                got = dict(self.launched())
                self.assertEqual(got.pop('title'), 'Replay')
                expected.pop('title')
                self.assertEqual(got, expected)

    async def test_the_models(self) -> None:
        for model, selection in ((None, DEFAULT_MODEL),
                                 ('claude-proxy/m', {'instanceId': 'claude-proxy', 'model': 'm'}),
                                 ('other/a/b', {'instanceId': 'other', 'model': 'a/b'}),
                                 ('m', {'instanceId': 'claude-proxy', 'model': 'm'})):
            with self.subTest(model=model):
                await self.start(model=model)
                self.assertEqual(self.launched()['modelSelection'], selection)

    async def test_no_model_and_no_usable_default_fails(self) -> None:
        settings = self.home / 'userdata' / 'settings.json'
        cases = {
            'null default': {'defaultModelSelection': None},
            'missing default': {'other': 1},
            'string default': {'defaultModelSelection': 'claude'},
            'list default': {'defaultModelSelection': [DEFAULT_MODEL]},
            'missing settings': None,
            'settings not an object': [1],
        }
        for name, data in cases.items():
            with self.subTest(name):
                if data is None:
                    settings.unlink(missing_ok=True)
                else:
                    write_json(settings, data)
                message = await self.start_fails()
                self.assertTrue(message.startswith('t3: no model: '), message)
                self.assertEqual(self.http.count('t3_thread_launch'), 0)

    async def test_a_bare_model_needs_the_default_instance(self) -> None:
        write_json(self.home / 'userdata' / 'settings.json',
                   {'defaultModelSelection': {'model': 'x'}})
        message = await self.start_fails(model='m')
        self.assertTrue(message.startswith('t3: no model: '))

    async def test_a_failed_launch_is_not_retried(self) -> None:
        cases = {
            'is error': (tool_error('launch_failed', 'no'), 't3: launch_failed: no'),
            'timeout': (TimeoutError(), 't3: timeout: T3 did not answer within 10 s'),
            'connection': (ConnectionResetError(), 't3: unreachable: ConnectionResetError'),
            'lost twice': (lost_session(), 't3: protocol: T3 dropped the MCP session twice'),
        }
        for name, (reply, text) in cases.items():
            with self.subTest(name):
                self.http = FakeHttp()
                self.http.on('t3_project_list', projects())
                self.http.on('t3_thread_launch', reply, reply, launch())
                self.runner = self.make_runner()
                await self.start_fails(text)
                expected = 2 if name == 'lost twice' else 1
                self.assertEqual(self.http.count('t3_thread_launch'), expected)
                self.assertFalse((self.data_dir / 't3' / THREAD_ID).exists())

    async def check_bad_thread_id(self, change) -> None:
        value = dict(recorded('launch-idle')['body']['result']['structuredContent'])
        change(value)
        self.http.on('t3_thread_launch', tool(value))
        message = await self.start_fails()
        self.assertTrue(message.startswith('t3: protocol: '))
        self.assertFalse((self.data_dir / 't3').exists())

    async def test_a_null_thread_id_fails(self) -> None:
        await self.check_bad_thread_id(lambda value: value.update(threadId=None))

    async def test_a_missing_thread_id_fails(self) -> None:
        await self.check_bad_thread_id(lambda value: value.pop('threadId'))

    async def test_a_wrong_typed_thread_id_fails(self) -> None:
        await self.check_bad_thread_id(lambda value: value.update(threadId=[T3_THREAD]))
        await self.check_bad_thread_id(lambda value: value.update(threadId='../x'))

    async def test_an_invalid_thread_spec_fails(self) -> None:
        for changes in ({'id': 'NOT-A-UUID'}, {'workspace_path': 'relative'},
                        {'permission_mode': 'yolo'}, {'model': '-x'}):
            with self.subTest(changes=changes):
                message = await self.start_fails(**changes)
                self.assertTrue(message.startswith('t3: invalid: '))


class ResumeTest(Base):
    def setUp(self) -> None:
        super().setUp()
        self.record = self.data_dir / 't3' / THREAD_ID

    async def test_resume_checks_the_record_and_never_launches(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        session = await self.start(resume=True)
        self.assertEqual(session.t3_id, T3_THREAD)
        self.assertEqual(self.http.keys(), ['initialize', 'notifications/initialized',
                                            't3_thread_read'])
        self.assertEqual(self.http.sent('t3_thread_read')[0].arguments,
                         {'threadId': T3_THREAD, 'view': 'activity', 'afterPosition': None})
        self.assertEqual(self.git.calls, [])

    async def test_a_missing_record_fails(self) -> None:
        await self.start_fails(f't3: record: no T3 thread is stored for {THREAD_ID}',
                               resume=True)
        self.assertEqual(self.http.calls, [])

    async def test_an_unreadable_record_fails(self) -> None:
        real = self.base / 'real'
        real.write_text(T3_THREAD)
        cases = {
            'a symlink': lambda: self.record.symlink_to(real),
            'a directory': lambda: self.record.mkdir(),
            'too long': lambda: self.record.write_text('x' * 5000),
            'not the id shape': lambda: self.record.write_text('00000000-0000-4000-8000-0'),
        }
        for name, make in cases.items():
            with self.subTest(name):
                self.record.parent.mkdir(mode=0o700, exist_ok=True)
                if self.record.is_dir() and not self.record.is_symlink():
                    self.record.rmdir()
                else:
                    self.record.unlink(missing_ok=True)
                make()
                message = await self.start_fails(resume=True)
                self.assertTrue(message.startswith(
                    f't3: record: the T3 thread record for {THREAD_ID} is unreadable'), message)
                self.assertNotIn('t3_thread_launch', self.http.keys())

    async def test_an_unknown_thread_fails(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        self.http.on('t3_thread_read', from_recording('read-unknown-thread'))
        message = await self.start_fails(resume=True)
        self.assertTrue(message.startswith('t3: thread_not_found: '))
        self.assertNotIn('t3_thread_launch', self.http.keys())

    async def test_an_archived_thread_fails(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        self.http.on('t3_thread_read', from_recording('read-archived-thread'))
        await self.start_fails(f't3: archived: the T3 thread {T3_THREAD} is archived',
                               resume=True)
        self.assertNotIn('t3_thread_launch', self.http.keys())

    async def test_resume_pages_to_the_end(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        first = running_read(1, [user_item(0), user_item(1)], status='completed', active=None)
        first['hasMore'] = True
        last = running_read(2, [user_item(2)], status='completed', active=None)
        self.http.on('t3_thread_read', tool(first), tool(last))
        await self.start(resume=True)
        self.assertEqual([call.arguments['afterPosition']
                          for call in self.http.sent('t3_thread_read')], [None, 1])


class SendTest(Base):
    async def test_send_maps_the_modes_with_a_new_request_id_each(self) -> None:
        session = await self.start()
        for mode, sent in ((DispatchMode.STEER, 'steer'), (DispatchMode.QUEUE, 'queue'),
                           (DispatchMode.INTERRUPT, 'restart'), ('queue', 'queue')):
            with self.subTest(mode=mode):
                await session.send('hello', mode)
                self.assertEqual(self.http.sent('t3_thread_send')[-1].arguments, {
                    'threadId': T3_THREAD, 'message': 'hello', 'mode': sent,
                    'clientRequestId': self.ids[-1]})
        request_ids = [call.arguments['clientRequestId']
                       for call in self.http.sent('t3_thread_send')]
        self.assertEqual(len(set(request_ids)), 4)
        for request_id in request_ids:
            self.assertEqual(str(uuid.UUID(request_id)), request_id)

    async def test_the_default_request_id_is_a_uuid4(self) -> None:
        self.runner = self.make_runner(new_id=T3Runner().__dict__['_new_id'])
        session = await self.start()
        await session.send('a', DispatchMode.QUEUE)
        await session.send('b', DispatchMode.QUEUE)
        first, second = [uuid.UUID(call.arguments['clientRequestId'])
                         for call in self.http.sent('t3_thread_send')]
        self.assertNotEqual(first, second)
        self.assertEqual(first.version, 4)

    async def test_a_send_retried_after_a_lost_session_keeps_its_request_id(self) -> None:
        session = await self.start()
        self.http.on('t3_thread_send', lost_session(), from_recording('send'))
        await session.send('hello', DispatchMode.QUEUE)
        first, retry = self.http.sent('t3_thread_send')
        self.assertEqual(first.body, retry.body)
        self.assertEqual(self.http.count('initialize'), 2)

    async def test_a_failed_send_raises_and_is_not_retried(self) -> None:
        session = await self.start()
        self.http.on('t3_thread_send', TimeoutError(), from_recording('send'))
        with self.assertRaisesRegex(T3Error, '^t3: timeout: '):
            await session.send('hello', DispatchMode.QUEUE)
        self.assertEqual(self.http.count('t3_thread_send'), 1)

    async def test_interrupt_has_its_own_request_id(self) -> None:
        session = await self.start()
        await session.send('hello', DispatchMode.QUEUE)
        await session.interrupt()
        sent = self.http.sent('t3_thread_interrupt')
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0].arguments, {'threadId': T3_THREAD,
                                             'clientRequestId': self.ids[-1]})
        self.assertNotEqual(self.ids[-1],
                            self.http.sent('t3_thread_send')[0].arguments['clientRequestId'])

    async def test_respond_raises_and_calls_nothing(self) -> None:
        session = await self.start()
        before = len(self.http.calls)
        with self.assertRaises(T3Error) as caught:
            await session.respond('r1', RequestAnswer(Decision.ALLOW))
        self.assertEqual(str(caught.exception), ANSWERED_IN_T3)
        self.assertEqual(len(self.http.calls), before)


class ReplayTest(Base):
    async def play(self, case: str, sends: list[tuple[str, str]], resume: bool = False,
                   interrupt: bool = False, settle: int = 0, **changes: object) -> list:
        self.http.on('t3_thread_read', *reads(case))
        session = await self.start(resume=resume, **changes)
        for message, mode in sends:
            await session.send(message, mode)
        if interrupt:
            await session.interrupt()
        self.sleep.open()
        events = await self.turn(session)
        await until(lambda: self.http.count('t3_thread_read') >= settle)
        self.assertEqual(await self.rest(session), [Exited(None)])
        return events

    async def test_idle_launch(self) -> None:
        self.http.on('t3_thread_read', *reads('idle-launch'))
        session = await self.start()
        self.sleep.open()
        await until(lambda: self.http.count('t3_thread_read') >= 4)
        self.assertEqual(await self.rest(session), [Exited(None)])

    async def test_turn(self) -> None:
        events = await self.play('turn', [(OK, 'queue')])
        self.assertEqual(events, [TurnStarted(), MessageEntered(OK), TextOut('ok'),
                                  TurnResult(TurnState.COMPLETED, 'ok', None)])

    async def test_tool(self) -> None:
        self.runner = self.make_runner(git=FakeGit(top=WORKTREE, branch='loom/t3-record-1'))
        self.http.on('t3_thread_launch', launch('launch-worktree'))
        events = await self.play('tool', [(README, 'queue')], workspace_path=WORKTREE)
        self.assertEqual(events, [
            TurnStarted(),
            MessageEntered(README),
            ToolUse('turn-item:provider:claudeAgent:native-item:toolu_replay01', 'Read',
                    {'file_path': '/tmp/loom-replay/worktree/README.md', 'limit': 5}),
            TextOut(FIRST_LINE),
            TurnResult(TurnState.COMPLETED, FIRST_LINE, None),
        ])

    async def test_interrupt(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        counted = '\n'.join(str(number) for number in range(1, 168))
        events = await self.play('interrupt', [(COUNT_200, 'queue')], resume=True,
                                 interrupt=True)
        self.assertEqual(events, [TurnStarted(), MessageEntered(COUNT_200), TextOut(counted),
                                  TurnResult(TurnState.STOPPED, counted, 'interrupted')])

    async def test_steer(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD_2)
        events = await self.play('steer', [(COUNT_200, 'queue'), (OK, 'steer')], resume=True)
        self.assertEqual(events, [TurnStarted(), MessageEntered(COUNT_200), MessageEntered(OK),
                                  TextOut('ok'), TurnResult(TurnState.COMPLETED, 'ok', None)])
        modes = [call.arguments['mode'] for call in self.http.sent('t3_thread_send')]
        self.assertEqual(modes, ['queue', 'steer'])

    async def test_failed(self) -> None:
        self.http.on('t3_thread_launch', launch('launch-unknown-model'))
        events = await self.play('failed', [(OK, 'queue')],
                                 model='claude-proxy/loom-t3-record-no-such-model')
        self.assertEqual(events, [
            TurnStarted(), MessageEntered(OK), TextOut(PROVIDER_ERROR),
            TurnResult(TurnState.FAILED, PROVIDER_ERROR, f't3: failed: {PROVIDER_ERROR}'),
        ])

    async def test_reads_follow_the_position(self) -> None:
        await self.play('turn', [(OK, 'queue')], settle=8)
        positions = [call.arguments['afterPosition']
                     for call in self.http.sent('t3_thread_read')][:7]
        self.assertEqual(positions, [None, None, 0, 0, 0, 0, 2])
        self.assertEqual(self.sleep.calls[0], 15.0)
        for call in self.http.sent('t3_thread_read'):
            self.assertEqual(call.arguments['threadId'], T3_THREAD)
            self.assertEqual(call.arguments['view'], 'activity')

    async def test_the_streams_cover_every_case(self) -> None:
        names = sorted(path.stem for path in (Path(__file__).parent / 'streams').glob('*.jsonl'))
        self.assertEqual(names, ['failed', 'idle-launch', 'interrupt', 'steer', 'tool', 'turn'])


class PollTest(Base):
    async def test_the_poll_intervals(self) -> None:
        self.http.on('t3_thread_read', tool(idle_read()), tool(idle_read()),
                     *reads('turn')[1:])
        session = await self.start()
        await until(lambda: self.sleep.calls == [15.0])
        self.sleep.step()
        await until(lambda: self.http.count('t3_thread_read') == 1)
        await until(lambda: len(self.sleep.calls) == 2)
        self.assertEqual(self.sleep.calls, [15.0, 15.0])
        await session.send(OK, DispatchMode.QUEUE)
        await until(lambda: len(self.sleep.calls) == 3)
        self.assertEqual(self.sleep.calls[2], 1.0)
        self.sleep.step()
        await until(lambda: self.http.count('t3_thread_read') == 2)
        self.sleep.open()
        events = await self.turn(session)
        self.assertEqual(events[-1], TurnResult(TurnState.COMPLETED, 'ok', None))
        await until(lambda: len(self.sleep.calls) >= 9)
        self.assertEqual(self.sleep.calls[:9],
                         [15.0, 15.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 15.0])

    async def test_the_intervals_are_settings(self) -> None:
        self.runner = self.make_runner(running_interval=0.25, idle_interval=4.0)
        session = await self.start()
        await until(lambda: self.sleep.calls == [4.0])
        await session.send(OK, DispatchMode.QUEUE)
        await until(lambda: self.sleep.calls[-1:] == [0.25])

    async def test_a_running_thread_polls_at_the_running_interval(self) -> None:
        self.http.on('t3_thread_read', tool(running_read()))
        await self.start()
        self.sleep.open()
        await until(lambda: len(self.sleep.calls) >= 5)
        self.assertEqual(self.sleep.calls[0], 15.0)
        self.assertEqual(set(self.sleep.calls[1:5]), {1.0})

    async def test_no_result_while_a_request_is_pending(self) -> None:
        self.http.on('t3_thread_read',
                     tool(running_read(0, [user_item(0)])),
                     tool(running_read(status='completed', active=None, pending=1)),
                     tool(running_read(status='completed', active=None, pending=1)),
                     tool(running_read(status='completed', active=None, pending=0)))
        session = await self.start()
        await session.send(OK, DispatchMode.QUEUE)
        self.sleep.open()
        events = await self.turn(session)
        self.assertEqual(events, [TurnStarted(), MessageEntered(OK),
                                  TurnResult(TurnState.COMPLETED, None, None)])
        self.assertGreaterEqual(self.http.count('t3_thread_read'), 4)

    async def check_bad_read(self, change) -> None:
        broken = running_read(1, [user_item(1, 'more')])
        change(broken)
        self.http = FakeHttp()
        self.http.on('t3_project_list', projects())
        self.http.on('t3_thread_launch', launch())
        self.http.on('t3_thread_send', from_recording('send'))
        self.sleep = FakeSleep()
        self.runner = self.make_runner()
        self.http.on('t3_thread_read', tool(running_read(0, [user_item(0)])), tool(broken),
                     tool(running_read(status='completed', active=None)))
        session = await self.start()
        await session.send(OK, DispatchMode.QUEUE)
        self.sleep.open()
        events = await self.turn(session)
        self.assertEqual(events[:2], [TurnStarted(), MessageEntered(OK)])
        self.assertEqual(len(events), 3)
        self.assertEqual(events[2].state, TurnState.FAILED)
        self.assertTrue(events[2].reason.startswith('t3: protocol: '), events[2].reason)
        await until(lambda: self.http.count('t3_thread_read') >= 5)
        await session.close()
        self.assertIn(1.0, self.sleep.calls)
        self.assertEqual(self.sleep.calls[-3:], [15.0, 15.0, 15.0])
        self.assertEqual([event async for event in session.events], [Exited(None)])

    async def test_a_null_position_or_status_fails_the_turn_once(self) -> None:
        await self.check_bad_read(lambda data: data.update(nextPosition=None))
        await self.check_bad_read(lambda data: data['thread'].update(status=None))

    async def test_a_missing_position_or_status_fails_the_turn_once(self) -> None:
        await self.check_bad_read(lambda data: data.pop('nextPosition'))
        await self.check_bad_read(lambda data: data['thread'].pop('status'))

    async def test_a_wrong_typed_position_or_status_fails_the_turn_once(self) -> None:
        await self.check_bad_read(lambda data: data.update(nextPosition='1'))
        await self.check_bad_read(lambda data: data['thread'].update(status=3))

    async def test_a_bad_read_while_idle_is_only_the_next_poll(self) -> None:
        broken = idle_read()
        broken['thread']['status'] = None
        self.http.on('t3_thread_read', tool(broken), tool(broken), tool(idle_read()))
        session = await self.start()
        self.sleep.open()
        await until(lambda: self.http.count('t3_thread_read') >= 4)
        self.assertEqual(await self.rest(session), [Exited(None)])

    async def test_a_failed_poll_during_a_turn_ends_it_once(self) -> None:
        self.http.on('t3_thread_read', tool(running_read(0, [user_item(0)])), TimeoutError(),
                     OSError(), tool(running_read(status='completed', active=None)))
        session = await self.start()
        await session.send(OK, DispatchMode.QUEUE)
        self.sleep.open()
        events = await self.turn(session)
        self.assertEqual(events, [TurnStarted(), MessageEntered(OK), TurnResult(
            TurnState.FAILED, None, 't3: timeout: T3 did not answer within 10 s')])
        await until(lambda: self.http.count('t3_thread_read') >= 6)
        self.assertEqual(await self.rest(session), [Exited(None)])

    async def test_a_failed_poll_right_after_a_send_ends_the_turn(self) -> None:
        self.http.on('t3_thread_read', tool_error('boom', 'gone'), tool(idle_read()))
        session = await self.start()
        await session.send(OK, DispatchMode.QUEUE)
        self.sleep.open()
        self.assertEqual(await self.turn(session),
                         [TurnResult(TurnState.FAILED, None, 't3: boom: gone')])

    async def test_a_401_during_a_turn_fails_it_and_exits(self) -> None:
        self.http.on('t3_thread_read', tool(running_read(0, [user_item(0)])), bad_token())
        session = await self.start()
        await session.send(OK, DispatchMode.QUEUE)
        self.sleep.open()
        events = [event async for event in session.events]
        self.assertEqual(events, [TurnStarted(), MessageEntered(OK),
                                  TurnResult(TurnState.FAILED, None, NOT_PAIRED), Exited(None)])
        reads_done = self.http.count('t3_thread_read')
        await asyncio.sleep(0.05)
        self.assertEqual(self.http.count('t3_thread_read'), reads_done)

    async def test_a_failed_origin_check_during_a_turn_fails_it_and_exits(self) -> None:
        runtime = self.home / 'userdata' / 'server-runtime.json'

        def moved(call) -> object:
            write_json(runtime, {'origin': 'http://127.0.0.1:2'})
            return tool(running_read(0, [user_item(0)]))

        self.http.on('t3_thread_read', moved)
        session = await self.start()
        await session.send(OK, DispatchMode.QUEUE)
        self.sleep.open()
        events = [event async for event in session.events]
        self.assertEqual(events, [TurnStarted(), MessageEntered(OK),
                                  TurnResult(TurnState.FAILED, None, NOT_RUNNING), Exited(None)])
        self.assertEqual(self.http.count('t3_thread_read'), 1)

    async def test_a_401_while_idle_only_exits(self) -> None:
        self.http.on('t3_thread_read', bad_token())
        session = await self.start()
        self.sleep.open()
        self.assertEqual([event async for event in session.events], [Exited(None)])


class CloseTest(Base):
    async def test_close_exits_once_and_calls_nothing(self) -> None:
        session = await self.start()
        before = len(self.http.calls)
        await session.close()
        await session.close()
        self.assertEqual([event async for event in session.events], [Exited(None)])
        self.sleep.open()
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.http.calls), before)

    async def test_close_during_a_turn_stops_polling(self) -> None:
        self.http.on('t3_thread_read', tool(running_read(0, [user_item(0)])))
        session = await self.start()
        await session.send(OK, DispatchMode.QUEUE)
        self.sleep.open()
        self.assertEqual(await self.next_event(session), TurnStarted())
        await session.close()
        reads_done = self.http.count('t3_thread_read')
        await asyncio.sleep(0.05)
        self.assertEqual(self.http.count('t3_thread_read'), reads_done)
        rest = [event async for event in session.events]
        self.assertEqual(rest[-1], Exited(None))
        self.assertEqual(rest.count(Exited(None)), 1)

    async def test_after_close_send_raises_and_interrupt_does_nothing(self) -> None:
        session = await self.start()
        await session.close()
        before = len(self.http.calls)
        with self.assertRaises(T3Error):
            await session.send('x', DispatchMode.QUEUE)
        await session.interrupt()
        self.assertEqual(len(self.http.calls), before)


class SecretTest(Base):
    async def test_no_event_or_error_holds_the_token(self) -> None:
        leak = f'echo {TOKEN}'
        texts = []
        for reply in (tool_error('leak', leak), OSError(leak), TimeoutError(leak),
                      bad_token(), lost_session()):
            self.http = FakeHttp()
            self.http.on('t3_project_list', projects())
            self.http.on('t3_thread_launch', reply)
            self.runner = self.make_runner()
            texts.append(await self.start_fails())
        self.http = FakeHttp()
        self.http.on('t3_project_list', projects())
        self.http.on('t3_thread_launch', launch())
        self.http.on('t3_thread_send', tool_error('leak', leak))
        self.http.on('t3_thread_read', tool(running_read(0, [user_item(0)])),
                     tool_error('leak', leak),
                     tool(running_read(1, [{'position': 1, 'runId': RUN_1, 'type': 'error',
                                            'status': 'failed', 'text': leak}],
                                       status='failed', active=None)),
                     bad_token())
        self.runner = self.make_runner()
        session = await self.start()
        with self.assertRaises(T3Error) as caught:
            await session.send(OK, DispatchMode.QUEUE)
        texts.append(str(caught.exception))
        self.http.on('t3_thread_send', from_recording('send'))
        await session.send(OK, DispatchMode.QUEUE)
        self.sleep.open()
        events = [event async for event in session.events]
        self.assertIn(TurnResult(TurnState.FAILED, None, 't3: leak: echo [redacted]'), events)
        texts.extend(repr(event) for event in events)
        for text in texts:
            self.assertNotIn(TOKEN, text)
        self.assertIn('t3: leak: echo [redacted]', texts)

    async def test_the_token_goes_only_to_the_paired_origin(self) -> None:
        session = await self.start()
        await session.send(OK, DispatchMode.QUEUE)
        for call in self.http.calls:
            self.assertTrue(call.url.startswith(ORIGIN + '/'))
            self.assertNotIn(TOKEN, call.body.decode())
            self.assertNotIn(TOKEN, call.url)
            holding = [key for key, value in call.headers.items() if TOKEN in value]
            self.assertEqual(holding, ['Authorization'])


if __name__ == '__main__':
    unittest.main()
