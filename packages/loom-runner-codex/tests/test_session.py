import asyncio
import contextlib
import errno
import json
import os
import signal
import sys
import tempfile
import unittest
import uuid
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import loom_runner_codex
from loom_contracts.runners import (
    Capability,
    Decision,
    DispatchMode,
    Exited,
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
from loom_runner_codex import CodexRunner, RunnerStartError, SessionClosed
from loom_runner_codex.command import codex_env
from loom_runner_codex.record import write_record

from .replay import (
    CODEX_ID,
    FIRST_PID,
    STREAMS,
    THREAD_ID,
    WORKSPACE,
    FakeHost,
    FakeProcess,
    FakeSpawn,
    changed,
    line,
    make_thread,
    stream_lines,
)

FAKE_CODEX = Path(__file__).resolve().parent / 'fake_codex.py'

TOKEN = 'loom-test-token-3f9c2a7e'

OTHER_ID = '01a163a2-40b1-7412-ab12-1043a0b6795a'

MESSAGE = 'Remember the word lynx, run `echo spike-233`, and reply with OK'

INTRO = 'I’ll run the command in the specified folder and remember “lynx.”'

COMMAND = "/bin/bash -lc 'echo spike-233'"

SANDBOX_OF = {
    PermissionMode.APPROVAL_REQUIRED: 'read-only',
    PermissionMode.AUTO_ACCEPT_EDITS: 'workspace-write',
    PermissionMode.AUTO: 'workspace-write',
    PermissionMode.FULL_ACCESS: 'danger-full-access',
}

UNREADABLE = f'the codex thread record for {THREAD_ID} is unreadable'


def first_argv(sandbox: str = 'workspace-write', workspace: str = WORKSPACE,
               codex_bin: str = 'codex') -> list[str]:
    return [codex_bin, 'exec', '--json', '-C', workspace, '-s', sandbox, '-']


def resume_argv(codex_id: str = CODEX_ID, sandbox: str = 'workspace-write',
                workspace: str = WORKSPACE, codex_bin: str = 'codex') -> list[str]:
    return [codex_bin, '-C', workspace, '-s', sandbox, 'exec', 'resume', codex_id, '--json', '-']


def completed(text: str | None) -> TurnResult:
    return TurnResult(TurnState.COMPLETED, text, None)


def failed(reason: str, text: str | None = None) -> TurnResult:
    return TurnResult(TurnState.FAILED, text, reason)


def stopped(text: str | None) -> TurnResult:
    return TurnResult(TurnState.STOPPED, text, 'interrupted')


FIRST_TURN = [
    TurnStarted(),
    TextOut(INTRO),
    ToolUse('item_1', 'command_execution', {'command': COMMAND}),
    TextOut('OK'),
    completed('OK'),
]

RESUME = [TurnStarted(), TextOut('lynx'), completed('lynx')]

STARTED = line({'type': 'thread.started', 'thread_id': CODEX_ID})

TURN_STARTED = line({'type': 'turn.started'})

TURN_COMPLETED = line({'type': 'turn.completed'})


def message_line(text: object, item_id: str = 'item_0') -> bytes:
    return line({'type': 'item.completed',
                 'item': {'id': item_id, 'type': 'agent_message', 'text': text}})


def thread_id_of(case: str) -> str:
    return json.loads(stream_lines(case)[0])['thread_id']


async def eventually(check: Callable[[], bool], timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not check():
        if loop.time() > deadline:
            raise AssertionError('the condition never held')
        await asyncio.sleep(0.001)


async def settle() -> None:
    for _ in range(20):
        await asyncio.sleep(0)


def is_gone(pid: int) -> bool:
    try:
        text = Path(f'/proc/{pid}/stat').read_text()
    except (FileNotFoundError, ProcessLookupError):
        return True
    return text[text.rindex(')') + 2] == 'Z'


class Events:
    def __init__(self, session: RunnerSession) -> None:
        self.seen: list[object] = []
        self.cursor = 0
        self.task = asyncio.get_running_loop().create_task(self._collect(session))

    async def _collect(self, session: RunnerSession) -> None:
        async for event in session.events:
            self.seen.append(event)

    async def take(self, kind: type, timeout: float = 1.0) -> list[object]:
        def found() -> int | None:
            for index in range(self.cursor, len(self.seen)):
                if isinstance(self.seen[index], kind):
                    return index
            return None

        await eventually(lambda: found() is not None, timeout)
        end = found() + 1
        taken, self.cursor = self.seen[self.cursor:end], end
        return taken

    def new(self) -> list[object]:
        taken, self.cursor = self.seen[self.cursor:], len(self.seen)
        return taken

    async def end(self, timeout: float = 2.0) -> list[object]:
        await asyncio.wait_for(asyncio.shield(self.task), timeout)
        return self.new()


class SessionTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.data_dir = Path(temp.name).resolve()

    def host(self, **options: object) -> FakeHost:
        return FakeHost(str(self.data_dir), **options)

    async def start(
        self,
        *outcomes: FakeProcess | OSError,
        thread: object = None,
        host: FakeHost | None = None,
        **options: object,
    ) -> RunnerSession:
        self.spawn = FakeSpawn(*outcomes)
        options = {
            'close_timeout': 0.05,
            'getpgid': self.spawn.getpgid,
            'killpg': self.spawn.killpg,
            **options,
        }
        runner = CodexRunner(spawn=self.spawn, **options)
        session = await runner.start(thread or make_thread(), host or self.host())
        self.events = Events(session)
        self.addAsyncCleanup(self.finish, session, self.events)
        return session

    async def finish(self, session: RunnerSession, events: Events) -> None:
        await asyncio.wait_for(session.close(), 2)
        await events.end()

    def process(self, index: int = 0) -> FakeProcess:
        return self.spawn.processes[index]

    @property
    def record(self) -> Path:
        return self.data_dir / 'codex' / THREAD_ID

    async def turn(self, session: RunnerSession, message: str = MESSAGE,
                   mode: DispatchMode = DispatchMode.QUEUE) -> list[object]:
        await session.send(message, mode)
        return await self.events.take(TurnResult)


class ProtocolTest(SessionTest):
    async def test_runner_and_session_meet_the_protocols(self) -> None:
        runner = CodexRunner()
        self.assertIsInstance(runner, Runner)
        self.assertEqual(runner.name, 'codex')
        self.assertEqual(runner.capabilities, frozenset({'interrupt', 'resume'}))
        self.assertEqual(runner.capabilities,
                         frozenset({Capability.INTERRUPT, Capability.RESUME}))
        self.assertNotIn(Capability.STEER, runner.capabilities)
        self.assertNotIn(Capability.PERMISSION_REQUESTS, runner.capabilities)
        session = await self.start()
        self.assertIsInstance(session, RunnerSession)

    def test_package_exports(self) -> None:
        self.assertEqual(
            sorted(loom_runner_codex.__all__),
            ['CodexRunner', 'RunnerStartError', 'SessionClosed'],
        )
        self.assertIs(loom_runner_codex.CodexRunner, CodexRunner)

    async def test_respond_always_raises_key_error(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')[:2], stay=True))
        answer = RequestAnswer(Decision.ALLOW)
        with self.assertRaises(KeyError):
            await session.respond('item_1', answer)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(TurnStarted)
        with self.assertRaises(KeyError):
            await session.respond('item_1', answer)
        await session.close()
        with self.assertRaises(KeyError):
            await session.respond('item_1', answer)


class StartTest(SessionTest):
    async def test_start_spawns_nothing(self) -> None:
        await self.start()
        await settle()
        self.assertEqual(self.spawn.calls, [])
        self.assertEqual(self.events.new(), [])

    async def test_codex_bin_setting(self) -> None:
        cases = [
            ('/opt/codex/bin/codex', '/opt/codex/bin/codex'),
            ('', 'codex'),
            (None, 'codex'),
            (7, 'codex'),
        ]
        for value, expected in cases:
            with self.subTest(codex_bin=value):
                host = self.host(settings={'codex_bin': value})
                session = await self.start(FakeProcess(stream_lines('first-turn')), host=host)
                await self.turn(session)
                self.assertEqual(self.spawn.argvs, [first_argv(codex_bin=expected)])

    async def test_invalid_thread_raises_and_spawns_nothing(self) -> None:
        cases = {
            'non-uuid id': {'id': 'not-a-uuid'},
            'uppercase id': {'id': THREAD_ID.upper()},
            'unknown mode': {'permission_mode': 'yolo'},
            'dash model': {'model': '-x'},
            'relative workspace': {'workspace_path': 'repo'},
        }
        for name, change in cases.items():
            with self.subTest(case=name):
                spawn = FakeSpawn()
                with self.assertRaises(ValueError):
                    await CodexRunner(spawn=spawn).start(make_thread(**change), self.host())
                self.assertEqual(spawn.calls, [])

    async def test_resume_with_a_record_resumes_on_the_first_turn(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, CODEX_ID)
        session = await self.start(FakeProcess(stream_lines('resume')),
                                   thread=make_thread(resume=True))
        self.assertEqual(await self.turn(session, 'Reply with the word you remembered'), RESUME)
        self.assertEqual(self.spawn.argvs, [resume_argv()])

    async def test_resume_without_a_record_runs_a_first_turn(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')),
                                   thread=make_thread(resume=True))
        self.assertEqual(await self.turn(session), FIRST_TURN)
        self.assertEqual(self.spawn.argvs, [first_argv()])

    async def test_no_resume_reads_no_record(self) -> None:
        (self.data_dir / 'codex').mkdir(mode=0o700)
        self.record.mkdir()
        session = await self.start(FakeProcess(stream_lines('first-turn')))
        self.assertEqual(await self.turn(session), FIRST_TURN)
        self.assertEqual(self.spawn.argvs, [first_argv()])

    async def test_unreadable_record_raises_and_spawns_nothing(self) -> None:
        target = self.data_dir / 'target'
        target.write_bytes(f'{CODEX_ID}\n'.encode())
        cases = {
            'symlink': lambda: self.record.symlink_to(target),
            'directory': lambda: self.record.mkdir(),
            'too long': lambda: self.record.write_bytes(CODEX_ID.encode() + b' ' * 4_100),
            'not utf-8': lambda: self.record.write_bytes(b'\xff' + CODEX_ID.encode()),
            'not a uuid': lambda: self.record.write_bytes(b'--help\n'),
        }
        (self.data_dir / 'codex').mkdir(mode=0o700)
        for name, make in cases.items():
            with self.subTest(case=name):
                make()
                spawn = FakeSpawn()
                with self.assertRaises(RunnerStartError) as caught:
                    await CodexRunner(spawn=spawn).start(make_thread(resume=True), self.host())
                self.assertEqual(str(caught.exception), UNREADABLE)
                self.assertEqual(spawn.calls, [])
                if self.record.is_dir() and not self.record.is_symlink():
                    self.record.rmdir()
                else:
                    self.record.unlink()


class TurnTest(SessionTest):
    async def test_first_turn_argv_cwd_env_and_stdin(self) -> None:
        host = self.host(
            environment={'PATH': '/usr/bin:/bin', 'HOME': '/home/someone'},
            secrets={'agent-token': TOKEN},
        )
        thread = make_thread()
        session = await self.start(FakeProcess(stream_lines('first-turn')), host=host)
        await self.turn(session)
        self.assertEqual(self.spawn.calls, [(
            first_argv(), WORKSPACE, codex_env(thread, host, 'codex'), 8_388_608,
        )])
        self.assertEqual(bytes(self.process().stdin.data), MESSAGE.encode())
        self.assertTrue(self.process().stdin.closed)
        for item in self.spawn.argvs[0]:
            self.assertNotIn('lynx', item)
            self.assertNotIn(TOKEN, item)

    async def test_first_turn_with_a_model(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')),
                                   FakeProcess(stream_lines('resume')),
                                   thread=make_thread(model='gpt-6.1-sol'))
        await self.turn(session)
        await self.turn(session, 'again')
        self.assertEqual(self.spawn.argvs, [
            ['codex', 'exec', '--json', '-C', WORKSPACE, '-m', 'gpt-6.1-sol',
             '-s', 'workspace-write', '-'],
            ['codex', '-C', WORKSPACE, '-s', 'workspace-write', 'exec', 'resume', CODEX_ID,
             '--json', '-m', 'gpt-6.1-sol', '-'],
        ])

    async def test_replay_first_turn_and_stay_open(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')))
        self.assertEqual(await self.turn(session), FIRST_TURN)
        await eventually(lambda: self.process().returncode == 0)
        await asyncio.sleep(0.1)
        self.assertEqual(self.events.new(), [])
        self.assertEqual(self.process().group_signals, [(FIRST_PID, 0)])

    async def test_second_turn_resumes_the_thread_of_the_first(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')),
                                   FakeProcess(stream_lines('resume')))
        self.assertEqual(await self.turn(session), FIRST_TURN)
        second = 'Reply with the word you remembered'
        self.assertEqual(await self.turn(session, second), RESUME)
        self.assertEqual(self.spawn.argvs, [first_argv(), resume_argv()])
        self.assertEqual(self.spawn.argvs[1][7], '01a163a1-4140-78d3-b4ab-ee2d93e984ad')
        self.assertEqual(bytes(self.process(1).stdin.data), second.encode())
        self.assertEqual([call[1] for call in self.spawn.calls], [WORKSPACE, WORKSPACE])

    async def test_each_mode_gives_its_sandbox_on_both_forms(self) -> None:
        streams = {
            PermissionMode.APPROVAL_REQUIRED: 'read-only',
            PermissionMode.AUTO_ACCEPT_EDITS: 'workspace-write',
            PermissionMode.AUTO: 'workspace-write',
            PermissionMode.FULL_ACCESS: 'full-access',
        }
        for mode, case in streams.items():
            with self.subTest(mode=mode):
                session = await self.start(FakeProcess(stream_lines(case)),
                                           FakeProcess(stream_lines('resume')),
                                           thread=make_thread(permission_mode=mode))
                await self.turn(session)
                await self.turn(session, 'again')
                sandbox = SANDBOX_OF[mode]
                self.assertEqual(self.spawn.argvs, [
                    first_argv(sandbox), resume_argv(thread_id_of(case), sandbox),
                ])

    async def test_replay_read_only(self) -> None:
        session = await self.start(FakeProcess(stream_lines('read-only')),
                                   thread=make_thread(permission_mode='approval-required'))
        last = 'Both commands ran, but both writes failed: Read-only file system.'
        self.assertEqual(await self.turn(session), [
            TurnStarted(),
            TextOut('I’ll try both commands and report whether the read-only environment '
                    'allows them.\n'),
            TextOut(last),
            completed(last),
        ])

    async def test_replay_workspace_write(self) -> None:
        session = await self.start(FakeProcess(stream_lines('workspace-write')))
        command = ("/bin/bash -lc 'echo spike-233 > "
                   "/home/yoieh/.cache/thread-voice-spike/no-t3/repo/codex-workspace-write.txt'")
        last = ('Done. The workspace file was written; the outside-workspace write failed '
                'because the filesystem is read-only.')
        self.assertEqual(await self.turn(session), [
            TurnStarted(),
            TextOut('I’ll run the two commands and check their results.\n'),
            ToolUse('item_1', 'command_execution', {'command': command}),
            TextOut(last),
            completed(last),
        ])

    async def test_replay_full_access_gives_one_tool_use_per_item(self) -> None:
        session = await self.start(FakeProcess(stream_lines('full-access')),
                                   thread=make_thread(permission_mode='full-access'))
        events = await self.turn(session)
        tools = [event for event in events if isinstance(event, ToolUse)]
        self.assertEqual([tool.tool_use_id for tool in tools], ['item_1', 'item_2'])
        self.assertTrue(all(set(tool.input) == {'command'} for tool in tools))
        self.assertEqual(events[0], TurnStarted())
        self.assertEqual(events[-1], completed('done'))

    async def test_item_ids_count_again_in_a_new_turn(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')),
                                   FakeProcess(stream_lines('workspace-write')))
        await self.turn(session)
        events = await self.turn(session, 'again')
        self.assertEqual([event.tool_use_id for event in events if isinstance(event, ToolUse)],
                         ['item_1'])


class RecordTest(SessionTest):
    async def test_thread_started_writes_a_private_record(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')))
        await self.turn(session)
        self.assertEqual(self.record.read_bytes(), f'{CODEX_ID}\n'.encode())
        self.assertEqual(self.record.stat().st_mode & 0o777, 0o600)
        self.assertEqual((self.data_dir / 'codex').stat().st_mode & 0o777, 0o700)

    async def test_a_wide_record_dir_is_narrowed(self) -> None:
        directory = self.data_dir / 'codex'
        directory.mkdir()
        directory.chmod(0o755)
        session = await self.start(FakeProcess(stream_lines('first-turn')))
        await self.turn(session)
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.record.read_bytes(), f'{CODEX_ID}\n'.encode())

    async def test_bad_thread_ids_write_nothing_and_change_no_argv(self) -> None:
        lines = stream_lines('first-turn')
        cases = {
            'null': lambda data: data.update(thread_id=None),
            'missing': lambda data: data.pop('thread_id'),
            'number': lambda data: data.update(thread_id=7),
            'help flag': lambda data: data.update(thread_id='--help'),
            'path': lambda data: data.update(thread_id='../x'),
            'uppercase': lambda data: data.update(thread_id=CODEX_ID.upper()),
        }
        for name, change in cases.items():
            with self.subTest(case=name):
                first = [changed(lines[0], change), *lines[1:]]
                session = await self.start(FakeProcess(first),
                                           FakeProcess(stream_lines('first-turn')))
                self.assertEqual(await self.turn(session), FIRST_TURN)
                self.assertFalse(self.record.exists())
                self.assertFalse((self.data_dir / 'codex').exists())
                await self.turn(session, 'again')
                self.assertEqual(self.spawn.argvs, [first_argv(), first_argv()])
                self.record.unlink()
                self.record.parent.rmdir()

    async def test_a_failed_write_keeps_the_id_in_memory(self) -> None:
        (self.data_dir / 'codex').write_bytes(b'')
        session = await self.start(FakeProcess(stream_lines('first-turn')),
                                   FakeProcess(stream_lines('resume')))
        self.assertEqual(await self.turn(session), FIRST_TURN)
        self.assertEqual(await self.turn(session, 'again'), RESUME)
        self.assertEqual(self.spawn.argvs, [first_argv(), resume_argv()])

    async def test_a_new_thread_id_replaces_the_record(self) -> None:
        resume = stream_lines('resume')
        other = [changed(resume[0], lambda data: data.update(thread_id=OTHER_ID)), *resume[1:]]
        session = await self.start(FakeProcess(stream_lines('first-turn')),
                                   FakeProcess(other), FakeProcess(stream_lines('resume')))
        await self.turn(session)
        await self.turn(session, 'again')
        self.assertEqual(self.record.read_bytes(), f'{OTHER_ID}\n'.encode())
        await self.turn(session, 'and again')
        self.assertEqual(self.spawn.argvs, [first_argv(), resume_argv(), resume_argv(OTHER_ID)])

    async def test_a_resumed_session_reads_the_record_a_first_one_wrote(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')))
        await self.turn(session)
        await session.close()
        session = await self.start(FakeProcess(stream_lines('resume')),
                                   thread=make_thread(resume=True))
        self.assertEqual(await self.turn(session, 'again'), RESUME)
        self.assertEqual(self.spawn.argvs, [resume_argv()])


class ItemTest(SessionTest):
    async def test_unreadable_items_emit_nothing_and_the_turn_goes_on(self) -> None:
        bad_items = [None, 7, 'item']
        for key in ('id', 'type'):
            for value in (None, 7, ...):
                item = {'id': 'item_1', 'type': 'command_execution', 'command': 'ls'}
                if value is ...:
                    del item[key]
                else:
                    item[key] = value
                bad_items.append(item)
                message = {'id': 'item_0', 'type': 'agent_message', 'text': 'hidden'}
                if value is ...:
                    del message[key]
                else:
                    message[key] = value
                bad_items.append(message)
        lines = [STARTED, TURN_STARTED, line({'type': 'item.started'}),
                 line({'type': 'item.completed'})]
        for item in bad_items:
            for kind in ('item.started', 'item.completed'):
                lines.append(line({'type': kind, 'item': item}))
        session = await self.start(FakeProcess([*lines, TURN_COMPLETED]))
        self.assertEqual(await self.turn(session), [TurnStarted(), completed(None)])

    async def test_agent_message_without_text_keeps_the_last_text(self) -> None:
        lines = [STARTED, TURN_STARTED, message_line('OK'), message_line(None, 'item_1'),
                 message_line(7, 'item_2'),
                 line({'type': 'item.completed', 'item': {'id': 'item_3',
                                                          'type': 'agent_message'}}),
                 TURN_COMPLETED]
        session = await self.start(FakeProcess(lines))
        self.assertEqual(await self.turn(session),
                         [TurnStarted(), TextOut('OK'), completed('OK')])

    async def test_silent_items_emit_nothing(self) -> None:
        lines = [STARTED, TURN_STARTED,
                 line({'type': 'item.completed',
                       'item': {'id': 'item_0', 'type': 'reasoning', 'text': 'hmm'}}),
                 line({'type': 'item.started',
                       'item': {'id': 'item_1', 'type': 'todo_list', 'items': []}}),
                 line({'type': 'item.completed',
                       'item': {'id': 'item_2', 'type': 'error', 'message': 'x'}}),
                 TURN_COMPLETED]
        session = await self.start(FakeProcess(lines))
        self.assertEqual(await self.turn(session), [TurnStarted(), completed(None)])


class FailureTest(SessionTest):
    async def test_turn_failed_gives_its_message_and_stays_open(self) -> None:
        session = await self.start(FakeProcess(stream_lines('synthetic-failed')),
                                   FakeProcess(stream_lines('resume')))
        text = 'I could not reach the model.'
        self.assertEqual(await self.turn(session), [
            TurnStarted(),
            TextOut(text),
            failed('stream disconnected before completion', text),
        ])
        self.assertEqual(await self.turn(session, 'again'), RESUME)
        self.assertEqual(self.spawn.argvs[1], resume_argv())

    async def test_turn_failed_without_a_usable_message(self) -> None:
        cases = {
            'error null': {'type': 'turn.failed', 'error': None},
            'error missing': {'type': 'turn.failed'},
            'error number': {'type': 'turn.failed', 'error': 7},
            'message null': {'type': 'turn.failed', 'error': {'message': None}},
            'message missing': {'type': 'turn.failed', 'error': {}},
            'message number': {'type': 'turn.failed', 'error': {'message': 7}},
        }
        for name, data in cases.items():
            with self.subTest(case=name):
                session = await self.start(FakeProcess([STARTED, TURN_STARTED, line(data)],
                                                       exit_code=1))
                self.assertEqual(await self.turn(session),
                                 [TurnStarted(), failed('turn failed')])

    async def test_reason_with_the_token_is_redacted_and_cut(self) -> None:
        reason = f'proxy refused {TOKEN} ' + 'x' * 1_000
        host = self.host(secrets={'agent-token': f' {TOKEN}\n'})
        lines = [STARTED, TURN_STARTED,
                 line({'type': 'turn.failed', 'error': {'message': reason}})]
        session = await self.start(FakeProcess(lines), host=host)
        result = (await self.turn(session))[-1]
        self.assertEqual(result.state, TurnState.FAILED)
        self.assertTrue(result.reason.startswith('proxy refused [redacted] x'))
        self.assertEqual(len(result.reason), 500)
        self.assertNotIn(TOKEN, result.reason)

    async def test_error_line_is_the_reason_of_an_exit_before_turn_started(self) -> None:
        session = await self.start(FakeProcess(stream_lines('synthetic-error'), exit_code=1),
                                   FakeProcess(stream_lines('first-turn')))
        message = ('Not inside a trusted directory and --skip-git-repo-check was not '
                   'specified.')
        self.assertEqual(await self.turn(session), [failed(message)])
        self.assertEqual(await self.turn(session, 'again'), FIRST_TURN)
        self.assertEqual(self.spawn.argvs, [first_argv(), first_argv()])

    async def test_error_line_reason_is_redacted(self) -> None:
        lines = [line({'type': 'error', 'message': f'bad key {TOKEN}'})]
        session = await self.start(FakeProcess(lines, exit_code=1),
                                   host=self.host(secrets={'agent-token': TOKEN}))
        self.assertEqual(await self.turn(session), [failed('bad key [redacted]')])

    async def test_error_line_without_a_string_message_is_ignored(self) -> None:
        for name, data in {'null': {'type': 'error', 'message': None},
                           'missing': {'type': 'error'},
                           'number': {'type': 'error', 'message': 7}}.items():
            with self.subTest(case=name):
                session = await self.start(FakeProcess([line(data)], exit_code=2))
                self.assertEqual(await self.turn(session),
                                 [failed('codex exited with code 2')])

    async def test_exit_before_turn_started_fails_the_turn_only(self) -> None:
        session = await self.start(FakeProcess([STARTED], exit_code=3),
                                   FakeProcess(stream_lines('resume')))
        self.assertEqual(await self.turn(session), [failed('codex exited with code 3')])
        self.assertEqual(await self.turn(session, 'again'), RESUME)
        self.assertEqual(self.spawn.argvs, [first_argv(), resume_argv()])

    async def test_exit_without_any_line_fails_the_turn(self) -> None:
        session = await self.start(FakeProcess([], exit_code=127),
                                   FakeProcess(stream_lines('first-turn')))
        self.assertEqual(await self.turn(session), [failed('codex exited with code 127')])
        self.assertEqual(await self.turn(session, 'again'), FIRST_TURN)
        self.assertEqual(self.spawn.argvs, [first_argv(), first_argv()])

    async def test_exit_after_turn_started_without_a_result_ends_the_session(self) -> None:
        for code in (1, 0):
            with self.subTest(code=code):
                lines = stream_lines('first-turn')[:3]
                session = await self.start(FakeProcess(lines, exit_code=code))
                await session.send(MESSAGE, DispatchMode.QUEUE)
                self.assertEqual(await self.events.end(),
                                 [TurnStarted(), TextOut(INTRO), Exited(code)])
                with self.assertRaises(SessionClosed):
                    await session.send('again', DispatchMode.QUEUE)
                self.assertEqual(len(self.spawn.calls), 1)


class LinesTest(SessionTest):
    async def test_malformed_lines_are_skipped(self) -> None:
        session = await self.start(FakeProcess(stream_lines('synthetic-malformed')))
        self.assertEqual(await self.turn(session),
                         [TurnStarted(), TextOut('OK'), completed('OK')])

    async def test_a_line_of_the_limit_is_parsed_and_a_longer_one_dropped(self) -> None:
        exact = message_line('a' * 100)
        limit = len(exact)
        longer = message_line('b' * 101, 'item_1')
        session = await self.start(
            FakeProcess([STARTED, TURN_STARTED, exact, longer, TURN_COMPLETED]),
            line_limit=limit,
        )
        self.assertEqual(await self.turn(session),
                         [TurnStarted(), TextOut('a' * 100), completed('a' * 100)])

    async def test_a_long_line_in_pieces_is_dropped(self) -> None:
        huge = message_line('c' * 200_000)
        session = await self.start(
            FakeProcess([STARTED, TURN_STARTED, huge, message_line('OK'), TURN_COMPLETED]),
            line_limit=100_000,
        )
        self.assertEqual(await self.turn(session),
                         [TurnStarted(), TextOut('OK'), completed('OK')])

    async def test_last_line_without_a_newline_is_parsed(self) -> None:
        process = FakeProcess([STARTED, TURN_STARTED, message_line('OK')], stay=True)
        session = await self.start(process)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(TextOut)
        process.stdout.feed_data(TURN_COMPLETED)
        process.exit(0)
        self.assertEqual(await self.events.take(TurnResult), [completed('OK')])


class InterruptTest(SessionTest):
    async def test_interrupt_without_a_turn_does_nothing(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')))
        await session.interrupt()
        self.assertEqual(self.spawn.calls, [])
        await self.turn(session)
        await session.interrupt()
        await settle()
        self.assertEqual(self.process().group_sent(), [])
        self.assertEqual(self.process().signals, [])
        self.assertEqual(self.events.new(), [])

    async def test_interrupt_during_a_turn_stops_it_and_stays_open(self) -> None:
        lines = stream_lines('first-turn')
        session = await self.start(FakeProcess(lines[:4], stay=True),
                                   FakeProcess(stream_lines('resume')))
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(ToolUse)
        await asyncio.wait_for(session.interrupt(), 1)
        self.assertEqual(self.process().group_sent(), [signal.SIGTERM])
        self.assertEqual(self.process().signals, [])
        self.assertEqual(self.events.new(), [stopped(INTRO)])
        self.assertEqual(await self.turn(session, 'next'), RESUME)
        self.assertEqual(self.spawn.argvs, [first_argv(), resume_argv()])

    async def test_interrupt_before_turn_started(self) -> None:
        session = await self.start(FakeProcess([STARTED], stay=True))
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await settle()
        await asyncio.wait_for(session.interrupt(), 1)
        self.assertEqual(self.events.new(), [stopped(None)])

    async def test_a_process_that_ignores_sigterm_gets_sigkill(self) -> None:
        process = FakeProcess(stream_lines('first-turn')[:3], stay=True, ignore_terminate=True)
        times: list[tuple[int, float]] = []
        loop = asyncio.get_running_loop()

        def killpg(group: int, signum: int) -> None:
            if signum != 0:
                times.append((signum, loop.time()))
            self.spawn.killpg(group, signum)

        session = await self.start(process, close_timeout=0.1, killpg=killpg)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(TextOut)
        await asyncio.wait_for(session.interrupt(), 1)
        self.assertEqual(process.group_sent(), [signal.SIGTERM, signal.SIGKILL])
        (_, term_at), (_, kill_at) = times
        self.assertGreaterEqual(kill_at - term_at, 0.09)
        self.assertEqual(self.events.new(), [stopped(INTRO)])
        self.assertEqual(process.returncode, -signal.SIGKILL)

    async def test_without_a_group_the_process_is_signalled(self) -> None:
        cases = {
            'own group': {'group': os.getpgrp()},
            'init group': {'group': 1},
            'lookup fails': {'group_error': ProcessLookupError()},
        }
        for name, options in cases.items():
            with self.subTest(case=name):
                process = FakeProcess(stream_lines('first-turn')[:3], stay=True,
                                      ignore_terminate=True, **options)
                session = await self.start(process)
                await session.send(MESSAGE, DispatchMode.QUEUE)
                await self.events.take(TextOut)
                await asyncio.wait_for(session.interrupt(), 1)
                self.assertEqual(process.signals, ['terminate', 'kill'])
                self.assertEqual(process.group_signals, [])
                self.assertEqual(self.events.new(), [stopped(INTRO)])

    async def test_two_interrupts_signal_once(self) -> None:
        process = FakeProcess(stream_lines('first-turn')[:3], stay=True, ignore_terminate=True)
        session = await self.start(process, close_timeout=0.1)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(TextOut)
        await asyncio.wait_for(asyncio.gather(session.interrupt(), session.interrupt()), 1)
        self.assertEqual(process.group_sent(), [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual(self.events.new(), [stopped(INTRO)])

    async def test_interrupt_mode_stops_the_turn_then_starts_the_message(self) -> None:
        lines = stream_lines('first-turn')
        session = await self.start(FakeProcess(lines[:3], stay=True),
                                   FakeProcess(stream_lines('resume')))
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(TextOut)
        await asyncio.wait_for(session.send('stop and do this', DispatchMode.INTERRUPT), 1)
        self.assertEqual(self.process().group_sent(), [signal.SIGTERM])
        self.assertEqual(await self.events.take(TurnResult), [stopped(INTRO)])
        self.assertEqual(await self.events.take(TurnResult), RESUME)
        self.assertEqual(self.spawn.argvs, [first_argv(), resume_argv()])
        self.assertEqual(bytes(self.process(1).stdin.data), b'stop and do this')

    async def test_interrupt_mode_without_a_turn_just_starts(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')))
        self.assertEqual(await self.turn(session, MESSAGE, DispatchMode.INTERRUPT), FIRST_TURN)


class QueueTest(SessionTest):
    async def test_queue_and_steer_wait_for_the_turn_result(self) -> None:
        lines = stream_lines('first-turn')
        first = FakeProcess(lines[:3], stay=True)
        session = await self.start(first, FakeProcess(stream_lines('resume')),
                                   FakeProcess(stream_lines('resume')))
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(TextOut)
        await session.send('two', DispatchMode.QUEUE)
        await session.send('three', DispatchMode.STEER)
        await settle()
        self.assertEqual(len(self.spawn.calls), 1)
        first.feed(*lines[3:])
        first.exit(0)
        self.assertEqual(await self.events.take(TurnResult), FIRST_TURN[2:])
        self.assertEqual(await self.events.take(TurnResult), RESUME)
        self.assertEqual(await self.events.take(TurnResult), RESUME)
        self.assertEqual(self.spawn.argvs, [first_argv(), resume_argv(), resume_argv()])
        self.assertEqual([bytes(process.stdin.data) for process in self.spawn.processes],
                         [MESSAGE.encode(), b'two', b'three'])
        self.assertEqual(first.group_sent(), [])

    async def test_next_turn_waits_for_the_last_process_then_ends_it(self) -> None:
        cases = [('exits on sigterm', False, [signal.SIGTERM]),
                 ('ignores sigterm', True, [signal.SIGTERM, signal.SIGKILL])]
        for name, ignore, expected in cases:
            with self.subTest(case=name):
                lingering = FakeProcess(stream_lines('first-turn'), stay=True,
                                        ignore_terminate=ignore)
                session = await self.start(lingering, FakeProcess(stream_lines('resume')),
                                           close_timeout=0.1)
                self.assertEqual(await self.turn(session), FIRST_TURN)
                loop = asyncio.get_running_loop()
                began = loop.time()
                await asyncio.wait_for(session.send('again', DispatchMode.QUEUE), 2)
                self.assertGreaterEqual(loop.time() - began, 0.09)
                self.assertEqual(lingering.group_sent(), expected)
                self.assertIsNotNone(lingering.returncode)
                self.assertEqual(await self.events.take(TurnResult), RESUME)
                self.assertEqual(self.events.new(), [])

    async def test_a_process_that_exits_in_time_is_not_signalled(self) -> None:
        late = FakeProcess(stream_lines('first-turn'), stay=True)
        session = await self.start(late, FakeProcess(stream_lines('resume')), close_timeout=1)
        await self.turn(session)
        sending = asyncio.create_task(session.send('again', DispatchMode.QUEUE))
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.spawn.calls), 1)
        late.exit(0)
        await asyncio.wait_for(sending, 1)
        self.assertEqual(late.group_sent(), [])
        self.assertEqual(await self.events.take(TurnResult), RESUME)


class SpawnErrorTest(SessionTest):
    async def test_spawn_error_raises_and_opens_no_turn(self) -> None:
        missing = FileNotFoundError(errno.ENOENT, 'No such file or directory', 'codex')
        session = await self.start(missing, FakeProcess(stream_lines('first-turn')))
        with self.assertRaises(RunnerStartError) as caught:
            await session.send(MESSAGE, DispatchMode.QUEUE)
        self.assertEqual(str(caught.exception), 'could not start codex: No such file or directory')
        self.assertIs(caught.exception.__cause__, missing)
        self.assertEqual(self.events.new(), [])
        self.assertEqual(await self.turn(session), FIRST_TURN)
        self.assertEqual(self.spawn.argvs, [first_argv(), first_argv()])

    async def test_text_holds_no_environment_value_or_token(self) -> None:
        codex_bin = f'/opt/{TOKEN}/codex'
        host = self.host(
            environment={'HOME': '/home/someone-secret', 'PATH': '/usr/local/secret-bin'},
            settings={'codex_bin': codex_bin, 'codex_home': '/home/someone-secret/.codex'},
            secrets={'agent-token': TOKEN},
        )
        error = PermissionError(errno.EACCES, 'Permission denied', codex_bin)
        session = await self.start(error, host=host)
        with self.assertRaises(RunnerStartError) as caught:
            await session.send(MESSAGE, DispatchMode.QUEUE)
        text = str(caught.exception)
        self.assertEqual(text, 'could not start /opt/[redacted]/codex: Permission denied')
        for secret in (TOKEN, 'someone-secret', 'secret-bin', 'lynx'):
            self.assertNotIn(secret, text)

    async def test_text_names_another_filename_and_a_bare_error(self) -> None:
        cases = [
            (FileNotFoundError(errno.ENOENT, 'No such file or directory', '/usr/bin/node'),
             'could not start codex: No such file or directory: /usr/bin/node'),
            (OSError(), 'could not start codex: OSError'),
        ]
        for error, expected in cases:
            with self.subTest(error=expected):
                session = await self.start(error)
                with self.assertRaises(RunnerStartError) as caught:
                    await session.send(MESSAGE, DispatchMode.QUEUE)
                self.assertEqual(str(caught.exception), expected)

    async def test_long_text_is_cut_to_500(self) -> None:
        codex_bin = '/opt/' + 'x' * 1_000
        session = await self.start(OSError(errno.ENOENT, 'No such file or directory'),
                                   host=self.host(settings={'codex_bin': codex_bin}))
        with self.assertRaises(RunnerStartError) as caught:
            await session.send(MESSAGE, DispatchMode.QUEUE)
        self.assertEqual(len(str(caught.exception)), 500)

    async def test_held_message_that_cannot_spawn_fails_its_turn(self) -> None:
        first = FakeProcess(stream_lines('first-turn'), stay=True)
        missing = FileNotFoundError(errno.ENOENT, 'No such file or directory', 'codex')
        session = await self.start(first, missing)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await session.send('two', DispatchMode.QUEUE)
        self.assertEqual(await self.events.take(TurnResult), FIRST_TURN)
        first.exit(0)
        self.assertEqual(await self.events.take(TurnResult),
                         [failed('could not start codex: No such file or directory')])


class CloseTest(SessionTest):
    async def test_close_without_a_turn(self) -> None:
        session = await self.start()
        await asyncio.wait_for(session.close(), 1)
        self.assertEqual(await self.events.end(), [Exited(None)])
        self.assertEqual(self.spawn.calls, [])
        with self.assertRaises(SessionClosed):
            await session.send(MESSAGE, DispatchMode.QUEUE)
        await session.interrupt()

    async def test_close_after_a_finished_turn(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')))
        await self.turn(session)
        await eventually(lambda: self.process().returncode == 0)
        await asyncio.sleep(0.05)
        await asyncio.wait_for(session.close(), 1)
        self.assertEqual(await self.events.end(), [Exited(None)])

    async def test_close_during_a_turn_ends_the_group(self) -> None:
        process = FakeProcess(stream_lines('first-turn')[:3], stay=True)
        session = await self.start(process)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(TextOut)
        await asyncio.wait_for(session.close(), 1)
        self.assertEqual(process.group_sent(), [signal.SIGTERM])
        self.assertEqual(await self.events.end(), [Exited(-signal.SIGTERM)])

    async def test_close_kills_after_the_timeout(self) -> None:
        process = FakeProcess(stream_lines('first-turn')[:3], stay=True, ignore_terminate=True)
        session = await self.start(process, close_timeout=0.1)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(TextOut)
        loop = asyncio.get_running_loop()
        began = loop.time()
        await asyncio.wait_for(session.close(), 1)
        self.assertGreaterEqual(loop.time() - began, 0.09)
        self.assertEqual(process.group_sent(), [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual(await self.events.end(), [Exited(-signal.SIGKILL)])

    async def test_a_second_close_gives_no_second_event(self) -> None:
        process = FakeProcess(stream_lines('first-turn')[:3], stay=True)
        session = await self.start(process)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await self.events.take(TextOut)
        await asyncio.wait_for(asyncio.gather(session.close(), session.close()), 1)
        await asyncio.wait_for(session.close(), 1)
        self.assertEqual(process.group_sent(), [signal.SIGTERM])
        self.assertEqual(await self.events.end(), [Exited(-signal.SIGTERM)])

    async def test_close_waits_for_a_process_after_its_result(self) -> None:
        lingering = FakeProcess(stream_lines('first-turn'), stay=True)
        session = await self.start(lingering, close_timeout=0.1)
        await self.turn(session)
        await asyncio.wait_for(session.close(), 1)
        self.assertEqual(lingering.group_sent(), [signal.SIGTERM])
        self.assertEqual(await self.events.end(), [Exited(-signal.SIGTERM)])

    async def test_close_drops_held_messages(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')[:3], stay=True))
        await session.send(MESSAGE, DispatchMode.QUEUE)
        await session.send('two', DispatchMode.QUEUE)
        await self.events.take(TextOut)
        await asyncio.wait_for(session.close(), 1)
        await settle()
        self.assertEqual(len(self.spawn.calls), 1)
        self.assertEqual(await self.events.end(), [Exited(-signal.SIGTERM)])

    async def test_close_after_a_crash_gives_no_second_event(self) -> None:
        session = await self.start(FakeProcess(stream_lines('first-turn')[:2], exit_code=1))
        await session.send(MESSAGE, DispatchMode.QUEUE)
        self.assertEqual(await self.events.end(), [TurnStarted(), Exited(1)])
        await asyncio.wait_for(session.close(), 1)
        self.assertEqual(self.events.new(), [])


class GroupTest(SessionTest):
    async def test_what_is_left_of_the_group_is_ended_after_codex_exits(self) -> None:
        cases = [
            ('no member left', False, False, []),
            ('member ends on sigterm', True, False, [signal.SIGTERM]),
            ('member ignores sigterm', True, True, [signal.SIGTERM, signal.SIGKILL]),
        ]
        for name, grandchild, ignore, expected in cases:
            with self.subTest(case=name):
                process = FakeProcess(stream_lines('first-turn'), grandchild=grandchild,
                                      ignore_terminate=ignore)
                session = await self.start(process, FakeProcess(stream_lines('resume')))
                self.assertEqual(await self.turn(session), FIRST_TURN)
                await self.turn(session, 'again')
                self.assertEqual(process.returncode, 0)
                self.assertEqual(process.group_sent(), expected)
                self.assertEqual(process.group_signals[0], (FIRST_PID, 0))
                self.assertFalse(process.grandchild)
                self.assertEqual(process.signals, [])


class RealProcessTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name).resolve()
        self.workspace = root / 'workspace'
        self.workspace.mkdir()
        self.data_dir = root / 'data'
        self.data_dir.mkdir()
        (root / 'bin').mkdir()
        self.codex_bin = root / 'bin' / 'codex'
        self.codex_bin.write_text(f'#!{sys.executable} -I\n' + FAKE_CODEX.read_text())
        self.codex_bin.chmod(0o700)
        node = root / 'bin' / 'node'
        node.write_text('#!/bin/sh\n')
        node.chmod(0o700)
        (self.workspace / 'turn-0.jsonl').write_bytes((STREAMS / 'first-turn.jsonl').read_bytes())
        (self.workspace / 'turn-1.jsonl').write_bytes((STREAMS / 'resume.jsonl').read_bytes())
        self.thread = make_thread(id=str(uuid.uuid4()), workspace_path=str(self.workspace))
        self.host = FakeHost(
            str(self.data_dir),
            environment={
                'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
                'HOME': str(root),
                'LANG': 'C.UTF-8',
                'ANTHROPIC_AUTH_TOKEN': 'host-token',
            },
            settings={'codex_bin': str(self.codex_bin), 'codex_home': str(root / 'codex-home')},
            secrets={'agent-token': TOKEN},
        )
        self.addCleanup(self.kill_grandchild)

    def kill_grandchild(self) -> None:
        with contextlib.suppress(FileNotFoundError, ValueError, ProcessLookupError):
            os.kill(int((self.workspace / 'grandchild.pid').read_text()), signal.SIGKILL)

    def spawned(self, number: int) -> dict:
        return json.loads((self.workspace / f'spawned-{number}.json').read_text())

    async def start(self, **options: object) -> tuple[RunnerSession, Events]:
        session = await CodexRunner(**options).start(self.thread, self.host)
        events = Events(session)
        self.addAsyncCleanup(session.close)
        return session, events

    async def test_two_turns_see_argv_cwd_env_and_stdin(self) -> None:
        session, events = await self.start()
        await session.send(MESSAGE, DispatchMode.QUEUE)
        self.assertEqual(await events.take(TurnResult, timeout=10), FIRST_TURN)
        await session.send('Reply with the word you remembered', DispatchMode.QUEUE)
        self.assertEqual(await events.take(TurnResult, timeout=10), RESUME)
        await asyncio.wait_for(session.close(), 10)
        self.assertIn(await events.end(), ([Exited(None)], [Exited(0)]))
        workspace = str(self.workspace)
        codex_bin = str(self.codex_bin)
        self.assertEqual(self.spawned(0)['argv'][1:],
                         first_argv(workspace=workspace, codex_bin=codex_bin)[1:])
        self.assertEqual(self.spawned(1)['argv'][1:],
                         resume_argv(workspace=workspace, codex_bin=codex_bin)[1:])
        env = codex_env(self.thread, self.host, codex_bin)
        self.assertEqual(env['PATH'].split(os.pathsep)[-1], str(self.codex_bin.parent))
        self.assertEqual(env['CLIPROXY_API_KEY'], TOKEN)
        for number in (0, 1):
            spawned = self.spawned(number)
            self.assertEqual(spawned['cwd'], workspace)
            self.assertEqual(spawned['env'], env)
            self.assertEqual(spawned['pgid'], spawned['pid'])
            self.assertNotEqual(spawned['pgid'], os.getpgrp())
        self.assertEqual((self.workspace / 'stdin-0.txt').read_bytes(), MESSAGE.encode())
        record = self.data_dir / 'codex' / self.thread.id
        self.assertEqual(record.read_bytes(), f'{CODEX_ID}\n'.encode())

    async def test_a_grandchild_is_ended_after_codex_exits(self) -> None:
        (self.workspace / 'grandchild').touch()
        session, events = await self.start(close_timeout=0.5)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        self.assertEqual(await events.take(TurnResult, timeout=10), FIRST_TURN)
        pid = int((self.workspace / 'grandchild.pid').read_text())
        await eventually(lambda: is_gone(pid), timeout=5)
        await asyncio.sleep(0.1)
        self.assertEqual(events.new(), [])

    async def test_interrupt_ends_a_hanging_turn(self) -> None:
        (self.workspace / 'hang-0').touch()
        (self.workspace / 'turn-0.jsonl').write_bytes(b''.join(
            raw + b'\n' for raw in stream_lines('first-turn')[:3]))
        session, events = await self.start(close_timeout=5.0)
        await session.send(MESSAGE, DispatchMode.QUEUE)
        self.assertEqual(await events.take(TextOut, timeout=10), FIRST_TURN[:2])
        await asyncio.wait_for(session.interrupt(), 10)
        self.assertEqual(events.new(), [stopped(INTRO)])
        await session.send('Reply with the word you remembered', DispatchMode.QUEUE)
        self.assertEqual(await events.take(TurnResult, timeout=10), RESUME)
        self.assertEqual(self.spawned(1)['argv'][1:], resume_argv(
            workspace=str(self.workspace), codex_bin=str(self.codex_bin))[1:])

    async def test_missing_binary_raises_runner_start_error(self) -> None:
        missing = str(self.codex_bin.parent / 'missing')
        self.host.settings = {'codex_bin': missing}
        session, _ = await self.start()
        with self.assertRaises(RunnerStartError) as caught:
            await session.send(MESSAGE, DispatchMode.QUEUE)
        self.assertIsInstance(caught.exception.__cause__, FileNotFoundError)
        self.assertIn(missing, str(caught.exception))
        self.assertNotIn(TOKEN, str(caught.exception))


class IsGoneTest(unittest.TestCase):
    def test_a_process_that_exits_mid_read_is_gone(self) -> None:
        with mock.patch.object(Path, 'read_text', side_effect=ProcessLookupError):
            self.assertTrue(is_gone(1))

    def test_a_process_with_no_stat_file_is_gone(self) -> None:
        with mock.patch.object(Path, 'read_text', side_effect=FileNotFoundError):
            self.assertTrue(is_gone(1))

    def test_only_a_zombie_stat_is_gone(self) -> None:
        with mock.patch.object(Path, 'read_text', return_value='1 (sh) Z 0'):
            self.assertTrue(is_gone(1))
        with mock.patch.object(Path, 'read_text', return_value='1 (sh) S 0'):
            self.assertFalse(is_gone(1))


if __name__ == '__main__':
    unittest.main()
