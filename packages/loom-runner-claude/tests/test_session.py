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
from pathlib import Path
from unittest import mock

import loom_runner_claude
from loom_contracts.runners import (
    Capability,
    Decision,
    DispatchMode,
    Exited,
    MessageEntered,
    PermissionRequest,
    RequestAnswer,
    Runner,
    RunnerSession,
    TextOut,
    ToolUse,
    TurnResult,
    TurnStarted,
)
from loom_contracts.states import TurnState
from loom_runner_claude import ClaudeRunner, RunnerStartError, SessionClosed
from loom_runner_claude.command import claude_argv, claude_env
from loom_runner_claude.stream import allow_line, deny_line, interrupt_line, user_line

from .replay import (
    FAKE_PID,
    STREAMS,
    THREAD_ID,
    FakeHost,
    FakeProcess,
    FakeSpawn,
    changed,
    make_thread,
    stream_lines,
    stream_turns,
)

FAKE_CLAUDE = Path(__file__).resolve().parent / 'fake_claude.py'

UNREADABLE = 'Loom could not read this permission request.'

A_TXT_ID = '2038e810-2aac-41ec-94f3-f181dcdbc712'
A_TXT_INPUT = {'command': 'echo spike-233 > a.txt', 'description': 'Write spike-233 to a.txt'}
B_TXT_ID = 'ac794f47-7556-4f5f-a8a6-2cec64a05505'

TOKEN = 'loom-test-token-3f9c2a7e'


def completed(text: str) -> TurnResult:
    return TurnResult(TurnState.COMPLETED, text, None)


def ok_turn(text: str) -> list[object]:
    return [TurnStarted(), TextOut(text), completed(text)]


STOPPED = TurnResult(TurnState.STOPPED, None, 'aborted_streaming')


async def next_event(session: RunnerSession, timeout: float = 1.0) -> object:
    return await asyncio.wait_for(anext(session.events), timeout)


async def events_until(session: RunnerSession, kind: type, timeout: float = 1.0) -> list[object]:
    events = []
    while True:
        events.append(await next_event(session, timeout))
        if isinstance(events[-1], kind):
            return events


async def remaining(session: RunnerSession, timeout: float = 2.0) -> list[object]:
    async def collect() -> list[object]:
        return [event async for event in session.events]

    return await asyncio.wait_for(collect(), timeout)


async def eventually(check: object, timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not check():
        if loop.time() > deadline:
            raise AssertionError('the condition never held')
        await asyncio.sleep(0.001)


async def settle() -> None:
    for _ in range(20):
        await asyncio.sleep(0)


def result_line(text: str) -> bytes:
    data = {'type': 'result', 'is_error': False, 'result': text}
    return json.dumps(data, separators=(',', ':')).encode()



def is_interrupt(line: bytes) -> bool:
    data = json.loads(line)
    return line == interrupt_line(data.get('request_id'))


def is_gone(pid: int) -> bool:
    try:
        text = Path(f'/proc/{pid}/stat').read_text()
    except (FileNotFoundError, ProcessLookupError):
        return True
    return text[text.rindex(')') + 2] == 'Z'


class SessionTest(unittest.IsolatedAsyncioTestCase):
    async def start(
        self,
        process: FakeProcess | None = None,
        thread: object = None,
        host: FakeHost | None = None,
        **options: object,
    ) -> RunnerSession:
        self.process = process or FakeProcess()
        self.spawn = FakeSpawn(self.process)
        options = {
            'close_timeout': 0.05,
            'interrupt_timeout': 0.2,
            'exit_grace': 0.05,
            'getpgid': self.process.getpgid,
            'killpg': self.process.killpg,
            **options,
        }
        runner = ClaudeRunner(spawn=self.spawn, **options)
        session = await runner.start(thread or make_thread(), host or FakeHost())
        self.addAsyncCleanup(self.finish, session, self.process)
        return session

    async def finish(self, session: RunnerSession, process: FakeProcess) -> None:
        process.exit(0)
        await remaining(session)

    @property
    def written(self) -> list[bytes]:
        return self.process.stdin.lines


class ProtocolTest(SessionTest):
    async def test_runner_and_session_meet_the_protocols(self) -> None:
        runner = ClaudeRunner()
        self.assertIsInstance(runner, Runner)
        self.assertEqual(runner.name, 'claude')
        self.assertEqual(
            runner.capabilities,
            frozenset({'steer', 'interrupt', 'resume', 'permission_requests'}),
        )
        self.assertEqual(runner.capabilities, frozenset(Capability))
        session = await self.start()
        self.assertIsInstance(session, RunnerSession)

    def test_package_exports(self) -> None:
        self.assertEqual(
            sorted(loom_runner_claude.__all__),
            ['ClaudeRunner', 'RunnerStartError', 'SessionClosed'],
        )
        self.assertIs(loom_runner_claude.ClaudeRunner, ClaudeRunner)


class StartTest(SessionTest):
    async def test_spawns_once_with_argv_cwd_and_env(self) -> None:
        host = FakeHost(
            environment={
                'PATH': '/usr/bin:/bin',
                'HOME': '/home/someone',
                'CLAUDECODE': '1',
                'ANTHROPIC_AUTH_TOKEN': 'host-token',
            },
            settings={'claude_config_dir': '/tmp/loom-replay/config', 'agent_base_url': 'http://x'},
            secrets={'agent-token': ' agent-token-value\n'},
        )
        thread = make_thread()
        await self.start(thread=thread, host=host)
        self.assertEqual(self.spawn.calls, [(
            claude_argv(thread, 'claude'),
            thread.workspace_path,
            claude_env(thread, host),
            8_388_608,
        )])

    async def test_claude_bin_setting(self) -> None:
        cases = [
            ('/opt/claude/bin/claude', '/opt/claude/bin/claude'),
            ('', 'claude'),
            (None, 'claude'),
            (7, 'claude'),
            (['/opt/claude'], 'claude'),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                await self.start(host=FakeHost(settings={'claude_bin': value}))
                self.assertEqual(len(self.spawn.calls), 1)
                self.assertEqual(self.spawn.calls[0][0], claude_argv(make_thread(), expected))

    async def test_start_writes_nothing_and_does_not_wait_for_init(self) -> None:
        await asyncio.wait_for(self.start(), 1)
        await settle()
        self.assertEqual(self.written, [])
        self.assertFalse(self.process.stdin.closed)
        self.assertEqual(self.process.signals, [])

    async def test_invalid_thread_id_raises_and_spawns_nothing(self) -> None:
        for bad in ['not-a-uuid', THREAD_ID.upper(), f'{{{THREAD_ID}}}', '', '--resume']:
            with self.subTest(id=bad):
                spawn = FakeSpawn(FakeProcess())
                with self.assertRaises(ValueError):
                    await ClaudeRunner(spawn=spawn).start(make_thread(id=bad), FakeHost())
                self.assertEqual(spawn.calls, [])

    async def test_spawn_os_error_becomes_runner_start_error(self) -> None:
        binary = '/opt/missing/claude'
        errors = [
            FileNotFoundError(2, 'No such file or directory', binary),
            PermissionError(13, 'Permission denied', binary),
        ]
        for error in errors:
            with self.subTest(error=type(error).__name__):
                spawn = FakeSpawn(error=error)
                host = FakeHost(settings={'claude_bin': binary})
                with self.assertRaises(RunnerStartError) as caught:
                    await ClaudeRunner(spawn=spawn).start(make_thread(), host)
                self.assertIn(binary, str(caught.exception))
                self.assertIs(caught.exception.__cause__, error)
                self.assertEqual(len(spawn.calls), 1)
        spawn = FakeSpawn(error=FileNotFoundError())
        with self.assertRaises(RunnerStartError) as caught:
            await ClaudeRunner(spawn=spawn).start(make_thread(), FakeHost())
        self.assertIn('claude', str(caught.exception))

    async def start_error(self, error: OSError, claude_bin: str | None = None) -> RunnerStartError:
        settings = {} if claude_bin is None else {'claude_bin': claude_bin}
        host = FakeHost(settings=settings, secrets={'agent-token': f' {TOKEN}\n'})
        spawn = FakeSpawn(error=error)
        with self.assertRaises(RunnerStartError) as caught:
            await ClaudeRunner(spawn=spawn).start(make_thread(), host)
        self.assertEqual(spawn.calls[0][2]['ANTHROPIC_AUTH_TOKEN'], TOKEN)
        self.assertIs(caught.exception.__cause__, error)
        return caught.exception

    async def test_token_in_the_os_error_filename_is_redacted(self) -> None:
        error = FileNotFoundError(2, 'No such file or directory', f'/work/{TOKEN}/repo')
        text = str(await self.start_error(error))
        self.assertEqual(
            text, 'could not start claude: No such file or directory: /work/[redacted]/repo',
        )

    async def test_token_in_claude_bin_is_redacted(self) -> None:
        binary = f'/opt/{TOKEN}/claude'
        error = PermissionError(13, 'Permission denied', binary)
        text = str(await self.start_error(error, binary))
        self.assertEqual(text, 'could not start /opt/[redacted]/claude: Permission denied')

    async def test_long_claude_bin_is_cut_to_500(self) -> None:
        binary = '/opt/' + 'c' * 1_995
        self.assertEqual(len(binary), 2_000)
        error = FileNotFoundError(2, 'No such file or directory', binary)
        text = str(await self.start_error(error, binary))
        self.assertEqual(len(text), 500)
        self.assertTrue(text.startswith('could not start /opt/ccc'))

    async def test_filename_other_than_claude_bin_is_named(self) -> None:
        error = FileNotFoundError(2, 'No such file or directory', '/tmp/loom-replay/missing')
        text = str(await self.start_error(error, '/opt/claude'))
        self.assertEqual(
            text,
            'could not start /opt/claude: No such file or directory: /tmp/loom-replay/missing',
        )

    async def test_error_without_strerror_names_its_type(self) -> None:
        text = str(await self.start_error(FileNotFoundError()))
        self.assertEqual(text, 'could not start claude: FileNotFoundError')


class SendTest(SessionTest):
    async def test_send_while_idle_writes_one_user_line(self) -> None:
        message = 'Remember the word heron\nthen reply OK'
        for mode in DispatchMode:
            with self.subTest(mode=mode):
                session = await self.start()
                await session.send(message, mode)
                self.assertEqual(self.written, [user_line(message)])
                argv = self.spawn.calls[0][0]
                self.assertFalse([item for item in argv if 'heron' in item or '\n' in item])

    async def test_replay_turns(self) -> None:
        session = await self.start()
        messages = [
            'Remember the word heron',
            'Reply with the word you remembered',
            'Reply with OK',
        ]
        for message, turn, text in zip(messages, stream_turns('turns'), ['OK', 'heron', 'OK']):
            await session.send(message, DispatchMode.STEER)
            self.process.feed(*turn)
            self.assertEqual(await events_until(session, TurnResult), ok_turn(text))
        self.assertEqual(self.written, [user_line(message) for message in messages])
        await session.close()
        self.assertEqual(await remaining(session), [Exited(0)])
        self.assertEqual(self.process.signals, [])

    async def test_second_init_in_one_turn_gives_no_second_turn_started(self) -> None:
        session = await self.start()
        init, text, result = stream_turns('turns')[0]
        await session.send('Reply with OK', DispatchMode.STEER)
        self.process.feed(init, init, text, init, result)
        self.assertEqual(await events_until(session, TurnResult), ok_turn('OK'))

    async def test_replay_steer(self) -> None:
        lines = stream_lines('steer-replay')
        session = await self.start()
        first = 'Run `sleep 20` with Bash, then reply with the word first'
        await session.send(first, DispatchMode.STEER)
        self.process.feed(*lines[:3])
        events = await events_until(session, ToolUse)
        await session.send('Reply with the word second', DispatchMode.STEER)
        self.assertEqual(self.written, [user_line(first), user_line('Reply with the word second')])
        self.process.feed(*lines[3:])
        events += await events_until(session, TurnResult)
        replayed = json.loads(lines[1])['message']['content']
        self.assertTrue(replayed.startswith('[Test session for a spike'))
        self.assertTrue(replayed.endswith(first))
        self.assertEqual(events, [
            TurnStarted(),
            MessageEntered(replayed),
            ToolUse(
                'toolu_01SteerReplaySleep20Bash',
                'Bash',
                {'command': 'sleep 20', 'description': 'Wait 20 seconds'},
            ),
            MessageEntered('Reply with the word second'),
            TextOut('first\n\nsecond'),
            completed('first\n\nsecond'),
        ])
        await session.close()
        self.assertEqual(await remaining(session), [Exited(0)])

    async def test_queue_writes_one_message_per_result_in_order(self) -> None:
        turns = stream_turns('turns')
        session = await self.start()
        await session.send('a', DispatchMode.QUEUE)
        self.process.feed(*turns[0][:2])
        await events_until(session, TextOut)
        await session.send('b', DispatchMode.QUEUE)
        await session.send('c', DispatchMode.QUEUE)
        await settle()
        self.assertEqual(self.written, [user_line('a')])
        self.process.feed(turns[0][2])
        await events_until(session, TurnResult)
        await eventually(lambda: len(self.written) == 2)
        self.assertEqual(self.written, [user_line('a'), user_line('b')])
        self.process.feed(*turns[1])
        await events_until(session, TurnResult)
        await eventually(lambda: len(self.written) == 3)
        self.process.feed(*turns[2])
        await events_until(session, TurnResult)
        await settle()
        self.assertEqual(self.written, [user_line('a'), user_line('b'), user_line('c')])

    async def test_interrupt_mode_stops_the_turn_then_writes(self) -> None:
        lines = stream_lines('interrupt')
        session = await self.start()
        await session.send('Run `sleep 60` with Bash', DispatchMode.STEER)
        self.process.feed(*lines[:2])
        await events_until(session, ToolUse)
        await session.send('later', DispatchMode.QUEUE)
        sending = asyncio.create_task(session.send('now', DispatchMode.INTERRUPT))
        await eventually(lambda: len(self.written) == 2)
        self.assertTrue(is_interrupt(self.written[1]))
        await settle()
        self.assertFalse(sending.done())
        self.assertEqual(len(self.written), 2)
        self.process.feed(*lines[2:])
        await asyncio.wait_for(sending, 1)
        self.assertEqual(await events_until(session, TurnResult), [STOPPED])
        self.assertEqual(self.written[2:], [user_line('now')])
        self.process.feed(*stream_turns('turns')[0])
        await events_until(session, TurnResult)
        await eventually(lambda: len(self.written) == 4)
        self.assertEqual(self.written[3], user_line('later'))
        self.assertFalse(self.process.stdin.closed)

    async def test_interrupt_mode_without_a_result_closes(self) -> None:
        session = await self.start(interrupt_timeout=0.05)
        await session.send('go', DispatchMode.STEER)
        self.process.feed(stream_lines('interrupt')[0])
        await events_until(session, TurnStarted)
        with self.assertRaises(SessionClosed):
            await session.send('now', DispatchMode.INTERRUPT)
        self.assertEqual(len(self.written), 2)
        self.assertEqual(self.written[0], user_line('go'))
        self.assertTrue(is_interrupt(self.written[1]))
        self.assertNotIn(user_line('now'), self.written)
        self.assertTrue(self.process.stdin.closed)
        self.assertEqual(await remaining(session), [Exited(0)])

    async def test_any_os_error_on_write_raises_session_closed(self) -> None:
        for where in ('write', 'drain'):
            for code in (errno.EIO, errno.EBADF):
                with self.subTest(where=where, errno=errno.errorcode[code]):
                    session = await self.start()
                    error = OSError(code, os.strerror(code))
                    if where == 'write':
                        self.process.stdin.error = error
                    else:
                        self.process.stdin.drain_error = error
                    with self.assertRaises(SessionClosed) as caught:
                        await session.send('hi', DispatchMode.STEER)
                    self.assertIs(caught.exception.__cause__, error)

    async def test_reader_goes_on_after_an_os_error_on_its_own_write(self) -> None:
        turn = stream_turns('perm-tool')[0]
        bad = changed(turn[2], lambda data: data['request'].pop('tool_name'))
        session = await self.start()
        await session.send('Run it', DispatchMode.STEER)
        self.process.stdin.drain_error = OSError(errno.EIO, os.strerror(errno.EIO))
        self.process.feed(turn[0], bad, turn[4])
        events = await events_until(session, TextOut)
        self.assertEqual([type(event) for event in events], [TurnStarted, TextOut])
        self.assertEqual(self.written, [user_line('Run it'), deny_line(A_TXT_ID, UNREADABLE)])
        self.process.feed(result_line('done'))
        self.assertEqual(await next_event(session), completed('done'))
        self.process.exit(0)
        self.assertEqual(await remaining(session), [Exited(0)])


class RespondTest(SessionTest):
    async def open_request(self, session: RunnerSession, turn: list[bytes]) -> PermissionRequest:
        await session.send('Run `echo spike-233 > a.txt` with Bash', DispatchMode.STEER)
        self.process.feed(*turn[:3])
        return (await events_until(session, PermissionRequest))[-1]

    async def test_replay_perm_tool(self) -> None:
        turns = stream_turns('perm-tool')
        session = await self.start()
        request = await self.open_request(session, turns[0])
        self.assertEqual(
            request,
            PermissionRequest(A_TXT_ID, 'Bash', A_TXT_INPUT, 'Write spike-233 to a.txt'),
        )
        await session.respond(A_TXT_ID, RequestAnswer(Decision.ALLOW))
        self.assertEqual(self.written[-1], allow_line(A_TXT_ID, A_TXT_INPUT))
        recorded = json.loads(turns[0][2])['request']['input']
        sent = json.loads(self.written[-1])['response']['response']['updatedInput']
        self.assertEqual(sent, recorded)
        self.process.feed(*turns[0][3:])
        await events_until(session, TurnResult)
        request = await self.open_request(session, turns[1])
        self.assertEqual(request.request_id, B_TXT_ID)
        await session.respond(B_TXT_ID, RequestAnswer(Decision.DENY, 'Not b.txt.'))
        self.assertEqual(self.written[-1], deny_line(B_TXT_ID, 'Not b.txt.'))

    async def test_allow_sends_the_input_as_asked_after_the_event_changes(self) -> None:
        turn = stream_turns('perm-tool')[0]
        session = await self.start()
        request = await self.open_request(session, turn)
        request.input['command'] = 'rm -rf /'
        await session.respond(A_TXT_ID, RequestAnswer(Decision.ALLOW))
        self.assertEqual(self.written[-1], allow_line(A_TXT_ID, A_TXT_INPUT))
        recorded = json.loads(turn[2])['request']['input']
        sent = json.loads(self.written[-1])['response']['response']['updatedInput']
        self.assertEqual(sent, recorded)

    async def test_allow_sends_nested_input_as_asked_after_the_event_changes(self) -> None:
        turn = stream_turns('perm-tool')[0]
        nested = changed(
            turn[2],
            lambda data: data['request']['input'].update(options={'timeout': 5}),
        )
        asked = json.loads(nested)['request']['input']
        session = await self.start()
        request = await self.open_request(session, [turn[0], turn[1], nested])
        self.assertEqual(request.input, asked)
        request.input['options']['timeout'] = 600
        await session.respond(A_TXT_ID, RequestAnswer(Decision.ALLOW))
        self.assertEqual(self.written[-1], allow_line(A_TXT_ID, asked))
        sent = json.loads(self.written[-1])['response']['response']['updatedInput']
        self.assertEqual(sent, {**A_TXT_INPUT, 'options': {'timeout': 5}})

    async def test_deny_without_a_message(self) -> None:
        for message in (None, ''):
            with self.subTest(message=message):
                session = await self.start()
                await self.open_request(session, stream_turns('perm-tool')[0])
                await session.respond(A_TXT_ID, RequestAnswer(Decision.DENY, message))
                self.assertEqual(self.written[-1], deny_line(A_TXT_ID, 'Denied.'))

    async def test_unknown_or_answered_id_raises_key_error(self) -> None:
        session = await self.start()
        await self.open_request(session, stream_turns('perm-tool')[0])
        before = list(self.written)
        with self.assertRaises(KeyError):
            await session.respond('no-such-request', RequestAnswer(Decision.ALLOW))
        self.assertEqual(self.written, before)
        await session.respond(A_TXT_ID, RequestAnswer(Decision.DENY))
        with self.assertRaises(KeyError):
            await session.respond(A_TXT_ID, RequestAnswer(Decision.ALLOW))
        self.assertEqual(self.written, [*before, deny_line(A_TXT_ID, 'Denied.')])

    async def test_open_requests_end_with_the_turn(self) -> None:
        turn = stream_turns('perm-tool')[0]
        session = await self.start()
        await self.open_request(session, turn)
        self.process.feed(turn[-1])
        await events_until(session, TurnResult)
        before = list(self.written)
        with self.assertRaises(KeyError):
            await session.respond(A_TXT_ID, RequestAnswer(Decision.ALLOW))
        self.assertEqual(self.written, before)

    async def test_unreadable_request_is_denied_without_an_event(self) -> None:
        turn = stream_turns('perm-tool')[0]
        bad = changed(turn[2], lambda data: data['request'].pop('tool_name'))
        session = await self.start()
        await session.send('Run it', DispatchMode.STEER)
        self.process.feed(turn[0], bad, turn[4])
        events = await events_until(session, TextOut)
        self.assertEqual([type(event) for event in events], [TurnStarted, TextOut])
        self.assertEqual(self.written, [user_line('Run it'), deny_line(A_TXT_ID, UNREADABLE)])
        with self.assertRaises(KeyError):
            await session.respond(A_TXT_ID, RequestAnswer(Decision.ALLOW))


class InterruptTest(SessionTest):
    async def test_interrupt_without_a_turn_writes_nothing(self) -> None:
        session = await self.start()
        await session.interrupt()
        self.assertEqual(self.written, [])
        await session.send('Reply with OK', DispatchMode.STEER)
        self.process.feed(*stream_turns('turns')[0])
        await events_until(session, TurnResult)
        await session.interrupt()
        self.assertEqual(self.written, [user_line('Reply with OK')])
        self.assertFalse(self.process.stdin.closed)
        self.assertEqual(self.process.signals, [])

    async def test_interrupt_during_a_turn(self) -> None:
        lines = stream_lines('interrupt')
        session = await self.start()
        await session.send('Run `sleep 60` with Bash', DispatchMode.STEER)
        self.process.feed(*lines[:2])
        await events_until(session, ToolUse)
        first = asyncio.create_task(session.interrupt())
        await eventually(lambda: len(self.written) == 2)
        self.assertTrue(is_interrupt(self.written[1]))
        second = asyncio.create_task(session.interrupt())
        await settle()
        self.assertEqual(len(self.written), 2)
        self.assertFalse(first.done() or second.done())
        self.process.feed(*lines[2:])
        await asyncio.wait_for(asyncio.gather(first, second), 1)
        self.assertEqual(await events_until(session, TurnResult), [STOPPED])
        self.assertEqual(len(self.written), 2)
        self.assertFalse(self.process.stdin.closed)
        self.assertEqual(self.process.signals, [])
        await session.send('next', DispatchMode.STEER)
        self.assertEqual(self.written[-1], user_line('next'))

    async def test_replay_interrupt_background(self) -> None:
        lines = stream_lines('interrupt-background')
        ended_first = [*lines[:6], lines[7], lines[6], lines[8]]
        for name, order, closes in (('live', lines, True), ('ended', ended_first, False)):
            with self.subTest(task=name):
                session = await self.start()
                await session.send('Run `sleep 60` in the background', DispatchMode.STEER)
                self.process.feed(*order[:3])
                await events_until(session, ToolUse)
                stopping = asyncio.create_task(session.interrupt())
                await eventually(lambda: len(self.written) == 2)
                self.process.feed(*order[3:])
                await asyncio.wait_for(stopping, 1)
                self.assertEqual(await events_until(session, TurnResult), [STOPPED])
                self.assertEqual(self.process.stdin.closed, closes)
                self.assertEqual(self.process.signals, [])
                if closes:
                    self.assertEqual(await remaining(session), [Exited(0)])

    async def test_interrupt_without_a_result_closes(self) -> None:
        session = await self.start(interrupt_timeout=0.05)
        await session.send('go', DispatchMode.STEER)
        self.process.feed(stream_lines('interrupt')[0])
        await events_until(session, TurnStarted)
        await asyncio.wait_for(session.interrupt(), 1)
        self.assertEqual(len(self.written), 2)
        self.assertTrue(is_interrupt(self.written[1]))
        self.assertTrue(self.process.stdin.closed)
        self.assertEqual(await remaining(session), [Exited(0)])

    async def test_aborted_result_without_an_interrupt_fails(self) -> None:
        lines = stream_lines('interrupt')
        result = changed(lines[4], lambda data: data.update(terminal_reason='aborted_tools'))
        session = await self.start()
        await session.send('go', DispatchMode.STEER)
        self.process.feed(lines[0], result)
        self.assertEqual(await events_until(session, TurnResult), [
            TurnStarted(),
            TurnResult(TurnState.FAILED, None, 'aborted_tools'),
        ])


class CloseTest(SessionTest):
    async def test_close_on_eof_sends_no_signal(self) -> None:
        session = await self.start()
        await session.close()
        self.assertTrue(self.process.stdin.closed)
        self.assertEqual(self.process.signals, [])
        self.assertEqual(await remaining(session), [Exited(0)])

    async def test_close_terminates_after_the_timeout(self) -> None:
        session = await self.start(FakeProcess(ignore_eof=True), close_timeout=0.1)
        closing = asyncio.create_task(session.close())
        await asyncio.sleep(0.02)
        self.assertTrue(self.process.stdin.closed)
        self.assertEqual(self.process.signals, [])
        self.assertFalse(closing.done())
        await asyncio.wait_for(closing, 1)
        self.assertEqual(self.process.signals, [])
        self.assertEqual(self.process.group_sent(), [signal.SIGTERM])
        self.assertEqual(await remaining(session), [Exited(-signal.SIGTERM)])

    async def test_close_kills_after_a_second_timeout(self) -> None:
        process = FakeProcess(ignore_eof=True, ignore_terminate=True)
        session = await self.start(process, close_timeout=0.05)
        loop = asyncio.get_running_loop()
        began = loop.time()
        await asyncio.wait_for(session.close(), 1)
        self.assertGreaterEqual(loop.time() - began, 0.09)
        self.assertEqual(self.process.signals, [])
        self.assertEqual(self.process.group_sent(), [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual(await remaining(session), [Exited(-signal.SIGKILL)])

    async def test_a_second_close_sends_nothing_new(self) -> None:
        session = await self.start(FakeProcess(ignore_eof=True))
        await asyncio.wait_for(asyncio.gather(session.close(), session.close()), 1)
        self.assertEqual(self.process.group_sent(), [signal.SIGTERM])
        sent = list(self.process.group_signals)
        await asyncio.wait_for(session.close(), 1)
        self.assertEqual(self.process.group_signals, sent)
        self.assertEqual(self.process.signals, [])

    def timed_killpg(self, process: FakeProcess) -> tuple[list[tuple[int, float]], object]:
        loop = asyncio.get_running_loop()
        times: list[tuple[int, float]] = []

        def killpg(group: int, signum: int) -> None:
            if signum != 0:
                times.append((signum, loop.time()))
            process.killpg(group, signum)

        return times, killpg

    async def test_close_signals_the_group_term_then_kill_after_each_timeout(self) -> None:
        process = FakeProcess(ignore_eof=True, ignore_terminate=True)
        times, killpg = self.timed_killpg(process)
        session = await self.start(process, close_timeout=0.05, killpg=killpg)
        loop = asyncio.get_running_loop()
        began = loop.time()
        await asyncio.wait_for(session.close(), 1)
        self.assertEqual(process.group_lookups, [FAKE_PID])
        self.assertEqual(
            process.group_signals, [(FAKE_PID, signal.SIGTERM), (FAKE_PID, signal.SIGKILL)],
        )
        (_, term_at), (_, kill_at) = times
        self.assertGreaterEqual(term_at - began, 0.04)
        self.assertGreaterEqual(kill_at - term_at, 0.04)
        self.assertEqual(process.signals, [])
        self.assertEqual(await remaining(session), [Exited(-signal.SIGKILL)])

    async def test_own_or_unknown_group_falls_back_to_the_process(self) -> None:
        cases = {
            'own group': {'group': os.getpgrp()},
            'init group': {'group': 1},
            'group 0': {'group': 0},
            'negative group': {'group': -FAKE_PID},
            'lookup fails': {'group_error': ProcessLookupError()},
        }
        for name, options in cases.items():
            with self.subTest(case=name):
                process = FakeProcess(ignore_eof=True, ignore_terminate=True, **options)
                session = await self.start(process)
                await asyncio.wait_for(session.close(), 1)
                self.assertEqual(process.group_lookups, [FAKE_PID])
                self.assertEqual(process.group_signals, [])
                self.assertEqual(process.signals, ['terminate', 'kill'])
                self.assertEqual(await remaining(session), [Exited(-signal.SIGKILL)])

    async def test_close_ends_what_is_left_of_the_group_after_claude_exits(self) -> None:
        cases = [
            ('no member left', False, False, []),
            ('member ends on sigterm', True, False, [signal.SIGTERM]),
            ('member ignores sigterm', True, True, [signal.SIGTERM, signal.SIGKILL]),
        ]
        for name, grandchild, ignore_terminate, expected in cases:
            with self.subTest(case=name):
                process = FakeProcess(grandchild=grandchild, ignore_terminate=ignore_terminate)
                times, killpg = self.timed_killpg(process)
                session = await self.start(process, close_timeout=0.05, killpg=killpg)
                await asyncio.wait_for(session.close(), 1)
                self.assertEqual(process.returncode, 0)
                self.assertEqual(process.group_sent(), expected)
                self.assertEqual(process.group_signals[0], (FAKE_PID, 0))
                self.assertFalse(process.grandchild)
                self.assertEqual(process.signals, [])
                if len(times) == 2:
                    self.assertGreaterEqual(times[1][1] - times[0][1], 0.04)
                self.assertEqual(await remaining(session), [Exited(0)])


class ExitTest(SessionTest):
    async def test_exit_mid_turn_gives_only_exited(self) -> None:
        init, text, _ = stream_turns('turns')[0]
        session = await self.start()
        await session.send('Reply with OK', DispatchMode.STEER)
        self.process.feed(init, text)
        self.assertEqual(await events_until(session, TextOut), [TurnStarted(), TextOut('OK')])
        self.process.exit(1)
        self.assertEqual(await remaining(session), [Exited(1)])
        self.assertEqual(await remaining(session), [])

    async def test_send_and_respond_after_exit_raise(self) -> None:
        turn = stream_turns('perm-tool')[0]
        session = await self.start()
        await session.send('Run it', DispatchMode.STEER)
        self.process.feed(*turn[:3])
        await events_until(session, PermissionRequest)
        self.process.exit(1)
        self.assertEqual(await remaining(session), [Exited(1)])
        before = list(self.written)
        for mode in DispatchMode:
            with self.subTest(mode=mode), self.assertRaises(SessionClosed):
                await session.send('again', mode)
        with self.assertRaises(SessionClosed):
            await session.respond(A_TXT_ID, RequestAnswer(Decision.ALLOW))
        self.assertEqual(self.written, before)

    async def test_send_after_close_raises(self) -> None:
        session = await self.start()
        await session.close()
        with self.assertRaises(SessionClosed):
            await session.send('again', DispatchMode.STEER)

    async def test_broken_stdin_raises_session_closed(self) -> None:
        for error in (BrokenPipeError, ConnectionResetError):
            with self.subTest(error=error.__name__):
                session = await self.start()
                self.process.stdin.error = error()
                with self.assertRaises(SessionClosed):
                    await session.send('hi', DispatchMode.STEER)

    async def test_exit_without_stdout_eof_gives_exited_within_the_grace(self) -> None:
        init, text, _ = stream_turns('turns')[0]
        session = await self.start(FakeProcess(grandchild=True), exit_grace=0.05)
        await session.send('Reply with OK', DispatchMode.STEER)
        loop = asyncio.get_running_loop()
        self.process.feed(init, text)
        self.process.exit(1)
        exited_at = loop.time()
        events = await remaining(session, timeout=1)
        self.assertLess(loop.time() - exited_at, 0.3)
        self.assertEqual(events, [TurnStarted(), TextOut('OK'), Exited(1)])
        self.assertTrue(self.process.grandchild)
        await asyncio.wait_for(session.close(), 1)
        self.assertEqual(self.process.group_sent(), [signal.SIGTERM])
        self.assertFalse(self.process.grandchild)


class FailureReasonTest(SessionTest):
    def error_result(self, **fields: object) -> bytes:
        data = {'type': 'result', 'is_error': True, 'result': 'partial', **fields}
        return json.dumps(data, separators=(',', ':')).encode()

    async def result_of(self, line: bytes) -> TurnResult:
        host = FakeHost(secrets={'agent-token': TOKEN})
        session = await self.start(host=host)
        await session.send('Reply with OK', DispatchMode.STEER)
        self.process.feed(line)
        return await next_event(session)

    async def test_token_in_terminal_reason_is_redacted_and_cut(self) -> None:
        reason = 'error_during_execution: ' + TOKEN
        reason += 'y' * (1_000 - len(reason))
        self.assertEqual(len(reason), 1_000)
        result = await self.result_of(self.error_result(terminal_reason=reason))
        self.assertEqual(result.state, TurnState.FAILED)
        self.assertEqual(result.text, 'partial')
        self.assertEqual(result.reason, ('error_during_execution: [redacted]' + 'y' * 500)[:500])
        self.assertNotIn(TOKEN, repr(result))

    async def test_missing_null_or_non_string_terminal_reason_gives_none(self) -> None:
        lines = {
            'missing': self.error_result(),
            'null': self.error_result(terminal_reason=None),
            'number': self.error_result(terminal_reason=7),
        }
        for case, line in lines.items():
            with self.subTest(case=case):
                result = await self.result_of(line)
                self.assertEqual(result, TurnResult(TurnState.FAILED, 'partial', None))

    async def test_short_reason_without_the_token_is_unchanged(self) -> None:
        result = await self.result_of(self.error_result(terminal_reason='error_max_turns'))
        self.assertEqual(result, TurnResult(TurnState.FAILED, 'partial', 'error_max_turns'))


class LineLimitTest(SessionTest):
    def sized(self, fill: str, size: int) -> bytes:
        line = result_line(fill * (size - len(result_line(''))))
        self.assertEqual(len(line), size)
        return line

    async def test_line_of_the_limit_is_parsed_and_a_longer_one_dropped(self) -> None:
        session = await self.start(line_limit=64)
        self.assertEqual(self.spawn.calls[0][3], 64)
        self.process.feed(self.sized('a', 64), self.sized('b', 65), result_line('c'))
        fill = 64 - len(result_line(''))
        self.assertEqual(await next_event(session), completed('a' * fill))
        self.assertEqual(await next_event(session), completed('c'))
        await session.close()
        self.assertEqual(await remaining(session), [Exited(0)])

    async def test_long_line_in_pieces_is_dropped(self) -> None:
        session = await self.start(line_limit=64)
        data = self.sized('b', 200) + b'\n' + self.sized('a', 64) + b'\n'
        for start in range(0, len(data), 10):
            self.process.stdout.feed_data(data[start:start + 10])
            await settle()
        fill = 64 - len(result_line(''))
        self.assertEqual(await next_event(session), completed('a' * fill))
        await session.close()
        self.assertEqual(await remaining(session), [Exited(0)])

    async def test_last_line_without_a_newline_is_parsed(self) -> None:
        session = await self.start()
        self.process.stdout.feed_data(result_line('end'))
        self.process.exit(0)
        self.assertEqual(await remaining(session), [completed('end'), Exited(0)])


class RealProcessTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name).resolve()
        self.workspace = root / 'workspace'
        self.workspace.mkdir()
        (root / 'bin').mkdir()
        self.claude_bin = root / 'bin' / 'claude'
        self.claude_bin.write_text(f'#!{sys.executable} -I\n' + FAKE_CLAUDE.read_text())
        self.claude_bin.chmod(0o700)
        (self.workspace / 'replay.jsonl').write_bytes((STREAMS / 'turns.jsonl').read_bytes())
        self.thread = make_thread(id=str(uuid.uuid4()), workspace_path=str(self.workspace))
        self.host = FakeHost(
            environment={
                'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
                'HOME': str(root),
                'LANG': 'C.UTF-8',
                'CLAUDECODE': '1',
                'ANTHROPIC_AUTH_TOKEN': 'host-token',
            },
            settings={'claude_bin': str(self.claude_bin), 'agent_base_url': 'http://127.0.0.1:9'},
            secrets={'agent-token': 'agent-token-value'},
        )
        self.addCleanup(self.kill_grandchild)

    def kill_grandchild(self) -> None:
        with contextlib.suppress(FileNotFoundError, ValueError, ProcessLookupError):
            os.kill(self.grandchild_pid(), signal.SIGKILL)

    def grandchild_pid(self) -> int:
        return int((self.workspace / 'grandchild.pid').read_text())

    async def test_child_sees_argv_cwd_env_and_replays(self) -> None:
        session = await ClaudeRunner(close_timeout=5.0).start(self.thread, self.host)
        message = 'Remember the word heron\nand reply OK'
        await session.send(message, DispatchMode.STEER)
        self.assertEqual(await events_until(session, TurnResult, timeout=10), ok_turn('OK'))
        await asyncio.wait_for(session.close(), 10)
        self.assertEqual(await remaining(session), [Exited(0)])
        spawned = json.loads((self.workspace / 'spawned.json').read_text())
        self.assertEqual(spawned['argv'], claude_argv(self.thread, str(self.claude_bin)))
        self.assertEqual(spawned['cwd'], str(self.workspace))
        self.assertEqual(spawned['env'], claude_env(self.thread, self.host))
        self.assertEqual((self.workspace / 'stdin.jsonl').read_bytes(), user_line(message))
        for name in ('spawned.json', 'stdin.jsonl'):
            with self.subTest(file=name):
                self.assertEqual((self.workspace / name).stat().st_mode & 0o777, 0o600)

    async def test_child_that_ignores_eof_gets_sigterm(self) -> None:
        (self.workspace / 'ignore-eof').touch()
        session = await ClaudeRunner(close_timeout=0.2).start(self.thread, self.host)
        await session.send('Reply with OK', DispatchMode.STEER)
        await events_until(session, TurnResult, timeout=10)
        await asyncio.wait_for(session.close(), 10)
        self.assertEqual(await remaining(session), [Exited(-signal.SIGTERM)])

    async def test_claude_leads_its_own_process_group(self) -> None:
        session = await ClaudeRunner(close_timeout=5.0).start(self.thread, self.host)
        await session.send('Reply with OK', DispatchMode.STEER)
        await events_until(session, TurnResult, timeout=10)
        await asyncio.wait_for(session.close(), 10)
        self.assertEqual(await remaining(session), [Exited(0)])
        spawned = json.loads((self.workspace / 'spawned.json').read_text())
        self.assertEqual(spawned['pgid'], spawned['pid'])
        self.assertNotEqual(spawned['pgid'], os.getpgrp())

    async def test_close_ends_a_grandchild(self) -> None:
        (self.workspace / 'grandchild').touch()
        runner = ClaudeRunner(close_timeout=0.2, exit_grace=0.2)
        session = await runner.start(self.thread, self.host)
        await session.send('Reply with OK', DispatchMode.STEER)
        await events_until(session, TurnResult, timeout=10)
        pid = self.grandchild_pid()
        self.assertFalse(is_gone(pid))
        await asyncio.wait_for(session.close(), 10)
        self.assertTrue(is_gone(pid))
        self.assertEqual(await remaining(session), [Exited(0)])

    async def test_exited_is_not_held_up_by_a_grandchild_on_stdout(self) -> None:
        (self.workspace / 'grandchild').touch()
        (self.workspace / 'exit-after-turn').touch()
        runner = ClaudeRunner(close_timeout=0.2, exit_grace=0.2)
        session = await runner.start(self.thread, self.host)
        await session.send('Reply with OK', DispatchMode.STEER)
        self.assertEqual(await events_until(session, TurnResult, timeout=10), ok_turn('OK'))
        self.assertEqual(await next_event(session, timeout=5), Exited(0))
        pid = self.grandchild_pid()
        self.assertFalse(is_gone(pid))
        await asyncio.wait_for(session.close(), 10)
        self.assertTrue(is_gone(pid))

    async def test_missing_binary_raises_runner_start_error(self) -> None:
        missing = str(self.workspace.parent / 'bin' / 'missing')
        host = FakeHost(settings={'claude_bin': missing})
        with self.assertRaises(RunnerStartError) as caught:
            await ClaudeRunner().start(self.thread, host)
        self.assertIn(missing, str(caught.exception))


class TokenHost(FakeHost):
    def __init__(self, token_file: Path, **options: object) -> None:
        super().__init__(**options)
        self.token_file = token_file

    def secret(self, name: str) -> str | None:
        if name != 'agent-token':
            return None
        return self.token_file.read_text()


class RealProcessSecretTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name).resolve()
        self.workspace = root / 'workspace'
        self.workspace.mkdir()
        (root / 'bin').mkdir()
        self.claude_bin = root / 'bin' / 'claude'
        self.claude_bin.write_text(f'#!{sys.executable} -I\n' + FAKE_CLAUDE.read_text())
        self.claude_bin.chmod(0o700)
        (self.workspace / 'replay.jsonl').write_bytes((STREAMS / 'turns.jsonl').read_bytes())
        token_file = root / 'agent-token'
        fd = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as file:
            file.write(TOKEN + '\n')
        self.assertEqual(token_file.stat().st_mode & 0o777, 0o600)
        self.thread = make_thread(id=str(uuid.uuid4()), workspace_path=str(self.workspace))
        self.host = TokenHost(
            token_file,
            environment={
                'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
                'HOME': str(root),
                'LANG': 'C.UTF-8',
                'ANTHROPIC_AUTH_TOKEN': TOKEN,
            },
            settings={'claude_bin': str(self.claude_bin)},
        )

    def assert_clean(self, *items: object) -> None:
        for item in items:
            text = str(item) if isinstance(item, BaseException) else repr(item)
            with self.subTest(item=type(item).__name__):
                self.assertNotIn(TOKEN, text)
                self.assertLessEqual(len(text), 500)

    def assert_child_had_the_token(self) -> None:
        spawned = json.loads((self.workspace / 'spawned.json').read_text())
        self.assertEqual(spawned['env']['ANTHROPIC_AUTH_TOKEN'], TOKEN)

    async def test_missing_binary(self) -> None:
        missing = str(self.workspace.parent / 'bin' / f'missing-{TOKEN}')
        self.host.settings = {'claude_bin': missing}
        with self.assertRaises(RunnerStartError) as caught:
            await ClaudeRunner().start(self.thread, self.host)
        self.assertIsInstance(caught.exception.__cause__, FileNotFoundError)
        self.assertIn('[redacted]', str(caught.exception))
        self.assert_clean(caught.exception)

    async def test_bad_model(self) -> None:
        thread = make_thread(id=self.thread.id, workspace_path=str(self.workspace), model='-x')
        with self.assertRaises(ValueError) as caught:
            await ClaudeRunner().start(thread, self.host)
        self.assert_clean(caught.exception)
        self.assertFalse((self.workspace / 'spawned.json').exists())

    async def test_unexpected_exit_with_token_on_stderr(self) -> None:
        (self.workspace / 'fail-exit').write_text('3')
        session = await ClaudeRunner(close_timeout=5.0).start(self.thread, self.host)
        await session.send('Reply with OK', DispatchMode.STEER)
        events = await remaining(session, timeout=10)
        self.assertEqual(events, [Exited(3)])
        self.assert_child_had_the_token()
        with self.assertRaises(SessionClosed) as sent:
            await session.send('again', DispatchMode.STEER)
        with self.assertRaises(SessionClosed) as responded:
            await session.respond(A_TXT_ID, RequestAnswer(Decision.ALLOW))
        self.assert_clean(*events, sent.exception, responded.exception)

    async def test_error_result_with_token_in_terminal_reason(self) -> None:
        init = stream_turns('turns')[0][0]
        result = {'type': 'result', 'is_error': True, 'terminal_reason': f'failed: {TOKEN}'}
        replay = init + b'\n' + json.dumps(result).encode() + b'\n'
        (self.workspace / 'replay.jsonl').write_bytes(replay)
        session = await ClaudeRunner(close_timeout=5.0).start(self.thread, self.host)
        await session.send('Reply with OK', DispatchMode.STEER)
        events = await events_until(session, TurnResult, timeout=10)
        await asyncio.wait_for(session.close(), 10)
        events += await remaining(session)
        self.assertEqual(events, [
            TurnStarted(),
            TurnResult(TurnState.FAILED, None, 'failed: [redacted]'),
            Exited(0),
        ])
        self.assert_child_had_the_token()
        self.assert_clean(*events)


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
