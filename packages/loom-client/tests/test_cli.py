import contextlib
import errno
import importlib.util
import io
import json
import os
import select
import subprocess
import sys
import threading
import unittest
import uuid
from unittest import mock

from loom_client import Client, cli

from .support import (
    AT, CLOSE, FakeServerCase, event, failed, line, ok, project_info, schedule_info, stream,
    thread_info,
)

COMMANDS = [
    (['threads', 'launch', 'demo', 'First', 'hello'], 'threads.launch',
     {'project_id': 'demo', 'title': 'First', 'message': 'hello'}),
    (['threads', 'launch', 'demo', 'First', 'hello', '--workspace', 'worktree', '--branch', 'b',
      '--base', 'main', '--runner', 'claude', '--model', 'm', '--requests', 'caller',
      '--origin', 'cli'], 'threads.launch',
     {'project_id': 'demo', 'title': 'First', 'message': 'hello',
      'workspace': {'strategy': 'worktree', 'branch': 'b', 'base': 'main'}, 'runner': 'claude',
      'model': 'm', 'requests': 'caller', 'origin': 'cli'}),
    (['threads', 'launch', 'demo', 'First', 'hello', '--workspace', 'root'], 'threads.launch',
     {'project_id': 'demo', 'title': 'First', 'message': 'hello',
      'workspace': {'strategy': 'root'}}),
    (['threads', 'launch', 'demo', 'First', 'hello', '--workspace', 'existing_worktree',
      '--path', '/w/t1', '--branch', 'b'], 'threads.launch',
     {'project_id': 'demo', 'title': 'First', 'message': 'hello',
      'workspace': {'strategy': 'existing_worktree', 'branch': 'b', 'path': '/w/t1'}}),
    (['threads', 'launch', 'demo', 'First', 'hello', '--audience', 'person'], 'threads.launch',
     {'project_id': 'demo', 'title': 'First', 'message': 'hello', 'audience': 'person'}),
    (['threads', 'launch', 'demo', 'First', 'hello', '--cause', '7'], 'threads.launch',
     {'project_id': 'demo', 'title': 'First', 'message': 'hello', 'cause': 7}),
    (['threads', 'delegate', 't1', 'check it'], 'threads.delegate',
     {'parent_id': 't1', 'task': 'check it'}),
    (['threads', 'delegate', 't1', 'check it', '--workspace', 'root', '--runner', 'claude',
      '--model', 'm'], 'threads.delegate',
     {'parent_id': 't1', 'task': 'check it', 'workspace': {'strategy': 'root'},
      'runner': 'claude', 'model': 'm'}),
    (['threads', 'send', 't1', 'again'], 'threads.send', {'thread_id': 't1', 'message': 'again'}),
    (['threads', 'send', 't1', 'again', '--mode', 'interrupt'], 'threads.send',
     {'thread_id': 't1', 'message': 'again', 'mode': 'interrupt'}),
    (['threads', 'send', 't1', 'again', '--cause', '7'], 'threads.send',
     {'thread_id': 't1', 'message': 'again', 'cause': 7}),
    (['threads', 'list'], 'threads.list', {}),
    (['threads', 'list', '--project', 'demo'], 'threads.list', {'project_id': 'demo'}),
    (['threads', 'list', '--audience', 'agent'], 'threads.list', {'audience': 'agent'}),
    (['threads', 'list', '--project', 'demo', '--audience', 'person'], 'threads.list',
     {'project_id': 'demo', 'audience': 'person'}),
    (['threads', 'get', 't1'], 'threads.get', {'thread_id': 't1'}),
    (['threads', 'stop', 't1'], 'threads.stop', {'thread_id': 't1'}),
    (['threads', 'settle', 't1'], 'threads.settle', {'thread_id': 't1'}),
    (['threads', 'update', 't1', '--audience', 'person'], 'threads.update',
     {'thread_id': 't1', 'audience': 'person'}),
    (['projects', 'add', 'Demo', '/tmp/demo'], 'projects.add',
     {'title': 'Demo', 'path': '/tmp/demo'}),
    (['projects', 'add', 'Demo', '/tmp/demo', '--mode', 'approval-required'], 'projects.add',
     {'title': 'Demo', 'path': '/tmp/demo', 'permission_mode': 'approval-required'}),
    (['projects', 'set-mode', 'demo', 'full-access'], 'projects.set_mode',
     {'project_id': 'demo', 'permission_mode': 'full-access'}),
    (['projects', 'remove', 'demo'], 'projects.remove', {'project_id': 'demo'}),
    (['projects', 'list'], 'projects.list', {}),
    (['project', 'list'], 'projects.list', {}),
    (['project', 'add', 'Demo', '/tmp/demo'], 'projects.add',
     {'title': 'Demo', 'path': '/tmp/demo'}),
    (['schedules', 'add', 'Stand-up', '--at', '2026-10-25T09:00', '--action', 'notify'],
     'schedules.add',
     {'trigger': {'type': 'at', 'at': '2026-10-25T09:00'}, 'text': 'Stand-up',
      'action': 'notify'}),
    (['schedules', 'add', 'Look', '--every', '3600'], 'schedules.add',
     {'trigger': {'type': 'interval', 'everyMs': 3600000}, 'text': 'Look'}),
    (['schedules', 'add', 'Look', '--time', '9:07', '--weekdays', '1,2,3,4,5'], 'schedules.add',
     {'trigger': {'type': 'fixed_time', 'timeOfDay': '9:07', 'weekdays': [1, 2, 3, 4, 5]},
      'text': 'Look'}),
    (['schedules', 'add', 'Look', '--time', '9:07'], 'schedules.add',
     {'trigger': {'type': 'fixed_time', 'timeOfDay': '9:07'}, 'text': 'Look'}),
    (['schedules', 'add', 'Look', '--time', '9:07', '--action', 'post', '--thread', 't1',
      '--title', 'Daily'], 'schedules.add',
     {'trigger': {'type': 'fixed_time', 'timeOfDay': '9:07'}, 'text': 'Look', 'action': 'post',
      'thread_id': 't1', 'title': 'Daily'}),
    (['schedule', 'add', 'Go', '--every', '60', '--action', 'launch', '--project', 'demo'],
     'schedules.add',
     {'trigger': {'type': 'interval', 'everyMs': 60000}, 'text': 'Go', 'action': 'launch',
      'project_id': 'demo'}),
    (['schedules', 'list'], 'schedules.list', {}),
    (['schedules', 'list', '--thread', 't1', '--project', 'demo'], 'schedules.list',
     {'thread_id': 't1', 'project_id': 'demo'}),
    (['schedule', 'list'], 'schedules.list', {}),
    (['schedules', 'pause', 's1'], 'schedules.pause', {'schedule_id': 's1'}),
    (['schedules', 'remove', 's1'], 'schedules.remove', {'schedule_id': 's1'}),
    (['requests', 'respond', 'r1', 'allow'], 'requests.respond',
     {'request_id': 'r1', 'decision': 'allow'}),
    (['requests', 'respond', 'r1', 'deny', '--message', 'Not this one.'], 'requests.respond',
     {'request_id': 'r1', 'decision': 'deny', 'message': 'Not this one.'}),
    (['request', 'respond', 'r1', 'deny'], 'requests.respond',
     {'request_id': 'r1', 'decision': 'deny'}),
    (['signals', 'emit', 'demo.done', '--payload', '{"to":"done"}', '--project', 'p', '--key',
      'k', '--cause', '3'], 'signals.emit',
     {'topic': 'demo.done', 'payload': {'to': 'done'}, 'project_id': 'p', 'key': 'k',
      'cause': 3}),
    (['signals', 'emit', 'demo.done'], 'signals.emit', {'topic': 'demo.done', 'payload': {}}),
    (['signal', 'emit', 'demo.done', '--payload', '{"n": [1, 2]}'], 'signals.emit',
     {'topic': 'demo.done', 'payload': {'n': [1, 2]}}),
]

WRITES = {'threads.launch', 'threads.delegate', 'threads.send', 'threads.stop',
          'threads.settle', 'threads.update', 'projects.add', 'projects.set_mode',
          'projects.remove', 'schedules.add', 'schedules.pause', 'schedules.remove',
          'requests.respond', 'signals.emit'}

USAGE_ERRORS = [
    [],
    ['nope'],
    ['threads'],
    ['threads', 'nope'],
    ['threads', 'get'],
    ['threads', 'launch', 'demo', 'First'],
    ['threads', 'send', 't1', 'x', '--mode', 'loud'],
    ['threads', 'launch', 'demo', 'First', 'm', '--workspace', 'cloud'],
    ['threads', 'launch', 'demo', 'First', 'm', '--runner', 'gpt'],
    ['threads', 'launch', 'demo', 'First', 'm', '--requests', 'all'],
    ['threads', 'launch', 'demo', 'First', 'm', '--branch', 'b'],
    ['threads', 'launch', 'demo', 'First', 'm', '--workspace', 'root', '--branch', 'b'],
    ['threads', 'launch', 'demo', 'First', 'm', '--workspace', 'worktree', '--path', '/p'],
    ['threads', 'delegate', 't1', 'task', '--workspace', 'existing_worktree', '--base', 'b'],
    ['threads', 'events', '--after', '-1'],
    ['threads', 'events', '--after', 'x'],
    ['threads', 'update', 't1'],
    ['threads', 'update', '--audience', 'person'],
    ['threads', 'update', 't1', '--audience', 'people'],
    ['threads', 'launch', 'demo', 'First', 'm', '--audience', 'people'],
    ['threads', 'list', '--audience', 'people'],
    ['threads', 'events', '--audience', 'people'],
    ['threads', 'events', '--follow', '--audience', 'people'],
    ['projects', 'add', 'Demo', '.', '--mode', 'root'],
    ['projects', 'set-mode', 'demo', 'everything'],
    ['projects', 'list', 'extra'],
    ['schedules', 'add', 'x'],
    ['schedules', 'add', 'x', '--at', '2026-10-25T09:00', '--every', '60'],
    ['schedules', 'add', 'x', '--every', '60', '--time', '9:07'],
    ['schedules', 'add', 'x', '--every', '0'],
    ['schedules', 'add', 'x', '--every', 'x'],
    ['schedules', 'add', 'x', '--every', '1.5'],
    ['schedules', 'add', 'x', '--weekdays', '1,2'],
    ['schedules', 'add', 'x', '--at', '2026-10-25T09:00', '--weekdays', '1,2'],
    ['schedules', 'add', 'x', '--time', '9:07', '--weekdays', '1,x'],
    ['schedules', 'add', 'x', '--every', '60', '--action', 'send'],
    ['schedules', 'add', '--every', '60'],
    ['schedules', 'list', 'extra'],
    ['schedules', 'pause'],
    ['schedules', 'remove'],
    ['requests', 'respond'],
    ['requests', 'respond', 'r1'],
    ['requests', 'respond', 'r1', 'maybe'],
    ['requests', 'respond', 'r1', 'allow', 'extra'],
    ['request', 'respond', 'r1'],
    ['requests'],
    ['threads', 'launch', 'demo', 'First', 'm', '--cause', 'x'],
    ['threads', 'send', 't1', 'm', '--cause', '1.5'],
    ['signals'],
    ['signals', 'emit'],
    ['signals', 'emit', 'demo.done', '--payload', '[1]'],
    ['signals', 'emit', 'demo.done', '--payload', 'nope'],
    ['signals', 'emit', 'demo.done', '--payload', '"text"'],
    ['signals', 'emit', 'demo.done', '--payload', ''],
    ['signals', 'emit', 'demo.done', '--cause', 'x'],
    ['signal', 'emit', 'demo.done', '--payload', '5'],
    ['--socket'],
    ['serve', '--port', '80'],
]


class BrokenStdout(io.StringIO):
    def __init__(self, broken: str) -> None:
        super().__init__()
        self.broken = broken

    def write(self, text: str) -> int:
        if self.broken == 'write':
            raise BrokenPipeError(errno.EPIPE, 'Broken pipe')
        return super().write(text)

    def flush(self) -> None:
        if self.broken == 'flush':
            raise BrokenPipeError(errno.EPIPE, 'Broken pipe')


class CliCase(FakeServerCase):
    def setUp(self) -> None:
        super().setUp()
        patcher = mock.patch.dict(os.environ, {'PATH': os.environ.get('PATH', '')}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_cli(self, *argv: str, stdin: str = '', socket: str | None = None
                ) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        args = ['--socket', socket or str(self.socket), *argv]
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), \
                mock.patch('sys.stdin', io.StringIO(stdin)):
            code = cli.main(args)
        return code, out.getvalue(), err.getvalue()


class CommandTest(CliCase):
    def test_each_command_sends_its_call_and_prints_the_result_as_one_json_line(self) -> None:
        for argv, call, params in COMMANDS:
            with self.subTest(argv=argv):
                result = [project_info()] if call.endswith('.list') else thread_info()
                self.answer(ok(result))
                code, out, err = self.run_cli('--json', *argv)
                self.assertEqual((code, err), (0, ''))
                self.assertEqual(out.count('\n'), 1)
                self.assertEqual(json.loads(out), result)
                request = self.server.requests[-1]
                self.assertEqual((request['v'], request['call']), ('v1', call))
                sent = dict(request['params'])
                if call in WRITES:
                    self.assertTrue(sent.pop('id'))
                self.assertEqual(sent, params)

    def test_a_given_id_is_sent(self) -> None:
        self.answer(ok(thread_info()))
        for argv in (['threads', 'stop', 't1'], ['threads', 'send', 't1', 'm'],
                     ['projects', 'remove', 'demo'], ['threads', 'launch', 'd', 'T', 'm'],
                     ['schedules', 'add', 'x', '--every', '60'], ['schedules', 'pause', 's1'],
                     ['schedules', 'remove', 's1'],
                     ['threads', 'update', 't1', '--audience', 'agent'],
                     ['requests', 'respond', 'r1', 'allow'], ['signals', 'emit', 'demo.done']):
            with self.subTest(argv=argv):
                self.assertEqual(self.run_cli(*argv, '--id', 'retry-7')[0], 0)
                self.assertEqual(self.server.requests[-1]['params']['id'], 'retry-7')

    def test_the_command_table_names_each_served_call_once(self) -> None:
        names = [cli.command_name(call) for call in cli.COMMANDS]
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual({call.value for call in cli.COMMANDS}, {
            'threads.launch', 'threads.delegate', 'threads.send', 'threads.list', 'threads.get',
            'threads.stop', 'threads.settle', 'threads.update', 'threads.events', 'projects.add',
            'projects.set_mode', 'projects.remove', 'projects.list', 'schedules.add',
            'schedules.list', 'schedules.pause', 'schedules.remove', 'requests.respond',
            'signals.emit',
        })

    def test_requests_respond_sends_a_new_uuid4_id_or_the_given_one(self) -> None:
        self.answer(ok(thread_info()))
        self.assertEqual(self.run_cli('requests', 'respond', 'r1', 'allow')[0], 0)
        sent = self.server.requests[-1]
        self.assertEqual(sent['call'], 'requests.respond')
        params = dict(sent['params'])
        self.assertEqual(uuid.UUID(params.pop('id')).version, 4)
        self.assertEqual(params, {'request_id': 'r1', 'decision': 'allow'})
        self.assertEqual(self.run_cli('request', 'respond', 'r1', 'deny', '--message', 'no',
                                      '--id', 'c1')[0], 0)
        self.assertEqual(self.server.requests[-1]['params'],
                         {'request_id': 'r1', 'decision': 'deny', 'message': 'no', 'id': 'c1'})

    def test_requests_respond_prints_the_thread_line_or_one_json_line(self) -> None:
        thread = thread_info(state='running')
        self.answer(ok(thread))
        self.assertEqual(self.run_cli('requests', 'respond', 'r1', 'allow'),
                         (0, 't1  running  demo  First\n', ''))
        code, out, err = self.run_cli('--json', 'requests', 'respond', 'r1', 'allow')
        self.assertEqual((code, err, out.count('\n')), (0, '', 1))
        self.assertEqual(json.loads(out), thread)

    def test_a_refused_answer_exits_1_with_its_code_and_message(self) -> None:
        self.answer(failed('conflict', 'request r1 is closed'))
        self.assertEqual(self.run_cli('requests', 'respond', 'r1', 'deny'),
                         (1, '', 'loom: conflict: request r1 is closed\n'))

    def test_a_relative_project_path_is_made_absolute(self) -> None:
        self.answer(ok(project_info()))
        work = self.base / 'work'
        work.mkdir()
        old = os.getcwd()
        os.chdir(work)
        self.addCleanup(os.chdir, old)
        for path, expected in (('.', str(work)), ('sub/repo', str(work / 'sub' / 'repo')),
                               ('../work', str(work))):
            with self.subTest(path=path):
                self.assertEqual(self.run_cli('projects', 'add', 'Demo', path)[0], 0)
                self.assertEqual(self.server.requests[-1]['params']['path'], expected)

    def test_a_dash_message_or_task_is_read_from_stdin(self) -> None:
        self.answer(ok(thread_info()))
        text = 'line one\nline two\n'
        for argv, field in ((['threads', 'launch', 'demo', 'First', '-'], 'message'),
                            (['threads', 'send', 't1', '-'], 'message'),
                            (['threads', 'delegate', 't1', '-'], 'task'),
                            (['schedules', 'add', '-', '--every', '60'], 'text')):
            with self.subTest(argv=argv):
                self.assertEqual(self.run_cli(*argv, stdin=text)[0], 0)
                self.assertEqual(self.server.requests[-1]['params'][field], text)

    def test_a_dash_payload_is_read_from_stdin(self) -> None:
        self.answer(ok(event(9, 'signal', None, topic='demo.done', payload={}, hops=0)))
        code, _, err = self.run_cli('signals', 'emit', 'demo.done', '--payload', '-',
                                    stdin='{"to": "done",\n "n": 2}\n')
        self.assertEqual((code, err), (0, ''))
        self.assertEqual(self.server.requests[-1]['params']['payload'], {'to': 'done', 'n': 2})
        self.assertEqual(self.run_cli('signals', 'emit', 'demo.done', '--payload', '-',
                                      stdin='[1]')[0], 2)
        self.assertEqual(len(self.server.requests), 1)

    def test_a_payload_that_is_not_a_json_object_says_so(self) -> None:
        for payload, message in (('[1]', '--payload must be a JSON object'),
                                 ('nope', '--payload is not JSON')):
            with self.subTest(payload=payload):
                code, out, err = self.run_cli('signals', 'emit', 'demo.done', '--payload',
                                              payload)
                self.assertEqual((code, out), (2, ''))
                self.assertIn(message, err)
        self.assertEqual(self.server.requests, [])

    def test_no_cause_is_sent_without_the_flag(self) -> None:
        self.answer(ok(thread_info()))
        for argv in (['threads', 'launch', 'demo', 'First', 'hello'],
                     ['threads', 'send', 't1', 'again'], ['signals', 'emit', 'demo.done']):
            with self.subTest(argv=argv):
                self.assertEqual(self.run_cli(*argv)[0], 0)
                self.assertNotIn('cause', self.server.requests[-1]['params'])

    def test_a_message_that_is_not_a_dash_does_not_read_stdin(self) -> None:
        self.answer(ok(thread_info()))
        self.run_cli('threads', 'send', 't1', '--', '-x', stdin='unused')
        self.assertEqual(self.server.requests[-1]['params']['message'], '-x')


class UsageTest(CliCase):
    def test_usage_errors_exit_2_and_send_nothing(self) -> None:
        for argv in USAGE_ERRORS:
            with self.subTest(argv=argv):
                out, err = io.StringIO(), io.StringIO()
                with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                    code = cli.main(argv)
                self.assertEqual(code, 2)
                self.assertIn('usage:', err.getvalue())
        self.assertEqual(self.server.requests, [])

    def test_schedule_usage_errors_say_what_is_wrong(self) -> None:
        for argv, message in (
            (['x'], 'one of the arguments --at --every --time is required'),
            (['x', '--at', 'a', '--time', '9:07'], 'not allowed with argument'),
            (['x', '--every', '0'], "not a whole number of seconds: '0'"),
            (['x', '--every', 'x'], "not a whole number of seconds: 'x'"),
            (['x', '--every', '60', '--weekdays', '1,2'], '--weekdays needs --time'),
            (['x', '--time', '9:07', '--weekdays', '1,x'], "not a list of weekday numbers: '1,x'"),
        ):
            with self.subTest(argv=argv):
                code, out, err = self.run_cli('schedules', 'add', *argv)
                self.assertEqual((code, out), (2, ''))
                self.assertIn(message, err)
        self.assertEqual(self.server.requests, [])

    def test_help_lists_the_commands_and_exits_0(self) -> None:
        for argv, names in (
            (['--help'], ['serve', 'install', 'threads', 'projects', 'schedules', 'requests',
                          'signals']),
            (['threads', '--help'],
             ['launch', 'delegate', 'send', 'list', 'get', 'stop', 'settle', 'update',
              'events']),
            (['projects', '--help'], ['add', 'set-mode', 'remove', 'list']),
            (['project', '--help'], ['add', 'set-mode', 'remove', 'list']),
            (['schedules', '--help'], ['add', 'list', 'pause', 'remove']),
            (['schedule', '--help'], ['add', 'list', 'pause', 'remove']),
            (['requests', '--help'], ['respond']),
            (['request', '--help'], ['respond']),
            (['signals', '--help'], ['emit']),
            (['signal', '--help'], ['emit']),
        ):
            with self.subTest(argv=argv):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    self.assertEqual(cli.main(argv), 0)
                for name in names:
                    self.assertRegex(out.getvalue(), rf'\n    {name}\b')


class ExitTest(CliCase):
    def test_an_api_error_exits_1_with_its_code_and_message(self) -> None:
        self.answer(failed('not_found', "no thread with id 'nope'"))
        code, out, err = self.run_cli('threads', 'get', 'nope')
        self.assertEqual((code, out), (1, ''))
        self.assertEqual(err, "loom: not_found: no thread with id 'nope'\n")

    def test_an_update_of_an_unknown_thread_exits_1(self) -> None:
        self.answer(failed('not_found', "no thread with id 'nope'"))
        code, out, err = self.run_cli('threads', 'update', 'nope', '--audience', 'person')
        self.assertEqual((code, out), (1, ''))
        self.assertEqual(err, "loom: not_found: no thread with id 'nope'\n")
        self.assertEqual(self.server.calls[-1][0], 'threads.update')

    def test_an_api_error_with_json_also_prints_the_error_object(self) -> None:
        self.answer(failed('forbidden', 'agent threads may not add projects'))
        code, out, err = self.run_cli('--json', 'projects', 'add', 'Demo', '/tmp/demo')
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out), {
            'error': {'code': 'forbidden', 'message': 'agent threads may not add projects'},
        })
        self.assertEqual(out.count('\n'), 1)
        self.assertEqual(err, 'loom: forbidden: agent threads may not add projects\n')

    def test_a_schedule_the_server_finds_invalid_exits_1(self) -> None:
        self.answer(failed('invalid', 'everyMs must be at least 60000'))
        code, out, err = self.run_cli('schedules', 'add', 'x', '--every', '30')
        self.assertEqual((code, out), (1, ''))
        self.assertEqual(err, 'loom: invalid: everyMs must be at least 60000\n')
        self.assertEqual(self.server.requests[-1]['params']['trigger'],
                         {'type': 'interval', 'everyMs': 30000})

    def test_an_api_error_message_is_escaped_on_stderr(self) -> None:
        self.answer(failed('invalid', 'bad\x1b[2J\nthing'))
        self.assertEqual(self.run_cli('threads', 'get', 'x')[2],
                         'loom: invalid: bad\\x1b[2J\\nthing\n')

    def test_no_server_exits_3_naming_the_socket(self) -> None:
        path = str(self.base / 'none.sock')
        for argv in (['projects', 'list'], ['threads', 'events'],
                     ['threads', 'events', '--follow']):
            with self.subTest(argv=argv):
                code, out, err = self.run_cli(*argv, socket=path)
                self.assertEqual((code, out), (3, ''))
                self.assertTrue(err.startswith(f'loom: cannot reach Loom at {path}: '), err)
                self.assertEqual(err.count('\n'), 1)

    def test_an_answer_that_breaks_the_protocol_exits_3(self) -> None:
        for chunks in ([b'nope\n'], [line({'ok': 'yes'})], [CLOSE]):
            with self.subTest(chunks=chunks):
                self.answer(chunks)
                code, out, err = self.run_cli('projects', 'list')
                self.assertEqual((code, out), (3, ''))
                self.assertTrue(err.startswith('loom: '))

    def test_with_no_socket_path_at_all_it_exits_3_and_says_so(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(['projects', 'list'])
        self.assertEqual(code, 3)
        self.assertIn('no socket path', err.getvalue())
        self.assertIn('LOOM_SOCKET', err.getvalue())
        self.assertIn('XDG_RUNTIME_DIR', err.getvalue())

    def test_loom_socket_is_used_and_socket_wins_over_it(self) -> None:
        self.answer(ok([]))
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, {'LOOM_SOCKET': str(self.socket)}), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.assertEqual(cli.main(['projects', 'list']), 0)
            self.assertEqual(len(self.server.requests), 1)
            other = str(self.base / 'other.sock')
            self.assertEqual(cli.main(['--socket', other, 'projects', 'list']), 3)
        self.assertEqual(len(self.server.requests), 1)
        self.assertIn(other, err.getvalue())

    def test_the_runtime_dir_socket_is_used_without_loom_socket(self) -> None:
        runtime = self.base / 'run'
        (runtime / 'loom').mkdir(parents=True)
        os.symlink(self.socket, runtime / 'loom' / 'api.sock')
        self.answer(ok([]))
        with mock.patch.dict(os.environ, {'XDG_RUNTIME_DIR': str(runtime)}), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(['projects', 'list']), 0)
        self.assertEqual(len(self.server.requests), 1)

    def test_ctrl_c_exits_130_without_a_traceback(self) -> None:
        def interrupted(*args: object, **kwargs: object):
            raise KeyboardInterrupt
            yield

        for argv in (['threads', 'events', '--follow'], ['projects', 'list']):
            with self.subTest(argv=argv), \
                    mock.patch.object(Client, 'follow_events', interrupted), \
                    mock.patch.object(Client, 'call', side_effect=KeyboardInterrupt):
                code, out, err = self.run_cli(*argv)
                self.assertEqual((code, out, err), (130, '', ''))

    def test_a_closed_stdout_exits_141_with_nothing_on_stderr(self) -> None:
        self.answer(ok([thread_info()]))
        for broken in ('write', 'flush'):
            with self.subTest(broken=broken):
                out, err = BrokenStdout(broken), io.StringIO()
                with self.assertRaises(OSError):
                    out.fileno()
                with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                    code = cli.main(['--socket', str(self.socket), 'threads', 'list'])
                self.assertEqual((code, err.getvalue()), (141, ''))


class EventsTest(CliCase):
    def page_server(self, total: int) -> None:
        events = [event(number) for number in range(1, total + 1)]

        def reply(request: dict) -> list[bytes]:
            after = request['params'].get('after', 0)
            return ok([item for item in events if item['id'] > after][:1000])

        self.answer_with(reply)

    def test_without_follow_it_pages_on_and_prints_every_event(self) -> None:
        self.page_server(2500)
        code, out, err = self.run_cli('--json', 'threads', 'events', '--after', '100')
        self.assertEqual((code, err), (0, ''))
        ids = [json.loads(text)['id'] for text in out.splitlines()]
        self.assertEqual(ids, list(range(101, 2501)))
        self.assertEqual([params['after'] for _, params in self.server.calls], [100, 1100, 2100])

    def test_a_last_page_of_exactly_1000_asks_once_more(self) -> None:
        self.page_server(2000)
        code, out, _ = self.run_cli('threads', 'events')
        self.assertEqual(code, 0)
        self.assertEqual(len(out.splitlines()), 2000)
        self.assertEqual([params['after'] for _, params in self.server.calls], [0, 1000, 2000])

    def test_events_sends_the_thread_and_no_follow(self) -> None:
        self.answer(ok([]))
        self.assertEqual(self.run_cli('threads', 'events', '--thread', 't1'), (0, '', ''))
        self.assertEqual(self.server.calls, [('threads.events', {'after': 0, 'thread_id': 't1'})])

    def test_audience_is_sent_on_every_page(self) -> None:
        self.page_server(2500)
        code, out, err = self.run_cli('threads', 'events', '--audience', 'person')
        self.assertEqual((code, err), (0, ''))
        self.assertEqual(len(out.splitlines()), 2500)
        self.assertEqual(self.server.calls, [
            ('threads.events', {'after': after, 'audience': 'person'})
            for after in (0, 1000, 2000)
        ])

    def test_follow_sends_the_audience(self) -> None:
        self.answer(stream(event(4)))
        code, out, _ = self.run_cli('threads', 'events', '--follow', '--audience', 'agent',
                                    '--thread', 't1')
        self.assertEqual(code, 3)
        self.assertEqual(len(out.splitlines()), 1)
        self.assertEqual(self.server.calls, [
            ('threads.events', {'after': 0, 'thread_id': 't1', 'audience': 'agent',
                                'follow': True}),
        ])

    def test_a_full_page_that_does_not_move_on_exits_3(self) -> None:
        self.answer(ok([event(5)] * 1000))
        code, _, err = self.run_cli('threads', 'events', '--after', '5')
        self.assertEqual(code, 3)
        self.assertTrue(err.startswith('loom: '))

    def test_follow_prints_each_event_then_says_how_to_resume(self) -> None:
        events = [event(4), event(5), event(9)]
        self.answer(stream(*events))
        code, out, err = self.run_cli('--json', 'threads', 'events', '--follow', '--after', '3',
                                      '--thread', 't1')
        self.assertEqual(code, 3)
        self.assertEqual([json.loads(text) for text in out.splitlines()], events)
        self.assertEqual(self.server.calls, [
            ('threads.events', {'after': 3, 'thread_id': 't1', 'follow': True}),
        ])
        self.assertIn('after event 9', err)
        self.assertTrue(err.endswith('; resume with --after 9\n'), err)
        self.assertEqual(err.count('\n'), 1)

    def test_a_follow_lost_before_any_event_resumes_from_after(self) -> None:
        self.answer(stream())
        code, out, err = self.run_cli('threads', 'events', '--follow', '--after', '12')
        self.assertEqual((code, out), (3, ''))
        self.assertTrue(err.endswith('; resume with --after 12\n'), err)

    def test_a_bad_event_line_in_follow_exits_3_and_says_how_to_resume(self) -> None:
        self.answer([*stream(event(4), close=False), line({'event': {'id': True}}), CLOSE])
        code, out, err = self.run_cli('threads', 'events', '--follow')
        self.assertEqual(code, 3)
        self.assertEqual(len(out.splitlines()), 1)
        self.assertTrue(err.endswith('; resume with --after 4\n'), err)

    def test_a_refused_follow_exits_1(self) -> None:
        self.answer(failed('invalid', 'thread_id must be a string'))
        self.assertEqual(self.run_cli('threads', 'events', '--follow')[0], 1)


class HumanOutputTest(CliCase):
    def events(self, *events: dict, follow: bool = False, as_json: bool = False) -> str:
        self.answer(stream(*events) if follow else ok(list(events)))
        argv = ['threads', 'events'] + (['--follow'] if follow else [])
        code, out, _ = self.run_cli(*(['--json'] if as_json else []), *argv)
        self.assertEqual(code, 3 if follow else 0)
        return out

    def test_control_characters_in_agent_text_print_as_escapes_one_line_per_event(self) -> None:
        nasty = 'a\x1b[2Jb\nc\td\x7fe\x9bf\rg\x00h\x85i'
        for follow in (False, True):
            with self.subTest(follow=follow):
                out = self.events(event(1, text=nasty), event(2, 'turn.result',
                                                              state='completed', text=nasty),
                                  follow=follow)
                lines = out.splitlines()
                self.assertEqual(len(lines), 2)
                escaped = 'a\\x1b[2Jb\\nc\\td\\x7fe\\x9bf\\rg\\x00h\\x85i'
                self.assertEqual(lines[0], f'1 {AT} t1 turn.text {escaped}')
                self.assertTrue(lines[1].endswith(f'turn.result completed {escaped}'))
                for code in [*range(0x20), 0x7f, *range(0x80, 0xa0)]:
                    self.assertNotIn(chr(code), out.replace('\n', ''))

    def test_every_c0_del_and_c1_character_is_escaped(self) -> None:
        text = ''.join(chr(code) for code in [*range(0x20), 0x7f, *range(0x80, 0xa0)])
        out = self.events(event(1, text=text))
        self.assertEqual(len(out.splitlines()), 1)
        self.assertTrue(out.isascii())
        self.assertIn('\\x00\\x01', out)
        self.assertIn('\\x1f\\x7f\\x80', out)
        self.assertIn('\\x9f', out)

    def test_a_text_of_exactly_2000_characters_prints_whole(self) -> None:
        out = self.events(event(1, text='x' * 1999 + 'y'))
        self.assertTrue(out.endswith(' ' + 'x' * 1999 + 'y\n'))
        self.assertNotIn('…', out)

    def test_a_text_of_2001_characters_prints_2000_then_an_ellipsis(self) -> None:
        out = self.events(event(1, text='x' * 2000 + 'y'))
        self.assertTrue(out.endswith(' ' + 'x' * 2000 + '…\n'))
        self.assertNotIn('y', out.split(' turn.text ')[1])

    def test_the_cut_comes_before_the_escapes(self) -> None:
        out = self.events(event(1, text='\n' * 2001))
        self.assertTrue(out.endswith(' ' + '\\n' * 2000 + '…\n'))

    def test_json_output_keeps_the_text_unchanged(self) -> None:
        nasty = 'a\x1b[2Jb\nc\td\x7fe\x9bf' + 'z' * 2500
        for follow in (False, True):
            with self.subTest(follow=follow):
                out = self.events(event(1, text=nasty), follow=follow, as_json=True)
                self.assertEqual(len(out.splitlines()), 1)
                self.assertEqual(json.loads(out)['data']['text'], nasty)
                self.assertTrue(out.isascii())

    def test_tool_names_input_reasons_and_requests_are_escaped(self) -> None:
        out = self.events(
            event(1, 'turn.tool', tool_use_id='toolu_1', name='Ba\x1bsh',
                  input={'command': 'echo \x9b\x7f', 'n': 1}),
            event(2, 'thread.failed', reason='crash\n\x1b]0;title\x07'),
            event(3, 'request.opened', request_id='r1', tool_name='Wri\x1bte',
                  input={'path': '/x'}, description='may \x9b I'),
            event(4, 'thread.state', state='sleeping', waiting=True, reason='wait\ning'),
            event(5, 'turn.result', state='failed', reason='bo\x1bom'),
            event(6, 'schedule.fired', schedule_id='s1', action='send', text='ti\x9bck',
                  by='t\x1b2', late=True),
            event(7, 'request.closed', request_id='r1', decision='allow'),
            event(8, 'turn.started'),
            event(9, 'some.future', thread_id=None, odd='x\x1by'),
            event(10, 'request.closed', request_id='r1', decision='deny', reason='time\x1bout'),
        )
        lines = out.splitlines()
        self.assertEqual(len(lines), 10)
        self.assertTrue(out.isascii())
        self.assertTrue(lines[0].endswith(
            'turn.tool Ba\\x1bsh {"command": "echo \\x9b\\x7f", "n": 1} toolu_1'))
        self.assertTrue(lines[1].endswith('thread.failed crash\\n\\x1b]0;title\\x07'))
        self.assertTrue(
            lines[2].endswith('request.opened r1 Wri\\x1bte may \\x9b I {"path": "/x"}'))
        self.assertTrue(lines[3].endswith('thread.state sleeping waiting wait\\ning'))
        self.assertTrue(lines[4].endswith('turn.result failed bo\\x1bom'))
        self.assertTrue(lines[5].endswith('schedule.fired s1 send t\\x1b2 late ti\\x9bck'))
        self.assertTrue(lines[6].endswith('request.closed r1 allow'))
        self.assertTrue(lines[7].endswith(' t1 turn.started'))
        self.assertEqual(lines[8], f'9 {AT} - some.future {{"odd": "x\\u001by"}}')
        self.assertTrue(lines[9].endswith('request.closed r1 deny time\\x1bout'))

    def test_a_closed_request_prints_its_reason_when_it_has_one(self) -> None:
        out = self.events(
            event(1, 'request.closed', request_id='r1', decision='deny', reason='timeout'),
            event(2, 'request.closed', request_id='r2', decision='allow'),
        )
        self.assertEqual(out.splitlines(), [
            f'1 {AT} t1 request.closed r1 deny timeout',
            f'2 {AT} t1 request.closed r2 allow',
        ])

    def test_a_thread_updated_event_prints_the_new_audience(self) -> None:
        out = self.events(event(1, 'thread.updated', audience='person', previous_audience='agent'),
                          event(2, 'thread.updated', thread_id='t2', audience='agent',
                                previous_audience='person'))
        self.assertEqual(out.splitlines(), [
            f'1 {AT} t1 thread.updated person',
            f'2 {AT} t2 thread.updated agent',
        ])

    def test_signal_and_trigger_skipped_events_print_their_fields(self) -> None:
        out = self.events(
            event(1, 'signal', None, topic='demo.done', payload={'to': 'done'}, hops=0),
            event(2, 'signal', None, topic='run.status', project_id='p', key='k', hops=2,
                  cause=1, by='t1', payload={}),
            {**event(3, 'trigger.skipped', None, schedule_id='s1', reason='hops'),
             'data': {'schedule_id': 's1', 'event_id': 2, 'reason': 'hops'}})
        self.assertEqual(out.splitlines(), [
            f'1 {AT} - signal demo.done 0 {{"to": "done"}}',
            f'2 {AT} - signal run.status p k 2 {{}}',
            f'3 {AT} - trigger.skipped s1 2 hops',
        ])

    def test_emit_prints_one_event_line_or_one_json_line(self) -> None:
        signal = event(9, 'signal', None, topic='demo.done', payload={'to': 'done'}, hops=1,
                       cause=3)
        self.answer(ok(signal))
        self.assertEqual(self.run_cli('signals', 'emit', 'demo.done', '--payload',
                                      '{"to":"done"}'),
                         (0, f'9 {AT} - signal demo.done 1 {{"to": "done"}}\n', ''))
        code, out, err = self.run_cli('--json', 'signal', 'emit', 'demo.done')
        self.assertEqual((code, err, out.count('\n')), (0, '', 1))
        self.assertEqual(json.loads(out), signal)

    def test_a_long_tool_input_is_cut(self) -> None:
        out = self.events(event(1, 'turn.tool', tool_use_id='t', name='Bash',
                                input={'command': 'x' * 3000}))
        self.assertIn(' {"command": "' + 'x' * 1987 + '…', out)
        self.assertNotIn('x' * 1988, out)

    def test_thread_and_project_results_are_short_escaped_lines(self) -> None:
        thread = thread_info(title='Ti\x1btle\nnext', state='sleeping', waiting=True,
                             reason='stop')
        self.answer(ok([thread, thread_info('t2')]))
        code, out, _ = self.run_cli('threads', 'list')
        self.assertEqual(code, 0)
        self.assertEqual(out.splitlines(), [
            't1  sleeping waiting  demo  Ti\\x1btle\\nnext  stop',
            't2  idle  demo  First',
        ])
        self.answer(ok(thread))
        self.assertEqual(self.run_cli('threads', 'stop', 't1')[1].splitlines(),
                         ['t1  sleeping waiting  demo  Ti\\x1btle\\nnext  stop'])
        self.answer(ok([project_info(), project_info('two', permission_mode='auto',
                                                     title='T\x9bwo')]))
        self.assertEqual(self.run_cli('projects', 'list')[1].splitlines(), [
            'demo  default  /tmp/demo  Demo',
            'two  auto  /tmp/demo  T\\x9bwo',
        ])
        self.answer(ok([]))
        self.assertEqual(self.run_cli('threads', 'list'), (0, '', ''))

    def test_update_prints_one_thread_line(self) -> None:
        self.answer(ok(thread_info(audience='person')))
        self.assertEqual(self.run_cli('threads', 'update', 't1', '--audience', 'person'),
                         (0, 't1  idle  demo  First\n', ''))

    def test_get_prints_each_field_on_its_own_line(self) -> None:
        self.answer(ok(thread_info(title='a\nb', origin=None)))
        code, out, _ = self.run_cli('threads', 'get', 't1')
        self.assertEqual(code, 0)
        lines = out.splitlines()
        self.assertEqual(len(lines), len(thread_info()))
        self.assertIn('title: a\\nb', lines)
        self.assertIn('origin: -', lines)
        self.assertIn('waiting: false', lines)

    def test_an_unexpected_result_shape_still_prints(self) -> None:
        for result in (None, 'text\x1b', 5, [1, None]):
            with self.subTest(result=result):
                self.answer(ok(result))
                code, out, _ = self.run_cli('threads', 'get', 't1')
                self.assertEqual(code, 0)
                self.assertTrue(out.isascii())


class ScheduleOutputTest(CliCase):
    def lines(self, *argv: str, result: object) -> list[str]:
        self.answer(ok(result))
        code, out, err = self.run_cli(*argv)
        self.assertEqual((code, err), (0, ''))
        return out.splitlines()

    def test_each_trigger_and_state_prints_as_one_line(self) -> None:
        schedules = [
            schedule_info(),
            schedule_info('s2', trigger={'type': 'interval', 'everyMs': 3600000},
                          action='post', thread_id='t1', title='Hourly', paused=True),
            schedule_info('s3', next_run=None),
            schedule_info('s4', trigger={'type': 'fixed_time', 'timeOfDay': '9:07',
                                         'weekdays': [1, 2, 3, 4, 5]},
                          action='launch', project_id='demo', title='Daily'),
            schedule_info('s5', trigger={'type': 'fixed_time', 'timeOfDay': '09:07'}),
            schedule_info('s6', trigger={'type': 'fixed_time', 'timeOfDay': '9:07',
                                         'weekdays': []}),
            schedule_info('s7', trigger={'type': 'interval', 'everyMs': 60500}),
        ]
        self.assertEqual(self.lines('schedules', 'list', result=schedules), [
            f's1  {AT}  notify  at 2026-10-25T09:00  -  Stand-up',
            's2  paused  post  every 3600s  t1  Hourly',
            's3  done  notify  at 2026-10-25T09:00  -  Stand-up',
            f's4  {AT}  launch  9:07 on 1,2,3,4,5  demo  Daily',
            f's5  {AT}  notify  09:07  -  Stand-up',
            f's6  {AT}  notify  9:07  -  Stand-up',
            f's7  {AT}  notify  every 60.5s  -  Stand-up',
        ])

    def test_add_pause_and_remove_print_the_schedule(self) -> None:
        for argv, result, expected in (
            (['schedules', 'add', 'Stand-up', '--at', '2026-10-25T09:00'], schedule_info(),
             f's1  {AT}  notify  at 2026-10-25T09:00  -  Stand-up'),
            (['schedules', 'pause', 's1'], schedule_info(paused=True),
             's1  paused  notify  at 2026-10-25T09:00  -  Stand-up'),
            (['schedules', 'remove', 's1'], schedule_info(next_run=None),
             's1  done  notify  at 2026-10-25T09:00  -  Stand-up'),
        ):
            with self.subTest(argv=argv):
                self.assertEqual(self.lines(*argv, result=result), [expected])

    def test_text_is_escaped_on_one_line_and_cut(self) -> None:
        nasty = schedule_info(text='Ti\x1b[2Jck\nnext\x9b', thread_id='t\x1b1')
        long = schedule_info('s2', text='x' * 2001)
        lines = self.lines('schedules', 'list', result=[nasty, long])
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0],
                         f's1  {AT}  notify  at 2026-10-25T09:00  t\\x1b1  '
                         'Ti\\x1b[2Jck\\nnext\\x9b')
        self.assertTrue(lines[1].endswith('  ' + 'x' * 2000 + '…'))

    def test_json_prints_the_result_as_one_line(self) -> None:
        result = [schedule_info(text='a\nb'), schedule_info('s2')]
        self.answer(ok(result))
        code, out, _ = self.run_cli('--json', 'schedules', 'list')
        self.assertEqual(code, 0)
        self.assertEqual(out.count('\n'), 1)
        self.assertEqual(json.loads(out), result)

    def test_a_bad_field_prints_a_dash_in_its_place(self) -> None:
        rest = '  notify  at 2026-10-25T09:00  -  Stand-up'
        cases = [
            (schedule_info(trigger=None), f's1  {AT}  notify  -  -  Stand-up'),
            ({key: value for key, value in schedule_info().items() if key != 'trigger'},
             f's1  {AT}  notify  -  -  Stand-up'),
            (schedule_info(trigger='at noon'), f's1  {AT}  notify  -  -  Stand-up'),
            (schedule_info(trigger=[1]), f's1  {AT}  notify  -  -  Stand-up'),
            (schedule_info(trigger={'type': 'at'}), f's1  {AT}  notify  -  -  Stand-up'),
            (schedule_info(trigger={'type': 'interval', 'everyMs': '60000'}),
             f's1  {AT}  notify  -  -  Stand-up'),
            (schedule_info(trigger={'type': 'interval', 'everyMs': True}),
             f's1  {AT}  notify  -  -  Stand-up'),
            (schedule_info(trigger={'type': 'fixed_time', 'timeOfDay': '9:07',
                                    'weekdays': [1, 'x']}),
             f's1  {AT}  notify  -  -  Stand-up'),
            (schedule_info(trigger={'type': 'fixed_time', 'timeOfDay': '9:07', 'weekdays': 0}),
             f's1  {AT}  notify  -  -  Stand-up'),
            (schedule_info(trigger={'type': 'webhook'}), f's1  {AT}  notify  -  -  Stand-up'),
            ({key: value for key, value in schedule_info().items() if key != 'next_run'},
             's1  -' + rest),
            (schedule_info(next_run=5), 's1  -' + rest),
            ({key: value for key, value in schedule_info().items() if key != 'paused'},
             's1  -' + rest),
            (schedule_info(paused='yes'), 's1  -' + rest),
            (schedule_info(paused=None), 's1  -' + rest),
        ]
        for schedule, expected in cases:
            with self.subTest(schedule=schedule):
                self.assertEqual(self.lines('schedules', 'list', result=[schedule]), [expected])
        for result in (None, 5, [None, 'x\x1b'], {}):
            with self.subTest(result=result):
                lines = self.lines('schedules', 'list', result=result)
                self.assertEqual(len(lines), len(result) if isinstance(result, list) else 1)
                self.assertTrue(''.join(lines).isascii())


class ServeTest(CliCase):
    def serve(self, *argv: str, found: bool = True) -> tuple[int, str, mock.Mock]:
        err = io.StringIO()
        spec = importlib.util.find_spec('loom_client') if found else None
        with mock.patch('importlib.util.find_spec', return_value=spec) as find_spec, \
                mock.patch('os.execv') as execv, contextlib.redirect_stderr(err):
            code = cli.main(list(argv))
        find_spec.assert_called_once_with('loom_service')
        return code, err.getvalue(), execv

    def test_serve_replaces_itself_with_loom_service_and_the_same_arguments(self) -> None:
        _, _, execv = self.serve('serve', '--socket', '/r/api.sock', '--data-dir', 'data',
                                 '--config', '/c/config.toml')
        execv.assert_called_once_with(sys.executable, [
            sys.executable, '-m', 'loom_service', 'serve', '--socket', '/r/api.sock',
            '--data-dir', 'data', '--config', '/c/config.toml',
        ])

    def test_serve_with_no_options_passes_none(self) -> None:
        _, _, execv = self.serve('serve')
        execv.assert_called_once_with(
            sys.executable, [sys.executable, '-m', 'loom_service', 'serve'])

    def test_the_global_socket_is_passed_on_unless_serve_has_its_own(self) -> None:
        _, _, execv = self.serve('--socket', '/g.sock', 'serve')
        self.assertEqual(execv.call_args.args[1][-2:], ['--socket', '/g.sock'])
        _, _, execv = self.serve('--socket', '/g.sock', 'serve', '--socket', '/own.sock')
        self.assertEqual(execv.call_args.args[1][4:], ['--socket', '/own.sock'])

    def test_serve_without_loom_service_exits_2(self) -> None:
        code, err, execv = self.serve('serve', '--socket', '/r/api.sock', found=False)
        self.assertEqual((code, err), (2, 'loom: loom-service is not installed\n'))
        execv.assert_not_called()

    def test_an_exec_that_fails_exits_2(self) -> None:
        err = io.StringIO()
        with mock.patch('os.execv', side_effect=PermissionError(13, 'Permission denied')), \
                contextlib.redirect_stderr(err):
            self.assertEqual(cli.main(['serve']), 2)
        self.assertEqual(err.getvalue(), 'loom: cannot start loom-service: Permission denied\n')


class RetryHintTest(CliCase):
    LOST = 'loom: the connection closed before a full answer line'

    def write_commands(self) -> list[list[str]]:
        return [argv for argv, call, _ in COMMANDS if call in WRITES]

    def test_a_write_lost_after_it_reached_loom_says_which_id_to_retry_with(self) -> None:
        self.assertEqual({call for _, call, _ in COMMANDS if call in WRITES}, WRITES)
        for argv in self.write_commands():
            for chunks in ([CLOSE], [b'{"ok": true, "result"', CLOSE]):
                for flags in ([], ['--json']):
                    with self.subTest(argv=argv, chunks=chunks, flags=flags):
                        self.answer(chunks)
                        code, out, err = self.run_cli(*flags, *argv)
                        self.assertEqual((code, out), (3, ''))
                        sent = self.server.requests[-1]['params']['id']
                        self.assertEqual(uuid.UUID(sent).version, 4)
                        self.assertEqual(err, f'{self.LOST}; retry with --id {sent}\n')

    def test_a_given_id_is_printed_and_retrying_with_it_sends_the_same_id(self) -> None:
        for argv in (['threads', 'launch', 'demo', 'First', 'hello'],
                     ['--json', 'projects', 'add', 'Demo', '/tmp/demo'],
                     ['schedules', 'remove', 's1']):
            with self.subTest(argv=argv):
                self.answer([CLOSE])
                code, out, err = self.run_cli(*argv, '--id', 'retry-1')
                self.assertEqual((code, out), (3, ''))
                self.assertEqual(err, f'{self.LOST}; retry with --id retry-1\n')
                printed = err.rstrip('\n').rsplit('; retry with --id ', 1)[1]
                self.answer(ok(thread_info()))
                self.assertEqual(self.run_cli(*argv, '--id', printed)[0], 0)
                ids = [request['params']['id'] for request in self.server.requests[-2:]]
                self.assertEqual(ids, ['retry-1', 'retry-1'])

    def test_a_given_id_with_a_control_character_prints_escaped_on_one_line(self) -> None:
        self.answer([CLOSE])
        code, out, err = self.run_cli('threads', 'send', 't1', 'hi', '--id', 'a\nb\x1b')
        self.assertEqual((code, out), (3, ''))
        self.assertEqual(err, f'{self.LOST}; retry with --id a\\nb\\x1b\n')
        self.assertEqual(err.count('\n'), 1)
        self.assertEqual(self.server.requests[-1]['params']['id'], 'a\nb\x1b')

    def test_a_write_that_cannot_reach_loom_has_no_retry_hint(self) -> None:
        path = str(self.base / 'none.sock')
        for argv in self.write_commands():
            for given in ([], ['--id', 'retry-1']):
                with self.subTest(argv=argv, given=given):
                    code, out, err = self.run_cli(*argv, *given, socket=path)
                    self.assertEqual((code, out), (3, ''))
                    self.assertTrue(err.startswith(f'loom: cannot reach Loom at {path}: '), err)
                    self.assertNotIn('retry with', err)
                    self.assertEqual(err.count('\n'), 1)

    def test_a_read_lost_after_it_reached_loom_has_no_retry_hint(self) -> None:
        self.answer([CLOSE])
        for argv in (['projects', 'list'], ['threads', 'get', 't1'], ['threads', 'events']):
            with self.subTest(argv=argv):
                code, out, err = self.run_cli(*argv)
                self.assertEqual((code, out, err), (3, '', f'{self.LOST}\n'))
        self.answer(stream(event(4)))
        code, _, err = self.run_cli('threads', 'events', '--follow')
        self.assertEqual(code, 3)
        self.assertTrue(err.endswith('; resume with --after 4\n'), err)
        self.assertNotIn('retry with', err)

    def test_an_api_error_on_a_write_has_no_retry_hint(self) -> None:
        self.answer(failed('invalid', 'bad input'))
        for argv in self.write_commands():
            with self.subTest(argv=argv):
                self.assertEqual(self.run_cli(*argv), (1, '', 'loom: invalid: bad input\n'))


class BrokenPipeTest(FakeServerCase):
    TIMEOUT = 10.0
    maxDiff = None

    def setUp(self) -> None:
        super().setUp()
        self.closed = threading.Event()
        self.addCleanup(self.closed.set)

    def stop(self, process: subprocess.Popen) -> None:
        if process.poll() is None:
            process.kill()
            process.wait()
        process.stdout.close()
        process.stderr.close()

    def close_after_one_line(self, argv: list[str]) -> tuple[int, bytes]:
        process = subprocess.Popen(
            [sys.executable, '-m', 'loom_client', '--socket', str(self.socket), *argv],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env={'PATH': os.environ.get('PATH', '')})
        self.addCleanup(self.stop, process)
        ready, _, _ = select.select([process.stdout], [], [], self.TIMEOUT)
        self.assertTrue(ready, 'no line on stdout')
        self.assertTrue(process.stdout.readline())
        process.stdout.close()
        self.closed.set()
        code = process.wait(timeout=self.TIMEOUT)
        return code, process.stderr.read()

    def test_events_cut_off_by_a_closed_stdout_exit_141_with_nothing_on_stderr(self) -> None:
        def reply(request: dict):
            if request['params']['after'] == 0:
                yield from ok([event(number) for number in range(1, cli.PAGE + 1)])
                return
            self.closed.wait(self.TIMEOUT)
            yield from ok([event(number) for number in range(cli.PAGE + 1, cli.PAGE + 301)])

        self.answer_with(reply)
        code, err = self.close_after_one_line(['threads', 'events'])
        self.assertEqual((code, err), (141, b''))

    def test_a_follow_cut_off_by_a_closed_stdout_exits_141_with_nothing_on_stderr(self) -> None:
        def reply(request: dict):
            chunks = stream(*[event(number) for number in range(1, 302)])
            yield from chunks[:2]
            self.closed.wait(self.TIMEOUT)
            yield b''.join(chunks[2:-1])
            yield CLOSE

        self.answer_with(reply)
        code, err = self.close_after_one_line(['--json', 'threads', 'events', '--follow'])
        self.assertEqual((code, err), (141, b''))


if __name__ == '__main__':
    unittest.main()
