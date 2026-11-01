import json
import os
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from loom_contracts.calls import ProjectInfo, ThreadInfo

from loom_client import Client, LoomApiError, LoomConnectionError

TIMEOUT = 10.0
FAKE_CLAUDE = Path(__file__).resolve().parents[2] / 'loom-service' / 'tests' / 'fake_claude.py'
LOOM = Path(sys.executable).parent / 'loom'
EVENT_KEYS = {'id', 'type', 'at', 'thread_id', 'data'}
ASK = 'Ask for permission.'
GIT_ENV = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'GIT_CONFIG_GLOBAL': '/dev/null',
           'GIT_CONFIG_NOSYSTEM': '1', 'HOME': '/nonexistent'}


def wait_for(predicate: Callable[[], object], what: str, timeout: float = TIMEOUT) -> object:
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f'{what} not seen within {timeout} s')
        time.sleep(0.02)


def make_repo(path: Path) -> Path:
    path.mkdir(parents=True)
    for args in (['init', '-q', '-b', 'main'],
                 ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '-q',
                  '--allow-empty', '-m', 'First']):
        subprocess.run(['git', *args], cwd=path, env=GIT_ENV, check=True, capture_output=True)
    return path.resolve()


class Lines:
    def __init__(self, pipe) -> None:
        self.lines: queue.Queue[str | None] = queue.Queue()
        self.thread = threading.Thread(target=self._read, args=(pipe,), daemon=True)
        self.thread.start()

    def _read(self, pipe) -> None:
        for text in pipe:
            self.lines.put(text)
        self.lines.put(None)

    def next(self, timeout: float = TIMEOUT) -> str | None:
        try:
            return self.lines.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(f'no line within {timeout} s') from None

    def events(self, count: int) -> list[dict]:
        found = []
        for _ in range(count):
            text = self.next()
            if text is None:
                raise AssertionError(f'the stream ended after {len(found)} of {count} events')
            found.append(json.loads(text))
        return found


class LiveCase(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name).resolve()
        (self.base / 'home').mkdir()
        self.repo = make_repo(self.base / 'repo')
        self.socket = self.base / 'api.sock'
        self.data_dir = self.base / 'data'
        self.config = self.base / 'config.toml'
        claude = self.base / 'claude'
        claude.write_text(f'#!/bin/sh\nexec {sys.executable} {FAKE_CLAUDE} "$@"\n')
        claude.chmod(0o700)
        self.config.write_text(f'claude_bin = "{claude}"\n')
        self.env = {
            'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'LANG': 'C.UTF-8',
            'HOME': str(self.base / 'home'), 'XDG_RUNTIME_DIR': str(self.base / 'run'),
            'XDG_DATA_HOME': str(self.base / 'xdg-data'),
            'XDG_CONFIG_HOME': str(self.base / 'xdg-config'),
        }
        self.client = Client(str(self.socket), timeout=TIMEOUT)

    def stop(self, process: subprocess.Popen, signum: int = signal.SIGTERM) -> int:
        if process.poll() is None:
            process.send_signal(signum)
        try:
            return process.wait(TIMEOUT)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise AssertionError(f'{process.args} did not stop') from None

    def serve(self, module: str = 'loom_service') -> subprocess.Popen:
        log = open(self.base / f'{module}.log', 'w+')
        self.addCleanup(log.close)
        process = subprocess.Popen(
            [sys.executable, '-m', module, 'serve', '--socket', str(self.socket),
             '--data-dir', str(self.data_dir), '--config', str(self.config)],
            env=self.env, cwd=self.base, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=log)
        self.addCleanup(self.stop, process)
        serving = f'loom: serving on {self.socket}\n'
        wait_for(lambda: serving in (self.base / f'{module}.log').read_text(), serving.strip())
        self.assertEqual(self.client.projects_list(), [])
        return process

    def cli(self, *argv: str, stdin: str | None = None, cwd: Path | None = None,
            socket: bool = True, **env: str) -> subprocess.CompletedProcess:
        args = [sys.executable, '-m', 'loom_client']
        if socket:
            args += ['--socket', str(self.socket)]
        return subprocess.run([*args, *argv], env={**self.env, **env}, cwd=cwd or self.base,
                              input=stdin, stdin=None if stdin is not None else subprocess.DEVNULL,
                              capture_output=True, text=True, timeout=TIMEOUT)

    def cli_json(self, *argv: str, **options: object) -> object:
        done = self.cli('--json', *argv, **options)
        self.assertEqual((done.returncode, done.stderr), (0, ''), argv)
        self.assertEqual(done.stdout.count('\n'), 1, done.stdout)
        return json.loads(done.stdout)

    def follower(self, *argv: str) -> tuple[subprocess.Popen, Lines, Path]:
        err = self.base / f'follow-{time.monotonic_ns()}.err'
        with open(err, 'w') as file:
            process = subprocess.Popen(
                [sys.executable, '-m', 'loom_client', '--socket', str(self.socket), '--json',
                 'threads', 'events', '--follow', *argv],
                env=self.env, cwd=self.base, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=file, text=True)
        self.addCleanup(process.stdout.close)
        self.addCleanup(self.stop, process, signal.SIGKILL)
        return process, Lines(process.stdout), err

    def all_events(self, thread_id: str | None = None) -> list[dict]:
        found, after = [], 0
        while page := self.client.threads_events(after, thread_id):
            found += page
            after = page[-1]['id']
        return found

    def results(self, thread_id: str, count: int = 1) -> list[dict]:
        def ready() -> list[dict]:
            found = [event for event in self.all_events(thread_id)
                     if event['type'] == 'turn.result']
            return found if len(found) >= count else []

        return wait_for(ready, f'{count} turn.result of {thread_id}')

    def idle(self, thread_id: str) -> dict:
        def ready() -> dict | None:
            info = self.client.threads_get(thread_id)
            return info if info['state'] == 'idle' else None

        return wait_for(ready, f'{thread_id} idle')

    def started(self, message: str = 'hello') -> dict:
        self.client.projects_add('Repo', str(self.repo), permission_mode='auto')
        thread = self.client.threads_launch('repo', 'First', message,
                                            workspace={'strategy': 'root'})
        self.results(thread['id'])
        self.idle(thread['id'])
        return thread


class ScriptTest(LiveCase):
    def test_the_loom_script_lists_its_commands(self) -> None:
        for argv, names in (
            (['--help'], ['serve', 'threads', 'projects', 'schedules', 'requests']),
            (['threads', '--help'],
             ['launch', 'delegate', 'send', 'list', 'get', 'stop', 'settle', 'update',
              'events']),
            (['projects', '--help'], ['add', 'set-mode', 'remove', 'list']),
            (['schedules', '--help'], ['add', 'list', 'pause', 'remove']),
        ):
            with self.subTest(argv=argv):
                done = subprocess.run([str(LOOM), *argv], env=self.env, capture_output=True,
                                      text=True, timeout=TIMEOUT)
                self.assertEqual(done.returncode, 0, done.stderr)
                for name in names:
                    self.assertRegex(done.stdout, rf'\n    {name}\b')

    def test_with_no_server_a_command_exits_3_naming_the_socket(self) -> None:
        done = self.cli('projects', 'list')
        self.assertEqual((done.returncode, done.stdout), (3, ''))
        self.assertTrue(done.stderr.startswith(f'loom: cannot reach Loom at {self.socket}: '))

    def test_with_no_socket_path_at_all_a_command_exits_3_and_says_so(self) -> None:
        env = dict(self.env)
        del env['XDG_RUNTIME_DIR']
        done = subprocess.run([sys.executable, '-m', 'loom_client', 'threads', 'list'], env=env,
                              capture_output=True, text=True, timeout=TIMEOUT)
        self.assertEqual(done.returncode, 3)
        self.assertIn('loom: no socket path', done.stderr)

    def test_loom_serve_starts_the_server(self) -> None:
        process = self.serve('loom_client')
        self.assertEqual(self.cli_json('projects', 'list'), [])
        self.assertEqual(self.stop(process), 0)
        self.assertFalse(self.socket.exists())


class ApiTest(LiveCase):
    def setUp(self) -> None:
        super().setUp()
        self.serve()

    def test_the_python_api_runs_every_served_call(self) -> None:
        client = self.client
        project = client.projects_add('Repo', str(self.repo), permission_mode='auto')
        self.assertEqual(set(project), set(ProjectInfo.__annotations__))
        self.assertEqual(project['workspace_root'], str(self.repo))
        self.assertEqual(client.projects_list(), [project])

        thread = client.threads_launch('repo', 'First', 'hello', workspace={'strategy': 'root'})
        self.assertLessEqual(set(ThreadInfo.__annotations__), set(thread))
        self.assertEqual(thread['workspace_path'], str(self.repo))
        self.results(thread['id'])
        followed = []
        for event in client.follow_events(thread_id=thread['id']):
            followed.append(event)
            if event['type'] == 'turn.result':
                break
        self.assertEqual(set(followed[0]), EVENT_KEYS)
        tools = [event['data'] for event in followed if event['type'] == 'turn.tool']
        self.assertEqual(tools[0]['tool_use_id'], 'toolu_fake_1')
        self.assertEqual(tools[0]['name'], 'Bash')
        self.assertEqual(followed[-1]['data'], {'state': 'completed', 'text': 'hello'})

        self.assertEqual(self.idle(thread['id'])['id'], thread['id'])
        self.assertEqual([item['id'] for item in client.threads_list('repo')], [thread['id']])
        self.assertEqual(client.threads_list('none'), [])
        self.assertEqual(client.threads_send(thread['id'], 'again')['id'], thread['id'])
        texts = [event['data']['text'] for event in self.results(thread['id'], 2)]
        self.assertEqual(texts, ['hello', 'again'])

        page = client.threads_events()
        ids = [event['id'] for event in page]
        self.assertEqual(ids, sorted(set(ids)))
        resumed = client.follow_events(after=ids[2])
        self.assertEqual(next(resumed)['id'], ids[3])
        resumed.close()

        child = client.threads_delegate(thread['id'], 'check it')
        self.assertEqual(child['parent_id'], thread['id'])
        self.assertEqual(self.results(child['id'])[0]['data']['text'], 'check it')
        self.assertEqual(client.threads_stop(child['id'])['id'], child['id'])
        self.assertEqual(client.threads_settle(child['id'])['state'], 'settled')

        changed = client.projects_set_mode('repo', 'approval-required')
        self.assertEqual(changed['permission_mode'], 'approval-required')
        other = client.projects_add('Other', str(make_repo(self.base / 'other')))
        self.assertEqual(client.projects_remove('other'), other)
        self.assertEqual(client.projects_list(), [changed])

        self.assertEqual(client.schedules_list(), [])
        with self.assertRaises(LoomApiError) as caught:
            client.requests_respond('r1', 'allow')
        self.assertEqual(caught.exception.code, 'not_found')
        with self.assertRaises(LoomApiError) as caught:
            client.threads_get('nope')
        self.assertEqual(caught.exception.code, 'not_found')

    def test_a_retried_launch_with_the_same_id_starts_one_thread(self) -> None:
        self.client.projects_add('Repo', str(self.repo))
        first = self.client.threads_launch('repo', 'First', 'hi', id='launch-1',
                                           workspace={'strategy': 'root'})
        again = self.client.threads_launch('repo', 'First', 'hi', id='launch-1',
                                           workspace={'strategy': 'root'})
        self.assertEqual(first['id'], again['id'])
        self.assertEqual(len(self.client.threads_list()), 1)


class CliTest(LiveCase):
    def setUp(self) -> None:
        super().setUp()
        self.serve()

    def test_the_cli_runs_every_served_call_with_json(self) -> None:
        project = self.cli_json('projects', 'add', 'Repo', '.', '--mode', 'auto', cwd=self.repo)
        self.assertEqual(set(project), set(ProjectInfo.__annotations__))
        self.assertEqual(project['workspace_root'], str(self.repo))
        self.assertEqual(self.cli_json('projects', 'list'), [project])
        self.assertEqual(self.cli_json('project', 'list'), [project])

        thread = self.cli_json('threads', 'launch', 'repo', 'First', '-', '--workspace', 'root',
                               stdin='from stdin\nline two')
        self.assertLessEqual(set(ThreadInfo.__annotations__), set(thread))
        self.assertEqual(self.results(thread['id'])[0]['data']['text'], 'from stdin\nline two')
        self.idle(thread['id'])
        got = self.cli_json('threads', 'get', thread['id'])
        self.assertEqual(got, self.client.threads_get(thread['id']))
        listed = self.cli_json('threads', 'list', '--project', 'repo')
        self.assertEqual(listed, self.client.threads_list('repo'))
        self.assertEqual([item['id'] for item in listed], [thread['id']])

        sent = self.cli_json('threads', 'send', thread['id'], 'again', '--mode', 'queue')
        self.assertLessEqual(set(ThreadInfo.__annotations__), set(sent))
        self.results(thread['id'], 2)

        done = self.cli('--json', 'threads', 'events', '--thread', thread['id'])
        self.assertEqual(done.returncode, 0)
        printed = [json.loads(text) for text in done.stdout.splitlines()]
        self.assertEqual(printed, self.all_events(thread['id'])[:len(printed)])
        self.assertTrue(all(set(event) == EVENT_KEYS for event in printed))

        child = self.cli_json('threads', 'delegate', thread['id'], 'check it')
        self.assertEqual(child['parent_id'], thread['id'])
        self.results(child['id'])
        self.assertEqual(self.cli_json('threads', 'stop', child['id'])['id'], child['id'])
        self.assertEqual(self.cli_json('threads', 'settle', child['id'])['state'], 'settled')

        changed = self.cli_json('projects', 'set-mode', 'repo', 'approval-required')
        self.assertEqual(changed['permission_mode'], 'approval-required')
        other = self.cli_json('projects', 'add', 'Other', str(make_repo(self.base / 'other')))
        self.assertEqual(self.cli_json('projects', 'remove', 'other'), other)
        self.assertEqual(self.cli_json('projects', 'list'), [changed])

    def test_audience_filters_lists_and_events_and_update_changes_it(self) -> None:
        self.cli_json('projects', 'add', 'Repo', str(self.repo), '--mode', 'auto')
        person = self.cli_json('threads', 'launch', 'repo', 'Person', 'hi', '--workspace', 'root',
                               '--audience', 'person')
        agent = self.cli_json('threads', 'launch', 'repo', 'Agent', 'hi', '--workspace', 'root')
        self.assertEqual((person['audience'], agent['audience']), ('person', 'agent'))
        for thread in (person, agent):
            self.results(thread['id'])
            self.idle(thread['id'])

        def listed(*argv: str) -> list[str]:
            return [thread['id'] for thread in self.cli_json('threads', 'list', *argv)]

        def events(audience: str) -> list[dict]:
            done = self.cli('--json', 'threads', 'events', '--audience', audience)
            self.assertEqual((done.returncode, done.stderr), (0, ''))
            return [json.loads(text) for text in done.stdout.splitlines()]

        self.assertCountEqual(listed(), [person['id'], agent['id']])
        self.assertEqual(listed('--audience', 'person'), [person['id']])
        self.assertEqual(listed('--audience', 'agent'), [agent['id']])
        agent_events = events('agent')
        self.assertIn(agent['id'], {event['thread_id'] for event in agent_events})
        self.assertNotIn(person['id'], {event['thread_id'] for event in agent_events})

        updated = self.cli_json('threads', 'update', agent['id'], '--audience', 'person')
        self.assertEqual((updated['id'], updated['audience']), (agent['id'], 'person'))
        changes = [event for event in events('person') if event['type'] == 'thread.updated']
        self.assertEqual([(event['thread_id'], event['data']) for event in changes], [
            (agent['id'], {'audience': 'person', 'previous_audience': 'agent'}),
        ])
        self.assertEqual(listed('--audience', 'agent'), [])

    def test_human_output_prints_one_line_per_event(self) -> None:
        thread = self.started('a\x1b[2Jb\tc\nd')
        done = self.cli('threads', 'events', '--thread', thread['id'])
        self.assertEqual(done.returncode, 0)
        lines = done.stdout.splitlines()
        self.assertEqual(len(lines), len(self.all_events(thread['id'])))
        self.assertTrue(any(text.endswith('turn.text a\\x1b[2Jb\\tc\\nd') for text in lines))
        self.assertNotIn('\x1b', done.stdout)

    def test_an_api_error_exits_1(self) -> None:
        done = self.cli('threads', 'get', 'nope')
        self.assertEqual((done.returncode, done.stdout), (1, ''))
        self.assertEqual(done.stderr, "loom: not_found: no thread with id 'nope'\n")
        done = self.cli('--json', 'threads', 'get', 'nope')
        self.assertEqual(done.returncode, 1)
        self.assertEqual(json.loads(done.stdout)['error']['code'], 'not_found')

    def test_the_schedules_commands_run_through_a_real_server(self) -> None:
        past = (datetime.now(UTC) - timedelta(minutes=5)).isoformat(timespec='seconds')
        done = self.cli('schedules', 'add', 'Ping', '--at', past, '--action', 'notify')
        self.assertEqual((done.returncode, done.stderr), (0, ''))
        ping = done.stdout.split()[0]

        def fired() -> list[str]:
            done = self.cli('threads', 'events')
            return [text for text in done.stdout.splitlines() if ' schedule.fired ' in text]

        self.assertEqual(len(wait_for(fired, 'schedule.fired')), 1)
        self.assertTrue(fired()[0].endswith(f' - schedule.fired {ping} notify late Ping'),
                        fired())
        done_line = f'{ping}  done  notify  at {past}  -  Ping'
        self.assertEqual(self.cli('schedules', 'list').stdout, done_line + '\n')

        done = self.cli('schedules', 'add', 'Every minute', '--every', '60', '--action', 'notify')
        self.assertEqual(done.returncode, 0, done.stderr)
        minute = done.stdout.split()[0]
        listed = self.cli('schedules', 'list').stdout.splitlines()
        self.assertEqual(listed[0], done_line)
        self.assertTrue(listed[1].startswith(f'{minute}  '), listed)
        self.assertTrue(listed[1].endswith('  notify  every 60s  -  Every minute'), listed)
        self.assertEqual(self.cli_json('schedules', 'list'), self.client.schedules_list())

        paused = f'{minute}  paused  notify  every 60s  -  Every minute\n'
        self.assertEqual(self.cli('schedules', 'pause', minute).stdout, paused)
        self.assertEqual(self.cli('schedules', 'remove', minute).stdout, paused)
        self.assertEqual(self.cli('schedules', 'remove', ping).stdout, done_line + '\n')
        self.assertEqual(self.cli('schedules', 'list').stdout, '')
        self.assertEqual(self.client.schedules_list(), [])

        done = self.cli('schedules', 'add', 'Too often', '--every', '30', '--action', 'notify')
        self.assertEqual((done.returncode, done.stdout), (1, ''))
        self.assertTrue(done.stderr.startswith('loom: invalid: '), done.stderr)

    def test_projects_set_mode_from_an_agent_thread_is_forbidden(self) -> None:
        done = self.cli('projects', 'add', 'Repo', str(self.repo), LOOM_THREAD='anything')
        self.assertEqual(done.returncode, 0, done.stderr)
        done = self.cli('projects', 'set-mode', 'repo', 'full-access', LOOM_THREAD='anything')
        self.assertEqual(done.returncode, 1)
        self.assertTrue(done.stderr.startswith('loom: forbidden: '), done.stderr)
        self.assertIsNone(self.client.projects_list()[0]['permission_mode'])

    def test_loom_socket_is_used_and_socket_wins_over_it(self) -> None:
        done = self.cli('--json', 'projects', 'list', socket=False, LOOM_SOCKET=str(self.socket))
        self.assertEqual((done.returncode, done.stdout), (0, '[]\n'))
        other = self.base / 'other.sock'
        done = subprocess.run(
            [sys.executable, '-m', 'loom_client', '--socket', str(other), 'projects', 'list'],
            env={**self.env, 'LOOM_SOCKET': str(self.socket)}, capture_output=True, text=True,
            timeout=TIMEOUT)
        self.assertEqual(done.returncode, 3)
        self.assertIn(str(other), done.stderr)


class RequestsTest(LiveCase):
    def asked(self) -> tuple[dict, str]:
        self.client.projects_add('Repo', str(self.repo), permission_mode='auto')
        thread = self.client.threads_launch('repo', 'Ask', ASK, workspace={'strategy': 'root'})

        def opened() -> list[dict]:
            return [event for event in self.all_events(thread['id'])
                    if event['type'] == 'request.opened']

        request_id = wait_for(opened, 'request.opened')[0]['data']['request_id']
        info = self.client.threads_get(thread['id'])
        self.assertEqual((info['state'], info['waiting']), ('running', True))
        return thread, request_id

    def responses(self, count: int = 1) -> list[dict]:
        path = self.repo / 'responses.jsonl'

        def recorded() -> list[dict]:
            if not path.exists():
                return []
            found = [json.loads(text)['response'] for text in path.read_text().splitlines()]
            return found if len(found) >= count else []

        return wait_for(recorded, f'{count} recorded responses')

    def closed(self, thread_id: str) -> list[dict]:
        def found() -> list[dict]:
            return [event['data'] for event in self.all_events(thread_id)
                    if event['type'] == 'request.closed']

        return wait_for(found, 'request.closed')

    def test_the_python_api_answers_a_request(self) -> None:
        self.serve()
        thread, request_id = self.asked()
        answered = self.client.requests_respond(request_id, 'allow')
        self.assertEqual((answered['id'], answered['waiting']), (thread['id'], False))
        [response] = self.responses()
        self.assertEqual(response['request_id'], request_id)
        self.assertEqual(response['response']['behavior'], 'allow')
        self.assertEqual(self.closed(thread['id']),
                         [{'request_id': request_id, 'decision': 'allow'}])
        self.results(thread['id'])
        self.assertEqual(self.idle(thread['id'])['waiting'], False)

    def test_the_cli_answers_a_request_and_a_second_answer_is_a_conflict(self) -> None:
        self.serve()
        thread, request_id = self.asked()
        done = self.cli('requests', 'respond', request_id, 'deny', '--message', 'no')
        self.assertEqual((done.returncode, done.stderr), (0, ''))
        self.assertTrue(done.stdout.startswith(f'{thread["id"]}  '), done.stdout)
        [response] = self.responses()
        self.assertEqual(response['response'], {'behavior': 'deny', 'message': 'no'})
        self.results(thread['id'])

        done = self.cli('request', 'respond', request_id, 'allow')
        self.assertEqual((done.returncode, done.stdout), (1, ''))
        self.assertEqual(done.stderr, f'loom: conflict: request {request_id} is closed\n')
        self.assertEqual(len(self.responses()), 1)
        lines = self.cli('threads', 'events', '--thread', thread['id']).stdout.splitlines()
        self.assertTrue(any(text.endswith(f'request.closed {request_id} deny')
                            for text in lines), lines)

    def test_an_unanswered_request_is_denied_after_request_timeout(self) -> None:
        with open(self.config, 'a') as file:
            file.write('request_timeout = 1\n')
        self.serve()
        thread, request_id = self.asked()
        self.assertEqual(self.closed(thread['id']),
                         [{'request_id': request_id, 'decision': 'deny', 'reason': 'timeout'}])
        [response] = self.responses()
        self.assertEqual(response['response'], {
            'behavior': 'deny',
            'message': 'No answer came within 1 s, so Loom denied this request.',
        })
        self.results(thread['id'])
        self.assertEqual(self.idle(thread['id'])['waiting'], False)
        lines = self.cli('threads', 'events', '--thread', thread['id']).stdout.splitlines()
        self.assertTrue(any(text.endswith(f'request.closed {request_id} deny timeout')
                            for text in lines), lines)


class FollowTest(LiveCase):
    def setUp(self) -> None:
        super().setUp()
        self.server = self.serve()

    def test_follow_prints_new_events_and_resumes_with_nothing_missed_or_repeated(self) -> None:
        thread = self.started()
        stored = self.all_events()
        after = stored[1]['id']
        process, lines, _ = self.follower('--after', str(after))
        self.assertEqual(lines.events(len(stored) - 2), stored[2:])

        self.client.threads_send(thread['id'], 'again')
        printed = []
        while not printed or printed[-1]['type'] != 'turn.result':
            printed += lines.events(1)
        self.assertEqual(printed[-1]['data']['text'], 'again')
        self.assertEqual(self.stop(process, signal.SIGINT), 130)
        last = printed[-1]['id']

        self.idle(thread['id'])
        expected = [event for event in self.all_events() if event['id'] > last]
        self.assertTrue(expected)
        _, resumed, _ = self.follower('--after', str(last))
        self.assertEqual(resumed.events(len(expected)), expected)
        self.client.threads_send(thread['id'], 'third')
        more = []
        while not more or more[-1]['type'] != 'turn.result':
            more += resumed.events(1)
        self.assertEqual(more[-1]['data']['text'], 'third')
        since = [event for event in self.all_events() if event['id'] > last]
        self.assertEqual(expected + more, since[:len(expected) + len(more)])

    def test_a_server_that_stops_ends_the_follow_with_exit_3_and_the_resume_id(self) -> None:
        self.started()
        stored = self.all_events()
        last = stored[-1]['id']
        process, lines, err = self.follower()
        self.assertEqual(lines.events(len(stored)), stored)
        followed = self.client.follow_events()
        self.assertEqual([next(followed) for _ in stored], stored)

        self.assertEqual(self.stop(self.server), 0)
        self.assertEqual(process.wait(TIMEOUT), 3)
        self.assertIsNone(lines.next())
        message = err.read_text()
        self.assertTrue(message.startswith('loom: '), message)
        self.assertIn(f'after event {last}', message)
        self.assertTrue(message.endswith(f'; resume with --after {last}\n'), message)
        self.assertNotIn('Traceback', message)
        with self.assertRaises(LoomConnectionError) as caught:
            next(followed)
        self.assertEqual(caught.exception.last_event_id, last)

    def test_ctrl_c_during_follow_exits_130_without_a_traceback(self) -> None:
        self.started()
        process, lines, err = self.follower()
        lines.events(1)
        self.assertEqual(self.stop(process, signal.SIGINT), 130)
        self.assertEqual(err.read_text(), '')


if __name__ == '__main__':
    unittest.main()
