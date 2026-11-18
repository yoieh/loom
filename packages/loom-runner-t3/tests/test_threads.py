import copy
import json
import tempfile
import unittest
from pathlib import Path

from loom_contracts.runners import DispatchMode
from loom_runner_t3 import T3Error, T3NotFound, T3Threads
from loom_runner_t3.reasons import NOT_PAIRED, NOT_RUNNING, NotPaired, NotRunning

from .replay import (
    NOW,
    PROJECT_ID,
    PROJECT_ROOT,
    T3_THREAD,
    TOKEN,
    FakeClock,
    FakeHost,
    FakeHttp,
    credential_text,
    from_recording,
    make_t3_home,
    projects,
    recorded,
    tool,
    tool_error,
    write_json,
)

REQUEST_ID = '00000000-0000-4000-8004-000000000001'
OTHER_ROOT = '/tmp/loom-replay/other'
OTHER_PROJECT = 'mcp:00000000-0000-4000-8001-000000000002'


def answer(name: str) -> dict:
    return recorded(name)['body']['result']['structuredContent']


def archived_read() -> dict:
    text = recorded('read-archived-thread')['body']['result']['content'][0]['text']
    return json.loads(text)


class Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name).resolve()
        self.home = make_t3_home(self.base)
        self.clock = FakeClock()
        self.host = FakeHost(str(self.base / 'data'), str(self.home), credential_text())
        self.reset()

    def reset(self) -> None:
        self.http = FakeHttp()
        self.http.on('t3_project_list', projects(PROJECT_ROOT, {
            'id': OTHER_PROJECT, 'workspaceRoot': OTHER_ROOT}))
        self.http.on('t3_thread_list', tool({'threads': [], 'nextCursor': None}))
        self.http.on('t3_thread_search', tool({'threads': []}))
        self.http.on('t3_thread_read', from_recording('read-archived-thread'))
        self.http.on('t3_thread_send', from_recording('send'))
        self.http.on('t3_thread_interrupt', from_recording('interrupt'))
        self.threads = T3Threads(self.host, http=self.http, clock=self.clock)

    def arguments(self, name: str) -> dict:
        self.assertEqual(self.http.count(name), 1)
        return self.http.sent(name)[0].arguments

    def every_method(self) -> dict:
        return {
            'list': lambda: self.threads.list_threads(OTHER_ROOT),
            'search': lambda: self.threads.search('LM-99'),
            'read': lambda: self.threads.read(T3_THREAD),
            'runtime_mode': lambda: self.threads.runtime_mode(T3_THREAD),
            'send': lambda: self.threads.send(T3_THREAD, 'ok', mode=DispatchMode.QUEUE,
                                              client_request_id=REQUEST_ID),
            'interrupt': lambda: self.threads.interrupt(T3_THREAD,
                                                        client_request_id=REQUEST_ID),
        }


class ArgumentsTest(Base):
    async def test_each_method_sends_one_call_with_exactly_its_arguments(self) -> None:
        cases = [
            ('list bare', lambda: self.threads.list_threads(OTHER_ROOT),
             't3_thread_list', {'projectId': OTHER_PROJECT}),
            ('list given', lambda: self.threads.list_threads(
                OTHER_ROOT, statuses=['running', 'idle'], title_contains='LM-99', limit=5,
                cursor=10),
             't3_thread_list', {'projectId': OTHER_PROJECT, 'statuses': ['running', 'idle'],
                                'titleContains': 'LM-99', 'limit': 5, 'cursor': 10}),
            ('search bare', lambda: self.threads.search('LM-99'),
             't3_thread_search', {'query': 'LM-99'}),
            ('search given', lambda: self.threads.search('LM-99', root=PROJECT_ROOT, limit=3),
             't3_thread_search', {'query': 'LM-99', 'projectId': PROJECT_ID, 'limit': 3}),
            ('read bare', lambda: self.threads.read(T3_THREAD),
             't3_thread_read', {'threadId': T3_THREAD}),
            ('read given', lambda: self.threads.read(
                T3_THREAD, view='transcript', after_position=4, limit=20, item_id='i4',
                text_offset=100),
             't3_thread_read', {'threadId': T3_THREAD, 'view': 'transcript',
                                'afterPosition': 4, 'limit': 20, 'itemId': 'i4',
                                'textOffset': 100}),
            ('runtime_mode', lambda: self.threads.runtime_mode(T3_THREAD),
             't3_thread_read', {'threadId': T3_THREAD, 'view': 'activity', 'limit': 1}),
            ('send', lambda: self.threads.send(T3_THREAD, 'ok', mode=DispatchMode.STEER,
                                               client_request_id=REQUEST_ID),
             't3_thread_send', {'threadId': T3_THREAD, 'message': 'ok', 'mode': 'steer',
                                'clientRequestId': REQUEST_ID}),
            ('interrupt bare', lambda: self.threads.interrupt(
                T3_THREAD, client_request_id=REQUEST_ID),
             't3_thread_interrupt', {'threadId': T3_THREAD, 'clientRequestId': REQUEST_ID}),
            ('interrupt given', lambda: self.threads.interrupt(
                T3_THREAD, client_request_id=REQUEST_ID, reason='stop'),
             't3_thread_interrupt', {'threadId': T3_THREAD, 'clientRequestId': REQUEST_ID,
                                     'reason': 'stop'}),
        ]
        tools = {'t3_thread_list', 't3_thread_search', 't3_thread_read', 't3_thread_send',
                 't3_thread_interrupt'}
        for case, method, name, expected in cases:
            with self.subTest(case=case):
                self.reset()
                await method()
                self.assertEqual(self.arguments(name), expected)
                others = [key for key in self.http.keys() if key in tools - {name}]
                self.assertEqual(others, [])


class ListTest(Base):
    async def test_list_matches_the_project_by_its_root(self) -> None:
        await self.threads.list_threads(PROJECT_ROOT)
        keys = [key for key in self.http.keys() if key.startswith('t3_')]
        self.assertEqual(keys, ['t3_project_list', 't3_thread_list'])
        self.assertEqual(self.arguments('t3_thread_list')['projectId'], PROJECT_ID)

    async def test_a_root_with_no_project_raises_not_found_and_lists_nothing(self) -> None:
        root = '/tmp/loom-replay/nowhere'
        with self.assertRaises(T3NotFound) as caught:
            await self.threads.list_threads(root)
        self.assertEqual(str(caught.exception), f't3: no T3 project at {root}')
        self.assertEqual(self.http.count('t3_thread_list'), 0)


class SearchTest(Base):
    async def test_search_without_a_root_sends_no_project(self) -> None:
        await self.threads.search('LM-99', limit=3)
        self.assertEqual(self.http.count('t3_project_list'), 0)
        self.assertNotIn('projectId', self.arguments('t3_thread_search'))


class ToolErrorTest(Base):
    async def test_thread_not_found_raises_not_found_and_other_codes_do_not(self) -> None:
        methods = {
            'read': ('t3_thread_read', lambda: self.threads.read(T3_THREAD)),
            'send': ('t3_thread_send', lambda: self.threads.send(
                T3_THREAD, 'ok', mode=DispatchMode.QUEUE, client_request_id=REQUEST_ID)),
            'interrupt': ('t3_thread_interrupt', lambda: self.threads.interrupt(
                T3_THREAD, client_request_id=REQUEST_ID)),
        }
        for case, (name, method) in methods.items():
            for reply in (tool_error('thread_not_found', f'Thread {T3_THREAD} is gone.'),
                          from_recording('read-unknown-thread')):
                with self.subTest(case=case, reply='thread_not_found'):
                    self.reset()
                    self.http.on(name, reply)
                    with self.assertRaises(T3NotFound) as caught:
                        await method()
                    self.assertTrue(str(caught.exception).startswith('t3: thread_not_found: '))
            with self.subTest(case=case, reply='invalid_request'):
                self.reset()
                self.http.on(name, tool_error('invalid_request', 'bad'))
                with self.assertRaises(T3Error) as caught:
                    await method()
                self.assertNotIsInstance(caught.exception, T3NotFound)
                self.assertEqual(str(caught.exception), 't3: invalid_request: bad')


class PairingTest(Base):
    async def test_no_usable_credential_or_another_origin_calls_nothing(self) -> None:
        cases = {
            'missing secret': (None, NotPaired, NOT_PAIRED),
            'expired credential': (credential_text(expires_at=NOW), NotPaired, NOT_PAIRED),
            'not credential json': ('{"origin": ', NotPaired, NOT_PAIRED),
        }
        for case, (secret, error, text) in cases.items():
            for method, make in self.every_method().items():
                with self.subTest(case=case, method=method):
                    self.host.secrets['t3-client'] = secret
                    self.reset()
                    with self.assertRaises(error) as caught:
                        await make()
                    self.assertEqual(str(caught.exception), text)
                    self.assertEqual(self.http.calls, [])
        self.host.secrets['t3-client'] = credential_text()
        write_json(self.home / 'userdata' / 'server-runtime.json',
                   {'origin': 'http://127.0.0.1:1'})
        for method, make in self.every_method().items():
            with self.subTest(case='another origin', method=method):
                self.reset()
                with self.assertRaises(NotRunning) as caught:
                    await make()
                self.assertEqual(str(caught.exception), NOT_RUNNING)
                self.assertEqual(self.http.calls, [])


class SessionTest(Base):
    async def test_one_initialize_per_credential(self) -> None:
        await self.threads.read(T3_THREAD)
        await self.threads.interrupt(T3_THREAD, client_request_id=REQUEST_ID)
        self.assertEqual(self.http.count('initialize'), 1)
        token = 'loom-t3-test-token-new-9a8b7c'
        self.host.secrets['t3-client'] = credential_text(access_token=token)
        await self.threads.read(T3_THREAD)
        initializes = self.http.sent('initialize')
        self.assertEqual(len(initializes), 2)
        self.assertEqual(initializes[1].headers['Authorization'], f'Bearer {token}')
        self.assertEqual(self.http.calls[-1].headers['Authorization'], f'Bearer {token}')


class SendTest(Base):
    async def test_send_maps_each_mode_and_keeps_the_request_id(self) -> None:
        modes = {DispatchMode.STEER: 'steer', DispatchMode.QUEUE: 'queue',
                 DispatchMode.INTERRUPT: 'restart'}
        for index, (mode, expected) in enumerate(modes.items()):
            with self.subTest(mode=mode):
                request_id = f'request-{index}'
                await self.threads.send(T3_THREAD, 'ok', mode=mode,
                                        client_request_id=request_id)
                sent = self.http.sent('t3_thread_send')[-1].arguments
                self.assertEqual(sent['mode'], expected)
                self.assertEqual(sent['clientRequestId'], request_id)


class RuntimeModeTest(Base):
    async def test_runtime_mode_of_the_recording(self) -> None:
        self.assertEqual(await self.threads.runtime_mode(T3_THREAD), 'approval-required')

    async def test_no_usable_runtime_mode_is_none(self) -> None:
        recording = archived_read()
        missing_thread = copy.deepcopy(recording)
        del missing_thread['thread']
        missing_mode = copy.deepcopy(recording)
        del missing_mode['thread']['runtimeMode']
        cases = {
            'no thread': missing_thread,
            'thread is a list': {**recording, 'thread': []},
            'thread is null': {**recording, 'thread': None},
            'no runtimeMode': missing_mode,
            'null runtimeMode': {**recording, 'thread': {**recording['thread'],
                                                         'runtimeMode': None}},
            'number runtimeMode': {**recording, 'thread': {**recording['thread'],
                                                           'runtimeMode': 3}},
        }
        for case, value in cases.items():
            with self.subTest(case=case):
                self.http.on('t3_thread_read', tool(value))
                self.assertIsNone(await self.threads.runtime_mode(T3_THREAD))


class AnswerTest(Base):
    async def test_send_and_interrupt_return_t3s_answer(self) -> None:
        sent = await self.threads.send(T3_THREAD, 'ok', mode=DispatchMode.QUEUE,
                                       client_request_id=REQUEST_ID)
        self.assertEqual(sent, answer('send'))
        interrupted = await self.threads.interrupt(T3_THREAD, client_request_id=REQUEST_ID)
        self.assertEqual(interrupted, answer('interrupt'))


class SecretTest(Base):
    async def test_a_token_in_an_error_is_redacted(self) -> None:
        self.http.on('t3_thread_read', tool_error('leak', f'echo {TOKEN}'))
        with self.assertRaises(T3Error) as caught:
            await self.threads.read(T3_THREAD)
        self.assertEqual(str(caught.exception), 't3: leak: echo [redacted]')
        self.http.on('t3_thread_send', tool_error('thread_not_found', f'no {TOKEN}'))
        with self.assertRaises(T3NotFound) as caught:
            await self.threads.send(T3_THREAD, 'ok', mode=DispatchMode.QUEUE,
                                    client_request_id=REQUEST_ID)
        self.assertEqual(str(caught.exception), 't3: thread_not_found: no [redacted]')


if __name__ == '__main__':
    unittest.main()
