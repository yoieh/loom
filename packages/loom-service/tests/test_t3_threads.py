import ast
import asyncio
import json
import unittest
import uuid
from pathlib import Path

from loom_core.errors import LoomError
from loom_core.service import Caller, Service
from loom_runner_t3 import T3Error, T3NotFound
from loom_runner_t3.reasons import NotPaired
from loom_runner_t3.session import SEND_MODES
from loom_service.t3_threads import CALL_LIMIT, serve_t3_threads

from .support import ServerCase, make_repo
from .test_t3 import NOT_PAIRED, NOT_RUNNING, TOKEN, credential_text, write_runtime

CLIENT_API = Path(__file__).resolve().parents[2] / 'loom-client' / 'loom_client' / 'api.py'
STEER = next(mode for mode in SEND_MODES if mode == 'steer')
CANNOT_TELL = 'cannot tell the permission mode of this thread or the T3 thread'
WIDER = 'a thread may not send to a T3 thread whose permission mode is wider than its own'
LONG_ID = 'x' * 201

GOOD = {
    't3_threads.list': {'project_id': 'repo'},
    't3_threads.search': {'query': 'hello'},
    't3_threads.read': {'thread_id': 'mcp:t1'},
    't3_threads.send': {'id': 'c1', 'thread_id': 'mcp:t1', 'message': 'hi'},
    't3_threads.interrupt': {'id': 'c1', 'thread_id': 'mcp:t1'},
}

LIMIT_TEXT = 'limit must be an integer from 1 to 100'
SEARCH_LIMIT_TEXT = 'limit must be an integer from 1 to 50'
THREAD_TEXT = 'thread_id must be a string of 1 to 200 characters'
ID_TEXT = 'id must be a string of 1 to 200 characters'
STATUSES_TEXT = 'statuses must be a list of at most 10 strings'

BAD = [
    ('t3_threads.list', {'project_id': None}, 'project_id is required'),
    ('t3_threads.list', {'project_id': 5}, 'project_id must be a string'),
    ('t3_threads.list', {'statuses': 'running'}, STATUSES_TEXT),
    ('t3_threads.list', {'statuses': ['running', 1]}, STATUSES_TEXT),
    ('t3_threads.list', {'statuses': ['s'] * 11}, STATUSES_TEXT),
    ('t3_threads.list', {'title_contains': 5}, 'title_contains must be a string'),
    ('t3_threads.list', {'limit': 0}, LIMIT_TEXT),
    ('t3_threads.list', {'limit': 101}, LIMIT_TEXT),
    ('t3_threads.list', {'limit': True}, LIMIT_TEXT),
    ('t3_threads.list', {'limit': '5'}, LIMIT_TEXT),
    ('t3_threads.list', {'limit': 1.5}, LIMIT_TEXT),
    ('t3_threads.list', {'cursor': -1}, 'cursor must be an integer of 0 or more'),
    ('t3_threads.list', {'cursor': False}, 'cursor must be an integer of 0 or more'),
    ('t3_threads.search', {'query': None}, 'query is required'),
    ('t3_threads.search', {'query': ''}, 'query must be a non-empty string'),
    ('t3_threads.search', {'query': 5}, 'query must be a non-empty string'),
    ('t3_threads.search', {'project_id': 5}, 'project_id must be a string'),
    ('t3_threads.search', {'limit': 0}, SEARCH_LIMIT_TEXT),
    ('t3_threads.search', {'limit': 51}, SEARCH_LIMIT_TEXT),
    ('t3_threads.search', {'limit': True}, SEARCH_LIMIT_TEXT),
    ('t3_threads.read', {'thread_id': None}, 'thread_id is required'),
    ('t3_threads.read', {'thread_id': ''}, THREAD_TEXT),
    ('t3_threads.read', {'thread_id': 'x' * 201}, THREAD_TEXT),
    ('t3_threads.read', {'thread_id': 5}, THREAD_TEXT),
    ('t3_threads.read', {'view': 'all'}, 'view must be one of messages, activity'),
    ('t3_threads.read', {'view': 5}, 'view must be one of messages, activity'),
    ('t3_threads.read', {'after_position': -1},
     'after_position must be an integer of 0 or more'),
    ('t3_threads.read', {'after_position': True},
     'after_position must be an integer of 0 or more'),
    ('t3_threads.read', {'limit': 0}, LIMIT_TEXT),
    ('t3_threads.read', {'limit': 101}, LIMIT_TEXT),
    ('t3_threads.read', {'item_id': 5}, 'item_id must be a string'),
    ('t3_threads.read', {'text_offset': -1}, 'text_offset must be an integer of 0 or more'),
    ('t3_threads.read', {'text_offset': '0'}, 'text_offset must be an integer of 0 or more'),
    ('t3_threads.send', {'id': None}, ID_TEXT),
    ('t3_threads.send', {'id': LONG_ID}, ID_TEXT),
    ('t3_threads.send', {'id': 5}, ID_TEXT),
    ('t3_threads.send', {'thread_id': None}, 'thread_id is required'),
    ('t3_threads.send', {'thread_id': 'x' * 201}, THREAD_TEXT),
    ('t3_threads.send', {'message': None}, 'message is required'),
    ('t3_threads.send', {'message': ''}, 'message must be a non-empty string'),
    ('t3_threads.send', {'message': ['hi']}, 'message must be a non-empty string'),
    ('t3_threads.send', {'mode': 'restart'}, 'mode must be one of steer, queue, interrupt'),
    ('t3_threads.send', {'mode': 1}, 'mode must be one of steer, queue, interrupt'),
    ('t3_threads.interrupt', {'id': None}, ID_TEXT),
    ('t3_threads.interrupt', {'id': LONG_ID}, ID_TEXT),
    ('t3_threads.interrupt', {'thread_id': None}, 'thread_id is required'),
    ('t3_threads.interrupt', {'thread_id': ''}, THREAD_TEXT),
    ('t3_threads.interrupt', {'reason': 'x' * 2001},
     'reason must be a string of at most 2000 characters'),
    ('t3_threads.interrupt', {'reason': 5}, 'reason must be a string of at most 2000 characters'),
]


def params_of(call: str, changes: dict) -> dict:
    params = {**GOOD[call], **changes}
    return {key: value for key, value in params.items() if value is not None}


def request_id(call: str, thread_id: str, command_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f'loom:{call}:{thread_id}:{command_id}'))


def client_timeout() -> float:
    tree = ast.parse(CLIENT_API.read_text(encoding='utf-8'))
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == 'DEFAULT_TIMEOUT'):
            return ast.literal_eval(node.value)
    raise AssertionError(f'no DEFAULT_TIMEOUT in {CLIENT_API}')


class FakeT3:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.answer = {'thread': {'id': 'mcp:t1'}, 'items': []}
        self.error: Exception | None = None
        self.mode: object = 'auto'
        self.gate: asyncio.Event | None = None

    def names(self) -> list[str]:
        return [name for name, _, _ in self.calls]

    async def _answer(self, name: str, *args: object, **kwargs: object) -> object:
        self.calls.append((name, args, kwargs))
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        return self.answer

    async def list_threads(self, root: str, **kwargs: object) -> object:
        return await self._answer('list_threads', root, **kwargs)

    async def search(self, query: str, **kwargs: object) -> object:
        return await self._answer('search', query, **kwargs)

    async def read(self, thread_id: str, **kwargs: object) -> object:
        return await self._answer('read', thread_id, **kwargs)

    async def runtime_mode(self, thread_id: str) -> object:
        self.calls.append(('runtime_mode', (thread_id,), {}))
        if self.error is not None:
            raise self.error
        return self.mode

    async def send(self, thread_id: str, message: str, **kwargs: object) -> object:
        return await self._answer('send', thread_id, message, **kwargs)

    async def interrupt(self, thread_id: str, **kwargs: object) -> object:
        return await self._answer('interrupt', thread_id, **kwargs)


class T3CallsCase(ServerCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.fake = FakeT3()
        self.service = self.served(self.fake)
        await self.person('projects.add', {'id': 'p-repo', 'title': 'Repo',
                                           'path': str(self.repo)})

    def served(self, fake: FakeT3, **options: float) -> Service:
        core = self.serving.service
        service = Service(core.store, core.projects, core.orchestrator)
        serve_t3_threads(service, fake, **options)
        return service

    async def person(self, call: str, params: dict) -> object:
        return await self.service.method(call)(params, Caller())

    async def answer(self, call: str, params: dict, caller: Caller | None = None,
                     service: Service | None = None) -> object:
        service = service or self.service
        return await service.method(call)(params, caller or Caller())

    async def refused(self, call: str, params: dict, caller: Caller | None = None,
                      service: Service | None = None) -> LoomError:
        with self.assertRaises(LoomError) as raised:
            await self.answer(call, params, caller, service)
        return raised.exception


class PassThroughTest(T3CallsCase):
    async def test_each_call_passes_its_arguments_and_returns_t3s_answer(self) -> None:
        root = str(self.repo)
        cases = [
            ('t3_threads.list', {'project_id': 'repo'},
             ('list_threads', (root,), {'statuses': None, 'title_contains': None,
                                        'limit': None, 'cursor': None})),
            ('t3_threads.list',
             {'project_id': 'repo', 'statuses': ['running', 'idle'], 'title_contains': 'LM',
              'limit': 5, 'cursor': 2},
             ('list_threads', (root,), {'statuses': ['running', 'idle'],
                                        'title_contains': 'LM', 'limit': 5, 'cursor': 2})),
            ('t3_threads.search', {'query': 'hello'},
             ('search', ('hello',), {'root': None, 'limit': None})),
            ('t3_threads.search', {'query': 'hello', 'project_id': 'repo', 'limit': 3},
             ('search', ('hello',), {'root': root, 'limit': 3})),
            ('t3_threads.read', {'thread_id': 'mcp:t1'},
             ('read', ('mcp:t1',), {'view': None, 'after_position': None, 'limit': None,
                                    'item_id': None, 'text_offset': None})),
            ('t3_threads.read',
             {'thread_id': 'mcp:t1', 'view': 'activity', 'after_position': 4, 'limit': 20,
              'item_id': 'i1', 'text_offset': 0},
             ('read', ('mcp:t1',), {'view': 'activity', 'after_position': 4, 'limit': 20,
                                    'item_id': 'i1', 'text_offset': 0})),
            ('t3_threads.send', {'id': 'c1', 'thread_id': 'mcp:t1', 'message': 'hi'},
             ('send', ('mcp:t1', 'hi'),
              {'mode': STEER,
               'client_request_id': request_id('t3_threads.send', 'mcp:t1', 'c1')})),
            ('t3_threads.send',
             {'id': 'c2', 'thread_id': 'mcp:t1', 'message': 'hi', 'mode': 'queue'},
             ('send', ('mcp:t1', 'hi'),
              {'mode': 'queue',
               'client_request_id': request_id('t3_threads.send', 'mcp:t1', 'c2')})),
            ('t3_threads.interrupt', {'id': 'c1', 'thread_id': 'mcp:t1', 'reason': 'stuck'},
             ('interrupt', ('mcp:t1',),
              {'reason': 'stuck',
               'client_request_id': request_id('t3_threads.interrupt', 'mcp:t1', 'c1')})),
        ]
        for call, params, expected in cases:
            with self.subTest(call=call, params=params):
                self.fake.calls.clear()
                self.assertIs(await self.answer(call, params), self.fake.answer)
                self.assertEqual(self.fake.calls, [expected])

    async def test_send_mode_defaults_to_steer(self) -> None:
        await self.answer('t3_threads.send', GOOD['t3_threads.send'])
        self.assertIs(self.fake.calls[-1][2]['mode'], STEER)

    async def test_keys_the_contract_does_not_list_are_ignored(self) -> None:
        await self.answer('t3_threads.read', {'thread_id': 'mcp:t1', 'thread': 'x', 'more': 1})
        self.assertEqual(self.fake.names(), ['read'])


class ParamsTest(T3CallsCase):
    async def test_each_bad_param_is_invalid_and_calls_nothing(self) -> None:
        for call, changes, message in BAD:
            with self.subTest(call=call, changes=changes):
                self.fake.calls.clear()
                error = await self.refused(call, params_of(call, changes))
                self.assertEqual((error.code, error.message), ('invalid', message))
                self.assertEqual(self.fake.calls, [])

    async def test_each_boundary_passes(self) -> None:
        cases = [
            ('t3_threads.list', {'limit': 1}), ('t3_threads.list', {'limit': 100}),
            ('t3_threads.list', {'cursor': 0}),
            ('t3_threads.list', {'statuses': ['s'] * 10}),
            ('t3_threads.search', {'limit': 1}), ('t3_threads.search', {'limit': 50}),
            ('t3_threads.read', {'limit': 1}), ('t3_threads.read', {'limit': 100}),
            ('t3_threads.read', {'thread_id': 'x' * 200}),
            ('t3_threads.send', {'thread_id': 'x' * 200}),
            ('t3_threads.send', {'id': 'x' * 200}),
            ('t3_threads.interrupt', {'thread_id': 'x' * 200}),
            ('t3_threads.interrupt', {'reason': 'x' * 2000}),
        ]
        for call, changes in cases:
            with self.subTest(call=call, changes=changes):
                self.fake.calls.clear()
                self.assertIs(await self.answer(call, params_of(call, changes)),
                              self.fake.answer)
                self.assertEqual(len(self.fake.calls), 1)

    async def test_params_that_are_not_an_object_are_invalid(self) -> None:
        error = await self.refused('t3_threads.read', ['mcp:t1'])
        self.assertEqual((error.code, error.message), ('invalid', 'params must be an object'))
        self.assertEqual(self.fake.calls, [])

    async def test_an_unknown_project_is_not_found_and_calls_nothing(self) -> None:
        for call, params in (('t3_threads.list', {'project_id': 'nope'}),
                             ('t3_threads.search', {'query': 'q', 'project_id': 'nope'})):
            with self.subTest(call=call):
                error = await self.refused(call, params)
                self.assertEqual((error.code, error.message),
                                 ('not_found', "no project with id 'nope'"))
                self.assertEqual(self.fake.calls, [])


class ErrorTest(T3CallsCase):
    async def test_t3_errors_answer_not_found_or_failed_with_their_text(self) -> None:
        errors = [
            (T3NotFound('t3: thread_not_found: gone'), 'not_found',
             't3: thread_not_found: gone'),
            (NotPaired(), 'failed', NOT_PAIRED),
            (T3Error('t3: timeout: T3 did not answer within 10 s'), 'failed',
             't3: timeout: T3 did not answer within 10 s'),
        ]
        for raised, code, message in errors:
            for call, params in GOOD.items():
                with self.subTest(error=message, call=call):
                    self.fake.error = raised
                    error = await self.refused(call, params)
                    self.assertEqual((error.code, error.message), (code, message))


class SendGuardTest(T3CallsCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        wide_repo = make_repo(self.base / 'wide')
        await self.person('projects.set_mode', {'id': 'm-repo', 'project_id': 'repo',
                                                'permission_mode': 'approval-required'})
        await self.person('projects.add', {'id': 'p-wide', 'title': 'Wide',
                                           'path': str(wide_repo),
                                           'permission_mode': 'full-access'})
        self.narrow = await self.agent_thread('repo')
        self.wide = await self.agent_thread('wide')
        get = self.serving.service.orchestrator.get
        self.assertEqual((get(self.narrow)['permission_mode'], get(self.wide)['permission_mode']),
                         ('approval-required', 'full-access'))

    async def agent_thread(self, project_id: str) -> str:
        thread = await self.person('threads.launch', {
            'id': f'launch-{project_id}', 'title': project_id, 'message': 'hello',
            'project_id': project_id, 'workspace': {'strategy': 'root'}})
        return thread['id']

    async def send(self, caller: Caller) -> object:
        return await self.answer('t3_threads.send', GOOD['t3_threads.send'], caller)

    async def test_a_persons_send_is_not_checked(self) -> None:
        self.assertIs(await self.send(Caller()), self.fake.answer)
        self.assertEqual(self.fake.names(), ['send'])

    async def test_a_send_to_a_wider_t3_thread_is_forbidden(self) -> None:
        self.fake.mode = 'full-access'
        error = await self.refused('t3_threads.send', GOOD['t3_threads.send'],
                                   Caller(thread_id=self.narrow))
        self.assertEqual((error.code, error.message), ('forbidden', WIDER))
        self.assertEqual(self.fake.calls, [('runtime_mode', ('mcp:t1',), {})])

    async def test_a_send_to_an_equal_or_narrower_t3_thread_is_sent(self) -> None:
        for thread, mode in ((self.narrow, 'approval-required'), (self.wide, 'auto'),
                             (self.wide, 'full-access')):
            with self.subTest(mode=mode):
                self.fake.calls.clear()
                self.fake.mode = mode
                self.assertIs(await self.send(Caller(thread_id=thread)), self.fake.answer)
                self.assertEqual(self.fake.names(), ['runtime_mode', 'send'])

    async def test_a_t3_mode_loom_cannot_tell_is_forbidden(self) -> None:
        for mode in (None, 'yolo', 3):
            with self.subTest(mode=mode):
                self.fake.calls.clear()
                self.fake.mode = mode
                error = await self.refused('t3_threads.send', GOOD['t3_threads.send'],
                                           Caller(thread_id=self.wide))
                self.assertEqual((error.code, error.message), ('forbidden', CANNOT_TELL))
                self.assertEqual(self.fake.names(), ['runtime_mode'])

    async def test_a_caller_loom_cannot_tell_is_forbidden_before_any_t3_call(self) -> None:
        for caller in (Caller(thread_id='nope'), Caller(verified=False),
                       Caller(thread_id=self.wide, verified=False),
                       Caller(schedule_id='s1'), Caller(thread_id=self.wide, schedule_id='s1')):
            with self.subTest(caller=caller):
                error = await self.refused('t3_threads.send', GOOD['t3_threads.send'], caller)
                self.assertEqual((error.code, error.message), ('forbidden', CANNOT_TELL))
                self.assertEqual(self.fake.calls, [])

    async def test_bad_params_answer_invalid_before_the_guard(self) -> None:
        error = await self.refused('t3_threads.send', params_of('t3_threads.send',
                                                                {'message': ''}),
                                   Caller(thread_id='nope'))
        self.assertEqual(error.code, 'invalid')
        self.assertEqual(self.fake.calls, [])

    async def test_interrupt_and_reads_are_not_guarded(self) -> None:
        self.fake.mode = 'full-access'
        for call, params in GOOD.items():
            if call == 't3_threads.send':
                continue
            with self.subTest(call=call):
                self.fake.calls.clear()
                self.assertIs(await self.answer(call, params, Caller(thread_id=self.narrow)),
                              self.fake.answer)
                self.assertNotIn('runtime_mode', self.fake.names())


class RequestIdTest(T3CallsCase):
    async def test_the_request_id_follows_the_command_id_and_thread(self) -> None:
        sent = []
        for command_id, thread_id in (('c1', 'mcp:t1'), ('c1', 'mcp:t1'), ('c2', 'mcp:t1'),
                                      ('c1', 'mcp:t2')):
            await self.answer('t3_threads.send',
                              {'id': command_id, 'thread_id': thread_id, 'message': 'hi'})
            sent.append(self.fake.calls[-1][2]['client_request_id'])
        self.assertEqual(sent[0], sent[1])
        self.assertEqual(len({sent[0], sent[2], sent[3]}), 3)
        self.assertEqual(sent[0], str(uuid.uuid5(uuid.NAMESPACE_URL,
                                                 'loom:t3_threads.send:mcp:t1:c1')))
        await self.answer('t3_threads.interrupt', {'id': 'c1', 'thread_id': 'mcp:t1'})
        self.assertEqual(self.fake.calls[-1][2]['client_request_id'],
                         str(uuid.uuid5(uuid.NAMESPACE_URL,
                                        'loom:t3_threads.interrupt:mcp:t1:c1')))


class CallLimitTest(T3CallsCase):
    async def test_a_call_t3_never_answers_fails_at_the_limit(self) -> None:
        fake = FakeT3()
        fake.gate = asyncio.Event()
        service = self.served(fake, limit=0.2)
        loop = asyncio.get_running_loop()
        started = loop.time()
        error = await self.refused('t3_threads.read', GOOD['t3_threads.read'], service=service)
        self.assertLess(loop.time() - started, 2.0)
        self.assertEqual((error.code, error.message),
                         ('failed', 't3: timeout: T3 did not answer within 0.2 s'))

    async def test_a_call_answered_before_the_limit_answers_normally(self) -> None:
        fake = FakeT3()
        fake.gate = asyncio.Event()
        service = self.served(fake, limit=0.2)
        asyncio.get_running_loop().call_later(0.05, fake.gate.set)
        self.assertIs(await self.answer('t3_threads.read', GOOD['t3_threads.read'],
                                        service=service), fake.answer)

    def test_the_call_limit_is_below_the_clients_answer_timeout(self) -> None:
        self.assertEqual(CALL_LIMIT, 25.0)
        self.assertEqual(client_timeout(), 30.0)
        self.assertLess(CALL_LIMIT, client_timeout())


class SocketTest(ServerCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.http_calls: list[str] = []

    def no_http(self, method: str, url: str, headers: dict, body: bytes | None,
                timeout: float) -> tuple[int, dict, bytes]:
        self.http_calls.append(url)
        raise AssertionError('no HTTP call was expected')

    async def serve_without_http(self, **values: str) -> None:
        await self.serving.close()
        self.serving = await self.serve(self.config(**values), t3_http=self.no_http)

    def write_credential(self) -> None:
        self.data_dir.mkdir(exist_ok=True)
        path = self.data_dir / 't3-client.json'
        path.write_text(credential_text())
        path.chmod(0o600)

    async def test_t3_threads_list_answers_not_paired_then_not_running(self) -> None:
        await self.serve_without_http()
        conn = await self.connect()
        await self.add_project(conn)
        answers = [await conn.call('t3_threads.list', project_id='repo')]
        self.assertEqual(answers[0], {'ok': False, 'error': {
            'code': 'failed', 'message': NOT_PAIRED}})
        self.write_credential()
        t3_home = self.base / 't3-home'
        write_runtime(t3_home, 'http://127.0.0.1:8')
        await self.serve_without_http(t3_home=str(t3_home))
        conn = await self.connect()
        answers.append(await conn.call('t3_threads.list', project_id='repo'))
        self.assertEqual(answers[1], {'ok': False, 'error': {
            'code': 'failed', 'message': NOT_RUNNING}})
        self.assertEqual(self.http_calls, [])
        for text in (json.dumps(answers), self.log.getvalue()):
            self.assertNotIn(TOKEN, text)


if __name__ == '__main__':
    unittest.main()
