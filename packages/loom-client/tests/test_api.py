import os
import socket
import threading
import unittest
import uuid
from collections.abc import Iterator
from unittest import mock

from loom_contracts.calls import CallName

from loom_client import (
    Client, LoomApiError, LoomConnectionError, LoomProtocolError, default_socket,
)

from .support import CLOSE, FakeServerCase, event, failed, line, ok, stream

WORKSPACE = {'strategy': 'worktree', 'branch': 'b', 'base': 'main'}
TRIGGER = {'type': 'interval', 'seconds': 60}

CALLS = [
    ('threads_launch', ('demo', 'First', 'hello'), {}, 'threads.launch',
     {'project_id': 'demo', 'title': 'First', 'message': 'hello'}),
    ('threads_launch', ('demo', 'First', 'hello'),
     {'workspace': WORKSPACE, 'runner': 'claude', 'model': 'm', 'requests': 'caller',
      'origin': 'cli'}, 'threads.launch',
     {'project_id': 'demo', 'title': 'First', 'message': 'hello', 'workspace': WORKSPACE,
      'runner': 'claude', 'model': 'm', 'requests': 'caller', 'origin': 'cli'}),
    ('threads_delegate', ('t1', 'check it'), {}, 'threads.delegate',
     {'parent_id': 't1', 'task': 'check it'}),
    ('threads_delegate', ('t1', 'check it'),
     {'workspace': {'strategy': 'root'}, 'runner': 'claude', 'model': 'm'}, 'threads.delegate',
     {'parent_id': 't1', 'task': 'check it', 'workspace': {'strategy': 'root'},
      'runner': 'claude', 'model': 'm'}),
    ('threads_send', ('t1', 'again'), {}, 'threads.send', {'thread_id': 't1', 'message': 'again'}),
    ('threads_send', ('t1', 'again'), {'mode': 'queue'}, 'threads.send',
     {'thread_id': 't1', 'message': 'again', 'mode': 'queue'}),
    ('threads_list', (), {}, 'threads.list', {}),
    ('threads_list', ('demo',), {}, 'threads.list', {'project_id': 'demo'}),
    ('threads_get', ('t1',), {}, 'threads.get', {'thread_id': 't1'}),
    ('threads_stop', ('t1',), {}, 'threads.stop', {'thread_id': 't1'}),
    ('threads_settle', ('t1',), {}, 'threads.settle', {'thread_id': 't1'}),
    ('threads_events', (), {}, 'threads.events', {'after': 0}),
    ('threads_events', (5, 't1'), {}, 'threads.events', {'after': 5, 'thread_id': 't1'}),
    ('requests_respond', ('r1', 'allow'), {}, 'requests.respond',
     {'request_id': 'r1', 'decision': 'allow'}),
    ('requests_respond', ('r1', 'deny'), {'message': 'no'}, 'requests.respond',
     {'request_id': 'r1', 'decision': 'deny', 'message': 'no'}),
    ('schedules_add', (TRIGGER, 'tick'), {}, 'schedules.add',
     {'trigger': TRIGGER, 'text': 'tick'}),
    ('schedules_add', (TRIGGER, 'tick'),
     {'action': 'send', 'thread_id': 't1', 'project_id': 'demo', 'title': 'Tick'},
     'schedules.add',
     {'trigger': TRIGGER, 'text': 'tick', 'action': 'send', 'thread_id': 't1',
      'project_id': 'demo', 'title': 'Tick'}),
    ('schedules_list', (), {}, 'schedules.list', {}),
    ('schedules_list', ('t1', 'demo'), {}, 'schedules.list',
     {'thread_id': 't1', 'project_id': 'demo'}),
    ('schedules_pause', ('s1',), {}, 'schedules.pause', {'schedule_id': 's1'}),
    ('schedules_remove', ('s1',), {}, 'schedules.remove', {'schedule_id': 's1'}),
    ('projects_add', ('Demo', '/tmp/demo'), {}, 'projects.add',
     {'title': 'Demo', 'path': '/tmp/demo'}),
    ('projects_add', ('Demo', '/tmp/demo'), {'permission_mode': 'auto'}, 'projects.add',
     {'title': 'Demo', 'path': '/tmp/demo', 'permission_mode': 'auto'}),
    ('projects_set_mode', ('demo', 'full-access'), {}, 'projects.set_mode',
     {'project_id': 'demo', 'permission_mode': 'full-access'}),
    ('projects_remove', ('demo',), {}, 'projects.remove', {'project_id': 'demo'}),
    ('projects_list', (), {}, 'projects.list', {}),
]

WRITES = {
    'threads.launch', 'threads.delegate', 'threads.send', 'threads.stop', 'threads.settle',
    'requests.respond', 'schedules.add', 'schedules.pause', 'schedules.remove', 'projects.add',
    'projects.set_mode', 'projects.remove',
}

BAD_ANSWERS = {
    'not JSON': b'nope\n',
    'not UTF-8': b'\xff\xfe\n',
    'a list': line([1]),
    'a string': line('ok'),
    'ok missing': line({'result': 1}),
    'ok null': line({'ok': None, 'result': 1}),
    'ok a number': line({'ok': 1, 'result': 1}),
    'ok a string': line({'ok': 'true', 'result': 1}),
    'result missing': line({'ok': True}),
    'error missing': line({'ok': False}),
    'error null': line({'ok': False, 'error': None}),
    'error a string': line({'ok': False, 'error': 'bad'}),
    'code missing': line({'ok': False, 'error': {'message': 'm'}}),
    'code null': line({'ok': False, 'error': {'code': None, 'message': 'm'}}),
    'code a number': line({'ok': False, 'error': {'code': 1, 'message': 'm'}}),
    'message missing': line({'ok': False, 'error': {'code': 'invalid'}}),
    'message null': line({'ok': False, 'error': {'code': 'invalid', 'message': None}}),
    'message a list': line({'ok': False, 'error': {'code': 'invalid', 'message': []}}),
}

BAD_EVENT_LINES = {
    'not JSON': b'{"event"\n',
    'not an object': line([1]),
    'event missing': line({'data': {}}),
    'event null': line({'event': None}),
    'event a list': line({'event': [1]}),
    'id missing': line({'event': {'type': 'turn.text'}}),
    'id null': line({'event': {'id': None}}),
    'id a string': line({'event': {'id': '7'}}),
    'id a float': line({'event': {'id': 7.0}}),
    'id a bool': line({'event': {'id': True}}),
}

BAD_ACKS = {
    'missing': [],
    'a half line': [b'{"ok": true, "res'],
    'follow false': ok({'follow': False}),
    'follow 1': ok({'follow': 1}),
    'null': ok(None),
    'a list': ok([]),
    'an extra key': ok({'follow': True, 'more': 1}),
    'not JSON': [b'nope\n'],
}


def is_uuid4(value: object) -> bool:
    try:
        return uuid.UUID(str(value)).version == 4 and str(uuid.UUID(str(value))) == value
    except ValueError:
        return False


class RequestTest(FakeServerCase):
    def test_each_method_sends_its_call_and_params_and_returns_the_result(self) -> None:
        client = Client(str(self.socket))
        for method, args, kwargs, call, params in CALLS:
            with self.subTest(method=method, kwargs=kwargs):
                answer = [event(1)] if method == 'threads_events' else {'answer': method}
                self.answer(ok(answer))
                self.assertEqual(getattr(client, method)(*args, **kwargs), answer)
                request = self.server.requests[-1]
                self.assertEqual(request['v'], 'v1')
                self.assertEqual(request['call'], call)
                sent = dict(request['params'])
                if call in WRITES:
                    self.assertTrue(is_uuid4(sent.pop('id')), request)
                self.assertEqual(sent, params)

    def test_every_v1_call_has_a_method(self) -> None:
        names = {call.value.replace('.', '_') for call in CallName}
        self.assertEqual(names, {method for method, *_ in CALLS})
        self.assertEqual({call for *_, call, _ in CALLS}, {call.value for call in CallName})

    def test_write_methods_send_a_new_id_each_time_and_a_given_one_as_is(self) -> None:
        client = Client(str(self.socket))
        self.answer(ok(None))
        for method, args, _, call, _ in CALLS:
            if call not in WRITES:
                continue
            with self.subTest(method=method):
                getattr(client, method)(*args)
                getattr(client, method)(*args)
                getattr(client, method)(*args, id='retry-1')
                first, second, given = (request['params']['id']
                                        for request in self.server.requests[-3:])
                self.assertNotEqual(first, second)
                self.assertTrue(is_uuid4(first) and is_uuid4(second))
                self.assertEqual(given, 'retry-1')

    def test_read_methods_send_no_id(self) -> None:
        client = Client(str(self.socket))
        self.answer(ok([]))
        for method, args, kwargs, call, _ in CALLS:
            if call not in WRITES:
                getattr(client, method)(*args, **kwargs)
                self.assertNotIn('id', self.server.requests[-1]['params'])

    def test_events_returns_the_page(self) -> None:
        page = [event(1), event(2)]
        self.answer(ok(page))
        self.assertEqual(Client(str(self.socket)).threads_events(), page)

    def test_raw_call_sends_the_name_and_params(self) -> None:
        self.answer(ok([1, 2]))
        self.assertEqual(Client(str(self.socket)).call('anything.new', {'a': 1}), [1, 2])
        self.assertEqual(self.server.requests[-1],
                         {'v': 'v1', 'call': 'anything.new', 'params': {'a': 1}})

    def test_each_call_uses_its_own_connection(self) -> None:
        self.answer(ok([]))
        client = Client(str(self.socket))
        client.projects_list()
        client.projects_list()
        self.assertEqual(self.server.connections, 2)


class AnswerTest(FakeServerCase):
    def test_an_error_answer_raises_loom_api_error(self) -> None:
        self.answer(failed('not_found', "no thread with id 'nope'"))
        with self.assertRaises(LoomApiError) as caught:
            Client(str(self.socket)).threads_get('nope')
        self.assertEqual(caught.exception.code, 'not_found')
        self.assertEqual(caught.exception.message, "no thread with id 'nope'")

    def test_a_null_result_is_none(self) -> None:
        self.answer(ok(None))
        self.assertIsNone(Client(str(self.socket)).call('projects.list', {}))

    def test_each_answer_that_breaks_the_protocol_raises_loom_protocol_error(self) -> None:
        for name, raw in BAD_ANSWERS.items():
            with self.subTest(name):
                self.answer([raw])
                with self.assertRaises(LoomProtocolError):
                    Client(str(self.socket)).projects_list()

    def test_a_connection_closed_before_a_full_line_is_a_protocol_error(self) -> None:
        for chunks in ([CLOSE], [b'{"ok": true, "result"', CLOSE]):
            with self.subTest(chunks=chunks):
                self.answer(chunks)
                with self.assertRaises(LoomProtocolError) as caught:
                    Client(str(self.socket)).projects_list()
                self.assertIsInstance(caught.exception, LoomConnectionError)

    def test_an_events_page_that_is_not_a_list_of_events_is_a_protocol_error(self) -> None:
        for result in ({'follow': True}, [1], [{'id': 'x'}], [{'id': True}], None):
            with self.subTest(result=result):
                self.answer(ok(result))
                with self.assertRaises(LoomProtocolError):
                    Client(str(self.socket)).threads_events()

    def test_a_slow_answer_times_out_as_a_connection_error(self) -> None:
        release = threading.Event()
        self.addCleanup(release.set)
        self.answer_with(lambda request: release.wait(5) and [])
        with self.assertRaises(LoomConnectionError) as caught:
            Client(str(self.socket), timeout=0.2).projects_list()
        self.assertNotIsInstance(caught.exception, LoomProtocolError)
        self.assertIn(str(self.socket), str(caught.exception))


class FollowTest(FakeServerCase):
    def test_follow_sends_the_stream_request_and_yields_each_event(self) -> None:
        events = [event(4), event(5, 'turn.tool', tool_use_id='toolu_1', name='Bash', input={})]
        self.answer(stream(*events))
        follow = Client(str(self.socket)).follow_events(3, 't1')
        self.assertEqual([next(follow), next(follow)], events)
        self.assertEqual(self.server.requests[-1], {
            'v': 'v1', 'call': 'threads.events',
            'params': {'after': 3, 'thread_id': 't1', 'follow': True},
        })
        with self.assertRaises(LoomConnectionError):
            next(follow)

    def test_a_lost_stream_names_the_last_yielded_event(self) -> None:
        self.answer(stream(event(8), event(9)))
        follow = Client(str(self.socket)).follow_events(7)
        self.assertEqual([item['id'] for item in (next(follow), next(follow))], [8, 9])
        with self.assertRaises(LoomConnectionError) as caught:
            next(follow)
        self.assertNotIsInstance(caught.exception, LoomProtocolError)
        self.assertEqual(caught.exception.last_event_id, 9)

    def test_a_stream_lost_before_any_event_names_after(self) -> None:
        self.answer(stream())
        with self.assertRaises(LoomConnectionError) as caught:
            next(Client(str(self.socket)).follow_events(42))
        self.assertEqual(caught.exception.last_event_id, 42)

    def test_a_follow_with_nothing_listening_raises_a_connection_error(self) -> None:
        with self.assertRaises(LoomConnectionError) as caught:
            next(Client(str(self.base / 'none.sock')).follow_events(3))
        self.assertIn(str(self.base / 'none.sock'), str(caught.exception))
        self.assertIsNone(caught.exception.last_event_id)

    def test_an_error_answer_to_follow_raises_loom_api_error(self) -> None:
        self.answer(failed('invalid', 'after must be an integer of 0 or more'))
        with self.assertRaises(LoomApiError) as caught:
            next(Client(str(self.socket)).follow_events())
        self.assertEqual(caught.exception.code, 'invalid')

    def test_a_bad_ack_is_a_protocol_error(self) -> None:
        for name, chunks in BAD_ACKS.items():
            with self.subTest(name):
                self.answer([*chunks, CLOSE])
                with self.assertRaises(LoomProtocolError) as caught:
                    next(Client(str(self.socket)).follow_events(2))
                self.assertEqual(caught.exception.last_event_id, 2)

    def test_a_bad_event_line_is_a_protocol_error_after_the_last_good_event(self) -> None:
        for name, raw in BAD_EVENT_LINES.items():
            with self.subTest(name):
                self.answer([*stream(event(6), close=False), raw, CLOSE])
                follow = Client(str(self.socket)).follow_events(5)
                self.assertEqual(next(follow)['id'], 6)
                with self.assertRaises(LoomProtocolError) as caught:
                    next(follow)
                self.assertEqual(caught.exception.last_event_id, 6)

    def test_a_half_event_line_is_a_protocol_error(self) -> None:
        self.answer([*stream(event(6), close=False), b'{"event": {"id": 7', CLOSE])
        follow = Client(str(self.socket)).follow_events(5)
        next(follow)
        with self.assertRaises(LoomProtocolError) as caught:
            next(follow)
        self.assertEqual(caught.exception.last_event_id, 6)

    def test_follow_waits_for_new_events_without_a_read_timeout(self) -> None:
        release = threading.Event()
        self.addCleanup(release.set)

        def reply(request: object) -> Iterator[object]:
            yield from stream(event(1), close=False)
            release.wait(5)
            yield line({'event': event(2)})
            yield CLOSE

        self.answer_with(reply)
        follow = Client(str(self.socket), timeout=0.2).follow_events()
        self.assertEqual(next(follow)['id'], 1)
        timer = threading.Timer(0.6, release.set)
        timer.start()
        self.addCleanup(timer.cancel)
        self.assertEqual(next(follow)['id'], 2)

    def test_closing_the_iterator_closes_the_connection(self) -> None:
        self.answer(stream(event(1), close=False))
        follow = Client(str(self.socket)).follow_events()
        next(follow)
        follow.close()
        self.assertEqual(len(self.server.requests), 1)


class ConnectionTest(FakeServerCase):
    def test_nothing_listening_raises_a_connection_error_naming_the_socket(self) -> None:
        path = self.base / 'none.sock'
        with self.assertRaises(LoomConnectionError) as caught:
            Client(str(path)).projects_list()
        self.assertIn(str(path), str(caught.exception))
        self.assertNotIsInstance(caught.exception, LoomProtocolError)
        self.assertIsNone(caught.exception.last_event_id)

    def test_a_path_that_is_no_socket_raises_a_connection_error(self) -> None:
        path = self.base / 'file'
        path.write_text('x')
        with self.assertRaises(LoomConnectionError):
            Client(str(path)).projects_list()

    def test_an_empty_or_nul_path_is_refused_before_connecting(self) -> None:
        for path in ('', '\x00loom', str(self.socket) + '\x00'):
            with self.subTest(path=path):
                with mock.patch.object(socket.socket, 'connect') as connect:
                    with self.assertRaises(LoomConnectionError):
                        Client(path).projects_list()
                connect.assert_not_called()

    def test_a_refusal_sent_before_the_request_is_read_is_still_an_api_error(self) -> None:
        def refuse(request: object) -> list[object]:
            return [*failed('forbidden', 'the peer runs as another user'), CLOSE]

        self.answer_with(refuse)
        with self.assertRaises(LoomApiError) as caught:
            Client(str(self.socket)).projects_add('Demo', '/tmp/demo')
        self.assertEqual(caught.exception.code, 'forbidden')


class SocketPathTest(unittest.TestCase):
    def env(self, **values: str):
        return mock.patch.dict(os.environ, values, clear=True)

    def test_loom_socket_comes_first(self) -> None:
        with self.env(LOOM_SOCKET='/x/loom.sock', XDG_RUNTIME_DIR='/run/user/1'):
            self.assertEqual(default_socket(), '/x/loom.sock')
            self.assertEqual(Client().socket_path, '/x/loom.sock')

    def test_the_runtime_dir_is_next(self) -> None:
        with self.env(XDG_RUNTIME_DIR='/run/user/1'):
            self.assertEqual(default_socket(), '/run/user/1/loom/api.sock')
        with self.env(LOOM_SOCKET='', XDG_RUNTIME_DIR='/run/user/1'):
            self.assertEqual(default_socket(), '/run/user/1/loom/api.sock')

    def test_with_neither_there_is_no_socket_path(self) -> None:
        for values in ({}, {'LOOM_SOCKET': '', 'XDG_RUNTIME_DIR': ''}):
            with self.subTest(values=values), self.env(**values):
                with self.assertRaises(LoomConnectionError) as caught:
                    default_socket()
                self.assertIn('no socket path', str(caught.exception))
                with self.assertRaises(LoomConnectionError):
                    Client()

    def test_a_given_path_wins(self) -> None:
        with self.env(LOOM_SOCKET='/x/loom.sock'):
            self.assertEqual(Client('/y/api.sock').socket_path, '/y/api.sock')
        with self.env():
            self.assertEqual(Client('/y/api.sock').socket_path, '/y/api.sock')


if __name__ == '__main__':
    unittest.main()
