import http.client
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from loom_runner_t3.credential import Credential
from loom_runner_t3.mcp import McpClient, http_call
from loom_runner_t3.reasons import NOT_PAIRED, NOT_RUNNING, NotPaired, NotRunning, T3Error

from .replay import (
    MCP_SESSION,
    MCP_URL,
    ORIGIN,
    TOKEN,
    FakeHttp,
    Reply,
    bad_token,
    credential,
    from_recording,
    lost_session,
    make_t3_home,
    rpc,
    rpc_error,
    tool,
    tool_error,
    unknown_session,
    write_json,
)

SEND = {'threadId': 'mcp:00000000-0000-4000-8000-000000000001', 'message': 'hi',
        'mode': 'queue', 'clientRequestId': '00000000-0000-4000-8004-000000000001'}


class ClientTest(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name).resolve()
        self.home = make_t3_home(self.base)
        self.http = FakeHttp()
        self.http.on('t3_thread_send', from_recording('send'))
        self.client = McpClient(Credential(**credential()), str(self.home), self.http)

    def send(self) -> dict:
        return self.client.call_tool('t3_thread_send', SEND)

    def set_origin(self, data: object) -> None:
        write_json(self.home / 'userdata' / 'server-runtime.json', data)

    def assert_fails(self, text: str, call=None) -> T3Error:
        with self.assertRaises(T3Error) as caught:
            (call or self.send)()
        self.assertEqual(str(caught.exception), text)
        self.assertNotIn(TOKEN, str(caught.exception))
        return caught.exception

    def test_the_first_call_initializes_then_calls(self) -> None:
        answer = self.send()
        self.assertEqual(answer['status'], 'starting')
        self.assertEqual(self.http.keys(),
                         ['initialize', 'notifications/initialized', 't3_thread_send'])
        first, initialized, call = self.http.calls
        self.assertEqual(first.payload['params']['protocolVersion'], '2025-06-18')
        self.assertNotIn('Mcp-Session-Id', first.headers)
        self.assertEqual(initialized.payload, {'jsonrpc': '2.0',
                                               'method': 'notifications/initialized'})
        for sent in self.http.calls:
            self.assertEqual(sent.method, 'POST')
            self.assertEqual(sent.url, MCP_URL)
            self.assertEqual(sent.timeout, 10.0)
            self.assertEqual(sent.headers['Authorization'], f'Bearer {TOKEN}')
            self.assertEqual(sent.headers['MCP-Protocol-Version'], '2025-06-18')
            self.assertEqual(sent.headers['Content-Type'], 'application/json')
            self.assertEqual(sent.headers['Accept'], 'application/json, text/event-stream')
        self.assertEqual(initialized.headers['Mcp-Session-Id'], MCP_SESSION)
        self.assertEqual(call.headers['Mcp-Session-Id'], MCP_SESSION)
        self.assertEqual(call.payload['jsonrpc'], '2.0')
        self.assertEqual(call.payload['method'], 'tools/call')
        self.assertEqual(call.payload['params'], {'name': 't3_thread_send', 'arguments': SEND})

    def test_later_calls_reuse_the_session(self) -> None:
        self.send()
        self.send()
        self.assertEqual(self.http.keys(), ['initialize', 'notifications/initialized',
                                            't3_thread_send', 't3_thread_send'])
        self.assertEqual(self.http.calls[-1].headers['Mcp-Session-Id'], MCP_SESSION)

    def test_a_lost_session_initializes_again_and_retries_once(self) -> None:
        for name, lost in (('400 -32600', lost_session()), ('404', unknown_session())):
            with self.subTest(name):
                self.http = FakeHttp()
                self.http.on('t3_thread_send', lost, from_recording('send'))
                self.client = McpClient(Credential(**credential()), str(self.home), self.http)
                self.assertEqual(self.send()['status'], 'starting')
                self.assertEqual(self.http.keys(), [
                    'initialize', 'notifications/initialized', 't3_thread_send',
                    'initialize', 'notifications/initialized', 't3_thread_send',
                ])
                first, retry = self.http.sent('t3_thread_send')
                self.assertEqual(first.body, retry.body)
                self.assertEqual(retry.arguments['clientRequestId'], SEND['clientRequestId'])

    def test_a_second_lost_session_fails_the_call(self) -> None:
        for name, lost in (('400 -32600', lost_session()), ('404', unknown_session())):
            with self.subTest(name):
                self.http = FakeHttp()
                self.http.on('t3_thread_send', lost)
                self.client = McpClient(Credential(**credential()), str(self.home), self.http)
                self.assert_fails('t3: protocol: T3 dropped the MCP session twice')
                self.assertEqual(self.http.count('t3_thread_send'), 2)
                self.assertEqual(self.http.count('initialize'), 2)

    def test_another_400_is_not_retried(self) -> None:
        self.http.on('t3_thread_send', rpc_error(400, -32602, 'bad params'))
        self.assert_fails('t3: -32602: bad params')
        self.assertEqual(self.http.count('t3_thread_send'), 1)

    def test_a_timeout_or_connection_error_is_not_retried(self) -> None:
        cases = {
            TimeoutError(f'timed out {TOKEN}'): 't3: timeout: T3 did not answer within 10 s',
            OSError(f'boom {TOKEN}'): 't3: unreachable: OSError',
            ConnectionRefusedError(111, TOKEN): 't3: unreachable: ConnectionRefusedError',
            http.client.RemoteDisconnected(TOKEN): 't3: unreachable: RemoteDisconnected',
            http.client.BadStatusLine(TOKEN): 't3: unreachable: BadStatusLine',
        }
        for error, text in cases.items():
            with self.subTest(type(error).__name__):
                self.http = FakeHttp()
                self.http.on('t3_thread_send', error, from_recording('send'))
                self.client = McpClient(Credential(**credential()), str(self.home), self.http)
                failed = self.assert_fails(text)
                self.assertIsNone(failed.__cause__)
                self.assertEqual(self.http.count('t3_thread_send'), 1)
                self.assertEqual(self.http.count('initialize'), 1)

    def test_a_timeout_during_initialize_fails_with_no_retry(self) -> None:
        self.http.on('initialize', TimeoutError())
        self.assert_fails('t3: timeout: T3 did not answer within 10 s')
        self.assertEqual(self.http.keys(), ['initialize'])

    def test_json_and_event_stream_bodies_decode_alike(self) -> None:
        value = {'threadId': 'mcp:x', 'status': 'running'}
        self.http.on('t3_thread_read', tool(value))
        plain = self.client.call_tool('t3_thread_read', {})
        self.http.on('t3_thread_read', tool(value, sse=True))
        streamed = self.client.call_tool('t3_thread_read', {})
        self.assertEqual(plain, value)
        self.assertEqual(streamed, value)
        self.assertEqual(self.http.count('initialize'), 1)

    def test_the_initialize_answer_may_be_an_event_stream(self) -> None:
        reply = from_recording('initialize')
        reply.sse = True
        self.http.on('initialize', reply)
        self.assertEqual(self.send()['status'], 'starting')

    def test_an_event_stream_with_no_answer_fails(self) -> None:
        self.http.on('t3_thread_send', Reply(200, {'content-type': 'text/event-stream'},
                                             b'event: ping\n\n'))
        self.assert_fails('t3: protocol: the event stream holds no answer to the request')

    def test_another_content_type_fails(self) -> None:
        self.http.on('t3_thread_send', Reply(200, {'content-type': 'text/html'}, b'<html>'))
        self.assert_fails("t3: protocol: unexpected content type 'text/html'")

    def test_a_body_that_is_not_json_fails(self) -> None:
        self.http.on('t3_thread_send', Reply(200, {'content-type': 'application/json'},
                                             b'{oops'))
        self.assert_fails('t3: protocol: the answer is not JSON')

    def test_the_origin_check_runs_before_every_call(self) -> None:
        self.send()
        sent = len(self.http.calls)
        cases = {
            'missing': None,
            'another origin': {'origin': 'http://127.0.0.1:41774'},
            'null origin': {'origin': None},
            'no origin': {'serverVersion': '0.1.2'},
            'number origin': {'origin': 41773},
            'list origin': {'origin': [ORIGIN]},
            'not an object': [ORIGIN],
        }
        runtime = self.home / 'userdata' / 'server-runtime.json'
        for name, data in cases.items():
            with self.subTest(name):
                if data is None:
                    runtime.unlink()
                else:
                    self.set_origin(data)
                with self.assertRaises(NotRunning) as caught:
                    self.send()
                self.assertEqual(str(caught.exception), NOT_RUNNING)
                self.assertEqual(len(self.http.calls), sent)
        self.set_origin({'origin': ORIGIN})
        self.send()
        self.assertEqual(len(self.http.calls), sent + 1)

    def test_the_origin_check_runs_before_initialize(self) -> None:
        self.set_origin({'origin': 'http://127.0.0.1:1'})
        with self.assertRaises(NotRunning):
            self.send()
        self.assertEqual(self.http.calls, [])

    def test_a_401_means_not_paired(self) -> None:
        self.http.on('t3_thread_send', bad_token())
        failed = self.assert_fails(NOT_PAIRED)
        self.assertIsInstance(failed, NotPaired)

    def test_a_401_on_initialize_means_not_paired(self) -> None:
        self.http.on('initialize', bad_token())
        self.assertIsInstance(self.assert_fails(NOT_PAIRED), NotPaired)
        self.assertEqual(self.http.keys(), ['initialize'])

    def test_an_is_error_result_gives_its_code_and_message(self) -> None:
        self.http.on('t3_thread_read', from_recording('read-unknown-thread'))
        self.assert_fails(
            't3: thread_not_found: Thread mcp:00000000-0000-4000-8000-000000000004 is no '
            'longer available.',
            lambda: self.client.call_tool('t3_thread_read', {}),
        )

    def test_an_error_text_is_cut_to_200_characters(self) -> None:
        prefix = 't3: code: '
        for length, kept in ((200, 200), (201, 200), (199, 199)):
            with self.subTest(length=length):
                message = 'm' * (length - len(prefix))
                self.http.on('t3_thread_send', tool_error('code', message))
                failed = self.assert_fails((prefix + message)[:kept])
                self.assertEqual(len(str(failed)), kept)

    def test_an_error_text_is_one_line_and_redacted(self) -> None:
        self.http.on('t3_thread_send', tool_error('leak', f'token {TOKEN}\nsecond line'))
        self.assert_fails('t3: leak: token [redacted] second line')

    def test_an_is_error_result_with_plain_text(self) -> None:
        self.http.on('t3_thread_send', rpc({'content': [{'type': 'text', 'text': 'nope'}],
                                            'isError': True}))
        self.assert_fails('t3: error: nope')

    def test_a_null_missing_or_wrong_result_is_a_protocol_error(self) -> None:
        bodies = {
            'null': {'jsonrpc': '2.0', 'id': 0, 'result': None},
            'missing': {'jsonrpc': '2.0', 'id': 0},
            'a list': {'jsonrpc': '2.0', 'id': 0, 'result': [1]},
            'a string': {'jsonrpc': '2.0', 'id': 0, 'result': 'ok'},
        }
        for name, body in bodies.items():
            with self.subTest(name):
                self.http.on('t3_thread_send',
                             Reply(200, {'content-type': 'application/json'}, body))
                failed = self.assert_fails('t3: protocol: the answer has no result object')
                self.assertTrue(str(failed).startswith('t3: protocol: '))

    def test_an_answer_to_another_request_is_a_protocol_error(self) -> None:
        self.http.on('t3_thread_send', Reply(200, {'content-type': 'application/json'},
                                             {'jsonrpc': '1.0', 'result': {}}))
        self.assert_fails('t3: protocol: the answer is not a JSON-RPC answer to the request')

    def test_a_tool_answer_with_no_json_object_is_a_protocol_error(self) -> None:
        self.http.on('t3_thread_send', rpc({'content': [{'type': 'text', 'text': '[1]'}],
                                            'isError': False}))
        self.assert_fails('t3: protocol: the tool answer holds no JSON object')

    def test_a_tool_answer_falls_back_to_its_text(self) -> None:
        self.http.on('t3_thread_send', rpc({'content': [{'type': 'text',
                                                         'text': json.dumps({'a': 1})}]}))
        self.assertEqual(self.send(), {'a': 1})

    def test_initialize_with_no_session_id_fails(self) -> None:
        reply = from_recording('initialize')
        reply.headers = {'content-type': 'application/json'}
        self.http.on('initialize', reply)
        self.assert_fails('t3: protocol: initialize gave no Mcp-Session-Id')

    def test_an_http_error_status_fails(self) -> None:
        self.http.on('t3_thread_send', Reply(500, {'content-type': 'application/json'},
                                             {'jsonrpc': '2.0', 'id': 0, 'result': {}}))
        self.assert_fails('t3: http: status 500')


class HttpCallTest(unittest.TestCase):
    def test_a_redirect_is_returned_and_not_followed(self) -> None:
        connection = mock.Mock()
        response = connection.getresponse.return_value
        response.status = 302
        response.getheaders.return_value = [('Location', 'http://example.com/elsewhere')]
        response.read.return_value = b''
        with mock.patch('http.client.HTTPConnection', return_value=connection) as made:
            status, headers, body = http_call('GET', ORIGIN + '/oauth/mcp/authorize?a=1', {},
                                              None, 10.0)
        made.assert_called_once_with('127.0.0.1', 41773, timeout=10.0)
        connection.request.assert_called_once_with('GET', '/oauth/mcp/authorize?a=1',
                                                   body=None, headers={})
        self.assertEqual(connection.getresponse.call_count, 1)
        connection.close.assert_called_once()
        self.assertEqual((status, headers, body),
                         (302, {'location': 'http://example.com/elsewhere'}, b''))

    def test_only_http_urls_are_allowed(self) -> None:
        for url in ('https://127.0.0.1:1/mcp', 'file:///etc/passwd', 'http:///x'):
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    http_call('GET', url, {}, None, 1.0)


if __name__ == '__main__':
    unittest.main()
