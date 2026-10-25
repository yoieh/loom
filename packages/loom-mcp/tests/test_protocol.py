import io
import json
import unittest

from loom_mcp.server import MAX_LINE, Server

from .support import McpCase, ok

TEN = ['launch_thread', 'delegate', 'task_status', 'task_cancel', 'thread_send', 'threads',
       'emit', 'schedule', 'schedules', 'unschedule']

PROPERTIES = {
    'launch_thread': ['title', 'message', 'project_id', 'workspace', 'runner', 'model',
                      'audience', 'concurrency_key', 'on_busy', 'settle'],
    'delegate': ['task', 'workspace', 'runner', 'model', 'concurrency_key', 'on_busy'],
    'task_status': ['task_id'],
    'task_cancel': ['task_id'],
    'thread_send': ['thread_id', 'message', 'mode'],
    'threads': ['project_id'],
    'emit': ['topic', 'payload', 'project_id', 'key', 'cause'],
    'schedule': ['trigger', 'text', 'action', 'thread_id', 'project_id', 'title',
                 'concurrency_key', 'on_busy', 'settle'],
    'schedules': [],
    'unschedule': ['id'],
}


def padded_ping(size: int) -> bytes:
    head = b'{"jsonrpc":"2.0","id":"big","method":"ping","pad":"'
    tail = b'"}'
    return head + b'x' * (size - len(head) - len(tail)) + tail


class InitializeTest(McpCase):
    def test_a_known_version_is_answered(self) -> None:
        mcp = self.start()
        answer = mcp.request('initialize', {'protocolVersion': '2025-06-18'})
        self.assertEqual(answer, {'jsonrpc': '2.0', 'id': 1, 'result': {
            'protocolVersion': '2025-06-18', 'capabilities': {'tools': {}},
            'serverInfo': {'name': 'loom', 'version': '0.1.0'}}})

    def test_each_known_version_is_kept(self) -> None:
        mcp = self.start()
        for version in ('2024-11-05', '2025-03-26', '2025-06-18', '2025-11-25'):
            with self.subTest(version=version):
                answer = mcp.request('initialize', {'protocolVersion': version})
                self.assertEqual(answer['result']['protocolVersion'], version)

    def test_any_other_version_gets_the_latest(self) -> None:
        mcp = self.start()
        for params in ({'protocolVersion': '1999-01-01'}, {}, {'protocolVersion': None},
                       {'protocolVersion': 20250618}, {'protocolVersion': ['2025-06-18']}):
            with self.subTest(params=params):
                answer = mcp.request('initialize', params)
                self.assertEqual(answer['result']['protocolVersion'], '2025-11-25')

    def test_ping_answers_an_empty_object(self) -> None:
        mcp = self.start()
        self.assertEqual(mcp.request('ping'), {'jsonrpc': '2.0', 'id': 1, 'result': {}})


class ToolsListTest(McpCase):
    def test_lists_exactly_the_ten_tools(self) -> None:
        mcp = self.start()
        tools = mcp.request('tools/list')['result']['tools']
        self.assertEqual([tool['name'] for tool in tools], TEN)
        for tool in tools:
            with self.subTest(tool=tool['name']):
                self.assertIsInstance(tool['description'], str)
                self.assertTrue(tool['description'])
                schema = tool['inputSchema']
                self.assertEqual(schema['type'], 'object')
                self.assertEqual(list(schema['properties']), PROPERTIES[tool['name']])
                self.assertTrue(set(schema['required']) <= set(schema['properties']))

    def test_no_tool_names_a_project_write_or_a_mode(self) -> None:
        mcp = self.start()
        tools = mcp.request('tools/list')['result']['tools']
        names = [tool['name'] for tool in tools]
        names += [key for tool in tools for key in tool['inputSchema']['properties']]
        for name in names:
            with self.subTest(name=name):
                self.assertNotIn('permission', name)
                self.assertNotIn('projects', name)
                self.assertFalse(name.startswith('project') and name != 'project_id')
                self.assertFalse('mode' in name and name not in ('mode', 'model'))
        modes = [tool['name'] for tool in tools if 'mode' in tool['inputSchema']['properties']]
        self.assertEqual(modes, ['thread_send'])
        text = json.dumps(tools)
        for word in ('permission_mode', 'projects.add', 'projects.set_mode', 'projects.remove',
                     'set_mode', 'full-access'):
            self.assertNotIn(word, text)

    def test_required_arguments(self) -> None:
        mcp = self.start()
        tools = mcp.request('tools/list')['result']['tools']
        required = {tool['name']: tool['inputSchema']['required'] for tool in tools}
        self.assertEqual(required, {
            'launch_thread': ['title', 'message'], 'delegate': ['task'],
            'task_status': ['task_id'], 'task_cancel': ['task_id'],
            'thread_send': ['thread_id', 'message'], 'threads': [],
            'emit': ['topic', 'payload'], 'schedule': ['trigger', 'text'], 'schedules': [],
            'unschedule': ['id'],
        })

    def test_schedule_takes_event_triggers_and_emit_an_integer_cause(self) -> None:
        mcp = self.start()
        tools = {tool['name']: tool for tool in mcp.request('tools/list')['result']['tools']}
        trigger = tools['schedule']['inputSchema']['properties']['trigger']['description']
        self.assertIn('"type": "event"', trigger)
        self.assertIn('"topic"', trigger)
        self.assertIn('event trigger', tools['schedule']['description'])
        self.assertEqual(tools['emit']['inputSchema']['properties']['cause']['type'], 'integer')
        self.assertEqual(tools['emit']['inputSchema']['properties']['payload']['type'], 'object')

    def test_params_missing_or_null_lists_the_tools(self) -> None:
        mcp = self.start()
        for message in ({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'},
                        {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list', 'params': None}):
            with self.subTest(message=message):
                mcp.send(message)
                answer = mcp.read()
                self.assertEqual(answer['id'], message['id'])
                self.assertEqual(len(answer['result']['tools']), 10)


class NotificationTest(McpCase):
    def test_a_notification_gets_no_answer(self) -> None:
        mcp = self.start()
        mcp.send({'jsonrpc': '2.0', 'method': 'notifications/initialized'})
        mcp.send({'jsonrpc': '2.0', 'method': 'no/such/method'})
        mcp.send({'jsonrpc': '2.0', 'method': 'tools/call', 'params': {'name': 'schedules'}})
        mcp.send({'jsonrpc': '2.0', 'params': 'not an object'})
        self.assertEqual(mcp.request('ping'), {'jsonrpc': '2.0', 'id': 1, 'result': {}})
        self.assertEqual(self.loom.requests, [])


class EnvelopeErrorTest(McpCase):
    def assert_error(self, answer: dict, code: int, request_id: object = None) -> None:
        self.assertEqual(answer['jsonrpc'], '2.0')
        self.assertEqual(answer['id'], request_id)
        self.assertEqual(answer['error']['code'], code)
        self.assertIsInstance(answer['error']['message'], str)
        self.assertNotIn('result', answer)

    def test_a_line_that_is_not_json(self) -> None:
        mcp = self.start()
        for raw in (b'not json\n', b'{"id": 1,\n', b'\xff\xfe\n', b'\n',
                    b'[' * 100000 + b'\n'):
            with self.subTest(raw=raw[:20]):
                mcp.send_raw(raw)
                self.assert_error(mcp.read(), -32700)
        self.assertEqual(mcp.request('ping')['result'], {})

    def test_a_value_that_is_not_an_object(self) -> None:
        mcp = self.start()
        for raw in (b'[]\n', b'5\n', b'"ping"\n', b'null\n',
                    b'[{"jsonrpc":"2.0","id":1,"method":"ping"}]\n'):
            with self.subTest(raw=raw):
                mcp.send_raw(raw)
                self.assert_error(mcp.read(), -32600)
        self.assertEqual(mcp.request('ping')['result'], {})

    def test_an_unknown_method(self) -> None:
        mcp = self.start()
        mcp.send({'jsonrpc': '2.0', 'id': 'u', 'method': 'resources/list'})
        answer = mcp.read()
        self.assert_error(answer, -32601, 'u')
        self.assertEqual(answer['error']['message'], 'unknown method: resources/list')
        self.assertEqual(mcp.request('ping')['result'], {})

    def test_method_missing_null_or_not_a_string(self) -> None:
        mcp = self.start()
        for index, method in enumerate((None, 5, ['ping'])):
            with self.subTest(method=method):
                mcp.send({'jsonrpc': '2.0', 'id': index, 'method': method})
                self.assert_error(mcp.read(), -32600, index)
        mcp.send({'jsonrpc': '2.0', 'id': 'none'})
        self.assert_error(mcp.read(), -32600, 'none')
        self.assertEqual(mcp.request('ping')['result'], {})

    def test_params_not_an_object(self) -> None:
        mcp = self.start()
        for index, params in enumerate(('a string', 5, [], True)):
            with self.subTest(params=params):
                mcp.send({'jsonrpc': '2.0', 'id': index, 'method': 'tools/list',
                          'params': params})
                self.assert_error(mcp.read(), -32602, index)
        self.assertEqual(mcp.request('ping')['result'], {})

    def test_tool_name_missing_null_not_a_string_or_unknown(self) -> None:
        mcp = self.start()
        for params in ({}, {'name': None}, {'name': 5}, {'name': 'projects_add'},
                       {'name': 'projects.set_mode'}, {'name': 'signals.emit'}):
            with self.subTest(params=params):
                answer = mcp.request('tools/call', params)
                self.assert_error(answer, -32602, answer['id'])
        answer = mcp.request('tools/call', {'name': 'projects_add'})
        self.assertEqual(answer['error']['message'], 'unknown tool: projects_add')
        self.assertEqual(self.loom.requests, [])

    def test_arguments_not_an_object(self) -> None:
        mcp = self.start()
        for arguments in ([], 'x', 5):
            with self.subTest(arguments=arguments):
                answer = mcp.request('tools/call', {'name': 'schedules', 'arguments': arguments})
                self.assert_error(answer, -32602, answer['id'])
        self.assertEqual(self.loom.requests, [])

    def test_arguments_missing_or_null_mean_an_empty_object(self) -> None:
        self.loom.on('schedules.list', ok([]))
        mcp = self.start()
        for params in ({'name': 'schedules'}, {'name': 'schedules', 'arguments': None}):
            with self.subTest(params=params):
                answer = mcp.request('tools/call', params)
                self.assertEqual(answer['result'], {
                    'content': [{'type': 'text', 'text': '[]'}], 'isError': False})
        self.assertEqual(self.loom.calls, [('schedules.list', {}), ('schedules.list', {})])


class _Broken:
    def call(self, name: str, arguments: dict) -> object:
        raise RuntimeError('the handler broke')


class InternalErrorTest(unittest.TestCase):
    def test_an_exception_in_a_handler_is_an_internal_error_and_reading_goes_on(self) -> None:
        output = io.BytesIO()
        errors = io.StringIO()
        server = Server(_Broken(), output, errors)
        stream = io.BytesIO(
            b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"schedules"}}\n'
            b'{"jsonrpc":"2.0","id":2,"method":"ping"}\n')
        self.assertEqual(server.run(stream), 0)
        answers = [json.loads(raw) for raw in output.getvalue().splitlines()]
        self.assertEqual(answers, [
            {'jsonrpc': '2.0', 'id': 1, 'error': {'code': -32603, 'message': 'internal error'}},
            {'jsonrpc': '2.0', 'id': 2, 'result': {}},
        ])
        self.assertIn('Traceback', errors.getvalue())
        self.assertIn('RuntimeError: the handler broke', errors.getvalue())
        self.assertNotIn(b'Traceback', output.getvalue())


class LineCapTest(McpCase):
    def test_a_line_of_exactly_the_cap_is_answered(self) -> None:
        mcp = self.start()
        request = padded_ping(MAX_LINE)
        self.assertEqual(len(request), 1_048_576)
        mcp.send_raw(request + b'\n')
        self.assertEqual(mcp.read(), {'jsonrpc': '2.0', 'id': 'big', 'result': {}})
        self.assertEqual(mcp.request('ping')['result'], {})

    def test_a_line_one_byte_over_the_cap_is_refused(self) -> None:
        mcp = self.start()
        mcp.send_raw(padded_ping(MAX_LINE + 1) + b'\n')
        self.assertEqual(mcp.read(), {'jsonrpc': '2.0', 'id': None, 'error': {
            'code': -32600, 'message': 'request line is over 1048576 bytes'}})
        self.assertEqual(mcp.request('ping'), {'jsonrpc': '2.0', 'id': 1, 'result': {}})

    def test_a_much_longer_line_is_skipped_and_the_next_is_read(self) -> None:
        mcp = self.start()
        mcp.send_raw(padded_ping(5 * MAX_LINE) + b'\n')
        answer = mcp.read()
        self.assertEqual(answer['error']['code'], -32600)
        self.assertEqual(mcp.request('ping')['result'], {})


class StdoutTest(McpCase):
    def test_eof_ends_with_exit_0_and_stdout_holds_only_protocol_lines(self) -> None:
        self.loom.on('schedules.list', ok([]))
        mcp = self.start()
        mcp.request('initialize', {'protocolVersion': '2025-06-18'})
        mcp.send({'jsonrpc': '2.0', 'method': 'notifications/initialized'})
        mcp.request('tools/list')
        mcp.tool('schedules')
        mcp.tool('threads')
        mcp.send_raw(b'not json\n')
        mcp.send_raw(b'{"jsonrpc":"2.0","id":9,"method":"ping"}')
        self.assertEqual(mcp.close_stdin(), 0)
        rest = mcp.rest()
        self.assertEqual(len(rest), 2)
        for raw in rest:
            self.assertTrue(raw.endswith(b'\n'))
            message = json.loads(raw)
            self.assertEqual(message['jsonrpc'], '2.0')
            self.assertTrue('result' in message or 'error' in message)
        self.assertEqual(json.loads(rest[1]), {'jsonrpc': '2.0', 'id': 9, 'result': {}})

    def test_eof_at_once_exits_0(self) -> None:
        mcp = self.start()
        self.assertEqual(mcp.close_stdin(), 0)
        self.assertEqual(mcp.rest(), [])


if __name__ == '__main__':
    unittest.main()
