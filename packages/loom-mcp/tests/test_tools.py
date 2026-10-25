import json
import unittest
import uuid

from loom_mcp.tools import BY_NAME, ToolError, check_arguments

from .support import McpCase, dropped, failed, ok, schedule_info, signal_event, thread_info

AT_TRIGGER = {'type': 'at', 'at': '2026-10-26T09:00'}
EVENT_TRIGGER = {'type': 'event', 'topic': 'factory.run.status', 'project_id': 'demo',
                 'match': {'to': 'done'}, 'hops_max': 2, 'rate_max': 5}
WORKSPACE = {'strategy': 'worktree', 'branch': 'fix', 'base': 'main'}
MAY_HAVE_RUN = 'the call may have run, so check with threads or schedules before calling again'
MAY_HAVE_EMITTED = 'the signal may have been written, and emitting it again writes a second one'
LOST = 'the connection closed before a full answer line'

VALID = {
    'launch_thread': {'title': 'Fix it', 'message': 'Make the test pass.', 'project_id': 'demo'},
    'delegate': {'task': 'Review it.'},
    'task_status': {'task_id': 'c1'},
    'task_cancel': {'task_id': 'c1'},
    'thread_send': {'thread_id': 't2', 'message': 'Hi.'},
    'threads': {},
    'emit': {'topic': 'demo.done', 'payload': {'to': 'done'}},
    'schedule': {'trigger': AT_TRIGGER, 'text': 'Stand-up'},
    'schedules': {},
    'unschedule': {'id': 'sc1'},
}

REQUIRED_STRINGS = [('launch_thread', 'title'), ('launch_thread', 'message'),
                    ('delegate', 'task'), ('task_status', 'task_id'), ('task_cancel', 'task_id'),
                    ('thread_send', 'thread_id'), ('thread_send', 'message'),
                    ('emit', 'topic'), ('schedule', 'text'), ('unschedule', 'id')]

OPTIONAL = [('launch_thread', 'project_id', 'string'), ('launch_thread', 'workspace', 'object'),
            ('launch_thread', 'runner', 'string'), ('launch_thread', 'model', 'string'),
            ('launch_thread', 'audience', 'string'),
            ('launch_thread', 'concurrency_key', 'string'),
            ('launch_thread', 'on_busy', 'string'), ('launch_thread', 'settle', 'string'),
            ('delegate', 'workspace', 'object'), ('delegate', 'runner', 'string'),
            ('delegate', 'model', 'string'), ('delegate', 'concurrency_key', 'string'),
            ('delegate', 'on_busy', 'string'),
            ('thread_send', 'mode', 'string'), ('threads', 'project_id', 'string'),
            ('emit', 'project_id', 'string'), ('emit', 'key', 'string'),
            ('emit', 'cause', 'integer'),
            ('schedule', 'action', 'string'), ('schedule', 'thread_id', 'string'),
            ('schedule', 'project_id', 'string'), ('schedule', 'title', 'string'),
            ('schedule', 'concurrency_key', 'string'), ('schedule', 'on_busy', 'string'),
            ('schedule', 'settle', 'string')]

GIVEN = {'string': 'value', 'object': {'strategy': 'root'}, 'integer': 5}
WRONG = {'string': [5, True, ['x'], {'a': 1}], 'object': ['root', 5, ['root'], False],
         'integer': ['5', True, 5.5, 5.0, [5], {'a': 1}]}
ARTICLES = {'string': 'a string', 'object': 'an object', 'integer': 'an integer'}


def compact(value: object) -> str:
    return json.dumps(value, separators=(',', ':'))


def own_child(task_id: str = 'c1') -> dict:
    return thread_info(task_id, parent_id='t1')


class ToolsCase(McpCase):
    def setUp(self) -> None:
        super().setUp()
        self.answer_everything()

    def tearDown(self) -> None:
        for call, params in self.loom.calls:
            self.assertFalse(call.startswith('projects.'), call)
            self.assertNotIn('permission_mode', params)
            self.assertNotIn('requests', params)
        super().tearDown()

    def answer_everything(self) -> None:
        self.loom.on_get({'t1': thread_info('t1'), 'c1': own_child()})
        self.loom.on('threads.launch', ok(thread_info('new')))
        self.loom.on('threads.delegate', ok(own_child()))
        self.loom.on('threads.stop', ok(own_child()))
        self.loom.on('threads.send', ok(thread_info('t2')))
        self.loom.on('threads.list', ok([thread_info('t1')]))
        self.loom.on('signals.emit', ok(signal_event()))
        self.loom.on('schedules.add', ok(schedule_info()))
        self.loom.on('schedules.list', ok([schedule_info()]))
        self.loom.on('schedules.remove', ok(schedule_info()))

    def start_thread(self, **extra: str):
        return self.start(LOOM_THREAD='t1', **extra)

    def assert_command_id(self, params: dict) -> str:
        command_id = params['id']
        self.assertEqual(str(uuid.UUID(command_id, version=4)), command_id)
        return command_id


class EachToolTest(ToolsCase):
    def test_launch_thread(self) -> None:
        thread = thread_info('new', origin='thread:t1', audience='person')
        self.loom.on('threads.launch', ok(thread))
        mcp = self.start_thread()
        text, is_error = mcp.tool('launch_thread', {
            'title': 'Fix it', 'message': 'Make the test pass.', 'project_id': 'other',
            'workspace': WORKSPACE, 'runner': 'codex', 'model': 'gpt-5', 'audience': 'person',
            'concurrency_key': 'pm:demo', 'on_busy': 'coalesce', 'settle': 'on_result'})
        self.assertEqual((text, is_error), (compact(thread), False))
        [(call, params)] = self.loom.calls
        self.assertEqual(call, 'threads.launch')
        self.assert_command_id(params)
        self.assertEqual({key: value for key, value in params.items() if key != 'id'}, {
            'project_id': 'other', 'title': 'Fix it', 'message': 'Make the test pass.',
            'workspace': WORKSPACE, 'runner': 'codex', 'model': 'gpt-5',
            'origin': 'thread:t1', 'audience': 'person', 'concurrency_key': 'pm:demo',
            'on_busy': 'coalesce', 'settle': 'on_result'})

    def test_delegate(self) -> None:
        mcp = self.start_thread()
        text, is_error = mcp.tool('delegate', {
            'task': 'Review it.', 'workspace': {'strategy': 'root'}, 'runner': 'claude',
            'model': 'opus', 'concurrency_key': 'review:demo', 'on_busy': 'queue'})
        self.assertEqual((text, is_error),
                         (compact({'task_id': 'c1', 'thread': own_child()}), False))
        [(call, params)] = self.loom.calls
        self.assertEqual(call, 'threads.delegate')
        self.assert_command_id(params)
        self.assertEqual({key: value for key, value in params.items() if key != 'id'}, {
            'parent_id': 't1', 'task': 'Review it.', 'workspace': {'strategy': 'root'},
            'runner': 'claude', 'model': 'opus', 'concurrency_key': 'review:demo',
            'on_busy': 'queue'})

    def test_delegate_ignores_settle(self) -> None:
        mcp = self.start_thread()
        mcp.ok('delegate', {'task': 'Review it.', 'settle': 'on_result'})
        [(call, params)] = self.loom.calls
        self.assertEqual(call, 'threads.delegate')
        self.assertNotIn('settle', params)

    def test_task_status(self) -> None:
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('task_status', {'task_id': 'c1'}),
                         (compact(own_child()), False))
        self.assertEqual(self.loom.calls, [('threads.get', {'thread_id': 'c1'})])

    def test_task_cancel(self) -> None:
        stopped = own_child() | {'reason': 'stopping'}
        self.loom.on('threads.stop', ok(stopped))
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('task_cancel', {'task_id': 'c1'}), (compact(stopped), False))
        self.assertEqual(self.loom.names(), ['threads.get', 'threads.stop'])
        params = self.loom.calls[1][1]
        self.assert_command_id(params)
        self.assertEqual(params['thread_id'], 'c1')
        self.assertEqual(set(params), {'id', 'thread_id'})

    def test_thread_send(self) -> None:
        mcp = self.start_thread()
        self.assertEqual(
            mcp.tool('thread_send', {'thread_id': 't2', 'message': 'Hi.', 'mode': 'queue'}),
            (compact(thread_info('t2')), False))
        [(call, params)] = self.loom.calls
        self.assertEqual(call, 'threads.send')
        self.assert_command_id(params)
        self.assertEqual({key: value for key, value in params.items() if key != 'id'},
                         {'thread_id': 't2', 'message': 'Hi.', 'mode': 'queue'})

    def test_threads(self) -> None:
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('threads'), (compact([thread_info('t1')]), False))
        self.assertEqual(mcp.tool('threads', {'project_id': 'demo'}),
                         (compact([thread_info('t1')]), False))
        self.assertEqual(self.loom.calls, [('threads.list', {}),
                                           ('threads.list', {'project_id': 'demo'})])

    def test_schedule(self) -> None:
        mcp = self.start_thread()
        trigger = {'type': 'fixed_time', 'timeOfDay': '9:00', 'weekdays': [1, 2]}
        self.assertEqual(mcp.tool('schedule', {
            'trigger': trigger, 'text': 'Stand-up', 'action': 'launch', 'thread_id': 't2',
            'project_id': 'demo', 'title': 'Notes'}), (compact(schedule_info()), False))
        [(call, params)] = self.loom.calls
        self.assertEqual(call, 'schedules.add')
        self.assert_command_id(params)
        self.assertEqual({key: value for key, value in params.items() if key != 'id'}, {
            'trigger': trigger, 'text': 'Stand-up', 'action': 'launch', 'thread_id': 't2',
            'project_id': 'demo', 'title': 'Notes'})

    def test_emit(self) -> None:
        event = signal_event(7, project_id='demo', key='LM-12', cause=3, by='t1', hops=1)
        self.loom.on('signals.emit', ok(event))
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('emit', {
            'topic': 'demo.done', 'payload': {'to': 'done'}, 'project_id': 'demo',
            'key': 'LM-12', 'cause': 3}), (compact(event), False))
        [(call, params)] = self.loom.calls
        self.assertEqual(call, 'signals.emit')
        self.assert_command_id(params)
        self.assertEqual({key: value for key, value in params.items() if key != 'id'}, {
            'topic': 'demo.done', 'payload': {'to': 'done'}, 'project_id': 'demo',
            'key': 'LM-12', 'cause': 3})

    def test_emit_sends_only_what_is_given(self) -> None:
        mcp = self.start_thread()
        mcp.ok('emit', VALID['emit'])
        mcp.ok('emit', {**VALID['emit'], 'project_id': None, 'key': None, 'cause': None})
        for call, params in self.loom.calls:
            self.assertEqual(call, 'signals.emit')
            self.assertEqual(set(params), {'id', 'topic', 'payload'})

    def test_emit_sends_cause_0_and_passes_the_refusal_on(self) -> None:
        self.loom.on('signals.emit', failed('invalid', 'cause must be an event id'))
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('emit', {**VALID['emit'], 'cause': 0}),
                         ('invalid: cause must be an event id', True))
        [(call, params)] = self.loom.calls
        self.assertEqual((call, params['cause']), ('signals.emit', 0))

    def test_schedule_event_trigger(self) -> None:
        mcp = self.start_thread()
        mcp.ok('schedule', {'trigger': EVENT_TRIGGER, 'text': 'Done: {{payload.to}}',
                            'action': 'notify'})
        [(call, params)] = self.loom.calls
        self.assertEqual(call, 'schedules.add')
        self.assertEqual(params['trigger'], EVENT_TRIGGER)
        self.assertEqual(params['text'], 'Done: {{payload.to}}')

    def test_schedule_launch_keys(self) -> None:
        mcp = self.start_thread()
        launch = {'trigger': EVENT_TRIGGER, 'text': 'Land {{payload.run}}', 'action': 'launch',
                  'project_id': 'demo'}
        keys = {'concurrency_key': 'pm:demo', 'on_busy': 'coalesce', 'settle': 'on_result'}
        mcp.ok('schedule', {**launch, **keys})
        mcp.ok('schedule', launch)
        mcp.ok('schedule', {**launch, **{key: None for key in keys}})
        given, missing, null = (params for _, params in self.loom.calls)
        self.assertEqual({key: value for key, value in given.items()
                          if key not in ('id', 'trigger', 'text')},
                         {'action': 'launch', 'project_id': 'demo', **keys})
        for params in (missing, null):
            self.assertEqual(set(params) & set(keys), set())
        self.loom.requests.clear()
        for key in keys:
            with self.subTest(key=key):
                self.assertEqual(mcp.tool('schedule', {**launch, key: 5}),
                                 (f'{key} must be a string', True))
        self.assertEqual(self.loom.requests, [])

    def test_schedules(self) -> None:
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('schedules', {'thread_id': 't1'}),
                         (compact([schedule_info()]), False))
        self.assertEqual(self.loom.calls, [('schedules.list', {})])

    def test_unschedule(self) -> None:
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('unschedule', {'id': 'sc1'}),
                         (compact(schedule_info()), False))
        [(call, params)] = self.loom.calls
        self.assertEqual(call, 'schedules.remove')
        self.assert_command_id(params)
        self.assertEqual(params['schedule_id'], 'sc1')
        self.assertEqual(set(params), {'id', 'schedule_id'})

    def test_each_write_gets_its_own_command_id(self) -> None:
        mcp = self.start_thread()
        for _ in range(2):
            mcp.ok('thread_send', {'thread_id': 't2', 'message': 'Hi.'})
        first, second = (params['id'] for _, params in self.loom.calls)
        self.assertNotEqual(first, second)

    def test_arguments_outside_the_schema_are_ignored(self) -> None:
        mcp = self.start_thread()
        mcp.ok('launch_thread', {**VALID['launch_thread'], 'permission_mode': 'full-access',
                                 'requests': 'caller', 'origin': 'outside', 'id': 'mine'})
        mcp.ok('delegate', {'task': 'Review it.', 'parent_id': 'other',
                            'permission_mode': 'full-access'})
        mcp.ok('unschedule', {'id': 'sc1', 'schedule_id': 'other'})
        launch, delegate, remove = (params for _, params in self.loom.calls)
        self.assertEqual(launch['origin'], 'thread:t1')
        self.assertNotEqual(launch['id'], 'mine')
        self.assertEqual(delegate['parent_id'], 't1')
        self.assertEqual(remove['schedule_id'], 'sc1')


class DefaultProjectTest(ToolsCase):
    def test_the_callers_project_is_the_default(self) -> None:
        self.loom.on_get({'t1': thread_info('t1', project_id='home')})
        mcp = self.start_thread()
        mcp.ok('launch_thread', {'title': 'Fix it', 'message': 'Go.'})
        mcp.ok('launch_thread', {'title': 'Fix it', 'message': 'Go.', 'project_id': None})
        self.assertEqual(self.loom.names(), ['threads.get', 'threads.launch'] * 2)
        self.assertEqual(self.loom.calls[1][1]['project_id'], 'home')
        self.assertEqual(self.loom.calls[3][1]['project_id'], 'home')

    def test_a_caller_without_a_project_string_gives_an_error(self) -> None:
        caller = thread_info('t1')
        del caller['project_id']
        for thread in (thread_info('t1', project_id=None), caller,
                       thread_info('t1', project_id=5), [thread_info('t1')]):
            with self.subTest(thread=thread):
                self.loom.requests.clear()
                self.loom.on_get({'t1': ok(thread)})
                mcp = self.start_thread()
                self.assertEqual(mcp.tool('launch_thread', {'title': 'Fix it', 'message': 'Go.'}),
                                 ("cannot tell this thread's project; give project_id", True))
                self.assertEqual(self.loom.names(), ['threads.get'])

    def test_a_missing_caller_thread_is_passed_on(self) -> None:
        self.loom.on_get({})
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('launch_thread', {'title': 'Fix it', 'message': 'Go.'}),
                         ("not_found: no thread with id 't1'", True))
        self.assertEqual(self.loom.names(), ['threads.get'])


class AudienceTest(ToolsCase):
    def test_audience_is_sent_only_when_given(self) -> None:
        mcp = self.start_thread()
        mcp.ok('launch_thread', {**VALID['launch_thread'], 'audience': 'person'})
        mcp.ok('launch_thread', VALID['launch_thread'])
        mcp.ok('launch_thread', {**VALID['launch_thread'], 'audience': None})
        sent = [params for _, params in self.loom.calls]
        self.assertEqual(sent[0]['audience'], 'person')
        self.assertNotIn('audience', sent[1])
        self.assertNotIn('audience', sent[2])

    def test_audience_of_the_wrong_type_sends_nothing(self) -> None:
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('launch_thread', {**VALID['launch_thread'], 'audience': 5}),
                         ('audience must be a string', True))
        self.assertEqual(self.loom.requests, [])


class ArgumentCheckTest(ToolsCase):
    def test_required_strings(self) -> None:
        mcp = self.start_thread()
        for tool, name in REQUIRED_STRINGS:
            for case, value in (('missing', ...), ('null', None), ('number', 5),
                                ('bool', False), ('list', ['x'])):
                with self.subTest(tool=tool, name=name, case=case):
                    arguments = dict(VALID[tool])
                    arguments.pop(name, None)
                    if value is not ...:
                        arguments[name] = value
                    expected = (f'{name} is required' if value is None or value is ...
                                else f'{name} must be a string')
                    self.assertEqual(mcp.tool(tool, arguments), (expected, True))
        self.assertEqual(self.loom.requests, [])

    def test_trigger(self) -> None:
        mcp = self.start_thread()
        for value, expected in ((..., 'trigger is required'), (None, 'trigger is required'),
                                ('at 9:00', 'trigger must be an object'),
                                ([AT_TRIGGER], 'trigger must be an object'),
                                (5, 'trigger must be an object')):
            with self.subTest(value=value):
                arguments = {'text': 'Stand-up'}
                if value is not ...:
                    arguments['trigger'] = value
                self.assertEqual(mcp.tool('schedule', arguments), (expected, True))
        self.assertEqual(self.loom.requests, [])

    def test_payload(self) -> None:
        mcp = self.start_thread()
        for value, expected in ((..., 'payload is required'), (None, 'payload is required'),
                                ('{}', 'payload must be an object'),
                                (5, 'payload must be an object'),
                                ([{'to': 'done'}], 'payload must be an object'),
                                (False, 'payload must be an object')):
            with self.subTest(value=value):
                arguments = {'topic': 'demo.done'}
                if value is not ...:
                    arguments['payload'] = value
                self.assertEqual(mcp.tool('emit', arguments), (expected, True))
        self.assertEqual(self.loom.requests, [])

    def test_optional_arguments_missing_or_null_are_left_out(self) -> None:
        mcp = self.start_thread()
        for tool, name, kind in OPTIONAL:
            for case in ('missing', 'null'):
                with self.subTest(tool=tool, name=name, case=case):
                    self.loom.requests.clear()
                    arguments = {key: value for key, value in VALID[tool].items()
                                 if key != name}
                    if case == 'null':
                        arguments[name] = None
                    mcp.ok(tool, arguments)
                    params = self.loom.calls[-1][1]
                    if (tool, name) == ('launch_thread', 'project_id'):
                        self.assertEqual(self.loom.names(), ['threads.get', 'threads.launch'])
                        self.assertEqual(params['project_id'], 'demo')
                    else:
                        self.assertNotIn(name, params)

    def test_optional_arguments_given_are_sent(self) -> None:
        mcp = self.start_thread()
        for tool, name, kind in OPTIONAL:
            with self.subTest(tool=tool, name=name):
                self.loom.requests.clear()
                mcp.ok(tool, {**VALID[tool], name: GIVEN[kind]})
                self.assertEqual(self.loom.calls[-1][1][name], GIVEN[kind])

    def test_optional_arguments_of_the_wrong_type_send_nothing(self) -> None:
        mcp = self.start_thread()
        for tool, name, kind in OPTIONAL:
            for value in WRONG[kind]:
                with self.subTest(tool=tool, name=name, value=value):
                    self.assertEqual(mcp.tool(tool, {**VALID[tool], name: value}),
                                     (f'{name} must be {ARTICLES[kind]}', True))
        self.assertEqual(self.loom.requests, [])

    def test_arguments_are_checked_in_schema_order(self) -> None:
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('launch_thread', {'message': 5, 'audience': 5}),
                         ('title is required', True))
        self.assertEqual(mcp.tool('launch_thread', {'title': 'x', 'message': 5, 'model': 5}),
                         ('message must be a string', True))
        self.assertEqual(mcp.tool('schedule', {'text': 5, 'title': 5}),
                         ('trigger is required', True))
        self.assertEqual(self.loom.requests, [])

    def test_values_are_left_to_the_server(self) -> None:
        self.loom.on('threads.launch', failed('invalid', 'title must not be empty'))
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('launch_thread', {**VALID['launch_thread'], 'title': ''}),
                         ('invalid: title must not be empty', True))
        self.assertEqual(self.loom.calls[0][1]['title'], '')

    def test_check_arguments_in_process(self) -> None:
        tool = BY_NAME['thread_send']
        self.assertEqual(check_arguments(tool, {'thread_id': 't', 'message': 'm', 'mode': None,
                                                'other': 1}),
                         {'thread_id': 't', 'message': 'm'})
        with self.assertRaises(ToolError) as caught:
            check_arguments(tool, {'thread_id': 't', 'message': 'm', 'mode': 1})
        self.assertEqual(str(caught.exception), 'mode must be a string')

    def test_an_integer_is_an_int_and_never_a_bool(self) -> None:
        tool = BY_NAME['emit']
        arguments = {'topic': 'demo.done', 'payload': {}}
        self.assertEqual(check_arguments(tool, {**arguments, 'cause': 5})['cause'], 5)
        self.assertEqual(check_arguments(tool, {**arguments, 'cause': 0})['cause'], 0)
        for value in (True, False, 5.0):
            with self.subTest(value=value):
                with self.assertRaises(ToolError) as caught:
                    check_arguments(tool, {**arguments, 'cause': value})
                self.assertEqual(str(caught.exception), 'cause must be an integer')

    def test_the_runner_text_names_each_runner(self) -> None:
        for name in ('launch_thread', 'delegate'):
            with self.subTest(tool=name):
                runner = BY_NAME[name].listing()['inputSchema']['properties']['runner']
                self.assertEqual(runner['description'],
                                 'The agent that runs it: claude, codex or t3.')


class OwnTaskTest(ToolsCase):
    def test_a_thread_that_is_not_a_child_of_the_caller(self) -> None:
        missing = thread_info('c1')
        del missing['parent_id']
        mcp = self.start_thread()
        for case, thread in (('another parent', thread_info('c1', parent_id='t9')),
                             ('null', thread_info('c1', parent_id=None)),
                             ('missing', missing), ('number', thread_info('c1', parent_id=5)),
                             ('not a thread', 'c1')):
            for tool in ('task_status', 'task_cancel'):
                with self.subTest(case=case, tool=tool):
                    self.loom.requests.clear()
                    self.loom.on_get({'c1': ok(thread)})
                    self.assertEqual(mcp.tool(tool, {'task_id': 'c1'}),
                                     ('c1 is not a task of this thread', True))
                    self.assertEqual(self.loom.names(), ['threads.get'])

    def test_an_unknown_task_is_passed_on(self) -> None:
        self.loom.on_get({})
        mcp = self.start_thread()
        for tool in ('task_status', 'task_cancel'):
            with self.subTest(tool=tool):
                self.assertEqual(mcp.tool(tool, {'task_id': 'c9'}),
                                 ("not_found: no thread with id 'c9'", True))
        self.assertEqual(self.loom.names(), ['threads.get', 'threads.get'])


class ErrorMappingTest(ToolsCase):
    def test_server_errors_come_back_as_tool_errors(self) -> None:
        cases = [('launch_thread', 'threads.launch', 'not_found', "no project with id 'demo'"),
                 ('delegate', 'threads.delegate', 'conflict', 'thread t1 is being stopped'),
                 ('thread_send', 'threads.send', 'conflict', 'thread t2 is settled'),
                 ('threads', 'threads.list', 'invalid', 'project_id must be a string'),
                 ('emit', 'signals.emit', 'invalid', 'topic must not be a Loom event type'),
                 ('schedule', 'schedules.add', 'invalid',
                  'trigger.type must be one of at, interval, fixed_time'),
                 ('schedules', 'schedules.list', 'failed', 'internal error'),
                 ('unschedule', 'schedules.remove', 'not_found', "no schedule with id 'sc1'"),
                 ('task_cancel', 'threads.stop', 'not_found', "no thread with id 'c1'")]
        mcp = self.start_thread()
        for tool, call, code, message in cases:
            with self.subTest(tool=tool, code=code):
                self.loom.requests.clear()
                self.loom.on(call, failed(code, message))
                self.assertEqual(mcp.tool(tool, VALID[tool]), (f'{code}: {message}', True))
                self.assertEqual(self.loom.names().count(call), 1)

    def test_a_key_without_a_launch_is_left_to_the_server(self) -> None:
        self.loom.on('schedules.add', failed('invalid', 'concurrency_key goes only with launch'))
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('schedule', {**VALID['schedule'], 'action': 'post',
                                               'thread_id': 't2', 'concurrency_key': 'pm:demo'}),
                         ('invalid: concurrency_key goes only with launch', True))
        [(call, params)] = self.loom.calls
        self.assertEqual((call, params['concurrency_key']), ('schedules.add', 'pm:demo'))

    def test_a_stopping_caller_gets_the_conflict_word_for_word(self) -> None:
        self.loom.on('threads.launch', failed('conflict', 'thread t1 is stopping'))
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('launch_thread', VALID['launch_thread']),
                         ('conflict: thread t1 is stopping', True))
        self.assertEqual(self.loom.names(), ['threads.launch'])

    def test_a_broken_answer_is_a_tool_error(self) -> None:
        self.loom.on('threads.list', [b'not json\n'])
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('threads'), ('the answer is not JSON', True))
        self.assertEqual(self.loom.names(), ['threads.list'])


class RetryTest(ToolsCase):
    def test_a_dropped_launch_is_sent_once_more_with_the_same_id(self) -> None:
        thread = thread_info('new', origin='thread:t1')
        self.loom.on('threads.launch', dropped(), ok(thread))
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('launch_thread', VALID['launch_thread']),
                         (compact(thread), False))
        first, second = self.loom.calls
        self.assertEqual(first, second)
        self.assertEqual(first[0], 'threads.launch')

    def test_a_dropped_emit_is_sent_once_more_with_the_same_id(self) -> None:
        self.loom.on('signals.emit', dropped(), ok(signal_event()))
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('emit', VALID['emit']), (compact(signal_event()), False))
        first, second = self.loom.calls
        self.assertEqual(first, second)
        self.assertEqual(first[0], 'signals.emit')

    def test_two_drops_say_the_call_may_have_run(self) -> None:
        self.loom.on('threads.launch', dropped())
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('launch_thread', VALID['launch_thread']),
                         (f'{LOST}; {MAY_HAVE_RUN}', True))
        self.assertEqual(self.loom.names(), ['threads.launch', 'threads.launch'])
        self.assertEqual(self.loom.calls[0], self.loom.calls[1])

    def test_a_retry_that_gets_a_server_error_passes_it_on(self) -> None:
        self.loom.on('threads.launch', dropped(), failed('conflict', 'thread t1 is stopping'))
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('launch_thread', VALID['launch_thread']),
                         ('conflict: thread t1 is stopping', True))
        self.assertEqual(self.loom.names(), ['threads.launch', 'threads.launch'])

    def test_every_write_is_retried_once_with_the_same_id(self) -> None:
        writes = [('delegate', 'threads.delegate'), ('task_cancel', 'threads.stop'),
                  ('thread_send', 'threads.send'), ('schedule', 'schedules.add'),
                  ('unschedule', 'schedules.remove')]
        mcp = self.start_thread()
        for tool, call in writes:
            with self.subTest(tool=tool):
                self.loom.requests.clear()
                self.loom.on(call, dropped())
                self.assertEqual(mcp.tool(tool, VALID[tool]), (f'{LOST}; {MAY_HAVE_RUN}', True))
                sent = [params for name, params in self.loom.calls if name == call]
                self.assertEqual(len(sent), 2)
                self.assertEqual(sent[0], sent[1])
                self.assert_command_id(sent[0])

    def test_two_dropped_emits_say_a_second_emit_writes_a_second_signal(self) -> None:
        self.loom.on('signals.emit', dropped())
        mcp = self.start_thread()
        self.assertEqual(mcp.tool('emit', VALID['emit']), (f'{LOST}; {MAY_HAVE_EMITTED}', True))
        self.assertEqual(self.loom.names(), ['signals.emit', 'signals.emit'])
        first, second = (params for _, params in self.loom.calls)
        self.assertEqual(first, second)
        self.assert_command_id(first)

    def test_a_dropped_read_is_not_retried(self) -> None:
        reads = [('threads', 'threads.list'), ('schedules', 'schedules.list'),
                 ('task_status', 'threads.get')]
        mcp = self.start_thread()
        for tool, call in reads:
            with self.subTest(tool=tool):
                self.loom.requests.clear()
                self.loom.on(call, dropped())
                self.assertEqual(mcp.tool(tool, VALID[tool]), (LOST, True))
                self.assertEqual(self.loom.names(), [call])

    def test_a_server_error_is_not_retried(self) -> None:
        self.loom.on('threads.send', failed('conflict', 'thread t2 is settled'))
        mcp = self.start_thread()
        mcp.tool('thread_send', VALID['thread_send'])
        self.assertEqual(self.loom.names(), ['threads.send'])

    def test_an_unreachable_loom_fails_each_tool(self) -> None:
        mcp = self.start_thread(LOOM_SOCKET=str(self.base / 'none.sock'))
        text, is_error = mcp.tool('threads')
        self.assertTrue(is_error)
        self.assertEqual(text, f"cannot reach Loom at {self.base / 'none.sock'}: "
                               'No such file or directory')
        text, is_error = mcp.tool('thread_send', VALID['thread_send'])
        self.assertTrue(is_error)
        self.assertTrue(text.endswith(MAY_HAVE_RUN), text)

    def test_an_unreachable_loom_fails_emit_with_its_own_text(self) -> None:
        mcp = self.start_thread(LOOM_SOCKET=str(self.base / 'none.sock'))
        text, is_error = mcp.tool('emit', VALID['emit'])
        self.assertTrue(is_error)
        self.assertTrue(text.startswith(f"cannot reach Loom at {self.base / 'none.sock'}"), text)
        self.assertTrue(text.endswith(MAY_HAVE_EMITTED), text)

    def test_no_socket_path_is_a_tool_error(self) -> None:
        mcp = self.start_thread(LOOM_SOCKET='')
        text, is_error = mcp.tool('threads')
        self.assertTrue(is_error)
        self.assertTrue(text.startswith('no socket path'), text)
        self.assertEqual(mcp.request('ping')['result'], {})


if __name__ == '__main__':
    unittest.main()
