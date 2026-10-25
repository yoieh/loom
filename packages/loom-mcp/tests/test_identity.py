import unittest

from .support import McpCase, dropped, failed, ok, schedule_info, thread_info

OUTSIDE_ERROR = 'this agent is not a Loom thread, so it has no tasks; use launch_thread'
LAUNCH = {'title': 'Fix it', 'message': 'Make the test pass.'}


class LoomThreadTest(McpCase):
    def test_loom_thread_sends_its_origin_without_a_lookup(self) -> None:
        self.loom.on('threads.launch', ok(thread_info('new', origin='thread:t1')))
        mcp = self.start(LOOM_THREAD='t1', CLAUDE_CODE_SESSION_ID='s1')
        mcp.ok('launch_thread', {**LAUNCH, 'project_id': 'demo'})
        self.assertEqual(self.loom.names(), ['threads.launch'])
        self.assertEqual(self.loom.calls[0][1]['origin'], 'thread:t1')

    def test_the_default_project_is_the_only_lookup(self) -> None:
        self.loom.on_get({'t1': thread_info('t1', project_id='home')})
        self.loom.on('threads.launch', ok(thread_info('new')))
        mcp = self.start(LOOM_THREAD='t1')
        mcp.ok('launch_thread', LAUNCH)
        self.assertEqual(self.loom.calls[0], ('threads.get', {'thread_id': 't1'}))
        self.assertEqual(self.loom.names(), ['threads.get', 'threads.launch'])
        params = self.loom.calls[1][1]
        self.assertEqual((params['project_id'], params['origin']), ('home', 'thread:t1'))

    def test_delegate_uses_loom_thread_as_the_parent(self) -> None:
        self.loom.on('threads.delegate', ok(thread_info('c1', parent_id='t1')))
        mcp = self.start(LOOM_THREAD='t1')
        mcp.ok('delegate', {'task': 'Review it.'})
        self.assertEqual(self.loom.names(), ['threads.delegate'])
        self.assertEqual(self.loom.calls[0][1]['parent_id'], 't1')


class SessionTest(McpCase):
    def test_a_known_session_is_the_loom_thread(self) -> None:
        self.loom.on_get({'s1': thread_info('s1')})
        self.loom.on('threads.delegate', ok(thread_info('c1', parent_id='s1')))
        self.loom.on('threads.launch', ok(thread_info('new')))
        mcp = self.start(LOOM_THREAD='', CLAUDE_CODE_SESSION_ID='s1')
        mcp.ok('delegate', {'task': 'Review it.'})
        mcp.ok('delegate', {'task': 'Review it again.'})
        mcp.ok('launch_thread', {**LAUNCH, 'project_id': 'demo'})
        self.assertEqual(self.loom.names(), [
            'threads.get', 'threads.delegate', 'threads.delegate', 'threads.launch'])
        self.assertEqual(self.loom.calls[0][1], {'thread_id': 's1'})
        self.assertEqual(self.loom.calls[1][1]['parent_id'], 's1')
        self.assertEqual(self.loom.calls[2][1]['parent_id'], 's1')
        self.assertEqual(self.loom.calls[3][1]['origin'], 'thread:s1')

    def test_an_unknown_session_is_an_outside_thread(self) -> None:
        self.loom.on_get({})
        self.loom.on('threads.launch', ok(thread_info('new', origin='outside:s1')))
        mcp = self.start(LOOM_THREAD='', CLAUDE_CODE_SESSION_ID='s1')
        mcp.ok('launch_thread', {**LAUNCH, 'project_id': 'demo'})
        for name, arguments in (('delegate', {'task': 'Review it.'}),
                                ('task_status', {'task_id': 'c1'}),
                                ('task_cancel', {'task_id': 'c1'})):
            with self.subTest(tool=name):
                self.assertEqual(mcp.tool(name, arguments), (OUTSIDE_ERROR, True))
        self.assertEqual(mcp.tool('launch_thread', LAUNCH),
                         ('project_id is required outside a Loom thread', True))
        self.assertEqual(self.loom.names(), ['threads.get', 'threads.launch'])
        self.assertEqual(self.loom.calls[1][1]['origin'], 'outside:s1')

    def test_a_lost_lookup_fails_the_call_and_is_tried_again(self) -> None:
        self.loom.on('threads.get', dropped(), failed('failed', 'internal error'),
                     ok(thread_info('s1')))
        self.loom.on('threads.delegate', ok(thread_info('c1', parent_id='s1')))
        mcp = self.start(CLAUDE_CODE_SESSION_ID='s1')
        text, is_error = mcp.tool('delegate', {'task': 'Review it.'})
        self.assertTrue(is_error)
        self.assertIn('the connection closed before a full answer line', text)
        self.assertNotIn('may have run', text)
        self.assertEqual(mcp.tool('delegate', {'task': 'Review it.'}),
                         ('failed: internal error', True))
        mcp.ok('delegate', {'task': 'Review it.'})
        mcp.ok('delegate', {'task': 'Review it.'})
        self.assertEqual(self.loom.names(), [
            'threads.get', 'threads.get', 'threads.get', 'threads.delegate', 'threads.delegate'])

    def test_an_unreachable_loom_fails_the_lookup_and_it_is_tried_again(self) -> None:
        mcp = self.start(CLAUDE_CODE_SESSION_ID='s1', LOOM_SOCKET=str(self.base / 'none.sock'))
        for _ in range(2):
            text, is_error = mcp.tool('launch_thread', {**LAUNCH, 'project_id': 'demo'})
            self.assertTrue(is_error)
            self.assertTrue(text.startswith('cannot reach Loom at '), text)
            self.assertNotIn('may have run', text)


class NoIdentityTest(McpCase):
    def test_launch_with_a_project_is_outside(self) -> None:
        self.loom.on('threads.launch', ok(thread_info('new', origin='outside')))
        mcp = self.start()
        mcp.ok('launch_thread', {**LAUNCH, 'project_id': 'demo'})
        self.assertEqual(self.loom.names(), ['threads.launch'])
        self.assertEqual(self.loom.calls[0][1]['origin'], 'outside')

    def test_launch_without_a_project_sends_nothing(self) -> None:
        mcp = self.start(LOOM_THREAD='', CLAUDE_CODE_SESSION_ID='')
        for arguments in (LAUNCH, {**LAUNCH, 'project_id': None}):
            with self.subTest(arguments=arguments):
                self.assertEqual(mcp.tool('launch_thread', arguments),
                                 ('project_id is required outside a Loom thread', True))
        self.assertEqual(self.loom.requests, [])

    def test_task_tools_are_refused_without_a_call(self) -> None:
        mcp = self.start()
        for name, arguments in (('delegate', {'task': 'Review it.'}),
                                ('task_status', {'task_id': 'c1'}),
                                ('task_cancel', {'task_id': 'c1'})):
            with self.subTest(tool=name):
                self.assertEqual(mcp.tool(name, arguments), (OUTSIDE_ERROR, True))
        self.assertEqual(self.loom.requests, [])


class NoLookupTest(McpCase):
    def test_other_tools_never_look_up_the_session(self) -> None:
        self.loom.on('threads.send', ok(thread_info('t2')))
        self.loom.on('threads.list', ok([]))
        self.loom.on('schedules.add', ok(schedule_info()))
        self.loom.on('schedules.list', ok([]))
        self.loom.on('schedules.remove', ok(schedule_info()))
        mcp = self.start(CLAUDE_CODE_SESSION_ID='s1')
        mcp.ok('thread_send', {'thread_id': 't2', 'message': 'Hi.'})
        mcp.ok('threads')
        mcp.ok('schedule', {'trigger': {'type': 'at', 'at': '2026-10-26T09:00'}, 'text': 'x'})
        mcp.ok('schedules')
        mcp.ok('unschedule', {'id': 'sc1'})
        self.assertEqual(self.loom.names(), [
            'threads.send', 'threads.list', 'schedules.add', 'schedules.list',
            'schedules.remove'])


if __name__ == '__main__':
    unittest.main()
