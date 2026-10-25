import asyncio
import os
import socket
import unittest
from unittest import mock

from loom_service import server
from loom_service.server import MAX_LINE

from .support import (
    TIMEOUT, ServerCase, clean_env, make_repo, pad_line, run_client, wait_until,
)

AGENT_ENV_KEYS = {'HOME', 'LANG', 'PATH', 'LOOM_SOCKET', 'LOOM_THREAD', 'ANTHROPIC_API_KEY'}
SHELL_KEYS = {'PWD', 'OLDPWD', 'SHLVL', '_', 'LC_CTYPE'}
TOOL_INPUT = {'command': 'true', 'description': 'Fake tool call 1'}


class CallsTest(ServerCase):
    async def test_each_served_call_answers_as_the_contract_says(self) -> None:
        conn = await self.connect()
        project = await self.add_project(conn, permission_mode='auto')
        self.assertEqual(project['id'], 'repo')
        self.assertEqual(project['workspace_root'], str(self.repo))
        self.assertEqual(project['permission_mode'], 'auto')
        self.assertEqual(await conn.ok('projects.list'), [project])

        thread = await self.launch(conn, 'hello')
        self.assertEqual(thread['state'], 'queued')
        self.assertEqual(thread['workspace_path'], str(self.repo))
        await self.wait_events(conn, 'turn.result', thread['id'])
        got = await conn.ok('threads.get', thread_id=thread['id'])
        self.assertEqual(got['state'], 'idle')
        listed = await conn.ok('threads.list', project_id='repo')
        self.assertEqual([item['id'] for item in listed], [thread['id']])
        self.assertEqual(await conn.ok('threads.list', project_id='none'), [])

        sent = await conn.ok('threads.send', id=conn.new_id(), thread_id=thread['id'],
                             message='again')
        self.assertEqual(sent['id'], thread['id'])
        results = await self.wait_events(conn, 'turn.result', thread['id'], 2)
        self.assertEqual([event['data']['text'] for event in results[:2]], ['hello', 'again'])

        child = await conn.ok('threads.delegate', id=conn.new_id(), parent_id=thread['id'],
                              task='check it')
        self.assertEqual(child['parent_id'], thread['id'])
        child_results = await self.wait_events(conn, 'turn.result', child['id'])
        self.assertEqual(child_results[0]['data'], {'state': 'completed', 'text': 'check it'})
        posted = await self.wait_events(conn, 'turn.result', thread['id'], 3)
        self.assertIn('check it', posted[2]['data']['text'])

        stopped = await conn.ok('threads.stop', id=conn.new_id(), thread_id=child['id'])
        self.assertEqual(stopped['id'], child['id'])
        settled = await conn.ok('threads.settle', id=conn.new_id(), thread_id=child['id'])
        self.assertEqual(settled['state'], 'settled')

        changed = await conn.ok('projects.set_mode', id=conn.new_id(), project_id='repo',
                                permission_mode='approval-required')
        self.assertEqual(changed['permission_mode'], 'approval-required')

        events = await conn.ok('threads.events')
        ids = [event['id'] for event in events]
        self.assertEqual(ids, sorted(set(ids)))
        self.assertEqual(set(events[0]), {'id', 'type', 'at', 'thread_id', 'data'})
        page = await conn.ok('threads.events', after=ids[2], thread_id=thread['id'])
        self.assertTrue(page)
        self.assertTrue(all(event['id'] > ids[2] for event in page))
        self.assertEqual({event['thread_id'] for event in page}, {thread['id']})

        other = make_repo(self.base / 'other')
        added = await self.add_project(conn, title='Other', path=str(other))
        removed = await conn.ok('projects.remove', id=conn.new_id(), project_id='other')
        self.assertEqual(removed, added)
        self.assertEqual(await conn.ok('projects.list'), [changed])

    async def test_core_errors_keep_their_codes(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        missing = await conn.call('threads.get', thread_id='nope')
        self.assertEqual(missing['ok'], False)
        self.assertEqual(missing['error']['code'], 'not_found')
        twice = await conn.call('projects.add', id='again', title='Twice', path=str(self.repo))
        self.assertEqual(twice['error']['code'], 'conflict')
        bad = await conn.call('threads.launch', id='l1', project_id='repo', message='x')
        self.assertEqual(bad['error'], {'code': 'invalid', 'message': 'title is required'})

    async def test_turn_tool_carries_the_fakes_tool_use_id(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        thread = await self.launch(conn, 'look around')
        result = await self.wait_events(conn, 'turn.result', thread['id'])
        self.assertEqual(result[0]['data'], {'state': 'completed', 'text': 'look around'})
        tools = await self.wait_events(conn, 'turn.tool', thread['id'])
        self.assertEqual(tools[0]['data'],
                         {'tool_use_id': 'toolu_fake_1', 'name': 'Bash', 'input': TOOL_INPUT})


class AgentEnvironmentTest(ServerCase):
    async def test_agent_gets_its_socket_and_thread_and_nothing_else(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        thread = await self.launch(conn)
        spawned = await self.spawned(thread['id'])
        env = spawned['env']
        self.assertEqual(env['LOOM_SOCKET'], str(self.socket))
        self.assertEqual(env['LOOM_THREAD'], thread['id'])
        self.assertNotIn('OUTSIDE', env)
        self.assertEqual(set(env) - SHELL_KEYS, AGENT_ENV_KEYS)
        self.assertEqual(env['ANTHROPIC_API_KEY'], '')
        self.assertEqual(spawned['cwd'], str(self.repo))
        self.assertIn(thread['id'], spawned['argv'])

    async def test_config_settings_reach_the_runner(self) -> None:
        await self.restart(self.config(claude_config_dir=str(self.base / 'claude-config'),
                                       agent_base_url='http://127.0.0.1:9/'))
        conn = await self.connect()
        await self.add_project(conn)
        thread = await self.launch(conn)
        env = (await self.spawned(thread['id']))['env']
        self.assertEqual(env['CLAUDE_CONFIG_DIR'], str(self.base / 'claude-config'))
        self.assertEqual(env['ANTHROPIC_BASE_URL'], 'http://127.0.0.1:9/')

    async def token_env(self, text: str | None, mode: int = 0o600) -> dict:
        if text is not None:
            path = self.data_dir / 'agent-token'
            path.write_text(text)
            path.chmod(mode)
        conn = await self.connect()
        await self.add_project(conn)
        thread = await self.launch(conn)
        return (await self.spawned(thread['id']))['env']

    async def test_a_private_token_reaches_the_agent(self) -> None:
        env = await self.token_env('sk-private-token\n')
        self.assertEqual(env['ANTHROPIC_AUTH_TOKEN'], 'sk-private-token')
        self.assertNotIn('warning', self.log.getvalue())

    async def test_a_missing_token_gives_none(self) -> None:
        self.assertNotIn('ANTHROPIC_AUTH_TOKEN', await self.token_env(None))

    async def test_an_empty_token_gives_none(self) -> None:
        self.assertNotIn('ANTHROPIC_AUTH_TOKEN', await self.token_env('  \n'))

    async def test_a_token_others_can_read_is_not_used(self) -> None:
        env = await self.token_env('sk-shared-token', 0o644)
        self.assertNotIn('ANTHROPIC_AUTH_TOKEN', env)
        warnings = [line for line in self.log.getvalue().splitlines() if 'warning' in line]
        self.assertEqual(len(warnings), 1)
        self.assertIn('agent-token', warnings[0])
        self.assertNotIn('sk-shared-token', self.log.getvalue())


class PermissionModeTest(ServerCase):
    async def mode_of_new_thread(self, default: str) -> tuple[str, list[str]]:
        await self.restart(self.config(permission_mode=default))
        conn = await self.connect()
        await self.add_project(conn)
        thread = await self.launch(conn)
        argv = (await self.spawned(thread['id']))['argv']
        return thread['permission_mode'], argv[argv.index('--permission-mode') + 1:][:1]

    async def test_config_mode_is_the_default_for_projects_without_one(self) -> None:
        self.assertEqual(await self.mode_of_new_thread('auto-accept-edits'),
                         ('auto-accept-edits', ['acceptEdits']))

    async def test_full_access_as_the_default_never_applies(self) -> None:
        self.assertEqual(await self.mode_of_new_thread('full-access'), ('auto', ['auto']))


class BadRequestTest(ServerCase):
    async def test_each_bad_request_is_invalid_and_the_connection_stays_open(self) -> None:
        conn = await self.connect()
        bad = {
            b'\xff\xfe{"call": "projects.list"}': 'request is not UTF-8',
            b'not json': 'request is not JSON',
            b'': 'request is not JSON',
            b'[' * 100_000: 'request is not JSON',
            b'[1, 2]': 'request must be an object',
            b'"projects.list"': 'request must be an object',
            b'null': 'request must be an object',
            b'{}': 'call must be a string',
            b'{"call": null}': 'call must be a string',
            b'{"call": 5}': 'call must be a string',
            b'{"v": null, "call": "projects.list"}': 'v must be "v1"',
            b'{"v": 1, "call": "projects.list"}': 'v must be "v1"',
            b'{"v": "v2", "call": "projects.list"}': 'v must be "v1"',
            b'{"call": "projects.list", "params": null}': 'params must be an object',
            b'{"call": "projects.list", "params": []}': 'params must be an object',
            b'{"call": "threads.nope"}': 'unknown call: threads.nope',
        }
        for line, message in bad.items():
            with self.subTest(line=line[:40]):
                await conn.send(line + b'\n')
                self.assertEqual(await conn.read(),
                                 {'ok': False, 'error': {'code': 'invalid', 'message': message}})
                self.assertEqual(await conn.request({'call': 'projects.list'}),
                                 {'ok': True, 'result': []})

    async def test_missing_v_and_params_take_their_defaults(self) -> None:
        conn = await self.connect()
        self.assertEqual(await conn.request({'call': 'projects.list'}),
                         {'ok': True, 'result': []})
        self.assertEqual(await conn.request({'v': 'v1', 'call': 'projects.list', 'params': {}}),
                         {'ok': True, 'result': []})

    async def test_requests_on_one_connection_are_answered_in_order(self) -> None:
        conn = await self.connect()
        await conn.send(b'{"call": "projects.list"}\nnope\n{"call": "threads.list"}\n')
        self.assertEqual(await conn.read(), {'ok': True, 'result': []})
        self.assertEqual((await conn.read())['error']['code'], 'invalid')
        self.assertEqual(await conn.read(), {'ok': True, 'result': []})


class LineCapTest(ServerCase):
    def test_the_cap_is_one_mebibyte(self) -> None:
        self.assertEqual(MAX_LINE, 1_048_576)

    async def test_a_lowered_cap_answers_its_boundary(self) -> None:
        socket_path = self.base / 'small.sock'
        await self.serve(socket=socket_path, data_dir=self.base / 'small', max_line=1000)
        conn = await self.connect(socket_path)
        await conn.send(pad_line(1000) + b'\n')
        self.assertEqual(await conn.read(), {'ok': True, 'result': []})
        await conn.send(pad_line(1001) + b'\n')
        self.assertEqual(await conn.read(), {'ok': False, 'error': {
            'code': 'invalid', 'message': 'request line is over 1000 bytes'}})
        self.assertTrue(await conn.at_eof())

    async def test_a_line_of_exactly_the_cap_is_answered(self) -> None:
        conn = await self.connect()
        await conn.send(pad_line(MAX_LINE) + b'\n')
        self.assertEqual(await conn.read(), {'ok': True, 'result': []})
        self.assertEqual(await conn.request({'call': 'projects.list'}),
                         {'ok': True, 'result': []})

    async def test_a_longer_line_is_refused_while_the_client_still_sends(self) -> None:
        total = 16 * MAX_LINE

        def flood() -> tuple[int, BaseException | None, bytes]:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.settimeout(TIMEOUT)
            client.connect(str(self.socket))
            sent = 0
            error = None
            chunk = b'x' * 65_536
            try:
                while sent < total:
                    sent += client.send(chunk)
            except (BrokenPipeError, ConnectionResetError) as caught:
                error = caught
            answer = b''
            try:
                while not answer.endswith(b'\n') and (data := client.recv(65_536)):
                    answer += data
            except ConnectionResetError:
                pass
            client.close()
            return sent, error, answer

        sent, error, answer = await asyncio.to_thread(flood)
        self.assertIsNotNone(error)
        self.assertLess(sent, total)
        self.assertGreater(sent, MAX_LINE)
        self.assertEqual(answer, b'{"ok":false,"error":{"code":"invalid",'
                                 b'"message":"request line is over 1048576 bytes"}}\n')


class ConcurrencyTest(ServerCase):
    async def test_a_follower_does_not_hold_up_other_clients(self) -> None:
        follower = await self.connect()
        self.assertEqual(await follower.call('threads.events', follow=True),
                         {'ok': True, 'result': {'follow': True}})
        other = await self.connect()
        self.assertEqual(await asyncio.wait_for(other.ok('projects.list'), 1.0), [])
        await self.add_project(other)
        thread = await asyncio.wait_for(self.launch(other), 1.0)
        first = await follower.read()
        self.assertEqual(first['event']['thread_id'], thread['id'])
        projects = await asyncio.wait_for(other.ok('projects.list'), 1.0)
        self.assertEqual([project['id'] for project in projects], ['repo'])

    async def test_two_connections_interleave(self) -> None:
        first = await self.connect()
        second = await self.connect()
        await first.send(b'{"call": "projects.list"')
        self.assertEqual(await second.ok('projects.list'), [])
        await first.send(b'}\n')
        self.assertEqual(await first.read(), {'ok': True, 'result': []})


class InternalErrorTest(ServerCase):
    async def test_an_unexpected_error_answers_failed_and_serving_goes_on(self) -> None:
        conn = await self.connect()
        projects = self.serving.service.projects
        with mock.patch.object(projects, 'list', side_effect=RuntimeError('boom')):
            self.assertEqual(await conn.call('projects.list'), {'ok': False, 'error': {
                'code': 'failed', 'message': 'internal error'}})
        log = self.log.getvalue()
        self.assertIn('loom: internal error in projects.list', log)
        self.assertIn('Traceback', log)
        self.assertIn('RuntimeError: boom', log)
        self.assertEqual(await conn.ok('projects.list'), [])
        self.assertEqual(await (await self.connect()).ok('projects.list'), [])


class FollowTest(ServerCase):
    async def read_until_result(self, conn, count: int) -> list[dict]:
        events = []
        while sum(event['type'] == 'turn.result' for event in events) < count:
            events.append((await conn.read())['event'])
        return events

    async def test_follow_streams_stored_then_new_events_and_resumes(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        thread = await self.launch(conn, 'one')
        await self.wait_events(conn, 'turn.result', thread['id'])
        stored = await self.all_events(conn)

        first = await self.connect()
        self.assertEqual(await first.call('threads.events', after=0, follow=True),
                         {'ok': True, 'result': {'follow': True}})
        self.assertEqual([(await first.read())['event'] for _ in stored], stored)
        await conn.ok('threads.send', id=conn.new_id(), thread_id=thread['id'], message='two')
        live = await self.read_until_result(first, 1)
        self.assertEqual(live[-1]['data'], {'state': 'completed', 'text': 'two'})
        last = live[-1]['id']

        second = await self.connect()
        self.assertEqual(
            await second.call('threads.events', after=last, thread_id=thread['id'], follow=True),
            {'ok': True, 'result': {'follow': True}})
        await conn.ok('threads.send', id=conn.new_id(), thread_id=thread['id'], message='three')
        later = await self.read_until_result(first, 1)
        resumed = await self.read_until_result(second, 1)
        self.assertEqual(resumed, [event for event in later if event['id'] > last])
        ids = [event['id'] for event in [*live, *later]]
        self.assertEqual(ids, sorted(set(ids)))

    async def test_follow_with_thread_id_yields_only_that_thread(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        one = await self.launch(conn, 'one')
        await self.wait_events(conn, 'turn.result', one['id'])
        two = await self.launch(conn, 'two', title='Second')
        await self.wait_events(conn, 'turn.result', two['id'])
        follower = await self.connect()
        await follower.call('threads.events', thread_id=two['id'], follow=True)
        events = await self.read_until_result(follower, 1)
        self.assertEqual({event['thread_id'] for event in events}, {two['id']})
        self.assertEqual(events[-1]['data']['text'], 'two')

    async def test_bad_follow_params_answer_one_invalid_line(self) -> None:
        conn = await self.connect()
        bad = [
            ({'after': -1, 'follow': True}, 'after must be an integer of 0 or more'),
            ({'thread_id': 5, 'follow': True}, 'thread_id must be a string'),
            ({'follow': 'yes'}, 'follow must be true or false'),
        ]
        for params, message in bad:
            with self.subTest(params=params):
                self.assertEqual(await conn.call('threads.events', **params), {
                    'ok': False, 'error': {'code': 'invalid', 'message': message}})
                self.assertEqual(await conn.ok('projects.list'), [])

    async def test_closing_a_follower_ends_its_stream(self) -> None:
        store = self.serving.service.store
        before = set(store.listeners)
        follower = await self.connect()
        await follower.call('threads.events', follow=True)
        await wait_until(lambda: store.listeners - before)
        await follower.close()
        await wait_until(lambda: store.listeners == before)
        conn = await self.connect()
        self.assertEqual(await conn.ok('projects.list'), [])


class AgentRefusalTest(ServerCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.conn = await self.connect()
        await self.add_project(self.conn)
        self.other = make_repo(self.base / 'other')

    def requests(self) -> list[dict]:
        return [
            {'call': 'projects.add', 'params': {'id': 'a1', 'title': 'Other',
                                                'path': str(self.other)}},
            {'call': 'projects.set_mode', 'params': {'id': 'a2', 'project_id': 'repo',
                                                     'permission_mode': 'full-access'}},
            {'call': 'threads.launch', 'params': {'id': 'a3', 'title': 'Task', 'message': 'hi',
                                                  'project_id': 'repo',
                                                  'workspace': {'strategy': 'root'}}},
        ]

    def assert_refused(self, answers: list[dict]) -> None:
        self.assertEqual([answer['ok'] for answer in answers], [False, False, True])
        self.assertEqual(answers[0]['error'], {
            'code': 'forbidden', 'message': 'agent threads may not add projects'})
        self.assertEqual(answers[1]['error'], {
            'code': 'forbidden', 'message': "agent threads may not change a project's mode"})
        self.assertEqual([project['id'] for project in self.serving.service.projects.list()],
                         ['repo'])
        self.assertIsNone(self.serving.service.projects.get('repo')['permission_mode'])

    async def test_a_process_with_loom_thread_may_not_manage_projects(self) -> None:
        answers = await run_client(self.socket, self.requests(),
                                   clean_env(LOOM_THREAD='agent-thread'))
        self.assert_refused(answers)

    async def test_a_child_that_unset_loom_thread_is_still_refused(self) -> None:
        answers = await run_client(self.socket, self.requests(),
                                   clean_env(LOOM_THREAD='agent-thread'), unset_parent=True)
        self.assert_refused(answers)

    async def test_a_process_without_loom_thread_may_manage_projects(self) -> None:
        answers = await run_client(self.socket, self.requests(), clean_env())
        self.assertEqual([answer['ok'] for answer in answers], [True, True, True])
        self.assertEqual(answers[1]['result']['permission_mode'], 'full-access')

    async def test_an_unverified_peer_may_not_manage_projects(self) -> None:
        socket_path = self.base / 'blind.sock'
        await self.serve(socket=socket_path, data_dir=self.base / 'blind',
                         proc=self.base / 'no-proc')
        conn = await self.connect(socket_path)
        answer = await conn.call('projects.add', id='b1', title='Repo', path=str(self.repo))
        self.assertEqual(answer['error']['code'], 'forbidden')
        self.assertEqual(await conn.ok('projects.list'), [])

    async def test_a_peer_of_another_user_is_refused(self) -> None:
        own = os.geteuid()
        with mock.patch.object(server, 'peer_credentials', return_value=(1, own + 1, own + 1)):
            conn = await self.connect()
            self.assertEqual(await conn.read(), {'ok': False, 'error': {
                'code': 'forbidden', 'message': 'the peer runs as another user'}})
            self.assertTrue(await conn.at_eof())


if __name__ == '__main__':
    unittest.main()
