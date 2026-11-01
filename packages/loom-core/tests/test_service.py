import asyncio
import dataclasses
import json
import sqlite3
import unittest

from loom_contracts.events import EventType
from loom_contracts.runners import ToolUse

from loom_core.errors import CODES, LoomError
from loom_core.service import SERVED, Caller, Service
from loom_core.store import MAX_EVENTS

from .fakes import wait_for
from .test_orchestrator import ROOT, OrchestratorTest
from .test_workspaces import make_repo

PERSON = Caller()
AGENT = Caller(thread_id='t1')
UNKNOWN = Caller(verified=False)
SCHEDULED = (
    Caller(schedule_id='s1'),
    Caller(thread_id='t1', schedule_id='s1'),
    Caller(verified=False, schedule_id='s1'),
)

ALL_CALLS = (
    'threads.launch', 'threads.delegate', 'threads.send', 'threads.steer', 'threads.list',
    'threads.get', 'threads.stop', 'threads.settle', 'threads.update', 'threads.events', 'projects.add',
    'projects.set_mode', 'projects.remove', 'projects.list', 'requests.respond', 'signals.emit',
)


class ServiceTest(OrchestratorTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.service = Service(self.store, self.projects, self.orchestrator)

    async def call(self, name: str, params: object, caller: Caller = PERSON) -> object:
        return await self.service.method(name)(params, caller)

    async def refused(self, name: str, params: object, code: str = 'invalid',
                      caller: Caller = PERSON) -> str:
        with self.assertRaises(LoomError) as caught:
            await self.call(name, params, caller)
        self.assertEqual(caught.exception.code, code)
        return caught.exception.message

    def follow_refused(self, params: object) -> str:
        with self.assertRaises(LoomError) as caught:
            self.service.follow(params, PERSON)
        self.assertEqual(caught.exception.code, 'invalid')
        return caught.exception.message

    def add_events(self, count: int, thread_id: str = 't1') -> None:
        with self.store.transaction() as tx:
            for number in range(count):
                self.store.append_event(tx, EventType.TURN_TEXT, thread_id,
                                        {'text': f'line {number}'})

    def receipts(self, command_id: str) -> int:
        return self.store.connection.execute(
            'SELECT COUNT(*) FROM commands WHERE id = ?', (command_id,)).fetchone()[0]

    def project_params(self, command_id: str, name: str = 'other') -> dict:
        root = make_repo(self.base / name)
        return {'id': command_id, 'title': name.title(), 'path': str(root)}

    def consume(self, stream: object) -> tuple[list[dict], asyncio.Task]:
        got: list[dict] = []

        async def run() -> None:
            async for event in stream:
                got.append(event)

        return got, asyncio.ensure_future(run())


class MethodTest(ServiceTest):
    async def test_served_names_the_fifteen_calls_in_order(self) -> None:
        self.assertEqual(SERVED, ALL_CALLS)

    async def test_method_gives_a_callable_for_each_served_call(self) -> None:
        for name in SERVED:
            with self.subTest(call=name):
                self.assertTrue(callable(self.service.method(name)))

    async def test_method_refuses_a_call_that_is_not_a_string(self) -> None:
        for call in (None, 1):
            with self.subTest(call=call), self.assertRaises(LoomError) as caught:
                self.service.method(call)
            self.assertEqual((caught.exception.code, caught.exception.message),
                             ('invalid', 'call must be a string'))

    async def test_method_refuses_an_unknown_call(self) -> None:
        with self.assertRaises(LoomError) as caught:
            self.service.method('threads.nope')
        self.assertEqual((caught.exception.code, caught.exception.message),
                         ('invalid', 'unknown call: threads.nope'))

    async def test_method_refuses_v1_calls_not_served_yet(self) -> None:
        for call in ('schedules.add', 'schedules.list', 'schedules.pause', 'schedules.remove'):
            with self.subTest(call=call), self.assertRaises(LoomError) as caught:
                self.service.method(call)
            self.assertEqual((caught.exception.code, caught.exception.message),
                             ('invalid', f'{call} is not served yet'))

    async def test_serve_adds_methods_for_v1_calls(self) -> None:
        async def listed(params: object, caller: Caller) -> object:
            return ['listed', caller]

        self.service.serve({'schedules.list': listed})
        self.assertIs(self.service.method('schedules.list'), listed)
        self.assertEqual(await self.call('schedules.list', {}), ['listed', PERSON])
        self.assertEqual(SERVED, ALL_CALLS)
        with self.assertRaises(LoomError) as caught:
            self.service.method('schedules.add')
        self.assertEqual((caught.exception.code, caught.exception.message),
                         ('invalid', 'schedules.add is not served yet'))

    async def test_serve_refuses_a_served_or_unknown_call(self) -> None:
        async def other(params: object, caller: Caller) -> object:
            return 'other'

        own = self.service.method('threads.list')
        for name in ('threads.list', 'threads.nope', 5):
            with self.subTest(call=name), self.assertRaises(ValueError):
                self.service.serve({name: other})
        self.service.serve({'schedules.list': other})
        with self.assertRaises(ValueError):
            self.service.serve({'schedules.list': other})
        with self.assertRaises(ValueError):
            self.service.serve({'schedules.add': other, 'threads.nope': other})
        self.assertIs(self.service.method('threads.list'), own)
        with self.assertRaises(LoomError):
            self.service.method('schedules.add')

    async def test_forbidden_is_an_error_code(self) -> None:
        self.assertIn('forbidden', CODES)
        self.assertEqual(LoomError('forbidden', 'no').to_api(),
                         {'code': 'forbidden', 'message': 'no'})


class SameAnswerTest(ServiceTest):
    async def test_launch_returns_the_orchestrators_answer_and_a_retry_the_first(self) -> None:
        params = self.launch_params()
        info = await self.call('threads.launch', params)
        self.assertEqual(info['title'], 'Work')
        self.assertEqual(info['state'], 'queued')
        self.assertEqual(await self.orchestrator.launch(params), info)
        self.assertEqual(await self.call('threads.launch', params), info)
        await self.orchestrator.drain()
        self.assertEqual(len(self.runner.starts), 1)

    async def test_write_calls_return_the_core_answer(self) -> None:
        parent = await self.call('threads.launch', self.launch_params())
        await self.orchestrator.drain()
        core = {
            'threads.delegate': self.orchestrator.delegate,
            'threads.send': self.orchestrator.send,
            'threads.stop': self.orchestrator.stop,
            'threads.settle': self.orchestrator.settle,
        }
        steps = [
            ('threads.delegate', {'id': 'd1', 'parent_id': parent['id'], 'task': 'Review'}),
            ('threads.send', {'id': 's1', 'thread_id': parent['id'], 'message': 'More'}),
            ('threads.stop', {'id': 'x1', 'thread_id': parent['id']}),
            ('threads.settle', {'id': 'z1', 'thread_id': parent['id']}),
        ]
        for name, params in steps:
            with self.subTest(call=name):
                answer = await self.call(name, params)
                await self.orchestrator.drain()
                self.assertEqual(await core[name](params), answer)
        add = self.project_params('a1')
        added = await self.call('projects.add', add)
        self.assertEqual(self.projects.add(add), added)
        mode = {'id': 'm1', 'project_id': added['id'], 'permission_mode': 'approval-required'}
        changed = await self.call('projects.set_mode', mode)
        self.assertEqual(changed['permission_mode'], 'approval-required')
        self.assertEqual(self.projects.set_mode(mode), changed)
        remove = {'id': 'r1', 'project_id': added['id']}
        removed = await self.call('projects.remove', remove)
        self.assertEqual(self.projects.remove(remove), removed)

    async def test_read_calls_return_the_core_answer(self) -> None:
        info = await self.call('threads.launch', self.launch_params())
        await self.orchestrator.drain()
        self.assertEqual(await self.call('threads.list', {}), self.orchestrator.list())
        self.assertEqual(await self.call('threads.list', {'project_id': 'loom'}),
                         self.orchestrator.list('loom'))
        self.assertEqual(await self.call('threads.list', {'project_id': 'none'}), [])
        self.assertEqual(await self.call('threads.get', {'thread_id': info['id']}),
                         self.orchestrator.get(info['id']))
        self.assertEqual(await self.call('projects.list', {}), self.projects.list())
        self.assertEqual(await self.call('threads.events', {}), self.store.events())
        self.assertEqual(await self.call('threads.events', {'after': 1, 'thread_id': info['id']}),
                         self.store.events(1, info['id']))

    async def test_every_answer_survives_json(self) -> None:
        answers = []
        parent = await self.call('threads.launch', self.launch_params())
        answers.append(parent)
        await self.orchestrator.drain()
        answers.append(await self.call(
            'threads.delegate', {'id': 'd1', 'parent_id': parent['id'], 'task': 'Review'}))
        answers.append(await self.call(
            'threads.send', {'id': 's1', 'thread_id': parent['id'], 'message': 'More'}))
        await self.orchestrator.drain()
        answers.append(await self.call('threads.list', {}))
        answers.append(await self.call('threads.get', {'thread_id': parent['id']}))
        answers.append(await self.call('threads.events', {}))
        answers.append(await self.call('threads.stop', {'id': 'x1', 'thread_id': parent['id']}))
        await self.orchestrator.drain()
        answers.append(await self.call('threads.settle',
                                       {'id': 'z1', 'thread_id': parent['id']}))
        added = await self.call('projects.add', self.project_params('a1'))
        answers.append(added)
        mode = {'id': 'm1', 'project_id': added['id'], 'permission_mode': 'auto'}
        answers.append(await self.call('projects.set_mode', mode))
        answers.append(await self.call('projects.list', {}))
        answers.append(await self.call('projects.remove', {'id': 'r1', 'project_id': added['id']}))
        self.assertTrue(answers[5])
        for answer in answers:
            with self.subTest(answer=answer):
                self.assertEqual(json.loads(json.dumps(answer)), answer)


class CallerTest(ServiceTest):
    async def test_caller_defaults_to_a_verified_person(self) -> None:
        self.assertEqual((PERSON.thread_id, PERSON.verified), (None, True))
        with self.assertRaises(AttributeError):
            PERSON.verified = False

    async def test_agents_and_unknown_callers_may_add_projects(self) -> None:
        before = self.projects.list()
        for number, caller in enumerate((AGENT, UNKNOWN)):
            with self.subTest(caller=caller):
                params = dict(self.project_params(f'a{number}', f'repo-{number}'),
                              permission_mode='full-access')
                added = await self.call('projects.add', params, caller)
                self.assertEqual(added['permission_mode'], 'full-access')
        self.assertEqual(len(self.projects.list()), len(before) + 2)

    async def test_agents_and_unknown_callers_may_not_change_a_mode(self) -> None:
        params = {'id': 'm1', 'project_id': 'loom', 'permission_mode': 'full-access'}
        for caller in (AGENT, UNKNOWN):
            with self.subTest(caller=caller):
                message = await self.refused('projects.set_mode', params, 'forbidden', caller)
                self.assertEqual(message, "agent threads may not change a project's mode")
                self.assertIsNone(self.projects.get('loom')['permission_mode'])
                self.assertEqual(self.receipts('m1'), 0)
        changed = await self.call('projects.set_mode', params)
        self.assertEqual(changed['permission_mode'], 'full-access')

    async def test_caller_without_a_schedule_is_unchanged(self) -> None:
        self.assertEqual(PERSON, Caller(None, True))
        self.assertEqual(AGENT, Caller('t1', True))
        self.assertEqual(UNKNOWN, Caller(None, False))
        for caller in (PERSON, AGENT, UNKNOWN):
            self.assertIsNone(caller.schedule_id)

    async def test_a_caller_is_scheduled_exactly_when_it_has_a_schedule_id(self) -> None:
        self.assertEqual([field.name for field in dataclasses.fields(Caller)],
                         ['thread_id', 'verified', 'schedule_id'])
        with self.assertRaises(TypeError):
            Caller(scheduled=True)
        for caller in (PERSON, AGENT, UNKNOWN):
            with self.subTest(caller=caller):
                self.assertIs(caller.scheduled, False)
        for caller in SCHEDULED:
            with self.subTest(caller=caller):
                self.assertIs(caller.scheduled, True)
                with self.assertRaises(AttributeError):
                    caller.scheduled = False
        with self.assertRaises(AttributeError):
            PERSON.scheduled = True

    async def test_scheduled_callers_may_add_projects(self) -> None:
        before = self.projects.list()
        for number, caller in enumerate(SCHEDULED):
            with self.subTest(caller=caller):
                await self.call('projects.add', self.project_params(
                    f's{number}', f'repo-{number}'), caller)
        self.assertEqual(len(self.projects.list()), len(before) + len(SCHEDULED))

    async def test_scheduled_callers_may_not_change_a_mode(self) -> None:
        params = {'id': 'm1', 'project_id': 'loom', 'permission_mode': 'full-access'}
        for caller in SCHEDULED:
            with self.subTest(caller=caller):
                message = await self.refused('projects.set_mode', params, 'forbidden', caller)
                self.assertEqual(message, "agent threads may not change a project's mode")
                self.assertIsNone(self.projects.get('loom')['permission_mode'])
                self.assertEqual(self.receipts('m1'), 0)

    async def test_scheduled_callers_launch_and_send_as_people_do(self) -> None:
        for number, caller in enumerate(SCHEDULED):
            with self.subTest(caller=caller):
                launched = await self.call('threads.launch', self.launch_params(), caller)
                self.assertEqual(launched['state'], 'queued')
                await self.orchestrator.drain()
                sent = await self.call('threads.send', {
                    'id': f'send-{number}', 'thread_id': launched['id'], 'message': 'More',
                }, caller)
                self.assertEqual(sent['id'], launched['id'])
                self.assertEqual(self.receipts(f'send-{number}'), 1)

    async def test_people_manage_projects(self) -> None:
        added = await self.call('projects.add', self.project_params('a1'))
        changed = await self.call('projects.set_mode', {
            'id': 'm1', 'project_id': added['id'], 'permission_mode': 'auto-accept-edits'})
        self.assertEqual(changed['permission_mode'], 'auto-accept-edits')
        removed = await self.call('projects.remove', {'id': 'r1', 'project_id': added['id']})
        self.assertEqual(removed['id'], added['id'])
        self.assertNotIn(added['id'], [project['id'] for project in self.projects.list()])

    async def test_agents_may_start_threads_remove_projects_and_read(self) -> None:
        parent = await self.call('threads.launch', self.launch_params(), AGENT)
        await self.orchestrator.drain()
        child = await self.call(
            'threads.delegate', {'id': 'd1', 'parent_id': parent['id'], 'task': 'Review'}, UNKNOWN)
        self.assertEqual(child['parent_id'], parent['id'])
        self.assertEqual(len(await self.call('threads.list', {}, AGENT)), 2)
        self.assertTrue(await self.call('threads.events', {}, UNKNOWN))
        self.assertEqual(len(await self.call('projects.list', {}, AGENT)), 1)
        added = await self.call('projects.add', self.project_params('a1'))
        removed = await self.call('projects.remove', {'id': 'r1', 'project_id': added['id']},
                                  AGENT)
        self.assertEqual(removed['id'], added['id'])


class ReadParamsTest(ServiceTest):
    async def test_events_after(self) -> None:
        self.add_events(3)
        self.assertEqual([event['id'] for event in await self.call('threads.events', {})],
                         [1, 2, 3])
        self.assertEqual([event['id'] for event in
                          await self.call('threads.events', {'after': 1})], [2, 3])
        for after in (None, -1, '3', True):
            with self.subTest(after=after):
                await self.refused('threads.events', {'after': after})

    async def test_events_thread_id(self) -> None:
        self.add_events(2, 't1')
        self.add_events(1, 't2')
        self.assertEqual(len(await self.call('threads.events', {})), 3)
        found = await self.call('threads.events', {'thread_id': 't2'})
        self.assertEqual([event['thread_id'] for event in found], ['t2'])
        for thread_id in (None, 3):
            with self.subTest(thread_id=thread_id):
                await self.refused('threads.events', {'thread_id': thread_id})

    async def test_events_follow(self) -> None:
        self.add_events(1)
        self.assertEqual(len(await self.call('threads.events', {'follow': False})), 1)
        for follow in (None, 'yes'):
            with self.subTest(follow=follow):
                await self.refused('threads.events', {'follow': follow})
        message = await self.refused('threads.events', {'follow': True})
        self.assertEqual(message, 'follow needs a stream')

    async def test_events_params_must_be_an_object(self) -> None:
        for params in (None, []):
            with self.subTest(params=params):
                await self.refused('threads.events', params)

    async def test_events_answer_holds_at_most_a_thousand(self) -> None:
        self.add_events(MAX_EVENTS + 5)
        found = await self.call('threads.events', {})
        self.assertEqual(len(found), MAX_EVENTS)
        self.assertEqual(found[-1]['id'], MAX_EVENTS)

    async def test_list_project_id(self) -> None:
        await self.call('threads.launch', self.launch_params())
        self.assertEqual(len(await self.call('threads.list', {})), 1)
        for project_id in (None, 3):
            with self.subTest(project_id=project_id):
                await self.refused('threads.list', {'project_id': project_id})
        for params in (None, []):
            with self.subTest(params=params):
                await self.refused('threads.list', params)

    async def test_get_thread_id(self) -> None:
        for params in ({}, {'thread_id': None}, {'thread_id': 3}, None):
            with self.subTest(params=params):
                await self.refused('threads.get', params)
        await self.refused('threads.get', {'thread_id': 'nope'}, 'not_found')

    async def test_projects_list_params_must_be_an_object(self) -> None:
        for params in (None, []):
            with self.subTest(params=params):
                await self.refused('projects.list', params)


class FollowTest(ServiceTest):
    async def asyncTearDown(self) -> None:
        self.assertEqual(self.store.listeners, set())
        await super().asyncTearDown()

    async def test_follow_refuses_bad_params_when_called(self) -> None:
        bad = [None, [], {'after': None}, {'after': -1}, {'after': '3'}, {'after': True},
               {'thread_id': None}, {'thread_id': 3}, {'follow': None}, {'follow': 'yes'}]
        for params in bad:
            with self.subTest(params=params):
                self.follow_refused(params)
        self.assertEqual(self.store.listeners, set())

    async def test_follow_accepts_follow_true_or_missing(self) -> None:
        self.add_events(1)
        for params in ({}, {'follow': True}):
            with self.subTest(params=params):
                stream = self.service.follow(params, PERSON)
                self.assertEqual((await anext(stream))['id'], 1)
                await stream.aclose()

    async def test_follow_yields_stored_events_then_new_ones_on_commit(self) -> None:
        self.add_events(4)
        got, task = self.consume(self.service.follow({'after': 1}, PERSON))
        await wait_for(lambda: len(got) == 3, timeout=1.0)
        self.assertEqual([event['id'] for event in got], [2, 3, 4])
        self.store.connection.execute(
            "INSERT INTO events (type, at, thread_id, data) "
            "VALUES ('turn.text', '2026-10-24T15:08:27.000+00:00', 't1', '{}')")
        await asyncio.sleep(0.3)
        self.assertEqual(len(got), 3)
        self.add_events(1)
        await wait_for(lambda: len(got) == 5, timeout=1.0)
        self.assertEqual([event['id'] for event in got], [2, 3, 4, 5, 6])
        self.assertEqual(got[-1]['data'], {'text': 'line 0'})
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_follow_sees_events_the_orchestrator_commits(self) -> None:
        got, task = self.consume(self.service.follow({}, PERSON))
        await asyncio.sleep(0)
        info = await self.call('threads.launch', self.launch_params())
        await wait_for(lambda: len(got) >= 1, timeout=1.0)
        self.assertEqual((got[0]['thread_id'], got[0]['type']), (info['id'], 'thread.state'))
        await self.orchestrator.drain()
        await wait_for(lambda: [event['data'].get('state') for event in got]
                       == ['queued', 'starting', 'running'], timeout=1.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_follow_with_thread_id_yields_only_that_thread(self) -> None:
        self.add_events(2, 't1')
        self.add_events(2, 't2')
        got, task = self.consume(self.service.follow({'thread_id': 't2'}, PERSON))
        await wait_for(lambda: len(got) == 2, timeout=1.0)
        self.add_events(1, 't1')
        self.add_events(1, 't2')
        await wait_for(lambda: len(got) == 3, timeout=1.0)
        await asyncio.sleep(0.05)
        self.assertEqual([event['id'] for event in got], [3, 4, 6])
        self.assertEqual({event['thread_id'] for event in got}, {'t2'})
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_follow_pages_past_a_thousand_events(self) -> None:
        total = 2 * MAX_EVENTS + 500
        self.add_events(total)
        got, task = self.consume(self.service.follow({}, PERSON))
        await wait_for(lambda: len(got) == total, timeout=5.0)
        self.add_events(1)
        await wait_for(lambda: len(got) == total + 1, timeout=1.0)
        self.assertEqual([event['id'] for event in got], list(range(1, total + 2)))
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_aclose_removes_the_listener(self) -> None:
        self.add_events(1)
        stream = self.service.follow({}, PERSON)
        self.assertEqual(self.store.listeners, set())
        await anext(stream)
        self.assertEqual(len(self.store.listeners), 1)
        await stream.aclose()
        self.assertEqual(self.store.listeners, set())

    async def test_cancelling_the_consumer_removes_the_listener(self) -> None:
        got, task = self.consume(self.service.follow({}, PERSON))
        await wait_for(lambda: len(self.store.listeners) == 1, timeout=1.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
        self.assertEqual(self.store.listeners, set())

    async def test_a_rolled_back_transaction_wakes_no_one(self) -> None:
        woken = []
        remove = self.store.listen(lambda: woken.append(1))
        with self.assertRaises(RuntimeError), self.store.transaction() as tx:
            self.store.append_event(tx, EventType.TURN_TEXT, 't1', {'text': 'x'})
            raise RuntimeError('boom')
        with self.store.transaction() as tx:
            tx.execute('SELECT 1')
        self.assertEqual(woken, [])
        self.add_events(2)
        self.assertEqual(woken, [1])
        remove()


class TurnToolTest(ServiceTest):
    async def test_turn_tool_data_carries_the_tool_use_id(self) -> None:
        info = await self.call('threads.launch', self.launch_params())
        await self.orchestrator.drain()
        self.runner.sessions[info['id']].emit(ToolUse('toolu_1', 'Read', {'path': 'a'}))
        await wait_for(lambda: any(event['type'] == 'turn.tool'
                                   for event in self.store.events()))
        found = await self.call('threads.events', {'thread_id': info['id']})
        tool = [event['data'] for event in found if event['type'] == 'turn.tool']
        self.assertEqual(tool, [{'tool_use_id': 'toolu_1', 'name': 'Read',
                                 'input': {'path': 'a'}}])
        self.assertEqual(list(tool[0]), ['tool_use_id', 'name', 'input'])


class CloseTest(ServiceTest):
    async def test_close_closes_the_sessions_and_the_store(self) -> None:
        info = await self.call('threads.launch', self.launch_params(workspace=ROOT))
        await self.orchestrator.drain()
        session = self.runner.sessions[info['id']]
        self.assertFalse(session.closed)
        await self.service.close()
        self.assertTrue(session.closed)
        with self.assertRaises(sqlite3.ProgrammingError):
            self.store.connection.execute('SELECT 1')


if __name__ == '__main__':
    unittest.main()
