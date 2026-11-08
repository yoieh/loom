import asyncio
import os
import sqlite3
import unittest
from datetime import UTC, datetime, timedelta
from unittest import mock

from loom_core.service import Caller
from loom_core.slots import StartSource
from loom_core.store import Store
from loom_core.threads import start_queue
from loom_scheduler import Maker, ScheduleError

from loom_service.config import StartError
from loom_service.scheduling import ServiceHost
from loom_service.serve import start

from .fake_claude import ENDLESS
from .support import TIMEOUT, Connection, ServerCase, clean_env, run_client

INFO_KEYS = {'id', 'trigger', 'action', 'text', 'thread_id', 'project_id', 'title',
             'concurrency_key', 'on_busy', 'settle', 'runner', 'model', 'workspace', 'by',
             'paused', 'next_run', 'created_at'}
FORBIDDEN_MAKERS = (Maker(None, True, 's1'), Maker('t1', True, 's1'), Maker(None, False, 's1'))


def at(seconds: float) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat(timespec='milliseconds')


class ScheduleCase(ServerCase):
    def host(self) -> ServiceHost:
        service = self.serving.service
        return ServiceHost(service.store, service)

    def record_callers(self) -> list[tuple[str, Caller]]:
        service = self.serving.service
        method = service.method
        seen: list[tuple[str, Caller]] = []

        def recording(call: object) -> object:
            found = method(call)

            async def run(params: object, caller: Caller) -> object:
                seen.append((call, caller))
                return await found(params, caller)

            return run

        patcher = mock.patch.object(service, 'method', recording)
        patcher.start()
        self.addCleanup(patcher.stop)
        return seen

    async def add_schedule(self, conn: Connection, seconds: float, **params: object) -> dict:
        params = {'trigger': {'type': 'at', 'at': at(seconds)}, 'text': 'tick', **params}
        return await conn.ok('schedules.add', id=conn.new_id(), **params)

    async def fired(self, conn: Connection, count: int = 1) -> list[dict]:
        deadline = asyncio.get_running_loop().time() + TIMEOUT
        while True:
            events = [event for event in await self.all_events(conn)
                      if event['type'] == 'schedule.fired']
            if len(events) >= count:
                return events
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f'{count} schedule.fired events not seen within {TIMEOUT} s')
            await asyncio.sleep(0.02)

    async def idle_thread(self, conn: Connection) -> dict:
        await self.add_project(conn)
        thread = await self.launch(conn, 'hello')
        await self.wait_events(conn, 'turn.result', thread['id'])
        return thread


class HostTest(ScheduleCase):
    async def test_command_stores_a_receipt_and_replays_it(self) -> None:
        host = self.host()
        self.assertEqual(host.command('h1', 'schedules.add', lambda tx: {'n': 1}),
                         ({'n': 1}, False))
        self.assertEqual(host.command('h1', 'schedules.add', lambda tx: {'n': 2}),
                         ({'n': 1}, True))

    async def test_command_errors_come_out_as_schedule_errors(self) -> None:
        host = self.host()
        host.command('h1', 'schedules.add', lambda tx: {'n': 1})
        with self.assertRaises(ScheduleError) as caught:
            host.command('h1', 'schedules.pause', lambda tx: {'n': 2})
        self.assertEqual((caught.exception.code, caught.exception.message),
                         ('conflict', 'id was already used for schedules.add'))
        self.assertIsNone(caught.exception.__cause__)
        self.assertTrue(caught.exception.__suppress_context__)
        with self.assertRaises(ScheduleError) as caught:
            host.command('', 'schedules.add', lambda tx: {})
        self.assertEqual((caught.exception.code, caught.exception.message),
                         ('invalid', 'id must be a string of 1 to 200 characters'))

    async def test_a_schedule_error_from_the_change_rolls_back_unchanged(self) -> None:
        host = self.host()
        refusal = ScheduleError('not_found', 'no such schedule')

        def change(tx: object) -> object:
            host.append_event(tx, 'schedule.fired', None, {'schedule_id': 's1'})
            raise refusal

        with self.assertRaises(ScheduleError) as caught:
            host.command('h2', 'schedules.pause', change)
        self.assertIs(caught.exception, refusal)
        store = self.serving.service.store
        self.assertEqual(store.connection.execute(
            "SELECT COUNT(*) FROM commands WHERE id = 'h2'").fetchone()[0], 0)
        self.assertEqual(store.events(), [])

    async def test_append_event_writes_inside_the_transaction(self) -> None:
        host = self.host()
        with host.transaction() as tx:
            event_id = host.append_event(tx, 'schedule.fired', 't1', {'schedule_id': 's1'})
        events = self.serving.service.store.events()
        self.assertEqual([(event['id'], event['type'], event['thread_id'], event['data'])
                          for event in events],
                         [(event_id, 'schedule.fired', 't1', {'schedule_id': 's1'})])

    async def test_call_reaches_the_service_with_the_makers_caller(self) -> None:
        seen = self.record_callers()
        self.assertEqual(await self.host().call('projects.list', {}, Maker('t9', False, 's9')),
                         [])
        self.assertEqual(await self.host().call('projects.list', {}, Maker('t8', True, None)),
                         [])
        self.assertEqual(seen, [('projects.list', Caller(thread_id='t9', verified=False,
                                                         schedule_id='s9')),
                                ('projects.list', Caller(thread_id='t8'))])

    async def test_call_adds_projects_but_refuses_set_mode_for_every_schedule(self) -> None:
        host = self.host()
        added = await host.call('projects.add', {'id': 'p0', 'title': 'Repo',
                                                 'path': str(self.repo)}, FORBIDDEN_MAKERS[0])
        self.assertEqual(added['id'], 'repo')
        for number, maker in enumerate(FORBIDDEN_MAKERS):
            with self.subTest(maker=maker), self.assertRaises(ScheduleError) as caught:
                await host.call('projects.set_mode', {'id': f'm{number}', 'project_id': 'repo',
                                                      'permission_mode': 'full-access'}, maker)
            self.assertEqual(caught.exception.code, 'forbidden')
        self.assertIsNone(self.serving.service.projects.get('repo')['permission_mode'])

    async def test_call_errors_keep_their_code_and_message(self) -> None:
        host = self.host()
        with self.assertRaises(ScheduleError) as caught:
            await host.call('threads.get', {'thread_id': 'nope'}, Maker(None, True, 's1'))
        self.assertEqual(caught.exception.code, 'not_found')
        self.assertIn('nope', caught.exception.message)
        self.assertIsNone(caught.exception.__cause__)
        with self.assertRaises(ScheduleError) as caught:
            await host.call('requests.respond', {}, Maker(None, True, 's1'))
        self.assertEqual((caught.exception.code, caught.exception.message),
                         ('invalid', 'request_id is required'))


class CallsTest(ScheduleCase):
    async def test_the_schedule_calls_answer_through_the_socket(self) -> None:
        conn = await self.connect()
        due = at(3600)
        added = await conn.ok('schedules.add', id='a1', action='notify', text='later',
                              trigger={'type': 'at', 'at': due})
        self.assertEqual(set(added), INFO_KEYS)
        self.assertEqual((added['action'], added['text'], added['by'], added['paused']),
                         ('notify', 'later', None, False))
        self.assertEqual(added['next_run'], due)
        self.assertEqual(await conn.ok('schedules.list'), [added])
        paused = await conn.ok('schedules.pause', id='p1', schedule_id=added['id'])
        self.assertEqual((paused['id'], paused['paused'], paused['next_run']),
                         (added['id'], True, None))
        self.assertEqual(await conn.ok('schedules.list'), [paused])
        removed = await conn.ok('schedules.remove', id='r1', schedule_id=added['id'])
        self.assertEqual(removed, paused)
        self.assertEqual(await conn.ok('schedules.list'), [])

    async def test_bad_params_answer_the_schedulers_error(self) -> None:
        conn = await self.connect()
        bad = [
            ('schedules.add', {'id': 'b1', 'text': 'x'}, 'invalid', 'trigger is required'),
            ('schedules.add', {'id': 'b2', 'text': 'x', 'trigger': {'type': 'webhook'}},
             'invalid', 'webhook triggers are not served yet'),
            ('schedules.add', {'id': 'b3', 'text': 'x', 'trigger': {'type': 'at', 'at': at(60)}},
             'invalid', 'thread_id is required to post from outside a thread'),
            ('schedules.list', {'thread_id': 5}, 'invalid', 'thread_id must be a string'),
            ('schedules.pause', {'id': 'b4', 'schedule_id': 'nope'}, 'not_found',
             "no schedule with id 'nope'"),
            ('schedules.remove', {'id': 'b5'}, 'invalid', 'schedule_id is required'),
        ]
        for call, params, code, message in bad:
            with self.subTest(call=call, params=params):
                self.assertEqual(await conn.call(call, **params), {
                    'ok': False, 'error': {'code': code, 'message': message}})
        unknown = await conn.call('schedules.add', id='b6', text='x', thread_id='nope',
                                  trigger={'type': 'at', 'at': at(60)})
        self.assertEqual(unknown['error']['code'], 'not_found')
        self.assertEqual(await conn.ok('schedules.list'), [])


class RunTest(ScheduleCase):
    async def test_a_past_notify_fires_once_and_says_when_it_is_late(self) -> None:
        conn = await self.connect()
        late = await self.add_schedule(conn, -120, action='notify', text='late one')
        on_time = await self.add_schedule(conn, -5, action='notify', text='on time')
        events = await self.fired(conn, 2)
        by_schedule = {event['data']['schedule_id']: event for event in events}
        self.assertEqual(by_schedule[late['id']]['data'], {
            'schedule_id': late['id'], 'action': 'notify', 'text': 'late one', 'late': True})
        self.assertEqual(by_schedule[on_time['id']]['data'], {
            'schedule_id': on_time['id'], 'action': 'notify', 'text': 'on time', 'late': False})
        self.assertEqual({event['thread_id'] for event in events}, {None})
        await asyncio.sleep(0.1)
        self.assertEqual(len(await self.fired(conn, 2)), 2)
        listed = await conn.ok('schedules.list')
        self.assertEqual([schedule['next_run'] for schedule in listed], [None, None])

    async def test_a_past_post_sends_its_text_into_the_thread(self) -> None:
        conn = await self.connect()
        thread = await self.idle_thread(conn)
        added = await self.add_schedule(conn, -5, thread_id=thread['id'], text='scheduled')
        self.assertEqual((added['action'], added['project_id']), ('post', 'repo'))
        results = await self.wait_events(conn, 'turn.result', thread['id'], 2)
        self.assertEqual(results[1]['data'], {'state': 'completed', 'text': 'scheduled'})
        fired = await self.wait_events(conn, 'schedule.fired', thread['id'])
        self.assertEqual(fired[0]['data'], {
            'schedule_id': added['id'], 'action': 'post', 'text': 'scheduled', 'late': False})

    async def test_a_keyed_launch_waits_for_the_holder_and_settles_on_its_result(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        holder = await self.launch(conn, ENDLESS, concurrency_key='k')
        await self.spawned(holder['id'])
        added = await self.add_schedule(conn, -5, action='launch', project_id='repo',
                                        text='from a schedule', concurrency_key='k',
                                        settle='on_result')
        self.assertEqual((added['concurrency_key'], added['on_busy'], added['settle']),
                         ('k', 'queue', 'on_result'))
        fired = await self.fired(conn)
        scheduled = fired[0]['thread_id']
        await asyncio.sleep(0.2)
        waiting = await conn.ok('threads.get', thread_id=scheduled)
        self.assertEqual((waiting['state'], waiting['concurrency_key'], waiting['settle'],
                          waiting['origin']),
                         ('queued', 'k', 'on_result', f'schedule:{added["id"]}'))
        self.assertEqual([event['type'] for event in await self.all_events(
            conn, thread_id=scheduled) if event['type'].startswith('turn.')], [])
        await conn.ok('threads.settle', id=conn.new_id(), thread_id=holder['id'])
        settled = await self.wait_events(conn, 'thread.settled', scheduled)
        self.assertEqual(settled[0]['data'], {'result': 'from a schedule'})
        self.assertEqual((await conn.ok('threads.get', thread_id=scheduled))['state'],
                         'settled')


class CallerTest(ScheduleCase):
    async def test_an_agents_schedule_runs_as_that_agent(self) -> None:
        conn = await self.connect()
        thread = await self.idle_thread(conn)
        seen = self.record_callers()
        answers = await run_client(self.socket, [{'call': 'schedules.add', 'params': {
            'id': 'g1', 'trigger': {'type': 'at', 'at': at(-5)}, 'text': 'from the agent',
        }}], clean_env(LOOM_THREAD=thread['id']))
        added = answers[0]['result']
        self.assertEqual((added['by'], added['thread_id'], added['action']),
                         (thread['id'], thread['id'], 'post'))
        results = await self.wait_events(conn, 'turn.result', thread['id'], 2)
        self.assertEqual(results[1]['data']['text'], 'from the agent')
        fired = await self.wait_events(conn, 'schedule.fired', thread['id'])
        self.assertEqual(fired[0]['data']['by'], thread['id'])
        self.assertIn(('threads.get', Caller(thread_id=thread['id'])), seen)
        scheduled = [(call, caller) for call, caller in seen if caller.schedule_id is not None]
        self.assertIn('threads.send', [call for call, _ in scheduled])
        self.assertEqual({caller for _, caller in scheduled},
                         {Caller(thread_id=thread['id'], schedule_id=added['id'])})

    async def test_a_persons_schedule_runs_with_no_thread_and_its_id(self) -> None:
        conn = await self.connect()
        thread = await self.idle_thread(conn)
        seen = self.record_callers()
        added = await self.add_schedule(conn, -5, thread_id=thread['id'], text='from a person')
        self.assertIsNone(added['by'])
        await self.wait_events(conn, 'schedule.fired', thread['id'])
        scheduled = [(call, caller) for call, caller in seen if caller.schedule_id is not None]
        self.assertIn('threads.send', [call for call, _ in scheduled])
        self.assertEqual({caller for _, caller in scheduled},
                         {Caller(schedule_id=added['id'])})

    async def test_a_scheduled_launch_queues_after_a_persons(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        orchestrator = self.serving.service.orchestrator
        patcher = mock.patch.object(orchestrator.lanes, 'submit')
        patcher.start()
        self.addCleanup(patcher.stop)
        added = await self.add_schedule(conn, -5, action='launch', project_id='repo',
                                        text='from a schedule')
        fired = await self.fired(conn)
        scheduled = fired[0]['thread_id']
        person = await self.launch(conn, 'from a person')
        store = self.serving.service.store
        with store.transaction() as tx:
            queue = start_queue(tx)
        self.assertEqual([(rank, thread_id) for rank, _, thread_id in queue],
                         [(StartSource.PERSON, person['id']),
                          (StartSource.SCHEDULE, scheduled)])
        self.assertEqual(fired[0]['data']['schedule_id'], added['id'])


class EventTriggerTest(ScheduleCase):
    async def on_event(self, conn: Connection, topic: str = 'demo.done', **params: object) -> dict:
        params = {'trigger': {'type': 'event', 'topic': topic, 'match': {'to': 'done'}},
                  'text': 'Run {{payload.to}} for {{event.id}}', 'action': 'notify', **params}
        return await conn.ok('schedules.add', id=conn.new_id(), **params)

    async def emit(self, conn: Connection, to: str = 'done') -> dict:
        return await conn.ok('signals.emit', id=conn.new_id(), topic='demo.done',
                             payload={'to': to})

    async def test_the_host_reports_new_events_and_the_last_event_id(self) -> None:
        host = self.host()
        self.assertEqual(host.last_event_id(), 0)
        seen: list[int] = []
        remove = host.listen(lambda: seen.append(host.last_event_id()))
        conn = await self.connect()
        signal = await self.emit(conn)
        remove()
        await self.emit(conn)
        self.assertEqual(seen, [signal['id']])
        self.assertEqual(host.last_event_id(), signal['id'] + 1)

    async def test_a_notify_event_schedule_fires_on_a_signal(self) -> None:
        conn = await self.connect()
        early = await self.emit(conn)
        added = await self.on_event(conn)
        self.assertEqual((added['trigger'], added['next_run']), (
            {'type': 'event', 'topic': 'demo.done', 'match': {'to': 'done'}}, None))
        await self.emit(conn, 'open')
        signal = await self.emit(conn)
        [fired] = await self.fired(conn)
        self.assertEqual((fired['thread_id'], fired['data']), (None, {
            'schedule_id': added['id'], 'action': 'notify',
            'text': f'Run done for {signal["id"]}', 'late': False, 'cause': signal['id'],
            'hops': 1}))
        await asyncio.sleep(0.1)
        self.assertEqual(len(await self.fired(conn)), 1)
        self.assertNotEqual(early['id'], signal['id'])

    async def test_a_launch_records_the_cause_and_hops_it_fired_with(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        added = await self.on_event(conn, action='launch', project_id='repo')
        signal = await self.emit(conn)
        [fired] = await self.fired(conn)
        thread = await conn.ok('threads.get', thread_id=fired['thread_id'])
        self.assertEqual((thread['cause'], thread['hops'], thread['origin']),
                         (signal['id'], 1, f'schedule:{added["id"]}'))
        self.assertEqual((fired['data']['cause'], fired['data']['hops']), (signal['id'], 1))

    async def test_after_a_restart_a_missed_signal_fires_once(self) -> None:
        conn = await self.connect()
        added = await self.on_event(conn)
        handled = await self.emit(conn)
        await self.fired(conn)
        await self.serving.close()
        store = Store.open(self.data_dir)
        with store.transaction() as tx:
            missed = store.append_event(tx, 'signal', None, {
                'topic': 'demo.done', 'payload': {'to': 'done'}, 'hops': 0})
        store.close()
        self.serving = await self.serve()
        conn = await self.connect()
        events = await self.fired(conn, 2)
        await asyncio.sleep(0.2)
        events = await self.fired(conn, 2)
        self.assertEqual([(event['data']['schedule_id'], event['data']['cause'])
                          for event in events],
                         [(added['id'], handled['id']), (added['id'], missed)])


class LifecycleTest(ScheduleCase):
    def ticking(self) -> list[asyncio.Task]:
        return [task for task in asyncio.all_tasks()
                if getattr(task.get_coro(), '__qualname__', '') == 'Tick.run']

    async def test_schedules_survive_a_restart_and_a_missed_at_fires_once(self) -> None:
        conn = await self.connect()
        later = await self.add_schedule(conn, 3600, action='notify', text='later')
        soon = await self.add_schedule(conn, 1, action='notify', text='soon')
        await self.serving.close()
        wait = (datetime.fromisoformat(soon['next_run']) - datetime.now(UTC)).total_seconds()
        await asyncio.sleep(max(wait, 0) + 0.05)
        restarted = datetime.now(UTC)
        self.serving = await self.serve()
        conn = await self.connect()
        await self.fired(conn)
        await asyncio.sleep(0.1)
        events = await self.fired(conn)
        self.assertEqual([event['data']['schedule_id'] for event in events], [soon['id']])
        self.assertGreaterEqual(datetime.fromisoformat(events[0]['at']),
                                restarted - timedelta(milliseconds=1))
        listed = {schedule['id']: schedule for schedule in await conn.ok('schedules.list')}
        self.assertEqual(listed[later['id']]['next_run'], later['next_run'])
        self.assertIsNone(listed[soon['id']]['next_run'])

    async def test_close_ends_the_schedulers_loop(self) -> None:
        self.assertEqual(len(self.ticking()), 1)
        await self.serving.close()
        self.assertEqual(self.ticking(), [])

    async def test_a_store_error_in_the_schedules_stops_the_start(self) -> None:
        data_dir = self.base / 'broken'
        data_dir.mkdir(mode=0o700)
        db = sqlite3.connect(data_dir / 'loom.db')
        db.execute('CREATE VIEW schedule_runs AS SELECT 1 AS at')
        db.close()
        socket_path = self.base / 'broken.sock'
        with self.assertRaises(StartError) as caught:
            await start(socket_path, data_dir, self.config(), environ=self.environ,
                        log=self.log)
        self.assertTrue(str(caught.exception).startswith(
            f'cannot open the schedules in {data_dir}: '), caught.exception)
        self.assertFalse(os.path.lexists(socket_path))
        self.assertEqual(len(self.ticking()), 1)


if __name__ == '__main__':
    unittest.main()
