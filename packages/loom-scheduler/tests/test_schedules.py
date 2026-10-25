import unittest

from loom_contracts.calls import CallName, ScheduleInfo

from loom_scheduler import Maker

from .fakes import AGENT, PERSON, UNKNOWN, SchedulerCase

ID_MESSAGE = 'id must be a string of 1 to 200 characters'
BAD_IDS = ({}, {'id': None}, {'id': 7}, {'id': 'x' * 201})
KEY_MESSAGE = 'concurrency_key must be a string of 1 to 200 characters'
LAUNCH = {'action': 'launch', 'thread_id': ..., 'project_id': 'p1'}
OLD_SCHEDULES = '''CREATE TABLE schedules (
    id TEXT PRIMARY KEY,
    trigger TEXT NOT NULL,
    action TEXT NOT NULL,
    text TEXT NOT NULL,
    thread_id TEXT,
    project_id TEXT,
    title TEXT,
    by TEXT,
    verified INTEGER NOT NULL,
    paused INTEGER NOT NULL DEFAULT 0,
    next_run TEXT,
    created_at TEXT NOT NULL
)'''


class MethodsTest(SchedulerCase):
    async def test_methods_are_the_four_schedule_calls(self) -> None:
        self.assertEqual(set(self.scheduler.methods()), {
            'schedules.add', 'schedules.list', 'schedules.pause', 'schedules.remove',
        })

    async def test_params_must_be_an_object(self) -> None:
        for name in self.scheduler.methods():
            with self.subTest(call=name):
                self.assertEqual(await self.refused(name, []), 'params must be an object')


class AddRefusalTest(SchedulerCase):
    async def assert_refused(self, cases: list[tuple[dict, str]], code: str = 'invalid',
                             caller: object = PERSON) -> None:
        for fields, message in cases:
            with self.subTest(fields=fields):
                params = {'id': 'a1', 'trigger': {'type': 'interval', 'everyMs': 60_000},
                          'text': 'hi', 'thread_id': 't1'}
                params.update(fields)
                params = {key: value for key, value in params.items() if value is not ...}
                self.assertEqual(
                    await self.refused('schedules.add', params, code, caller), message,
                )
                self.assertEqual(self.schedule_rows(), 0)
                self.assertEqual(self.receipts(), 0)

    async def test_id_is_checked(self) -> None:
        await self.assert_refused([
            ({'id': ...}, ID_MESSAGE),
            ({'id': None}, ID_MESSAGE),
            ({'id': 7}, ID_MESSAGE),
            ({'id': 'x' * 201}, ID_MESSAGE),
        ])

    async def test_trigger_is_checked(self) -> None:
        await self.assert_refused([
            ({'trigger': ...}, 'trigger is required'),
            ({'trigger': None}, 'trigger is required'),
            ({'trigger': ['at']}, 'trigger must be an object'),
            ({'trigger': {'type': 'webhook'}}, 'webhook triggers are not served yet'),
            ({'trigger': {'type': 'interval', 'everyMs': 59_999}},
             'trigger.everyMs must be an integer from 60000 to 31536000000'),
        ])

    async def test_text_is_checked(self) -> None:
        await self.assert_refused([
            ({'text': ...}, 'text is required'),
            ({'text': None}, 'text is required'),
            ({'text': 3}, 'text must be a non-empty string'),
            ({'text': ''}, 'text must be a non-empty string'),
        ])

    async def test_action_is_checked(self) -> None:
        await self.assert_refused([
            ({'action': None}, 'action must not be null'),
            ({'action': 'send'}, 'action must be one of post, launch, notify'),
        ])

    async def test_thread_project_and_title_are_checked(self) -> None:
        await self.assert_refused([
            ({'thread_id': None}, 'thread_id must not be null'),
            ({'thread_id': 1}, 'thread_id must be a string'),
            ({'project_id': None}, 'project_id must not be null'),
            ({'project_id': 1}, 'project_id must be a string'),
            ({'title': None}, 'title must not be null'),
            ({'title': 1}, 'title must be a non-empty string'),
            ({'title': ''}, 'title must be a non-empty string'),
        ])

    async def test_post_from_a_person_needs_a_thread(self) -> None:
        for caller in (PERSON, UNKNOWN):
            with self.subTest(caller=caller):
                await self.assert_refused(
                    [({'thread_id': ...},
                      'thread_id is required to post from outside a thread')],
                    caller=caller,
                )

    async def test_unknown_thread_is_not_found(self) -> None:
        await self.assert_refused([({'thread_id': 'nope'}, "no thread with id 'nope'")],
                                  'not_found')

    async def test_post_into_another_project_is_refused(self) -> None:
        await self.assert_refused(
            [({'project_id': 'p2'}, "project_id is not the thread's project")],
        )

    async def test_launch_is_checked(self) -> None:
        await self.assert_refused([
            ({'action': 'launch', 'project_id': 'p1'}, 'thread_id does not go with launch'),
            ({'action': 'launch', 'thread_id': ...}, 'project_id is required to launch'),
        ])
        await self.assert_refused(
            [({'action': 'launch', 'thread_id': ..., 'project_id': 'p9'},
              "no project with id 'p9'")],
            'not_found',
        )

    async def test_launch_options_are_checked(self) -> None:
        await self.assert_refused([({**LAUNCH, **fields}, message) for fields, message in [
            ({'concurrency_key': None}, 'concurrency_key must not be null'),
            ({'concurrency_key': 3}, KEY_MESSAGE),
            ({'concurrency_key': ''}, KEY_MESSAGE),
            ({'concurrency_key': 'k' * 201}, KEY_MESSAGE),
            ({'concurrency_key': 'k', 'on_busy': None}, 'on_busy must not be null'),
            ({'concurrency_key': 'k', 'on_busy': 'wait'},
             'on_busy must be one of queue, coalesce'),
            ({'concurrency_key': 'k', 'on_busy': 1}, 'on_busy must be one of queue, coalesce'),
            ({'on_busy': 'queue'}, 'on_busy needs concurrency_key'),
            ({'on_busy': 'coalesce'}, 'on_busy needs concurrency_key'),
            ({'settle': None}, 'settle must not be null'),
            ({'settle': 'idle'}, 'settle must be one of on_result'),
            ({'settle': 1}, 'settle must be one of on_result'),
        ]])

    async def test_launch_options_go_only_with_launch(self) -> None:
        actions = [{'action': ...}, {'action': 'post'},
                   {'action': 'notify', 'thread_id': ...}]
        options = [
            ({'concurrency_key': 'k'}, 'concurrency_key'),
            ({'on_busy': 'queue'}, 'on_busy'),
            ({'settle': 'on_result'}, 'settle'),
            ({'settle': None}, 'settle'),
            ({'concurrency_key': 'k', 'on_busy': 'coalesce', 'settle': 'on_result'},
             'concurrency_key'),
            ({'settle': 'on_result', 'on_busy': 'queue'}, 'on_busy'),
        ]
        await self.assert_refused([
            ({**action, **fields}, f'{field} goes only with launch')
            for action in actions for fields, field in options
        ])

    async def test_notify_with_an_unknown_project_is_not_found(self) -> None:
        await self.assert_refused(
            [({'action': 'notify', 'thread_id': ..., 'project_id': 'p9'},
              "no project with id 'p9'")],
            'not_found',
        )


class AddTest(SchedulerCase):
    async def test_answer_is_a_schedule_info(self) -> None:
        info = await self.add()
        self.assertEqual(set(info), set(ScheduleInfo.__annotations__))
        self.assertEqual(info['trigger'], {'type': 'interval', 'everyMs': 60_000})
        self.assertEqual(info['action'], 'post')
        self.assertEqual(info['text'], 'check the build')
        self.assertEqual((info['thread_id'], info['project_id'], info['title']),
                         ('t1', 'p1', None))
        self.assertIsNone(info['by'])
        self.assertFalse(info['paused'])
        self.assertEqual(info['created_at'], '2026-10-23T07:00:00.000+00:00')
        self.assertEqual(info['next_run'], '2026-10-23T07:01:00.000+00:00')
        self.assertEqual(self.host.rows('SELECT verified FROM schedules'), [(1,)])

    async def test_agent_posts_into_its_own_thread(self) -> None:
        info = await self.add(caller=AGENT, thread_id=...)
        self.assertEqual((info['thread_id'], info['project_id'], info['by']), ('t1', 'p1', 't1'))

    async def test_unverified_caller_is_stored_unverified(self) -> None:
        info = await self.add(caller=UNKNOWN)
        self.assertIsNone(info['by'])
        self.assertEqual(self.host.rows('SELECT verified FROM schedules'), [(0,)])

    async def test_launch_and_notify_keep_their_targets(self) -> None:
        launch = await self.add('a1', AGENT, action='launch', thread_id=..., project_id='p2',
                                title='Nightly')
        self.assertEqual((launch['thread_id'], launch['project_id'], launch['title']),
                         (None, 'p2', 'Nightly'))
        notify = await self.add('a2', AGENT, action='notify', thread_id=...)
        self.assertEqual((notify['thread_id'], notify['project_id']), ('t1', 'p1'))
        alone = await self.add('a3', PERSON, action='notify', thread_id=...)
        self.assertEqual((alone['thread_id'], alone['project_id']), (None, None))

    async def test_launch_shows_its_options_and_list_shows_the_same(self) -> None:
        info = await self.add(**LAUNCH, concurrency_key='inbox:p1', on_busy='coalesce',
                              settle='on_result')
        self.assertEqual((info['concurrency_key'], info['on_busy'], info['settle']),
                         ('inbox:p1', 'coalesce', 'on_result'))
        self.assertEqual(await self.call('schedules.list', {}), [info])

    async def test_a_key_without_on_busy_queues_and_no_key_shows_nulls(self) -> None:
        keyed = await self.add('a1', **LAUNCH, concurrency_key='pm:p1')
        self.assertEqual((keyed['concurrency_key'], keyed['on_busy'], keyed['settle']),
                         ('pm:p1', 'queue', None))
        settles = await self.add('a2', **LAUNCH, settle='on_result')
        self.assertEqual((settles['concurrency_key'], settles['on_busy'], settles['settle']),
                         (None, None, 'on_result'))
        for number, fields in enumerate([LAUNCH, {}, {'action': 'notify'}]):
            with self.subTest(fields=fields):
                info = await self.add(f'b{number}', **fields)
                self.assertEqual((info['concurrency_key'], info['on_busy'], info['settle']),
                                 (None, None, None))

    async def test_a_key_of_two_hundred_characters_is_taken(self) -> None:
        info = await self.add(**LAUNCH, concurrency_key='k' * 200)
        self.assertEqual(info['concurrency_key'], 'k' * 200)
        self.assertEqual(
            await self.refused('schedules.add', {
                'id': 'a2', 'trigger': {'type': 'interval', 'everyMs': 60_000}, 'text': 'x',
                'action': 'launch', 'project_id': 'p1', 'concurrency_key': 'k' * 201,
            }), KEY_MESSAGE,
        )
        self.assertEqual(self.schedule_rows(), 1)

    async def test_lookups_carry_the_caller_and_no_schedule(self) -> None:
        await self.add(caller=AGENT, thread_id=...)
        self.assertEqual([call.call for call in self.host.calls],
                         [CallName.THREADS_GET, CallName.PROJECTS_LIST])
        for call in self.host.calls:
            self.assertEqual(call.maker, Maker(by='t1', verified=True, schedule_id=None))
        self.assertEqual(self.host.calls[0].params, {'thread_id': 't1'})

    async def test_same_id_replays_the_first_answer(self) -> None:
        first = await self.add()
        self.clock.advance(seconds=5)
        self.assertEqual(await self.add(), first)
        self.assertEqual(self.schedule_rows(), 1)

    async def test_id_used_for_another_call_is_a_conflict(self) -> None:
        info = await self.add('a1')
        await self.call('schedules.pause', {'id': 'p1', 'schedule_id': info['id']})
        self.assertEqual(await self.refused('schedules.add', {
            'id': 'p1', 'trigger': {'type': 'interval', 'everyMs': 60_000}, 'text': 'x',
            'thread_id': 't1'}, 'conflict'), 'id was already used for schedules.pause')
        self.assertEqual(self.schedule_rows(), 1)

    async def test_id_of_two_hundred_characters_is_accepted(self) -> None:
        info = await self.add('x' * 200)
        self.assertEqual(self.schedule_rows(), 1)
        self.assertFalse(info['paused'])

    async def test_add_wakes_the_tick_for_the_new_run(self) -> None:
        self.assertIsNone(self.scheduler.next_wake())
        info = await self.add()
        self.assertEqual(self.scheduler.tick.delay(), 30.0)
        self.assertEqual(self.scheduler.next_wake().isoformat(timespec='milliseconds'),
                         info['next_run'])


class OldStoreTest(SchedulerCase):
    async def test_an_old_store_gains_the_columns_and_fires_as_before(self) -> None:
        self.host.connection.execute('DROP TABLE schedules')
        self.host.connection.execute(OLD_SCHEDULES)
        self.host.connection.execute(
            'INSERT INTO schedules (id, trigger, action, text, thread_id, project_id, title, '
            'by, verified, paused, next_run, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('s-old', '{"type": "interval", "everyMs": 60000}', 'launch', 'nightly', None,
             'p1', None, None, 1, 0, '2026-10-23T07:01:00.000+00:00',
             '2026-10-23T07:00:00.000+00:00'),
        )
        self.scheduler = self.new_scheduler()
        self.scheduler = self.new_scheduler()
        columns = [row[1] for row in self.host.rows('PRAGMA table_info(schedules)')]
        self.assertEqual(columns[-3:], ['concurrency_key', 'on_busy', 'settle'])
        [info] = await self.call('schedules.list', {})
        self.assertEqual((info['id'], info['concurrency_key'], info['on_busy'], info['settle']),
                         ('s-old', None, None, None))
        await self.run_pass(minutes=1)
        [launch] = self.host.calls_of(CallName.THREADS_LAUNCH)
        self.assertEqual(launch.params, {
            'id': 'schedule:s-old:2026-10-23T07:01:00.000+00:00', 'title': 'nightly',
            'message': 'nightly', 'project_id': 'p1', 'origin': 'schedule:s-old',
        })
        self.assertEqual(self.runs('s-old')[0][2], 'fired')


class ListTest(SchedulerCase):
    async def test_filters_are_checked(self) -> None:
        for field in ('thread_id', 'project_id'):
            with self.subTest(field=field):
                self.assertEqual(await self.refused('schedules.list', {field: None}),
                                 f'{field} must not be null')
                self.assertEqual(await self.refused('schedules.list', {field: 3}),
                                 f'{field} must be a string')

    async def test_list_filters_and_orders(self) -> None:
        first = await self.add('a1')
        second = await self.add('a2', thread_id='t2')
        self.clock.advance(seconds=-10)
        third = await self.add('a3', action='launch', thread_id=..., project_id='p1')
        fourth = await self.add('a4', action='notify', thread_id='t1')
        earliest = sorted([third, fourth], key=lambda info: info['id'])
        latest = sorted([first, second], key=lambda info: info['id'])
        self.assertEqual(await self.call('schedules.list', {}), [*earliest, *latest])
        self.assertEqual(await self.call('schedules.list', {'thread_id': 't1'}), [fourth, first])
        self.assertEqual(await self.call('schedules.list', {'project_id': 'p1'}),
                         [*earliest, first])
        self.assertEqual(
            await self.call('schedules.list', {'thread_id': 't2', 'project_id': 'p2'}), [second],
        )
        self.assertEqual(await self.call('schedules.list', {'project_id': 'p9'}), [])


class PauseAndRemoveTest(SchedulerCase):
    async def test_id_and_schedule_id_are_checked(self) -> None:
        info = await self.add()
        receipts = self.receipts()
        for name in ('schedules.pause', 'schedules.remove'):
            cases = [({**bad, 'schedule_id': info['id']}, ID_MESSAGE) for bad in BAD_IDS]
            cases += [
                ({'id': 'c1'}, 'schedule_id is required'),
                ({'id': 'c1', 'schedule_id': None}, 'schedule_id is required'),
                ({'id': 'c1', 'schedule_id': 4}, 'schedule_id must be a string'),
            ]
            for params, message in cases:
                with self.subTest(call=name, params=params):
                    self.assertEqual(await self.refused(name, params), message)
            with self.subTest(call=name, schedule_id='nope'):
                self.assertEqual(
                    await self.refused(name, {'id': 'c1', 'schedule_id': 'nope'}, 'not_found'),
                    "no schedule with id 'nope'",
                )
        self.assertEqual(self.receipts(), receipts)
        self.assertEqual(self.schedule_rows(), 1)
        self.assertFalse((await self.call('schedules.list', {}))[0]['paused'])

    async def test_pause_clears_next_run_and_stops_runs(self) -> None:
        info = await self.add()
        paused = await self.call('schedules.pause', {'id': 'c1', 'schedule_id': info['id']})
        self.assertEqual(paused, {**info, 'paused': True, 'next_run': None})
        self.assertEqual(await self.call('schedules.list', {}), [paused])
        self.assertIsNone(self.scheduler.next_wake())
        await self.run_pass(hours=2)
        self.assertEqual(self.host.calls_of(CallName.THREADS_SEND), [])
        self.assertEqual(self.runs(info['id']), [])

    async def test_pausing_again_changes_nothing_and_replays(self) -> None:
        info = await self.add()
        paused = await self.call('schedules.pause', {'id': 'c1', 'schedule_id': info['id']})
        self.assertEqual(
            await self.call('schedules.pause', {'id': 'c2', 'schedule_id': info['id']}), paused,
        )
        self.assertEqual(
            await self.call('schedules.pause', {'id': 'c1', 'schedule_id': info['id']}), paused,
        )
        self.assertEqual(await self.call('schedules.list', {}), [paused])

    async def test_remove_deletes_the_schedule_and_its_runs(self) -> None:
        info = await self.add()
        other = await self.add('a2')
        await self.run_pass(minutes=1)
        self.assertEqual(len(self.runs(info['id'])), 1)
        current = next(item for item in await self.call('schedules.list', {})
                       if item['id'] == info['id'])
        removed = await self.call('schedules.remove', {'id': 'r1', 'schedule_id': info['id']})
        self.assertEqual(removed, current)
        self.assertEqual(self.runs(info['id']), [])
        self.assertEqual(len(self.runs(other['id'])), 1)
        self.assertEqual([item['id'] for item in await self.call('schedules.list', {})],
                         [other['id']])
        self.assertEqual(
            await self.call('schedules.remove', {'id': 'r1', 'schedule_id': info['id']}), removed,
        )
        self.assertEqual(
            await self.refused('schedules.remove', {'id': 'r2', 'schedule_id': info['id']},
                               'not_found'),
            f"no schedule with id '{info['id']}'",
        )
        sends = len(self.host.calls_of(CallName.THREADS_SEND))
        await self.run_pass(minutes=5)
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_SEND)), sends + 1)
        self.assertEqual(self.host.calls_of(CallName.THREADS_SEND)[-1].maker.schedule_id,
                         other['id'])

    async def test_pause_takes_a_schedule_out_of_the_heap(self) -> None:
        info = await self.add()
        self.clock.advance(minutes=1)
        self.assertEqual(self.scheduler.tick.delay(), 0.0)
        await self.call('schedules.pause', {'id': 'c1', 'schedule_id': info['id']})
        self.assertEqual(self.scheduler.tick.delay(), 30.0)
        self.assertIsNone(self.scheduler.next_wake())


if __name__ == '__main__':
    unittest.main()
