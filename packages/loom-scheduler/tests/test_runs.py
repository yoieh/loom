import asyncio
import unittest
from datetime import timedelta

from loom_contracts.calls import CallName
from loom_contracts.events import EventType

from loom_scheduler import Maker, ScheduleError

from .fakes import AGENT, PERSON, Event, SchedulerCase

INTERVAL = {'type': 'interval', 'everyMs': 60_000}
DUE = '2026-10-23T07:01:00.000+00:00'


class FireTest(SchedulerCase):
    async def test_post_sends_into_the_schedule_thread(self) -> None:
        info = await self.add(caller=AGENT, thread_id=...)
        await self.run_pass(minutes=1)
        self.assertEqual(self.host.calls_of(CallName.THREADS_SEND)[0].params, {
            'id': f'schedule:{info["id"]}:{DUE}', 'thread_id': 't1',
            'message': 'check the build', 'mode': 'queue',
        })
        self.assertEqual(self.runs(info['id']),
                         [(DUE, DUE, 'fired', 0, 't1', None)])
        self.assertEqual(self.host.events, [Event(EventType.SCHEDULE_FIRED, 't1', {
            'schedule_id': info['id'], 'action': 'post', 'text': 'check the build',
            'late': False, 'by': 't1',
        })])
        self.assertEqual(self.next_run(info['id']), '2026-10-23T07:02:00.000+00:00')

    async def test_launch_starts_a_thread_with_the_title_rule(self) -> None:
        cases = [
            ({'title': 'Nightly'}, 'Nightly'),
            ({'text': '\n   \n  First line here  \nsecond'}, 'First line here'),
            ({'text': 'y' * 100}, 'y' * 80),
        ]
        for number, (fields, title) in enumerate(cases):
            with self.subTest(fields=fields):
                info = await self.add(f'a{number}', action='launch', thread_id=...,
                                      project_id='p2', **fields)
                await self.run_pass(minutes=1)
                origin = f'schedule:{info["id"]}'
                [launch] = [call for call in self.host.calls_of(CallName.THREADS_LAUNCH)
                            if call.params['origin'] == origin]
                due = self.runs(info['id'])[0][0]
                self.assertEqual(launch.params, {
                    'id': f'{origin}:{due}', 'title': title, 'message': info['text'],
                    'project_id': 'p2', 'origin': origin,
                })
                launched = self.runs(info['id'])[0][4]
                self.assertEqual(self.host.threads[launched]['project_id'], 'p2')
                self.assertEqual(self.runs(info['id'])[0][2:], ('fired', 0, launched, None))
                self.assertEqual(self.host.events[-1], Event(
                    EventType.SCHEDULE_FIRED, launched,
                    {'schedule_id': info['id'], 'action': 'launch', 'text': info['text'],
                     'late': False},
                ))

    async def test_launch_passes_its_key_on_busy_and_settle_only_when_set(self) -> None:
        keyed = await self.add('a1', action='launch', thread_id=..., project_id='p1',
                               concurrency_key='pm:p1', settle='on_result')
        coalescing = await self.add('a2', action='launch', thread_id=..., project_id='p1',
                                    concurrency_key='inbox:p1', on_busy='coalesce')
        plain = await self.add('a3', action='launch', thread_id=..., project_id='p1')
        await self.run_pass(minutes=1)
        sent = {call.params['origin']: call.params
                for call in self.host.calls_of(CallName.THREADS_LAUNCH)}
        self.assertEqual(sent[f'schedule:{keyed["id"]}'], {
            'id': f'schedule:{keyed["id"]}:{DUE}', 'title': 'check the build',
            'message': 'check the build', 'project_id': 'p1',
            'origin': f'schedule:{keyed["id"]}', 'concurrency_key': 'pm:p1',
            'on_busy': 'queue', 'settle': 'on_result',
        })
        coalesced = sent[f'schedule:{coalescing["id"]}']
        self.assertEqual((coalesced['concurrency_key'], coalesced['on_busy']),
                         ('inbox:p1', 'coalesce'))
        self.assertNotIn('settle', coalesced)
        self.assertEqual(set(sent[f'schedule:{plain["id"]}']),
                         {'id', 'title', 'message', 'project_id', 'origin'})
        for info in (keyed, coalescing, plain):
            self.assertEqual(self.runs(info['id'])[0][2], 'fired')

    async def test_launch_sends_its_runner_model_and_workspace(self) -> None:
        rooted = await self.add('a1', action='launch', thread_id=..., project_id='p1',
                                runner='t3', model='sonnet', workspace={'strategy': 'root'})
        based = await self.add('a2', action='launch', thread_id=..., project_id='p1',
                               workspace={'strategy': 'worktree', 'base': 'main'})
        plain = await self.add('a3', action='launch', thread_id=..., project_id='p1',
                               runner='codex', workspace={'strategy': 'worktree'})
        await self.run_pass(minutes=1)
        sent = {call.params['origin']: call.params
                for call in self.host.calls_of(CallName.THREADS_LAUNCH)}
        self.assertEqual(sent[f'schedule:{rooted["id"]}'], {
            'id': f'schedule:{rooted["id"]}:{DUE}', 'title': 'check the build',
            'message': 'check the build', 'project_id': 'p1',
            'origin': f'schedule:{rooted["id"]}', 'runner': 't3', 'model': 'sonnet',
            'workspace': {'strategy': 'root'},
        })
        self.assertEqual(sent[f'schedule:{based["id"]}']['workspace'],
                         {'strategy': 'worktree', 'base': 'main'})
        self.assertNotIn('runner', sent[f'schedule:{based["id"]}'])
        self.assertEqual((sent[f'schedule:{plain["id"]}']['runner'],
                          sent[f'schedule:{plain["id"]}']['workspace']),
                         ('codex', {'strategy': 'worktree'}))
        self.assertNotIn('model', sent[f'schedule:{plain["id"]}'])

    async def test_notify_only_appends_the_event(self) -> None:
        info = await self.add(caller=AGENT, action='notify', thread_id=...)
        alone = await self.add('a2', action='notify', thread_id=...)
        self.host.calls.clear()
        await self.run_pass(minutes=1)
        self.assertEqual(self.host.calls, [])
        self.assertEqual(self.runs(info['id']), [(DUE, DUE, 'fired', 0, None, None)])
        self.assertEqual(sorted(self.host.events, key=lambda event: str(event.thread_id)), [
            Event(EventType.SCHEDULE_FIRED, None, {
                'schedule_id': alone['id'], 'action': 'notify', 'text': 'check the build',
                'late': False,
            }),
            Event(EventType.SCHEDULE_FIRED, 't1', {
                'schedule_id': info['id'], 'action': 'notify', 'text': 'check the build',
                'late': False, 'by': 't1',
            }),
        ])

    async def test_every_call_for_a_schedule_carries_its_maker(self) -> None:
        agents = await self.add('a1', AGENT, thread_id=...)
        persons = await self.add('a2', PERSON, action='launch', thread_id=..., project_id='p1')
        self.host.calls.clear()
        await self.run_pass(minutes=1)
        await self.run_pass(minutes=1)
        makers = {(call.call, call.maker) for call in self.host.calls}
        self.assertEqual(makers, {
            (CallName.THREADS_SEND, Maker('t1', True, agents['id'])),
            (CallName.THREADS_GET, Maker('t1', True, agents['id'])),
            (CallName.THREADS_LAUNCH, Maker(None, True, persons['id'])),
            (CallName.THREADS_GET, Maker(None, True, persons['id'])),
        })
        self.assertTrue(all(call.maker.schedule_id is not None for call in self.host.calls))


class MissedRunTest(SchedulerCase):
    async def test_missed_at_runs_once_on_start_and_says_it_is_late(self) -> None:
        info = await self.add(trigger={'type': 'at', 'at': '2026-10-23T08:00+00:00'})
        self.clock.advance(hours=5)
        self.scheduler = self.new_scheduler()
        await self.scheduler.start()
        async with asyncio.timeout(1.0):
            while not self.host.events:
                await asyncio.sleep(0.005)
        due = '2026-10-23T08:00:00.000+00:00'
        self.assertEqual(self.host.calls_of(CallName.THREADS_SEND)[0].params['message'],
                         f'This run is late: it was due at {due}.\n\ncheck the build')
        self.assertEqual(self.host.events[0].data['late'], True)
        self.assertEqual(self.runs(info['id']),
                         [(due, '2026-10-23T12:00:00.000+00:00', 'fired', 1, 't1', None)])
        self.assertIsNone(self.next_run(info['id']))
        listed = await self.call('schedules.list', {})
        self.assertIsNone(listed[0]['next_run'])
        await self.run_pass(hours=1)
        self.assertEqual(len(self.runs(info['id'])), 1)
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_SEND)), 1)

    async def test_late_is_more_than_sixty_seconds(self) -> None:
        on_time = await self.add('a1')
        await self.run_pass(minutes=2)
        late = await self.add('a2', thread_id='t2')
        await self.run_pass(minutes=2, milliseconds=1)
        self.assertEqual(self.runs(on_time['id'])[0][2:4], ('fired', 0))
        self.assertEqual(self.runs(late['id'])[0][2:4], ('fired', 1))
        messages = [call.params['message'] for call in self.host.calls_of(CallName.THREADS_SEND)]
        self.assertEqual(messages[0], 'check the build')
        self.assertTrue(messages[-1].startswith('This run is late: it was due at '))

    async def test_interval_that_missed_five_times_runs_once(self) -> None:
        info = await self.add()
        await self.run_pass(minutes=5, seconds=30)
        self.assertEqual(len(self.runs(info['id'])), 1)
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_SEND)), 1)
        self.assertEqual(self.next_run(info['id']), '2026-10-23T07:06:30.000+00:00')
        await self.scheduler.tick.run_once()
        self.assertEqual(len(self.runs(info['id'])), 1)

    async def test_fixed_time_ten_minutes_late_still_runs(self) -> None:
        info = await self.add(trigger={'type': 'fixed_time', 'timeOfDay': '09:30'})
        self.assertEqual(info['next_run'], '2026-10-23T07:30:00.000+00:00')
        await self.run_pass(minutes=40)
        self.assertEqual(self.runs(info['id'])[0][2:4], ('fired', 1))
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_SEND)), 1)
        self.assertEqual(self.next_run(info['id']), '2026-10-24T07:30:00.000+00:00')

    async def test_fixed_time_more_than_ten_minutes_late_is_skipped(self) -> None:
        info = await self.add(trigger={'type': 'fixed_time', 'timeOfDay': '09:30'})
        self.host.calls.clear()
        await self.run_pass(minutes=40, milliseconds=1)
        self.assertEqual(self.runs(info['id']), [(
            '2026-10-23T07:30:00.000+00:00', '2026-10-23T07:40:00.001+00:00', 'skipped_late', 1,
            None, None,
        )])
        self.assertEqual(self.host.calls, [])
        self.assertEqual(self.host.events, [])
        self.assertEqual(self.next_run(info['id']), '2026-10-24T07:30:00.000+00:00')


class OverlapTest(SchedulerCase):
    async def fire_once(self, **fields: object) -> dict:
        info = await self.add(**fields)
        await self.run_pass(minutes=1)
        self.assertEqual(self.runs(info['id'])[-1][2], 'fired')
        return info

    async def test_busy_thread_skips_post_and_launch(self) -> None:
        for state in ('queued', 'starting', 'running'):
            with self.subTest(state=state):
                post = await self.fire_once(id=f'p-{state}')
                launch = await self.fire_once(id=f'l-{state}', action='launch', thread_id=...,
                                              project_id='p1')
                self.host.threads['t1']['state'] = state
                launched = self.runs(launch['id'])[-1][4]
                self.host.threads[launched]['state'] = state
                calls = len(self.host.calls)
                await self.run_pass(minutes=1)
                self.assertEqual(self.runs(post['id'])[-1][2:], ('skipped_busy', 0, None, None))
                self.assertEqual(self.runs(launch['id'])[-1][2:],
                                 ('skipped_busy', 0, None, None))
                made = [call.call for call in self.host.calls[calls:]]
                self.assertNotIn(CallName.THREADS_SEND, made)
                self.assertNotIn(CallName.THREADS_LAUNCH, made)
                self.assertIn(CallName.THREADS_GET, made)
                self.host.threads['t1']['state'] = 'idle'
                for schedule in (post, launch):
                    await self.call('schedules.pause', {
                        'id': f'pause-{schedule["id"]}', 'schedule_id': schedule['id'],
                    })

    async def test_idle_or_unknown_thread_fires(self) -> None:
        post = await self.fire_once()
        launch = await self.fire_once(id='a2', action='launch', thread_id=..., project_id='p1')
        del self.host.threads[self.runs(launch['id'])[-1][4]]
        await self.run_pass(minutes=1)
        self.assertEqual(self.runs(post['id'])[-1][2], 'fired')
        self.assertEqual(self.runs(launch['id'])[-1][2], 'fired')
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_SEND)), 3)
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_LAUNCH)), 2)

    async def test_skipped_run_is_not_the_last_fired_one(self) -> None:
        post = await self.fire_once()
        self.host.threads['t1']['state'] = 'running'
        await self.run_pass(minutes=1)
        self.host.threads['t1']['state'] = 'idle'
        await self.run_pass(minutes=1)
        self.assertEqual([run[2] for run in self.runs(post['id'])],
                         ['fired', 'skipped_busy', 'fired'])


class FailureTest(SchedulerCase):
    async def test_schedule_error_fails_the_run_and_the_pass_goes_on(self) -> None:
        broken = await self.add('a1', action='launch', thread_id=..., project_id='p1')
        self.clock.advance(seconds=1)
        healthy = await self.add('a2')
        self.host.fail[CallName.THREADS_LAUNCH] = ScheduleError('conflict', 'x' * 600)
        await self.run_pass(minutes=1)
        now = '2026-10-23T07:01:01.000+00:00'
        self.assertEqual(self.runs(broken['id']), [(DUE, now, 'failed', 0, None, 'x' * 500)])
        self.assertEqual(self.runs(healthy['id']), [(now, now, 'fired', 0, 't1', None)])
        self.assertEqual([call.call for call in self.host.calls[-2:]],
                         [CallName.THREADS_LAUNCH, CallName.THREADS_SEND])
        self.assertEqual([event.data['schedule_id'] for event in self.host.events],
                         [healthy['id']])
        self.assertEqual(self.next_run(broken['id']), '2026-10-23T07:02:01.000+00:00')

    async def test_other_exception_fails_with_its_type_name(self) -> None:
        info = await self.add()
        self.host.fail[CallName.THREADS_SEND] = ConnectionResetError('secret detail')
        await self.run_pass(minutes=1)
        self.assertEqual(self.runs(info['id']),
                         [(DUE, DUE, 'failed', 0, None, 'ConnectionResetError')])
        self.assertEqual(self.host.events, [])
        del self.host.fail[CallName.THREADS_SEND]
        await self.run_pass(minutes=1)
        self.assertEqual(self.runs(info['id'])[-1][2], 'fired')

    async def test_failing_busy_check_fails_the_run(self) -> None:
        info = await self.add()
        await self.run_pass(minutes=1)
        self.host.fail[CallName.THREADS_GET] = ScheduleError('failed', 'store is locked')
        await self.run_pass(minutes=1)
        self.assertEqual(self.runs(info['id'])[-1][2:], ('failed', 0, None, 'store is locked'))
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_SEND)), 1)

    async def test_cut_off_run_fires_again_with_the_same_command_id(self) -> None:
        info = await self.add()
        self.host.cut_after = CallName.THREADS_SEND
        await self.run_pass(minutes=1)
        self.assertEqual(self.runs(info['id']), [])
        self.assertEqual(self.host.events, [])
        self.assertEqual(self.next_run(info['id']), DUE)
        await self.run_pass(seconds=5)
        sends = self.host.calls_of(CallName.THREADS_SEND)
        self.assertEqual([send.params['id'] for send in sends],
                         [f'schedule:{info["id"]}:{DUE}'] * 2)
        self.assertEqual(self.runs(info['id'])[0][:3],
                         (DUE, '2026-10-23T07:01:05.000+00:00', 'fired'))
        self.assertEqual(len(self.host.events), 1)

    async def test_cut_off_run_fires_again_after_a_restart(self) -> None:
        info = await self.add()
        self.host.cut_after = CallName.THREADS_SEND
        await self.run_pass(minutes=1)
        await self.scheduler.close()
        self.scheduler = self.new_scheduler()
        await self.scheduler.tick.run_once()
        sends = self.host.calls_of(CallName.THREADS_SEND)
        self.assertEqual([send.params['id'] for send in sends],
                         [f'schedule:{info["id"]}:{DUE}'] * 2)
        self.assertEqual(len(self.runs(info['id'])), 1)


class OpenAndCloseTest(SchedulerCase):
    async def test_open_keeps_rows_prunes_old_runs_and_loads_the_heap(self) -> None:
        info = await self.add()
        paused = await self.add('a2', thread_id='t2')
        await self.call('schedules.pause', {'id': 'c1', 'schedule_id': paused['id']})
        with self.host.transaction() as tx:
            for at in ('2026-09-23T07:10:00.000+00:00', '2026-09-23T07:09:59.999+00:00'):
                tx.execute(
                    "INSERT INTO schedule_runs (schedule_id, due, at, outcome, late) "
                    "VALUES (?, ?, ?, 'fired', 0)", (info['id'], at, at),
                )
        self.clock.advance(minutes=10)
        self.scheduler = self.new_scheduler()
        self.assertEqual([row[1] for row in self.runs(info['id'])],
                         ['2026-09-23T07:10:00.000+00:00'])
        self.assertEqual(self.schedule_rows(), 2)
        self.assertEqual(self.scheduler.tick.delay(), 0.0)
        await self.scheduler.tick.run_once()
        self.assertEqual(self.runs(info['id'])[-1][2:4], ('fired', 1))
        self.assertEqual(self.runs(paused['id']), [])

    async def test_open_twice_on_the_same_store_is_harmless(self) -> None:
        await self.add()
        self.new_scheduler()
        self.assertEqual(self.schedule_rows(), 1)

    async def test_start_runs_a_pass_and_close_ends_the_loop(self) -> None:
        info = await self.add()
        self.clock.advance(minutes=1)
        await self.scheduler.start()
        task = self.scheduler._task
        async with asyncio.timeout(1.0):
            while not self.runs(info['id']):
                await asyncio.sleep(0.005)
        await self.scheduler.close()
        self.assertTrue(task.done())
        await self.scheduler.close()
        self.assertIsNone(self.scheduler._task)

    async def test_close_before_start_does_nothing(self) -> None:
        await self.scheduler.close()
        self.assertIsNone(self.scheduler._task)


if __name__ == '__main__':
    unittest.main()
