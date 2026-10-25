import unittest
from datetime import timedelta

from loom_contracts.calls import CallName
from loom_contracts.events import EventType

from loom_scheduler import Maker, ScheduleError
from loom_scheduler.events import NOW

from .fakes import AGENT, PERSON, Event, SchedulerCase


class EventCase(SchedulerCase):
    async def on(self, topic: str, command_id: str = 'e1', caller: object = PERSON,
                 trigger: dict | None = None, **fields: object) -> dict:
        fields.setdefault('action', 'notify')
        return await self.add(command_id, caller,
                              trigger={'type': 'event', 'topic': topic, **(trigger or {})},
                              **fields)

    async def read_events(self) -> None:
        for _ in range(20):
            await self.scheduler.tick.run_once()
            if self.scheduler.events.next_wake() != NOW:
                return
        raise AssertionError('the events source is still due after 20 passes')

    def fired(self, schedule_id: str | None = None) -> list[Event]:
        return [event for event in self.host.events if event.type == EventType.SCHEDULE_FIRED
                and schedule_id in (None, event.data['schedule_id'])]

    def causes(self, schedule_id: str) -> list[int]:
        return [event.data['cause'] for event in self.fired(schedule_id)]

    def skips(self) -> list[dict]:
        return [event.data for event in self.host.events
                if event.type == EventType.TRIGGER_SKIPPED]

    def event_runs(self, schedule_id: str) -> list[tuple]:
        return self.host.rows(
            'SELECT outcome, event_id, due, thread_id, reason FROM schedule_runs '
            'WHERE schedule_id = ? ORDER BY id', schedule_id)

    def cursor(self, schedule_id: str) -> int:
        return self.host.rows('SELECT event_after FROM schedules WHERE id = ?',
                              schedule_id)[0][0]

    def at(self, event_id: int) -> str:
        return self.host.events[event_id - 1].at


class WakeTest(EventCase):
    async def test_new_events_make_the_source_due_only_with_an_event_schedule(self) -> None:
        self.host.signal('demo.done')
        self.assertIsNone(self.scheduler.events.next_wake())
        await self.add()
        self.host.signal('demo.done')
        self.assertIsNone(self.scheduler.events.next_wake())
        await self.on('demo.done')
        self.host.signal('demo.done')
        self.assertEqual(self.scheduler.events.next_wake(), NOW)
        await self.read_events()
        self.assertIsNone(self.scheduler.events.next_wake())
        self.assertEqual(len(self.fired()), 1)

    async def test_close_stops_listening(self) -> None:
        await self.on('demo.done')
        await self.scheduler.close()
        self.assertEqual(self.host.listeners, [])


class MatchTest(EventCase):
    async def test_a_signal_with_the_topic_fires_once(self) -> None:
        info = await self.on('demo.done', text='Done: {{payload.to}}')
        event_id = self.host.signal('demo.done', {'to': 'done'})
        await self.read_events()
        await self.read_events()
        self.assertEqual(self.fired(), [Event(EventType.SCHEDULE_FIRED, 't1', {
            'schedule_id': info['id'], 'action': 'notify', 'text': 'Done: done', 'late': False,
            'cause': event_id, 'hops': 1,
        })])
        self.assertEqual(self.event_runs(info['id']),
                         [('fired', event_id, self.at(event_id), None, None)])
        self.assertEqual(self.cursor(info['id']), self.host.last_event_id())

    async def test_events_that_do_not_match_do_not_fire(self) -> None:
        self.host.signal('demo.done', {'to': 'done', 'n': 1}, project_id='p1')
        info = await self.on('demo.done', trigger={'project_id': 'p1',
                                                   'match': {'to': 'done', 'n': 1}})
        for topic, payload, data in [
            ('demo.other', {'to': 'done', 'n': 1}, {'project_id': 'p1'}),
            ('demo.done', {'to': 'done', 'n': 1}, {'project_id': 'p2'}),
            ('demo.done', {'to': 'done', 'n': 1}, {}),
            ('demo.done', {'n': 1}, {'project_id': 'p1'}),
            ('demo.done', {'to': 'open', 'n': 1}, {'project_id': 'p1'}),
            ('demo.done', {'to': 'done', 'n': True}, {'project_id': 'p1'}),
            ('demo.done', {'to': 'done', 'n': 1.0}, {'project_id': 'p1'}),
            ('demo.done', {'to': 'done', 'n': '1'}, {'project_id': 'p1'}),
        ]:
            self.host.signal(topic, payload, **data)
        await self.read_events()
        self.assertEqual(self.fired(), [])
        self.assertEqual(self.event_runs(info['id']), [])
        self.assertEqual(self.cursor(info['id']), self.host.last_event_id())
        last = self.host.signal('demo.done', {'to': 'done', 'n': 1, 'more': [1]},
                                project_id='p1')
        await self.read_events()
        self.assertEqual(self.causes(info['id']), [last])

    async def test_match_compares_json_values_and_null_needs_the_field(self) -> None:
        nested = await self.on('a', 'e1', trigger={'match': {'o': {'x': 1, 'y': [1, 2]}}})
        null = await self.on('b', 'e2', trigger={'match': {'gone': None}})
        self.host.signal('a', {'o': {'y': [1, 2], 'x': 1}})
        self.host.signal('a', {'o': {'y': [2, 1], 'x': 1}})
        self.host.signal('b', {})
        found = self.host.signal('b', {'gone': None})
        await self.read_events()
        self.assertEqual(self.causes(nested['id']), [1])
        self.assertEqual(self.causes(null['id']), [found])

    async def test_a_signal_without_a_project_takes_its_threads(self) -> None:
        info = await self.on('demo.done', trigger={'project_id': 'p2'})
        self.host.signal('demo.done', by='t1')
        matched = self.host.signal('demo.done', by='t2')
        self.host.signal('demo.done', by='gone')
        await self.read_events()
        self.assertEqual(self.causes(info['id']), [matched])

    async def test_each_loom_event_fires_on_its_threads_project(self) -> None:
        cases = [
            (EventType.THREAD_SETTLED, {'result': 'ok'}),
            (EventType.THREAD_FAILED, {'reason': 'crashed'}),
            (EventType.REQUEST_OPENED, {'request_id': 'r1', 'tool': 'Bash'}),
            (EventType.SCHEDULE_FIRED, {'schedule_id': 's9', 'action': 'notify',
                                        'text': 'x', 'late': False}),
        ]
        for number, (kind, data) in enumerate(cases):
            with self.subTest(kind=kind):
                field, value = next(iter(data.items()))
                info = await self.on(kind.value, f'e{number}',
                                     trigger={'project_id': 'p2', 'match': {field: value}})
                self.host.emit(kind, 't1', **data)
                self.host.emit(kind, 't2', **{**data, field: 'other'})
                matched = self.host.emit(kind, 't2', **data)
                self.host.emit(EventType.SIGNAL, None, topic=kind.value, payload=data,
                               project_id='p2', hops=0)
                await self.read_events()
                self.assertEqual(self.causes(info['id']), [matched])
                await self.call('schedules.pause', {'id': f'p{number}',
                                                    'schedule_id': info['id']})

    async def test_a_threadless_schedule_fired_matches_on_its_schedules_project(self) -> None:
        in_p2 = await self.add('c1', action='notify', thread_id=..., project_id='p2')
        in_p1 = await self.add('c2', action='notify', thread_id=..., project_id='p1')
        info = await self.on('schedule.fired', trigger={'project_id': 'p2'})
        data = {'action': 'notify', 'text': 'x', 'late': False}
        matched = self.host.emit(EventType.SCHEDULE_FIRED, None, schedule_id=in_p2['id'], **data)
        self.host.emit(EventType.SCHEDULE_FIRED, None, schedule_id=in_p1['id'], **data)
        self.host.emit(EventType.SCHEDULE_FIRED, None, schedule_id='gone', **data)
        await self.read_events()
        self.assertEqual(self.causes(info['id']), [matched])


class FireTest(EventCase):
    async def test_launch_calls_with_the_cause_origin_and_filled_text(self) -> None:
        info = await self.on('demo.done', action='launch', thread_id=..., project_id='p1',
                             text='\nRun {{payload.to}} for {{event.id}}\nmore',
                             concurrency_key='pm:p1', settle='on_result')
        event_id = self.host.signal('demo.done', {'to': 'review'}, hops=2)
        await self.read_events()
        [launch] = self.host.calls_of(CallName.THREADS_LAUNCH)
        text = f'\nRun review for {event_id}\nmore'
        self.assertEqual(launch.params, {
            'id': f'schedule:{info["id"]}:event:{event_id}', 'title': f'Run review for {event_id}',
            'message': text, 'project_id': 'p1', 'origin': f'schedule:{info["id"]}',
            'cause': event_id, 'concurrency_key': 'pm:p1', 'on_busy': 'queue',
            'settle': 'on_result',
        })
        self.assertEqual(launch.maker, Maker(None, True, info['id']))
        self.assertEqual(self.fired(), [Event(EventType.SCHEDULE_FIRED, 'launched-1', {
            'schedule_id': info['id'], 'action': 'launch', 'text': text, 'late': False,
            'cause': event_id, 'hops': 3,
        })])
        self.assertEqual(self.host.threads['launched-1']['hops'], 3)
        self.assertEqual(self.event_runs(info['id']),
                         [('fired', event_id, self.at(event_id), 'launched-1', None)])

    async def test_launch_takes_the_schedules_title(self) -> None:
        await self.on('demo.done', action='launch', thread_id=..., project_id='p1',
                      title='Review', text='{{payload.to}}')
        self.host.signal('demo.done', {'to': 'x' * 100})
        await self.read_events()
        self.assertEqual(self.host.calls_of(CallName.THREADS_LAUNCH)[0].params['title'],
                         'Review')

    async def test_post_sends_with_the_cause_in_queue_mode(self) -> None:
        info = await self.on('demo.done', 'e1', AGENT, action='post', thread_id=...,
                             text='Look at {{payload.pr}}')
        event_id = self.host.signal('demo.done', {'pr': 14})
        await self.read_events()
        [send] = self.host.calls_of(CallName.THREADS_SEND)
        self.assertEqual(send.params, {
            'id': f'schedule:{info["id"]}:event:{event_id}', 'thread_id': 't1',
            'message': 'Look at 14', 'mode': 'queue', 'cause': event_id,
        })
        self.assertEqual(send.maker, Maker('t1', True, info['id']))
        self.assertEqual(self.fired(), [Event(EventType.SCHEDULE_FIRED, 't1', {
            'schedule_id': info['id'], 'action': 'post', 'text': 'Look at 14', 'late': False,
            'by': 't1', 'cause': event_id, 'hops': 1,
        })])

    async def test_notify_makes_no_call(self) -> None:
        info = await self.on('thread.settled', thread_id=...)
        self.host.threads['t2']['hops'] = 4
        event_id = self.host.emit(EventType.THREAD_SETTLED, 't2', result='ok')
        self.host.calls.clear()
        await self.read_events()
        self.assertEqual([call.call for call in self.host.calls],
                         [CallName.THREADS_EVENTS, CallName.THREADS_GET, CallName.THREADS_EVENTS])
        self.assertEqual(self.fired(), [Event(EventType.SCHEDULE_FIRED, None, {
            'schedule_id': info['id'], 'action': 'notify', 'text': 'check the build',
            'late': False, 'cause': event_id, 'hops': 5,
        })])

    async def test_event_runs_are_never_late_and_never_skipped_busy(self) -> None:
        info = await self.on('demo.done', action='post')
        self.host.threads['t1']['state'] = 'running'
        first = self.host.signal('demo.done')
        self.clock.advance(hours=2)
        second = self.host.signal('demo.done')
        await self.read_events()
        sends = self.host.calls_of(CallName.THREADS_SEND)
        self.assertEqual([send.params['message'] for send in sends],
                         ['check the build', 'check the build'])
        self.assertEqual([run[:2] for run in self.event_runs(info['id'])],
                         [('fired', first), ('fired', second)])
        self.assertEqual(self.host.rows('SELECT DISTINCT late FROM schedule_runs'), [(0,)])
        self.assertEqual({event.data['late'] for event in self.fired()}, {False})

    async def test_schedules_handle_each_event_oldest_first_in_created_order(self) -> None:
        newer = await self.on('demo.done', 'e1')
        self.clock.advance(seconds=-10)
        older = await self.on('demo.done', 'e2')
        self.clock.advance(seconds=10)
        first, second = self.host.signal('demo.done'), self.host.signal('demo.done')
        await self.read_events()
        self.assertEqual([(event.data['schedule_id'], event.data['cause'])
                          for event in self.fired()],
                         [(older['id'], first), (newer['id'], first),
                          (older['id'], second), (newer['id'], second)])


class SkipTest(EventCase):
    async def test_an_event_at_hops_max_is_skipped(self) -> None:
        default = await self.on('demo.done', 'e1')
        self.clock.advance(seconds=1)
        lowered = await self.on('demo.done', 'e2', trigger={'hops_max': 3})
        at_default = self.host.signal('demo.done', hops=8)
        below_default = self.host.signal('demo.done', hops=7)
        at_lowered = self.host.signal('demo.done', hops=3)
        below_lowered = self.host.signal('demo.done', hops=2)
        await self.read_events()
        self.assertEqual(self.causes(default['id']), [below_default, at_lowered, below_lowered])
        self.assertEqual(self.causes(lowered['id']), [below_lowered])
        self.assertEqual(self.skips(), [
            {'schedule_id': default['id'], 'event_id': at_default, 'reason': 'hops'},
            {'schedule_id': lowered['id'], 'event_id': at_default, 'reason': 'hops'},
            {'schedule_id': lowered['id'], 'event_id': below_default, 'reason': 'hops'},
            {'schedule_id': lowered['id'], 'event_id': at_lowered, 'reason': 'hops'},
        ])
        self.assertEqual(self.event_runs(default['id'])[0],
                         ('skipped_hops', at_default, self.at(at_default), None, None))
        self.assertEqual(self.cursor(lowered['id']), self.host.last_event_id())
        skipped = [event for event in self.host.events
                   if event.type == EventType.TRIGGER_SKIPPED]
        self.assertEqual({event.thread_id for event in skipped}, {None})

    async def test_a_threads_hops_count_when_the_event_has_none(self) -> None:
        info = await self.on('thread.settled')
        self.host.threads['t2']['hops'] = 8
        skipped = self.host.emit(EventType.THREAD_SETTLED, 't2', result='x')
        await self.read_events()
        self.host.threads['t2']['hops'] = 7
        fired = self.host.emit(EventType.THREAD_SETTLED, 't2', result='x', hops=True)
        await self.read_events()
        self.assertEqual(self.causes(info['id']), [fired])
        self.assertEqual(self.skips(), [
            {'schedule_id': info['id'], 'event_id': skipped, 'reason': 'hops'}])
        self.assertEqual(self.fired()[0].data['hops'], 8)

    async def test_events_from_its_own_threads_and_runs_are_skipped_as_own(self) -> None:
        launcher = await self.on('demo.done', 'e1', action='launch', thread_id=...,
                                 project_id='p1')
        watcher = await self.on('demo.done', 'e2')
        settles = await self.on('thread.settled', 'e3', action='launch', thread_id=...,
                                project_id='p1', trigger={'match': {'result': 'go'}})
        settle_watcher = await self.on('thread.settled', 'e4', trigger={'match': {'result': 'go'}})
        pinger = await self.on('schedule.fired', 'e5', trigger={'match': {'text': 'ping'}})
        ping_watcher = await self.on('schedule.fired', 'e6', trigger={'match': {'text': 'ping'}})
        start = self.host.signal('demo.done')
        await self.read_events()
        launched = self.host.calls_of(CallName.THREADS_LAUNCH)
        self.assertEqual(len(launched), 1)
        own_signal = self.host.signal('demo.done', by='launched-1')
        go = self.host.emit(EventType.THREAD_SETTLED, 't1', result='go')
        await self.read_events()
        own_settled = self.host.emit(EventType.THREAD_SETTLED, 'launched-2', result='go')
        own_fired = self.host.emit(EventType.SCHEDULE_FIRED, None, schedule_id=pinger['id'],
                                   action='notify', text='ping', late=False)
        await self.read_events()
        self.assertEqual(self.causes(watcher['id']), [start, own_signal])
        self.assertEqual(self.causes(settle_watcher['id']), [go, own_settled])
        self.assertEqual(self.causes(ping_watcher['id']), [own_fired])
        self.assertEqual(self.skips(), [
            {'schedule_id': launcher['id'], 'event_id': own_signal, 'reason': 'own'},
            {'schedule_id': settles['id'], 'event_id': own_settled, 'reason': 'own'},
            {'schedule_id': pinger['id'], 'event_id': own_fired, 'reason': 'own'},
        ])
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_LAUNCH)), 2)
        self.assertEqual(self.event_runs(launcher['id'])[1][:2], ('skipped_own', own_signal))

    async def test_rate_max_skips_and_notifies_once_an_hour(self) -> None:
        info = await self.on('demo.done', trigger={'rate_max': 2}, text='{{event.id}}')
        first = self.host.signal('demo.done')
        await self.read_events()
        self.clock.advance(minutes=1)
        second = self.host.signal('demo.done')
        await self.read_events()
        self.clock.advance(minutes=58)
        third = self.host.signal('demo.done', hops=4)
        await self.read_events()
        self.clock.advance(seconds=59)
        fourth = self.host.signal('demo.done')
        await self.read_events()
        self.assertEqual(self.causes(info['id']), [first, second, third])
        self.assertEqual(self.fired()[-1], Event(EventType.SCHEDULE_FIRED, 't1', {
            'schedule_id': info['id'], 'action': 'notify',
            'text': f'Schedule {info["id"]} fired 2 times in the last hour, so it skipped event '
                    f'{third}.',
            'late': False, 'cause': third, 'hops': 5,
        }))
        self.assertEqual(self.skips(), [
            {'schedule_id': info['id'], 'event_id': third, 'reason': 'rate'},
            {'schedule_id': info['id'], 'event_id': fourth, 'reason': 'rate'},
        ])
        self.assertEqual([run[:2] for run in self.event_runs(info['id'])], [
            ('fired', first), ('fired', second), ('skipped_rate', third),
            ('skipped_rate', fourth)])
        self.clock.advance(seconds=1)
        fifth = self.host.signal('demo.done')
        await self.read_events()
        self.assertEqual(self.event_runs(info['id'])[-1][:2], ('fired', fifth))
        self.assertEqual(self.fired()[-1].data['text'], str(fifth))

    async def test_the_default_rate_max_is_thirty(self) -> None:
        info = await self.on('demo.done')
        for _ in range(31):
            self.host.signal('demo.done')
        await self.read_events()
        outcomes = [run[0] for run in self.event_runs(info['id'])]
        self.assertEqual(outcomes, ['fired'] * 30 + ['skipped_rate'])
        self.assertIn('fired 30 times', self.fired()[-1].data['text'])

    async def test_a_skip_in_order_own_then_hops_then_rate(self) -> None:
        info = await self.on('demo.done', action='launch', thread_id=..., project_id='p1',
                             trigger={'rate_max': 1})
        self.host.signal('demo.done')
        await self.read_events()
        self.host.threads['launched-1']['hops'] = 9
        own = self.host.signal('demo.done', by='launched-1', hops=9)
        hops = self.host.signal('demo.done', hops=9)
        rate = self.host.signal('demo.done')
        await self.read_events()
        self.assertEqual([(skip['event_id'], skip['reason']) for skip in self.skips()],
                         [(own, 'own'), (hops, 'hops'), (rate, 'rate')])
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_LAUNCH)), 1)
        self.assertEqual(self.cursor(info['id']), self.host.last_event_id())


class CursorTest(EventCase):
    async def test_a_restart_fires_each_event_since_the_cursor_once_oldest_first(self) -> None:
        info = await self.on('demo.done', trigger={'match': {'go': True}})
        before = self.host.signal('demo.done', {'go': True})
        await self.read_events()
        await self.scheduler.close()
        self.clock.advance(hours=3)
        second = self.host.signal('demo.done', {'go': True})
        self.host.signal('demo.done', {'go': False})
        fourth = self.host.signal('demo.done', {'go': True})
        self.scheduler = self.new_scheduler()
        await self.read_events()
        await self.read_events()
        self.assertEqual(self.causes(info['id']), [before, second, fourth])
        self.assertEqual(self.cursor(info['id']), self.host.last_event_id())
        self.scheduler = self.new_scheduler()
        await self.read_events()
        self.assertEqual(self.causes(info['id']), [before, second, fourth])

    async def test_a_cut_off_run_sends_the_same_command_id_after_a_restart(self) -> None:
        info = await self.on('demo.done', action='post')
        event_id = self.host.signal('demo.done')
        self.host.cut_after = CallName.THREADS_SEND
        await self.read_events()
        self.assertEqual(self.event_runs(info['id']), [])
        self.assertEqual(self.fired(), [])
        self.assertEqual(self.cursor(info['id']), 0)
        await self.scheduler.close()
        self.scheduler = self.new_scheduler()
        await self.read_events()
        sends = self.host.calls_of(CallName.THREADS_SEND)
        self.assertEqual([send.params['id'] for send in sends],
                         [f'schedule:{info["id"]}:event:{event_id}'] * 2)
        self.assertEqual(len(self.event_runs(info['id'])), 1)
        self.assertEqual(self.causes(info['id']), [event_id])

    async def test_an_unexpected_error_keeps_the_cursor_and_retries_after_five_seconds(
            self) -> None:
        broken = await self.on('demo.done', 'e1', action='post')
        healthy = await self.on('demo.done', 'e2')
        passed = self.host.signal('demo.other')
        event_id = self.host.signal('demo.done')
        later = self.host.signal('demo.done')
        self.host.fail[CallName.THREADS_SEND] = ConnectionResetError('secret detail')
        await self.read_events()
        self.assertEqual(self.event_runs(broken['id']), [])
        self.assertEqual(self.cursor(broken['id']), passed)
        self.assertEqual(self.causes(healthy['id']), [event_id, later])
        self.assertEqual(self.scheduler.events.next_wake(),
                         self.clock() + timedelta(seconds=5))
        self.host.signal('demo.other')
        await self.read_events()
        await self.run_pass(seconds=4)
        self.assertEqual(len(self.host.calls_of(CallName.THREADS_SEND)), 1)
        del self.host.fail[CallName.THREADS_SEND]
        await self.run_pass(seconds=1)
        self.assertEqual([run[:2] for run in self.event_runs(broken['id'])],
                         [('fired', event_id), ('fired', later)])
        self.assertEqual([send.params['id'] for send in
                          self.host.calls_of(CallName.THREADS_SEND)],
                         [f'schedule:{broken["id"]}:event:{event_id}'] * 2
                         + [f'schedule:{broken["id"]}:event:{later}'])
        self.assertEqual(self.causes(healthy['id']), [event_id, later])

    async def test_a_lookup_that_fails_is_retried(self) -> None:
        info = await self.on('thread.settled')
        event_id = self.host.emit(EventType.THREAD_SETTLED, 't1', result='x')
        self.host.fail[CallName.THREADS_GET] = ScheduleError('failed', 'store is locked')
        await self.read_events()
        self.assertEqual((self.fired(), self.cursor(info['id'])), ([], 0))
        del self.host.fail[CallName.THREADS_GET]
        await self.run_pass(seconds=5)
        self.assertEqual(self.causes(info['id']), [event_id])

    async def test_a_schedule_error_records_failed_and_moves_on(self) -> None:
        info = await self.on('demo.done', action='launch', thread_id=..., project_id='p1')
        failing = self.host.signal('demo.done')
        self.host.fail[CallName.THREADS_LAUNCH] = ScheduleError('conflict', 'x' * 600)
        await self.read_events()
        del self.host.fail[CallName.THREADS_LAUNCH]
        fine = self.host.signal('demo.done')
        await self.read_events()
        self.assertEqual(self.event_runs(info['id']), [
            ('failed', failing, self.at(failing), None, 'x' * 500),
            ('fired', fine, self.at(fine), 'launched-1', None),
        ])
        self.assertEqual(self.causes(info['id']), [fine])
        self.assertIsNone(self.scheduler.events.next_wake())

    async def test_events_are_read_in_pages_of_a_thousand(self) -> None:
        info = await self.on('demo.done')
        for _ in range(1_001):
            self.host.signal('demo.other')
        last = self.host.signal('demo.done')
        self.host.calls.clear()
        await self.read_events()
        self.assertEqual([call.params for call in self.host.calls_of(CallName.THREADS_EVENTS)],
                         [{'after': 0}, {'after': 1_000}, {'after': last}])
        self.assertEqual(self.causes(info['id']), [last])

    async def test_reading_starts_at_the_smallest_cursor(self) -> None:
        old = await self.on('demo.done', 'e1')
        first = self.host.signal('demo.done')
        new = await self.on('demo.done', 'e2')
        second = self.host.signal('demo.done')
        self.host.calls.clear()
        await self.read_events()
        self.assertEqual(self.host.calls_of(CallName.THREADS_EVENTS)[0].params, {'after': 0})
        self.assertEqual(self.causes(old['id']), [first, second])
        self.assertEqual(self.causes(new['id']), [second])

    async def test_a_schedule_paused_or_removed_in_a_pass_gets_no_more_writes(self) -> None:
        for name in ('schedules.pause', 'schedules.remove'):
            with self.subTest(call=name):
                info = await self.on('demo.done', f'e-{name}', action='post')
                start = self.cursor(info['id'])
                original = self.host.call

                async def stopping(call: CallName, params: dict, maker: Maker) -> object:
                    if call == CallName.THREADS_GET:
                        self.host.call = original
                        await self.call(name, {'id': f'x-{name}', 'schedule_id': info['id']})
                    return await original(call, params, maker)

                self.host.call = stopping
                self.host.signal('demo.other')
                self.host.signal('demo.done', by='t2')
                self.host.calls.clear()
                await self.read_events()
                self.assertEqual(self.host.calls_of(CallName.THREADS_SEND), [])
                self.assertEqual(self.event_runs(info['id']), [])
                self.assertEqual(self.fired(info['id']), [])
                if name == 'schedules.pause':
                    self.assertEqual(self.cursor(info['id']), start)

    async def test_a_paused_schedule_reads_nothing(self) -> None:
        info = await self.on('demo.done')
        await self.call('schedules.pause', {'id': 'c1', 'schedule_id': info['id']})
        self.host.signal('demo.done')
        self.host.calls.clear()
        await self.read_events()
        self.assertEqual(self.host.calls, [])
        self.assertEqual(self.cursor(info['id']), 0)


class ChainTest(EventCase):
    async def test_two_schedules_that_trigger_each_other_stop_at_hops_max(self) -> None:
        ping = await self.on('pong', 'e1', action='launch', thread_id=..., project_id='p1')
        pong = await self.on('ping', 'e2', action='launch', thread_id=..., project_id='p1')
        self.host.signal('ping')
        for _ in range(12):
            await self.read_events()
            launched = [thread for thread in self.host.threads.values()
                        if thread['origin'] is not None and thread.get('signalled') is None]
            for thread in launched:
                thread['signalled'] = True
                topic = 'pong' if thread['origin'] == f'schedule:{pong["id"]}' else 'ping'
                self.host.signal(topic, by=thread['id'], cause=thread['cause'],
                                 hops=thread['hops'])
        hops = sorted(thread['hops'] for thread in self.host.threads.values()
                      if thread['origin'] is not None)
        self.assertEqual(hops, [1, 2, 3, 4, 5, 6, 7, 8])
        self.assertEqual(self.skips()[-1]['reason'], 'hops')
        self.assertEqual(len(self.fired(ping['id'])) + len(self.fired(pong['id'])), 8)


if __name__ == '__main__':
    unittest.main()
