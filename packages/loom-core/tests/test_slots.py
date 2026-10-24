import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from loom_contracts.events import EventType
from loom_contracts.runners import DispatchMode

from loom_core import slots
from loom_core.clock import iso
from loom_core.orchestrator import Orchestrator
from loom_core.projects import Projects
from loom_core.service import Caller
from loom_core.slots import StartSource, available_memory, default_agents_max
from loom_core.store import DB_NAME, Store

from .fakes import FakeHost, FakeRunner, FakeSession, ManualClock, wait_for
from .test_delegate import DelegateTest
from .test_orchestrator import START
from .test_service import ServiceTest
from .test_store import OLD_MESSAGES

QUEUE = DispatchMode.QUEUE
GIB = 2**30
MIB = 2**20
CONTINUATION = 'Your last turn was cut off; continue where you were.'
SYSCONF = {'SC_AVPHYS_PAGES': 1000, 'SC_PAGE_SIZE': 4096}


class DefaultAgentsMaxTest(unittest.TestCase):
    def test_the_default_is_memory_less_2_gib_over_350_mib_at_least_1(self) -> None:
        cases = {
            2 * GIB + 350 * MIB: 1,
            2 * GIB + 700 * MIB - 1: 1,
            2 * GIB + 700 * MIB: 2,
            24 * 10**9: 59,
            GIB: 1,
            0: 1,
        }
        for available, expected in cases.items():
            with self.subTest(available=available):
                self.assertEqual(default_agents_max(available), expected)


class AvailableMemoryTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)

    def write(self, name: str, text: str) -> Path:
        path = self.dir / name
        path.write_text(text)
        return path

    def test_mem_available_is_read_in_bytes(self) -> None:
        path = self.write('meminfo', 'MemTotal:       4096 kB\nMemFree:  1024 kB\n'
                          'MemAvailable:   2048 kB\nBuffers: 1 kB\n')
        with mock.patch.object(slots.os, 'sysconf', side_effect=AssertionError):
            self.assertEqual(available_memory(path), 2_097_152)
        self.assertEqual(available_memory(str(path)), 2_097_152)

    def test_no_line_a_bad_value_or_no_file_falls_back_to_sysconf(self) -> None:
        cases = {
            'no line': self.write('none', 'MemTotal: 4096 kB\nMemFree: 1024 kB\n'),
            'not a number': self.write('bad', 'MemTotal: 4096 kB\nMemAvailable: x kB\n'),
            'missing': self.dir / 'missing',
        }
        for name, path in cases.items():
            with self.subTest(case=name), mock.patch.object(
                    slots.os, 'sysconf', side_effect=SYSCONF.__getitem__) as sysconf:
                self.assertEqual(available_memory(path), 4_096_000)
                self.assertEqual(sysconf.call_count, 2)

    def test_none_when_sysconf_fails_too(self) -> None:
        for error in (OSError('no'), ValueError('unknown name')):
            with self.subTest(error=error), mock.patch.object(
                    slots.os, 'sysconf', side_effect=error):
                self.assertIsNone(available_memory(self.dir / 'missing'))

    def test_sources_rank_person_agent_schedule(self) -> None:
        self.assertEqual(sorted(StartSource),
                         [StartSource.PERSON, StartSource.AGENT, StartSource.SCHEDULE])


class SlotsTest(DelegateTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.clock = ManualClock(START)
        self.store.clock = self.clock

    async def limit(self, agents_max: int = 1000, **options: object) -> None:
        await self.orchestrator.close()
        self.orchestrator = Orchestrator(self.store, self.projects, {'claude': self.runner},
                                         FakeHost(str(self.data_dir)), agents_max=agents_max,
                                         **options)

    async def start(self, source: StartSource = StartSource.PERSON, **extra: object) -> str:
        info = await self.orchestrator.launch(self.launch_params(**extra), source=source)
        await self.orchestrator.drain()
        return info['id']

    async def sleeping_thread(self) -> str:
        thread_id, session = await self.idle()
        session.exit(0)
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        await self.orchestrator.drain()
        return thread_id

    def session(self, thread_id: str) -> FakeSession:
        return self.runner.sessions[thread_id]

    def started(self) -> list[tuple[str, bool]]:
        return [(spec.id, spec.resume) for spec in self.runner.starts]

    def start_row(self, thread_id: str) -> tuple | None:
        return self.store.connection.execute(
            'SELECT rank, queued_at FROM starts WHERE thread_id = ?', (thread_id,)).fetchone()


class AdmissionTest(SlotsTest):
    async def test_a_start_with_no_free_slot_sleeps_the_longest_idle_thread(self) -> None:
        await self.limit(2)
        first, first_session = await self.idle()
        self.clock.advance(10)
        second, second_session = await self.idle()
        self.clock.advance(10)
        third = await self.start()
        self.assertEqual(self.state(first), 'sleeping')
        self.assertTrue(first_session.closed)
        self.assertEqual(self.state(second), 'idle')
        self.assertFalse(second_session.closed)
        self.assertEqual(self.state(third), 'running')
        self.assertEqual(self.session(third).sent, [('Hello', QUEUE)])

    async def test_starts_wait_queued_and_go_person_then_agent_then_schedule(self) -> None:
        await self.limit(1)
        person, _ = await self.idle()
        busy = await self.start()
        self.assertEqual((self.state(person), self.state(busy)), ('sleeping', 'running'))
        self.clock.advance(1)
        agent = await self.start(StartSource.AGENT)
        self.clock.advance(1)
        schedule = await self.start(StartSource.SCHEDULE)
        self.clock.advance(1)
        sent = await self.send(person, 'mine')
        await self.orchestrator.drain()
        self.assertEqual(sent['id'], person)
        for thread_id in (person, agent, schedule):
            self.assertEqual(self.state(thread_id), 'queued')
        self.assertEqual(len(self.runner.starts), 2)

        await self.finish(busy, self.session(busy))
        self.assertEqual(self.state(busy), 'sleeping')
        self.assertEqual(self.started()[2:], [(person, True)])
        self.assertEqual(self.session(person).sent, [('mine', QUEUE)])
        self.assertEqual((self.state(agent), self.state(schedule)), ('queued', 'queued'))

        await self.finish(person, self.session(person))
        self.assertEqual(self.started()[3:], [(agent, False)])
        self.assertEqual(self.state(schedule), 'queued')
        await self.finish(agent, self.session(agent))
        self.assertEqual(self.started()[4:], [(schedule, False)])
        self.assertEqual([self.state(thread_id) for thread_id in (busy, person, agent)],
                         ['sleeping'] * 3)
        self.assertEqual(self.state(schedule), 'running')

    async def test_within_a_source_the_older_queue_time_goes_first(self) -> None:
        await self.limit(1)
        older = await self.sleeping_thread()
        self.clock.advance(1)
        newer = await self.sleeping_thread()
        self.clock.advance(1)
        busy = await self.start()
        await self.send(newer, 'first')
        await self.orchestrator.drain()
        self.clock.advance(1)
        await self.send(older, 'second')
        await self.orchestrator.drain()
        self.assertLess(self.start_row(newer)[1], self.start_row(older)[1])
        await self.finish(busy, self.session(busy))
        self.assertEqual(self.started()[-1], (newer, True))
        self.assertEqual(self.state(older), 'queued')
        await self.finish(newer, self.session(newer))
        self.assertEqual(self.started()[-1], (older, True))

    async def test_a_second_message_keeps_the_queue_time_and_takes_the_better_rank(self) -> None:
        await self.limit(1)
        thread_id = await self.sleeping_thread()
        busy = await self.start()
        queued_at = iso(self.clock())
        await self.orchestrator.send({'id': self.next_id(), 'thread_id': thread_id,
                                      'message': 'later'}, source=StartSource.SCHEDULE)
        await self.orchestrator.drain()
        self.assertEqual(self.start_row(thread_id), (StartSource.SCHEDULE, queued_at))
        self.clock.advance(5)
        await self.send(thread_id, 'now')
        await self.orchestrator.drain()
        self.assertEqual(self.start_row(thread_id), (StartSource.PERSON, queued_at))
        await self.orchestrator.send({'id': self.next_id(), 'thread_id': thread_id,
                                      'message': 'again'}, source=StartSource.AGENT)
        await self.orchestrator.drain()
        self.assertEqual(self.start_row(thread_id), (StartSource.PERSON, queued_at))
        await self.finish(busy, self.session(busy))
        self.assertEqual(self.state(thread_id), 'running')
        self.assertIsNone(self.start_row(thread_id))

    async def test_a_message_to_a_thread_being_put_to_sleep_is_delivered_after_it_wakes(
            self) -> None:
        await self.limit(1)
        idle, idle_session = await self.idle()
        self.runner.close_gate = asyncio.Event()
        try:
            newcomer = (await self.launch())['id']
            await wait_for(lambda: idle_session.closing)
            await self.send(idle, 'still there?')
        finally:
            self.runner.close_gate.set()
        await self.orchestrator.drain()
        self.assertEqual(self.state(newcomer), 'running')
        self.assertEqual(self.state(idle), 'queued')
        await self.finish(newcomer, self.session(newcomer))
        self.assertEqual(self.state(idle), 'running')
        self.assertEqual(self.started()[-1], (idle, True))
        self.assertEqual(self.session(idle).sent, [('still there?', QUEUE)])
        self.assertNotIn('failed', self.states(idle))
        self.assertEqual(self.state(newcomer), 'sleeping')

    async def test_a_start_that_fails_gives_its_slot_back(self) -> None:
        await self.limit(1)
        busy = await self.start()
        broken = await self.start()
        self.assertEqual(self.state(broken), 'queued')
        self.runner.fail_start = RuntimeError('no binary')
        await self.finish(busy, self.session(busy))
        info = self.orchestrator.get(broken)
        self.assertEqual((info['state'], info['reason']), ('failed', 'no binary'))
        self.assertEqual(self.state(busy), 'sleeping')
        self.runner.fail_start = None
        after = await self.start()
        self.assertEqual(self.state(after), 'running')
        self.assertEqual(self.session(after).sent, [('Hello', QUEUE)])


class IdleTest(SlotsTest):
    async def test_a_thread_idle_for_idle_after_seconds_sleeps_and_resumes(self) -> None:
        await self.limit(idle_after=1200)
        thread_id, session = await self.idle()
        self.clock.advance(1199.999)
        await self.orchestrator.sleep_idle()
        await self.orchestrator.drain()
        self.assertEqual(self.state(thread_id), 'idle')
        self.assertFalse(session.closed)
        self.clock.advance(0.001)
        await self.orchestrator.sleep_idle()
        self.assertEqual(self.state(thread_id), 'sleeping')
        self.assertTrue(session.closed)
        await self.send(thread_id, 'back')
        await self.orchestrator.drain()
        self.assertEqual(self.started(), [(thread_id, False), (thread_id, True)])
        self.assertEqual(self.session(thread_id).sent, [('back', QUEUE)])
        self.assertEqual(self.state(thread_id), 'running')

    async def test_running_and_stopping_threads_are_not_slept(self) -> None:
        await self.limit(idle_after=1)
        running, _ = await self.running()
        stopping, _ = await self.idle()
        with mock.patch.object(self.orchestrator.lanes, 'submit'):
            await self.orchestrator.stop({'id': self.next_id(), 'thread_id': stopping})
        self.clock.advance(5)
        await self.orchestrator.sleep_idle()
        self.assertEqual((self.state(running), self.state(stopping)), ('running', 'idle'))

    async def test_the_background_task_sleeps_idle_threads(self) -> None:
        await self.limit(idle_after=1, idle_check=0.01)
        await self.orchestrator.recover()
        names = lambda: {task.get_name() for task in asyncio.all_tasks()}
        self.assertIn('loom-idle', names())
        thread_id, session = await self.idle()
        await asyncio.sleep(0.05)
        self.assertEqual(self.state(thread_id), 'idle')
        self.clock.advance(1)
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        self.assertTrue(session.closed)
        await self.orchestrator.close()
        self.assertNotIn('loom-idle', names())


class CrashTest(SlotsTest):
    async def test_a_crash_continuation_waits_for_a_slot(self) -> None:
        await self.limit(1)
        crashed = await self.start()
        waiting = await self.start()
        self.assertEqual(self.state(waiting), 'queued')
        self.session(crashed).exit(1)
        await wait_for(lambda: len(self.events(crashed, EventType.TURN_RESULT)) == 1)
        await self.orchestrator.drain()
        self.assertEqual((self.state(crashed), self.state(waiting)), ('queued', 'running'))
        self.assertEqual(self.start_row(crashed)[0], StartSource.AGENT)
        await self.finish(waiting, self.session(waiting))
        self.assertEqual(self.started()[-1], (crashed, True))
        self.assertEqual(self.session(crashed).sent, [(CONTINUATION, QUEUE)])
        self.assertEqual(self.state(waiting), 'sleeping')


class RestartTest(SlotsTest):
    async def queued(self, source: StartSource) -> str:
        with mock.patch.object(self.orchestrator.lanes, 'submit'):
            info = await self.orchestrator.launch(self.launch_params(), source=source)
        self.clock.advance(1)
        return info['id']

    async def test_queued_threads_start_in_queue_order_within_agents_max(self) -> None:
        schedule = await self.queued(StartSource.SCHEDULE)
        agent = await self.queued(StartSource.AGENT)
        person = await self.queued(StartSource.PERSON)
        await self.limit(1)
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        self.assertEqual(self.started(), [(person, False)])
        self.assertEqual((self.state(agent), self.state(schedule)), ('queued', 'queued'))
        await self.finish(person, self.session(person))
        self.assertEqual(self.started()[1:], [(agent, False)])
        self.assertEqual(self.state(schedule), 'queued')
        await self.finish(agent, self.session(agent))
        self.assertEqual(self.started()[2:], [(schedule, False)])

    async def test_a_restart_resume_waits_for_a_slot(self) -> None:
        await self.limit(1)
        cut = await self.start()
        self.clock.advance(1)
        waiting = await self.start()
        self.assertEqual((self.state(cut), self.state(waiting)), ('running', 'queued'))
        await self.limit(1)
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        self.assertEqual((self.state(cut), self.state(waiting)), ('queued', 'running'))
        self.assertEqual(self.started()[-1], (waiting, False))
        await self.finish(waiting, self.session(waiting))
        self.assertEqual(self.started()[-1], (cut, True))
        self.assertEqual(self.session(cut).sent, [(CONTINUATION, QUEUE)])


class HeldTest(SlotsTest):
    async def stopping(self, grace: float = 0.2) -> str:
        self.runner = FakeRunner(stop_on_interrupt=False)
        await self.limit(1, stop_grace=grace)
        thread_id, _ = await self.running()
        self.clock.advance(1)
        return thread_id

    async def hold(self, thread_id: str, *sources: StartSource) -> None:
        session = self.session(thread_id)
        await self.orchestrator.stop({'id': self.next_id(), 'thread_id': thread_id})
        await wait_for(lambda: session.interrupts == 1)
        for source in sources:
            self.clock.advance(1)
            await self.orchestrator.send({'id': self.next_id(), 'thread_id': thread_id,
                                          'message': source.name.lower()}, source=source)
        self.assertEqual(self.orchestrator.get(thread_id)['reason'], 'stopping')
        self.assertEqual([text for text, _ in self.pending(thread_id)],
                         [source.name.lower() for source in sources])
        self.clock.advance(1)

    async def end_stop(self, thread_id: str, *sources: StartSource) -> str:
        first = await self.start(StartSource.AGENT)
        await self.hold(thread_id, *sources)
        await self.orchestrator.drain()
        self.assertEqual((self.state(thread_id), self.state(first)), ('queued', 'running'))
        return first

    async def restart(self) -> None:
        await self.limit(1)
        await self.orchestrator.recover()
        await self.orchestrator.drain()

    def sources(self, thread_id: str) -> list[tuple[str, int]]:
        return self.store.connection.execute(
            'SELECT command_id, source FROM messages WHERE thread_id = ? ORDER BY seq',
            (thread_id,)).fetchall()

    async def test_a_persons_message_held_across_a_stop_goes_before_an_earlier_agent(
            self) -> None:
        thread_id = await self.stopping()
        first = await self.start(StartSource.AGENT)
        self.clock.advance(1)
        agent = await self.start(StartSource.AGENT)
        await self.hold(thread_id, StartSource.PERSON)
        await self.orchestrator.drain()
        self.assertEqual([self.state(item) for item in (thread_id, first, agent)],
                         ['queued', 'running', 'queued'])
        self.assertEqual(self.start_row(thread_id), (StartSource.PERSON, iso(self.clock())))
        self.assertLess(self.start_row(agent)[1], self.start_row(thread_id)[1])
        await self.finish(first, self.session(first))
        self.assertEqual(self.started()[-1], (thread_id, True))
        self.assertEqual(self.session(thread_id).sent, [('person', QUEUE)])
        self.assertEqual(self.state(agent), 'queued')

    async def test_a_persons_message_held_across_a_restart_goes_before_an_earlier_agent(
            self) -> None:
        thread_id = await self.stopping(grace=5.0)
        agent = await self.start(StartSource.AGENT)
        await self.hold(thread_id, StartSource.PERSON)
        await self.restart()
        self.assertEqual(self.started()[-1], (thread_id, True))
        self.assertEqual(self.session(thread_id).sent, [('person', QUEUE)])
        self.assertEqual(self.state(agent), 'queued')
        await self.finish(thread_id, self.session(thread_id))
        self.assertEqual(self.started()[-1], (agent, False))

    async def test_an_agents_held_message_ranks_as_an_agents_at_the_stop_end(self) -> None:
        thread_id = await self.stopping()
        await self.end_stop(thread_id, StartSource.AGENT)
        self.assertEqual(self.start_row(thread_id), (StartSource.AGENT, iso(self.clock())))

    async def test_an_agents_held_message_goes_after_a_person_and_before_a_schedule(
            self) -> None:
        thread_id = await self.stopping(grace=5.0)
        person = await self.start(StartSource.PERSON)
        self.clock.advance(1)
        schedule = await self.start(StartSource.SCHEDULE)
        await self.hold(thread_id, StartSource.AGENT)
        await self.restart()
        self.assertEqual(self.started()[-1], (person, False))
        self.assertEqual(self.start_row(thread_id)[0], StartSource.AGENT)
        await self.finish(person, self.session(person))
        self.assertEqual(self.started()[-1], (thread_id, True))
        self.assertEqual(self.state(schedule), 'queued')
        await self.finish(thread_id, self.session(thread_id))
        self.assertEqual(self.started()[-1], (schedule, False))

    async def test_a_schedules_held_message_alone_ranks_as_a_schedules(self) -> None:
        thread_id = await self.stopping()
        await self.end_stop(thread_id, StartSource.SCHEDULE)
        self.assertEqual(self.start_row(thread_id)[0], StartSource.SCHEDULE)

    async def test_held_messages_rank_by_the_best_sender(self) -> None:
        thread_id = await self.stopping()
        first = await self.end_stop(thread_id, StartSource.AGENT, StartSource.PERSON)
        self.assertEqual(self.start_row(thread_id)[0], StartSource.PERSON)
        await self.finish(first, self.session(first))
        self.assertEqual(self.started()[-1], (thread_id, True))
        self.assertEqual(self.session(thread_id).sent, [('agent', QUEUE)])

    async def test_a_held_message_in_an_old_store_queues_as_an_agents(self) -> None:
        thread_id = await self.stopping(grace=5.0)
        person = await self.start(StartSource.PERSON)
        await self.hold(thread_id, StartSource.PERSON)
        await self.orchestrator.close()
        self.store.close()
        db = sqlite3.connect(self.data_dir / DB_NAME)
        db.executescript(
            'ALTER TABLE messages RENAME TO messages_new;'
            f'{OLD_MESSAGES};'
            'INSERT INTO messages SELECT seq, thread_id, command_id, text, mode, priority, '
            'created_at FROM messages_new;'
            'DROP TABLE messages_new;')
        db.close()
        self.store = Store.open(self.data_dir, self.clock)
        self.projects = Projects(self.store)
        await self.restart()
        self.assertEqual((self.state(person), self.state(thread_id)), ('running', 'queued'))
        self.assertEqual(self.start_row(thread_id)[0], StartSource.AGENT)
        self.assertEqual(self.pending(thread_id), [('person', 'steer')])

    async def test_send_records_its_callers_source(self) -> None:
        thread_id, _ = await self.running()
        for source in StartSource:
            await self.orchestrator.send({'id': f'send-{source.name}', 'thread_id': thread_id,
                                          'message': 'later', 'mode': 'queue'}, source=source)
        await self.orchestrator.drain()
        self.assertEqual(self.sources(thread_id),
                         [(f'send-{source.name}', source) for source in StartSource])

    async def test_a_delegated_result_and_a_crash_continuation_are_agents(self) -> None:
        parent_id, _ = await self.running()
        child, session = await self.child(parent_id)
        await self.finish(child['id'], session)
        self.assertEqual(self.sources(parent_id), [(f'{child["id"]}:result', StartSource.AGENT)])
        await self.limit(1)
        crashed = await self.start()
        waiting = await self.start()
        self.session(crashed).exit(1)
        await wait_for(lambda: len(self.events(crashed, EventType.TURN_RESULT)) == 1)
        await self.orchestrator.drain()
        self.assertEqual((self.state(crashed), self.state(waiting)), ('queued', 'running'))
        self.assertEqual(self.sources(crashed),
                         [(f'{crashed}:continuation:1', StartSource.AGENT)])


class SourceTest(ServiceTest):
    async def test_callers_map_to_start_sources(self) -> None:
        cases = {
            Caller(): StartSource.PERSON,
            Caller(thread_id='t'): StartSource.AGENT,
            Caller(verified=False): StartSource.AGENT,
            Caller(scheduled=True): StartSource.SCHEDULE,
        }
        for caller, source in cases.items():
            with self.subTest(caller=caller), \
                    mock.patch.object(self.orchestrator.lanes, 'submit'):
                info = await self.call('threads.launch', self.launch_params(), caller)
                rank = self.store.connection.execute(
                    'SELECT rank FROM starts WHERE thread_id = ?', (info['id'],)).fetchone()
                self.assertEqual(rank, (source,))
            with self.subTest(caller=caller, call='send'), mock.patch.object(
                    self.orchestrator, 'send', new_callable=mock.AsyncMock) as send:
                await self.call('threads.send', {'thread_id': 'x'}, caller)
                self.assertEqual(send.call_args.kwargs, {'source': source})

    async def test_delegate_is_an_agents_start(self) -> None:
        for caller in (Caller(), Caller(scheduled=True)):
            with mock.patch.object(self.orchestrator, 'delegate',
                                   new_callable=mock.AsyncMock) as delegate:
                await self.call('threads.delegate', {'parent_id': 'x'}, caller)
            self.assertEqual(delegate.call_args.kwargs, {'source': StartSource.AGENT})


if __name__ == '__main__':
    unittest.main()
