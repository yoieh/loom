import asyncio
import unittest

from loom_contracts.events import EventType
from loom_contracts.runners import DispatchMode, Exited, ThreadSpec, TurnResult
from loom_contracts.states import ThreadState, TurnState

from loom_core import threads

from .fakes import FakeRunner, FakeSession, ManualClock, wait_for
from .test_delegate import DelegateTest
from .test_orchestrator import START, done

QUEUE = DispatchMode.QUEUE
CONTINUATION = 'Your last turn was cut off; continue where you were.'
CRASHED = 'exited mid-turn after 3 continuations in 5 minutes'
EXITED = {'state': 'failed', 'reason': 'exited'}
CUT = {'state': 'stopped', 'reason': 'stop'}


class GatedRunner(FakeRunner):
    def __init__(self, **options: object) -> None:
        super().__init__(**options)
        self.gate = asyncio.Event()
        self.gate.set()

    async def start(self, thread: ThreadSpec, host: object) -> FakeSession:
        session = await super().start(thread, host)
        await self.gate.wait()
        return session


class CrashTest(DelegateTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.clock = ManualClock(START)
        self.store.clock = self.clock

    async def crash(self, thread_id: str, code: int | None = 1) -> FakeSession:
        count = len(self.events(thread_id, EventType.TURN_RESULT))
        self.runner.sessions[thread_id].exit(code)
        await wait_for(lambda: len(self.events(thread_id, EventType.TURN_RESULT)) > count)
        await self.orchestrator.drain()
        return self.runner.sessions[thread_id]

    def results(self, thread_id: str) -> list[dict]:
        return [event['data'] for event in self.events(thread_id, EventType.TURN_RESULT)]

    def continuations(self, thread_id: str) -> list[str]:
        return [row[0] for row in self.store.connection.execute(
            'SELECT at FROM continuations WHERE thread_id = ? ORDER BY at', (thread_id,))]

    def queue_of(self, thread_id: str) -> list[tuple]:
        return self.store.connection.execute(
            'SELECT command_id, text, mode, priority FROM messages WHERE thread_id = ? '
            'ORDER BY priority DESC, seq', (thread_id,)).fetchall()

    def assert_failed_past_continuations(self, thread_id: str) -> None:
        info = self.orchestrator.get(thread_id)
        self.assertEqual((info['state'], info['reason']), ('failed', CRASHED))
        self.assertEqual(self.events(thread_id, EventType.THREAD_FAILED)[-1]['data'],
                         {'reason': CRASHED})


class ContinuationTest(CrashTest):
    async def test_a_crash_fails_the_turn_sleeps_and_resumes_with_the_continuation(self) -> None:
        thread_id, session = await self.running()
        woken = await self.crash(thread_id)
        self.assertTrue(session.closed)
        self.assertIsNot(woken, session)
        logged = [(event['type'], event['data']) for event in self.events(thread_id)]
        cut = logged.index((EventType.TURN_RESULT, EXITED))
        self.assertEqual([data['state'] for kind, data in logged[cut + 1:]
                          if kind == EventType.THREAD_STATE], ['sleeping', 'starting', 'running'])
        self.assertEqual([spec.resume for spec in self.runner.starts], [False, True])
        self.assertEqual(woken.sent, [(CONTINUATION, QUEUE)])
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual(self.pending(thread_id), [])
        self.assertEqual(len(self.continuations(thread_id)), 1)

    async def test_the_continuation_is_a_queued_message_of_priority_2(self) -> None:
        await self.use(GatedRunner())
        thread_id, session = await self.running()
        self.runner.gate.clear()
        session.exit(1)
        await wait_for(lambda: len(self.runner.starts) == 2)
        self.assertEqual(self.state(thread_id), 'starting')
        self.assertEqual(self.queue_of(thread_id),
                         [(f'{thread_id}:continuation:1', CONTINUATION, 'queue', 2)])
        self.runner.gate.set()
        await self.orchestrator.drain()
        self.assertEqual(self.runner.sessions[thread_id].sent, [(CONTINUATION, QUEUE)])

    async def test_a_crash_while_waiting_on_a_request_also_resumes(self) -> None:
        thread_id, session = await self.running()
        with self.store.transaction() as tx:
            threads.transition(self.store, tx, thread_id, ThreadState.RUNNING, waiting=True)
        woken = await self.crash(thread_id)
        self.assertEqual(woken.sent, [(CONTINUATION, QUEUE)])
        self.assertIs(self.orchestrator.get(thread_id)['waiting'], False)

    async def test_a_stream_that_ends_without_exited_is_a_crash(self) -> None:
        thread_id, session = await self.running()
        await session.close()
        await wait_for(lambda: len(self.runner.starts) == 2)
        await self.orchestrator.drain()
        self.assertEqual(self.results(thread_id), [EXITED])
        self.assertEqual(self.runner.sessions[thread_id].sent, [(CONTINUATION, QUEUE)])

    async def test_three_crashes_in_5_minutes_resume_and_the_fourth_fails(self) -> None:
        thread_id, _ = await self.running()
        for count in range(1, 4):
            woken = await self.crash(thread_id)
            self.assertEqual(self.state(thread_id), 'running')
            self.assertEqual(woken.sent, [(CONTINUATION, QUEUE)])
            self.assertEqual(len(self.runner.starts), count + 1)
            self.clock.advance(60)
        await self.crash(thread_id)
        self.assert_failed_past_continuations(thread_id)
        self.assertEqual(len(self.runner.starts), 4)
        self.assertEqual(self.results(thread_id), [EXITED] * 4)
        types = [event['type'] for event in self.events(thread_id)][-2:]
        self.assertEqual(types, [EventType.THREAD_STATE, EventType.THREAD_FAILED])
        self.assertEqual(self.states(thread_id)[-1], 'failed')

    async def crash_three_times(self, thread_id: str) -> None:
        for _ in range(3):
            await self.crash(thread_id)
            self.clock.advance(1)

    async def test_a_continuation_exactly_300_s_old_no_longer_counts(self) -> None:
        thread_id, _ = await self.running()
        first = self.clock.now
        await self.crash_three_times(thread_id)
        self.clock.now = first
        self.clock.advance(300)
        woken = await self.crash(thread_id)
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual(woken.sent, [(CONTINUATION, QUEUE)])
        self.assertEqual(len(self.runner.starts), 5)
        self.assertEqual(len(self.continuations(thread_id)), 3)

    async def test_a_continuation_299_999_s_old_still_counts(self) -> None:
        thread_id, _ = await self.running()
        first = self.clock.now
        await self.crash_three_times(thread_id)
        self.clock.now = first
        self.clock.advance(299.999)
        await self.crash(thread_id)
        self.assert_failed_past_continuations(thread_id)
        self.assertEqual(len(self.runner.starts), 4)

    async def test_a_start_raising_during_the_resume_fails_the_thread(self) -> None:
        thread_id, _ = await self.running()
        self.runner.fail_start = RuntimeError('claude is gone')
        await self.crash(thread_id)
        info = self.orchestrator.get(thread_id)
        self.assertEqual((info['state'], info['reason']), ('failed', 'claude is gone'))
        self.assertEqual(self.events(thread_id, EventType.THREAD_FAILED)[-1]['data'],
                         {'reason': 'claude is gone'})
        self.assertEqual([spec.resume for spec in self.runner.starts], [False, True])

    async def test_the_resume_takes_a_narrowed_project_mode(self) -> None:
        thread_id, _ = await self.running()
        self.projects.set_mode({'id': self.next_id(), 'project_id': 'loom',
                                'permission_mode': 'approval-required'})
        await self.crash(thread_id)
        self.assertEqual([spec.permission_mode for spec in self.runner.starts],
                         ['auto', 'approval-required'])

    async def test_a_runner_that_cannot_resume_fails_the_thread(self) -> None:
        await self.use(FakeRunner(capabilities=frozenset({'steer', 'interrupt'})))
        thread_id, _ = await self.running()
        await self.crash(thread_id)
        self.assertEqual(self.orchestrator.get(thread_id)['reason'], 'runner cannot resume')
        self.assertEqual(len(self.runner.starts), 1)

    async def test_the_continuation_its_count_and_the_sleep_share_one_transaction(self) -> None:
        thread_id, _ = await self.running()
        statements: list[str] = []
        self.store.connection.set_trace_callback(statements.append)
        try:
            await self.crash(thread_id)
        finally:
            self.store.connection.set_trace_callback(None)
        groups: list[list[str]] = []
        group: list[str] | None = None
        for statement in statements:
            word = statement.strip().upper()
            if word.startswith('BEGIN'):
                group = []
            elif word == 'COMMIT' and group is not None:
                groups.append(group)
                group = None
            elif group is not None:
                group.append(statement)
        counted = [group for group in groups
                   if any(s.startswith('INSERT INTO continuations') for s in group)]
        self.assertEqual(len(counted), 1)
        joined = '\n'.join(counted[0])
        self.assertIn('INSERT INTO messages', joined)
        self.assertIn("'turn.result'", joined)
        self.assertIn('"state": "sleeping"', joined)

    async def test_settle_deletes_the_threads_continuations(self) -> None:
        thread_id, _ = await self.running()
        await self.crash(thread_id)
        self.assertEqual(len(self.continuations(thread_id)), 1)
        await self.orchestrator.settle({'id': self.next_id(), 'thread_id': thread_id})
        await self.orchestrator.drain()
        self.assertEqual(self.continuations(thread_id), [])


class ParentTest(CrashTest):
    async def crash_past_continuations(self, thread_id: str) -> None:
        for _ in range(4):
            await self.crash(thread_id)
        self.assert_failed_past_continuations(thread_id)

    async def test_a_child_failing_past_its_continuations_posts_a_failed_result(self) -> None:
        parent_id, parent = await self.running()
        await self.send(parent_id, 'First', mode='queue')
        info, _ = await self.child(parent_id, 'Review it')
        await self.crash_past_continuations(info['id'])
        text = f'Task {info["id"]} (Review it) failed: {CRASHED}.'
        self.assertEqual(self.queue_of(parent_id), [
            (f'{info["id"]}:result', text, 'queue', 1),
            ('cmd-2', 'First', 'queue', 0),
        ])
        await self.finish(parent_id, parent)
        self.assertEqual(self.sent_to(parent_id), ['Hello', text])

    async def test_an_idle_parent_gets_the_failed_result_at_once(self) -> None:
        parent_id, parent = await self.idle()
        info, _ = await self.child(parent_id, 'Review it')
        await self.crash_past_continuations(info['id'])
        await self.orchestrator.drain()
        self.assertEqual(parent.sent[-1],
                         (f'Task {info["id"]} (Review it) failed: {CRASHED}.', QUEUE))
        self.assertEqual(self.state(parent_id), 'running')

    async def test_a_child_that_gets_a_continuation_posts_nothing(self) -> None:
        parent_id, parent = await self.idle()
        info, _ = await self.child(parent_id)
        woken = await self.crash(info['id'])
        self.assertEqual(woken.sent, [(CONTINUATION, QUEUE)])
        self.assertEqual(parent.sent, [('Hello', QUEUE)])
        self.assertEqual(self.pending(parent_id), [])
        self.assertEqual(self.state(parent_id), 'idle')

    async def test_no_failed_result_to_a_stopping_or_ended_parent(self) -> None:
        cases = {
            'stopping': lambda tx, parent_id: threads.request_stop(self.store, tx, parent_id),
            'settled': lambda tx, parent_id: threads.transition(
                self.store, tx, parent_id, ThreadState.SETTLED),
            'failed': lambda tx, parent_id: threads.transition(
                self.store, tx, parent_id, ThreadState.FAILED, 'boom'),
        }
        for name, change in cases.items():
            with self.subTest(parent=name):
                parent_id, parent = await self.idle()
                info, _ = await self.child(parent_id)
                with self.store.transaction() as tx:
                    change(tx, parent_id)
                await self.crash_past_continuations(info['id'])
                self.assertEqual(self.pending(parent_id), [])
                self.assertEqual(parent.sent, [('Hello', QUEUE)])


class NotACrashTest(CrashTest):
    def assert_no_continuation(self, thread_id: str) -> None:
        self.assertNotIn(EXITED, self.results(thread_id))
        self.assertEqual(self.continuations(thread_id), [])
        self.assertNotIn({'reason': CRASHED}, [
            event['data'] for event in self.events(thread_id, EventType.THREAD_FAILED)])
        self.assertEqual(len(self.runner.starts), 1)

    async def test_an_exit_after_a_failed_result_only_sleeps(self) -> None:
        thread_id, session = await self.running()
        await self.finish(thread_id, session, TurnResult(TurnState.FAILED, None, 'bad model'))
        self.assertEqual(self.state(thread_id), 'idle')
        session.exit(1)
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        await self.orchestrator.drain()
        self.assertEqual(self.results(thread_id), [{'state': 'failed', 'reason': 'bad model'}])
        self.assert_no_continuation(thread_id)
        self.assertEqual(self.events(thread_id, EventType.THREAD_FAILED), [])
        self.assertEqual(self.state(thread_id), 'sleeping')
        self.assertTrue(session.closed)

    async def test_an_exit_after_settle(self) -> None:
        await self.use(FakeRunner(exit_on_close=True))
        thread_id, session = await self.running()
        await self.orchestrator.settle({'id': self.next_id(), 'thread_id': thread_id})
        await wait_for(lambda: session.closed)
        await self.orchestrator.drain()
        self.assertEqual(self.state(thread_id), 'settled')
        self.assert_no_continuation(thread_id)

    async def test_an_exit_after_a_stops_cut(self) -> None:
        await self.use(FakeRunner(capabilities=frozenset({'steer', 'resume'}),
                                  exit_on_close=True))
        thread_id, session = await self.running()
        await self.orchestrator.stop({'id': self.next_id(), 'thread_id': thread_id})
        await self.orchestrator.drain()
        self.assertTrue(session.closed)
        self.assertEqual(self.results(thread_id), [CUT])
        self.assertEqual((self.state(thread_id), self.orchestrator.get(thread_id)['reason']),
                         ('sleeping', None))
        self.assert_no_continuation(thread_id)

    async def test_an_exit_after_a_failure_closed_the_session(self) -> None:
        await self.use(FakeRunner(exit_on_close=True))
        thread_id, session = await self.running()
        session.fail_send = OSError('broken pipe')
        await self.send(thread_id, 'Steer')
        await self.orchestrator.drain()
        self.assertTrue(session.closed)
        self.assertEqual(self.orchestrator.get(thread_id)['reason'], 'broken pipe')
        self.assertEqual(self.results(thread_id), [])
        self.assert_no_continuation(thread_id)

    async def test_an_exit_after_an_empty_start_was_closed(self) -> None:
        await self.use(FakeRunner(exit_on_close=True))
        info = await self.launch()
        with self.store.transaction() as tx:
            tx.execute('DELETE FROM messages WHERE thread_id = ?', (info['id'],))
        await self.orchestrator.drain()
        session = self.runner.sessions[info['id']]
        self.assertTrue(session.closed)
        self.assertEqual(self.state(info['id']), 'sleeping')
        self.assertEqual(self.results(info['id']), [])
        self.assert_no_continuation(info['id'])

    async def test_an_exit_after_close(self) -> None:
        await self.use(FakeRunner(exit_on_close=True))
        thread_id, session = await self.running()
        await self.orchestrator.close()
        self.assertTrue(session.closed)
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual(self.results(thread_id), [])
        self.assert_no_continuation(thread_id)

    async def test_an_exit_mid_turn_while_stopping_sleeps_without_a_continuation(self) -> None:
        await self.use(FakeRunner(stop_on_interrupt=False))
        thread_id, session = await self.running()
        await self.orchestrator.stop({'id': self.next_id(), 'thread_id': thread_id})
        await wait_for(lambda: session.interrupts == 1)
        session.exit(1)
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        await self.orchestrator.drain()
        info = self.orchestrator.get(thread_id)
        self.assertEqual((info['state'], info['reason']), ('sleeping', None))
        self.assertEqual(threads.fetch(self.store.connection, thread_id)['stop_requested'], 0)
        self.assertEqual(self.continuations(thread_id), [])
        self.assertEqual(self.pending(thread_id), [])
        self.assertEqual(len(self.runner.starts), 1)


class KeptMessagesTest(CrashTest):
    async def test_queued_messages_follow_the_continuation(self) -> None:
        await self.use(FakeRunner(stop_on_interrupt=False))
        thread_id, session = await self.running()
        await self.send(thread_id, 'Later', mode='queue')
        await self.send(thread_id, 'Now', mode='interrupt')
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 1)
        woken = await self.crash(thread_id)
        self.assertEqual(woken.sent, [(CONTINUATION, QUEUE)])
        self.assertEqual(self.pending(thread_id), [('Later', 'queue'), ('Now', 'interrupt')])
        await self.finish(thread_id, woken)
        self.assertEqual(woken.sent, [(CONTINUATION, QUEUE), ('Later', QUEUE)])
        await self.finish(thread_id, woken)
        self.assertEqual(woken.sent[-1], ('Now', QUEUE))
        self.assertEqual(self.pending(thread_id), [])

    async def test_a_task_result_for_a_crashed_parent_follows_the_continuation(self) -> None:
        parent_id, _ = await self.running()
        info, child = await self.child(parent_id, 'Review it')
        await self.finish(info['id'], child, done(text='done'))
        result = f'Task {info["id"]} (Review it) completed.\n\ndone'
        self.assertEqual(self.pending(parent_id), [(result, 'queue')])
        woken = await self.crash(parent_id)
        self.assertEqual(woken.sent, [(CONTINUATION, QUEUE)])
        await self.finish(parent_id, woken)
        self.assertEqual(woken.sent, [(CONTINUATION, QUEUE), (result, QUEUE)])


class CloseTest(CrashTest):
    async def test_close_closes_every_session_at_once_and_keeps_states(self) -> None:
        first, one = await self.running()
        second, two = await self.running()
        third, three = await self.idle()
        gate = asyncio.Event()
        self.runner.close_gate = gate
        closing = asyncio.ensure_future(self.orchestrator.close())
        await wait_for(lambda: one.closing and two.closing and three.closing)
        self.assertFalse(closing.done())
        self.assertEqual([one.closed, two.closed, three.closed], [False] * 3)
        gate.set()
        await closing
        self.assertEqual([one.closed, two.closed, three.closed], [True] * 3)
        self.assertEqual([self.state(first), self.state(second), self.state(third)],
                         ['running', 'running', 'idle'])

    async def test_events_after_close_began_are_ignored(self) -> None:
        thread_id, session = await self.running()
        gate = asyncio.Event()
        self.runner.close_gate = gate
        closing = asyncio.ensure_future(self.orchestrator.close())
        await wait_for(lambda: session.closing)
        session.emit(done(text='late'))
        session.emit(Exited(1))
        await asyncio.sleep(0.02)
        gate.set()
        await closing
        self.assertEqual(self.results(thread_id), [])
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual(len(self.runner.starts), 1)

    async def test_a_crash_handled_while_closing_starts_no_agent(self) -> None:
        thread_id, session = await self.running()
        blocker = asyncio.Event()
        self.orchestrator.lanes.submit(thread_id, blocker.wait)
        session.exit(1)
        await wait_for(lambda: session._queue.qsize() == 1)
        closing = asyncio.ensure_future(self.orchestrator.close())
        await asyncio.sleep(0)
        blocker.set()
        await closing
        self.assertEqual(len(self.runner.starts), 1)
        self.assertEqual(self.state(thread_id), 'sleeping')
        self.assertEqual(self.pending(thread_id), [(CONTINUATION, 'queue')])


if __name__ == '__main__':
    unittest.main()
