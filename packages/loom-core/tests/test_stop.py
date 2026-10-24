import asyncio
import unittest
from unittest import mock

from loom_contracts.events import EventType
from loom_contracts.runners import DispatchMode, Exited, ThreadSpec
from loom_contracts.states import TurnState

from loom_core import threads
from loom_core.errors import LoomError
from loom_core.orchestrator import Orchestrator
from loom_core.projects import Projects

from .fakes import FakeHost, FakeRunner, FakeSession, wait_for
from .test_delegate import DelegateTest
from .test_orchestrator import done

QUEUE = DispatchMode.QUEUE
CUT = {'state': 'stopped', 'reason': 'stop'}
RUNNING_STOP = {'state': 'running', 'waiting': False, 'reason': 'stopping'}


class StopTest(DelegateTest):
    grace = 5.0

    def make(self, *runners: FakeRunner, projects: Projects | None = None) -> Orchestrator:
        return Orchestrator(self.store, projects or self.projects,
                            {runner.name: runner for runner in runners},
                            FakeHost(str(self.data_dir)), stop_grace=self.grace)

    async def stop(self, thread_id: str, command_id: str | None = None) -> dict:
        return await self.orchestrator.stop(
            {'id': command_id or self.next_id(), 'thread_id': thread_id})

    async def tree(self) -> list[str]:
        parent_id, _ = await self.running()
        child, _ = await self.child(parent_id)
        grandchild, _ = await self.child(child['id'])
        return [parent_id, child['id'], grandchild['id']]

    def reason(self, thread_id: str) -> str | None:
        return self.orchestrator.get(thread_id)['reason']

    def results(self, thread_id: str) -> list[dict]:
        return [event['data'] for event in self.events(thread_id, EventType.TURN_RESULT)]

    def stopping_events(self, thread_id: str) -> list[dict]:
        return [event['data'] for event in self.events(thread_id, EventType.THREAD_STATE)
                if event['data'].get('reason') == 'stopping']


class StopTreeTest(StopTest):
    async def test_stop_is_recorded_at_once_for_the_whole_tree(self) -> None:
        ids = await self.tree()
        info = await self.stop(ids[0])
        self.assertEqual((info['state'], info['reason']), ('running', 'stopping'))
        for thread_id in ids:
            with self.subTest(thread_id=thread_id):
                self.assertEqual(self.reason(thread_id), 'stopping')
                self.assertEqual(self.stopping_events(thread_id), [RUNNING_STOP])
                self.assertEqual(self.runner.sessions[thread_id].interrupts, 0)

    async def test_after_drain_each_turn_is_interrupted_and_idle(self) -> None:
        ids = await self.tree()
        await self.stop(ids[0])
        await self.orchestrator.drain()
        for thread_id in ids:
            with self.subTest(thread_id=thread_id):
                session = self.runner.sessions[thread_id]
                self.assertEqual(session.interrupts, 1)
                self.assertFalse(session.closed)
                self.assertEqual(self.results(thread_id),
                                 [{'state': 'stopped', 'reason': 'interrupted'}])
                info = self.orchestrator.get(thread_id)
                self.assertEqual((info['state'], info['reason']), ('idle', None))
                self.assertEqual(threads.fetch(self.store.connection,
                                               thread_id)['stop_requested'], 0)
                self.assertEqual(self.pending(thread_id), [])
        self.assertEqual(self.sent_to(ids[0]), ['Hello'])

    async def test_stop_reaches_only_descendants(self) -> None:
        parent_id, _ = await self.running()
        child, _ = await self.child(parent_id)
        sibling, _ = await self.child(parent_id)
        grandchild, _ = await self.child(child['id'])
        await self.stop(child['id'])
        await self.orchestrator.drain()
        for thread_id in (child['id'], grandchild['id']):
            self.assertEqual(self.runner.sessions[thread_id].interrupts, 1)
            self.assertEqual(self.state(thread_id), 'idle')
        for thread_id in (parent_id, sibling['id']):
            with self.subTest(thread_id=thread_id):
                self.assertEqual(self.runner.sessions[thread_id].interrupts, 0)
                self.assertEqual((self.state(thread_id), self.reason(thread_id)),
                                 ('running', None))
                self.assertEqual(self.stopping_events(thread_id), [])

    async def test_stop_passes_through_an_ended_child(self) -> None:
        parent_id, _ = await self.running()
        child, _ = await self.child(parent_id)
        grandchild, _ = await self.child(child['id'])
        await self.orchestrator.settle({'id': self.next_id(), 'thread_id': child['id']})
        await self.orchestrator.drain()
        await self.stop(parent_id)
        await self.orchestrator.drain()
        self.assertEqual(self.runner.sessions[grandchild['id']].interrupts, 1)
        self.assertEqual(self.state(child['id']), 'settled')
        self.assertEqual(self.stopping_events(child['id']), [])


class StopTurnTest(StopTest):
    async def test_runner_without_interrupt_is_closed_at_once(self) -> None:
        await self.use(FakeRunner(capabilities=frozenset({'steer', 'resume'})))
        thread_id, session = await self.running()
        await self.stop(thread_id)
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 0)
        self.assertTrue(session.closed)
        self.assertEqual(self.results(thread_id), [CUT])
        info = self.orchestrator.get(thread_id)
        self.assertEqual((info['state'], info['reason']), ('sleeping', None))

    async def test_an_unanswered_interrupt_is_closed_after_the_grace(self) -> None:
        self.grace = 0.05
        await self.use(FakeRunner(stop_on_interrupt=False))
        thread_id, session = await self.running()
        await self.stop(thread_id)
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 1)
        self.assertTrue(session.closed)
        self.assertEqual(self.results(thread_id), [CUT])
        info = self.orchestrator.get(thread_id)
        self.assertEqual((info['state'], info['reason']), ('sleeping', None))
        self.assertEqual(self.orchestrator._stop_waits, {})

    async def test_a_late_answer_within_the_grace_leaves_the_thread_idle(self) -> None:
        await self.use(FakeRunner(stop_on_interrupt=False))
        thread_id, session = await self.running()
        await self.stop(thread_id)
        await wait_for(lambda: session.interrupts == 1)
        await asyncio.sleep(0.05)
        self.assertEqual((self.state(thread_id), self.reason(thread_id)),
                         ('running', 'stopping'))
        session.emit(done(state=TurnState.STOPPED, text=None, reason='interrupted'))
        await self.orchestrator.drain()
        self.assertFalse(session.closed)
        self.assertEqual(self.results(thread_id), [{'state': 'stopped', 'reason': 'interrupted'}])
        self.assertEqual((self.state(thread_id), self.reason(thread_id)), ('idle', None))

    async def test_grace_waits_on_the_turn_ending(self) -> None:
        self.grace = 0.05
        await self.use(FakeRunner(stop_on_interrupt=False))
        thread_id, session = await self.running()
        calls: list[float] = []
        real = asyncio.wait_for

        async def spy(future: object, timeout: float) -> object:
            calls.append(timeout)
            return await real(future, timeout)

        with mock.patch('loom_core.orchestrator.asyncio.wait_for', spy):
            await self.stop(thread_id)
            await self.orchestrator.drain()
        self.assertEqual(calls, [0.05])
        self.assertEqual(self.state(thread_id), 'sleeping')

    async def test_interrupt_raising_closes_at_once(self) -> None:
        thread_id, session = await self.running()
        session.fail_interrupt = RuntimeError('pipe closed')
        await self.stop(thread_id)
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 1)
        self.assertTrue(session.closed)
        self.assertEqual(self.results(thread_id), [CUT])
        self.assertEqual((self.state(thread_id), self.reason(thread_id)), ('sleeping', None))

    async def test_idle_and_sleeping_threads_keep_their_state(self) -> None:
        thread_id, session = await self.idle()
        await self.stop(thread_id)
        await self.orchestrator.drain()
        self.assertEqual((self.state(thread_id), self.reason(thread_id)), ('idle', None))
        self.assertEqual(session.interrupts, 0)
        self.assertFalse(session.closed)
        session.emit(Exited(0))
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        await self.stop(thread_id)
        await self.orchestrator.drain()
        self.assertEqual((self.state(thread_id), self.reason(thread_id)), ('sleeping', None))
        self.assertEqual(len(self.runner.starts), 1)
        self.assertEqual(self.states(thread_id)[-3:], ['sleeping', 'sleeping', 'sleeping'])

    async def test_a_queued_thread_never_starts(self) -> None:
        info = await self.launch()
        await self.stop(info['id'])
        await self.orchestrator.drain()
        self.assertEqual(self.runner.starts, [])
        self.assertEqual((self.state(info['id']), self.reason(info['id'])), ('sleeping', None))
        await self.send(info['id'], 'Now')
        await self.orchestrator.drain()
        self.assertEqual([spec.resume for spec in self.runner.starts], [False])
        self.assertEqual(self.runner.sessions[info['id']].sent, [('Now', QUEUE)])
        self.assertEqual(self.state(info['id']), 'running')

    async def test_a_starting_thread_closes_the_new_session(self) -> None:
        gate = asyncio.Event()

        class SlowRunner(FakeRunner):
            async def start(self, thread: ThreadSpec, host: object) -> FakeSession:
                session = await super().start(thread, host)
                await gate.wait()
                return session

        await self.use(SlowRunner())
        info = await self.launch()
        await wait_for(lambda: len(self.runner.starts) == 1)
        self.assertEqual(self.state(info['id']), 'starting')
        stopped = await self.stop(info['id'])
        self.assertEqual((stopped['state'], stopped['reason']), ('starting', 'stopping'))
        gate.set()
        await self.orchestrator.drain()
        session = self.runner.sessions[info['id']]
        self.assertTrue(session.closed)
        self.assertEqual(session.sent, [])
        self.assertEqual((self.state(info['id']), self.reason(info['id'])), ('sleeping', None))
        self.assertEqual(threads.fetch(self.store.connection, info['id'])['has_session'], 0)


class StopMessagesTest(StopTest):
    async def test_stop_drops_pending_messages(self) -> None:
        ids = await self.tree()
        for thread_id in ids:
            await self.send(thread_id, 'Later', mode='queue')
        await self.stop(ids[0])
        for thread_id in ids:
            self.assertEqual(self.pending(thread_id), [])
        await self.orchestrator.drain()
        for thread_id in ids:
            self.assertNotIn('Later', self.sent_to(thread_id))

    async def test_a_message_after_the_stop_is_delivered(self) -> None:
        thread_id, session = await self.running()
        await self.stop(thread_id)
        await self.orchestrator.drain()
        await self.send(thread_id, 'After')
        await self.orchestrator.drain()
        self.assertEqual(session.sent[-1], ('After', QUEUE))
        self.assertEqual((self.state(thread_id), self.reason(thread_id)), ('running', None))

    async def test_a_message_during_the_stop_waits_for_its_end(self) -> None:
        thread_id, session = await self.running()
        await self.stop(thread_id)
        await self.send(thread_id, 'During')
        await self.orchestrator.drain()
        self.assertEqual(session.sent, [('Hello', QUEUE), ('During', QUEUE)])
        self.assertEqual(session.interrupts, 1)
        self.assertEqual((self.state(thread_id), self.reason(thread_id)), ('running', None))
        tail = [event['data'] for event in self.events(thread_id, EventType.THREAD_STATE)][-4:]
        self.assertEqual(tail, [RUNNING_STOP,
                                {'state': 'idle', 'waiting': False, 'reason': 'stopping'},
                                {'state': 'idle', 'waiting': False},
                                {'state': 'running', 'waiting': False}])

    async def test_a_sleeping_thread_wakes_for_a_message_after_the_stop(self) -> None:
        await self.use(FakeRunner(capabilities=frozenset({'steer', 'resume'})))
        thread_id, session = await self.running()
        await self.stop(thread_id)
        await self.orchestrator.drain()
        await self.send(thread_id, 'Wake up')
        await self.orchestrator.drain()
        self.assertEqual([spec.resume for spec in self.runner.starts], [False, True])
        self.assertEqual(self.runner.sessions[thread_id].sent, [('Wake up', QUEUE)])

    async def test_a_child_result_during_the_stop_is_not_posted(self) -> None:
        parent_id, _ = await self.idle()
        child, session = await self.child(parent_id)
        await self.stop(child['id'])
        session.emit(done(text='late'))
        await self.orchestrator.drain()
        self.assertEqual(self.sent_to(parent_id), ['Hello'])
        self.assertEqual(self.pending(parent_id), [])


class StopRetryTest(StopTest):
    async def test_retry_returns_the_first_answer_and_interrupts_once(self) -> None:
        thread_id, session = await self.running()
        first = await self.stop(thread_id, 's1')
        await self.orchestrator.drain()
        with mock.patch.object(self.orchestrator.lanes, 'submit') as submit:
            again = await self.stop(thread_id, 's1')
        submit.assert_not_called()
        await self.orchestrator.drain()
        self.assertEqual(again, first)
        self.assertEqual(again['reason'], 'stopping')
        self.assertEqual(session.interrupts, 1)
        self.assertEqual(self.stopping_events(thread_id).count(RUNNING_STOP), 1)

    async def test_a_second_stop_while_stopping_interrupts_once(self) -> None:
        await self.use(FakeRunner(stop_on_interrupt=False))
        thread_id, session = await self.running()
        await self.stop(thread_id)
        await wait_for(lambda: session.interrupts == 1)
        await self.stop(thread_id)
        session.emit(done(state=TurnState.STOPPED, text=None, reason='interrupted'))
        await self.orchestrator.drain()
        self.assertEqual(session.interrupts, 1)
        self.assertEqual(self.stopping_events(thread_id).count(RUNNING_STOP), 1)
        self.assertEqual((self.state(thread_id), self.reason(thread_id)), ('idle', None))

    async def test_a_stopping_parent_cannot_delegate(self) -> None:
        self.grace = 0.05
        await self.use(FakeRunner(stop_on_interrupt=False))
        parent_id, session = await self.running()
        await self.stop(parent_id)
        await wait_for(lambda: session.interrupts == 1)
        before = self.counts()
        with self.assertRaises(LoomError) as caught:
            await self.orchestrator.delegate(self.delegate_params(parent_id))
        self.assertEqual((caught.exception.code, caught.exception.message),
                         ('conflict', f'thread {parent_id} is stopping'))
        self.assertEqual(self.counts(), before)
        await self.orchestrator.drain()
        self.assertEqual([spec.id for spec in self.runner.starts], [parent_id])
        self.assertEqual([info['id'] for info in self.orchestrator.list()], [parent_id])
        self.assertEqual(self.state(parent_id), 'sleeping')
        child = await self.delegate(parent_id)
        await self.orchestrator.drain()
        self.assertEqual(self.state(child['id']), 'running')

    async def test_stop_of_a_settled_thread_changes_nothing(self) -> None:
        parent_id, _ = await self.running()
        child, session = await self.child(parent_id)
        settled = await self.orchestrator.settle({'id': self.next_id(), 'thread_id': parent_id})
        await self.orchestrator.drain()
        events = len(self.events())
        info = await self.stop(parent_id)
        await self.orchestrator.drain()
        self.assertEqual(info, settled)
        self.assertEqual(len(self.events()), events)
        self.assertEqual((self.state(child['id']), self.reason(child['id'])), ('running', None))
        self.assertEqual(session.interrupts, 0)


class StopParamsTest(StopTest):
    async def test_bad_params(self) -> None:
        thread_id, _ = await self.running()
        for value in (None, 5, 'missing'):
            params = {'id': self.next_id(), 'thread_id': value}
            if value == 'missing':
                del params['thread_id']
            with self.subTest(value=value):
                await self.assert_error('invalid', self.orchestrator.stop, params, 'thread_id')
        await self.assert_error('not_found', self.orchestrator.stop,
                                {'id': self.next_id(), 'thread_id': 'nope'})
        await self.assert_error('invalid', self.orchestrator.stop, None, 'params')
        self.assertEqual((self.state(thread_id), self.reason(thread_id)), ('running', None))


if __name__ == '__main__':
    unittest.main()
