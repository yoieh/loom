import asyncio
import unittest
from unittest import mock

from loom_contracts.events import EventType
from loom_contracts.runners import DispatchMode

from loom_core import threads
from loom_core.orchestrator import Orchestrator

from .fakes import FakeRunner, ManualClock, wait_for
from .test_delegate import DelegateTest
from .test_orchestrator import START

QUEUE = DispatchMode.QUEUE
CONTINUATION = 'Your last turn was cut off; continue where you were.'
CRASHED = 'exited mid-turn after 3 continuations in 5 minutes'
RESTART = {'state': 'failed', 'reason': 'restart'}
CUT = {'state': 'stopped', 'reason': 'stop'}


class RestartTest(DelegateTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.clock = ManualClock(START)
        self.store.clock = self.clock

    async def stop_server(self) -> FakeRunner:
        await self.orchestrator.close()
        self.runner = FakeRunner()
        self.orchestrator = self.make(self.runner)
        return self.runner

    async def queued(self) -> str:
        with mock.patch.object(self.orchestrator.lanes, 'submit'):
            info = await self.launch()
        self.clock.advance(1)
        return info['id']

    async def stop_unanswered(self, thread_id: str) -> None:
        with mock.patch.object(self.orchestrator.lanes, 'submit'):
            await self.orchestrator.stop({'id': self.next_id(), 'thread_id': thread_id})

    async def sleeping(self) -> str:
        thread_id, session = await self.idle()
        session.exit(0)
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        await self.orchestrator.drain()
        return thread_id

    async def crash(self, thread_id: str) -> None:
        count = len(self.events(thread_id, EventType.TURN_RESULT))
        self.runner.sessions[thread_id].exit(1)
        await wait_for(lambda: len(self.events(thread_id, EventType.TURN_RESULT)) > count)
        await self.orchestrator.drain()

    def results(self, thread_id: str) -> list[dict]:
        return [event['data'] for event in self.events(thread_id, EventType.TURN_RESULT)]

    def row(self, thread_id: str):
        return threads.fetch(self.store.connection, thread_id)

    def started(self, runner: FakeRunner) -> dict[str, bool]:
        return {spec.id: spec.resume for spec in runner.starts}

    def continuations(self, thread_id: str) -> int:
        return self.store.connection.execute(
            'SELECT count(*) FROM continuations WHERE thread_id = ?', (thread_id,)).fetchone()[0]

    def table_rows(self) -> tuple:
        return tuple(
            self.store.connection.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall()
            for table in ('threads', 'messages', 'events', 'continuations', 'commands')
        )


class RecoverTest(RestartTest):
    async def test_each_state_is_recovered(self) -> None:
        await self.use(FakeRunner(stop_on_interrupt=False))
        running, _ = await self.running()
        self.clock.advance(1)
        idle, _ = await self.idle()
        self.clock.advance(1)
        starting = (await self.launch())['id']
        with self.store.transaction() as tx:
            threads.transition(self.store, tx, starting, 'starting')
        await self.orchestrator.drain()
        self.clock.advance(1)
        queued = await self.queued()
        sleeping = await self.sleeping()
        stopping, _ = await self.running()
        await self.orchestrator.stop({'id': self.next_id(), 'thread_id': stopping})
        await wait_for(lambda: self.runner.sessions[stopping].interrupts == 1)
        before = self.runner
        runner = await self.stop_server()
        self.assertEqual([self.state(thread_id) for thread_id in
                          (running, idle, starting, queued, sleeping, stopping)],
                         ['running', 'idle', 'starting', 'queued', 'sleeping', 'running'])
        self.assertEqual(self.row(stopping)['stop_requested'], 1)

        await self.orchestrator.recover()
        self.assertEqual([self.state(thread_id) for thread_id in
                          (running, idle, starting, queued, sleeping, stopping)],
                         ['sleeping', 'sleeping', 'sleeping', 'queued', 'sleeping', 'sleeping'])
        await self.orchestrator.drain()

        self.assertEqual(self.started(runner), {running: True, starting: False, queued: False})
        self.assertEqual(runner.sessions[running].sent, [(CONTINUATION, QUEUE)])
        self.assertEqual(self.results(running), [RESTART])
        self.assertEqual(self.state(running), 'running')
        self.assertEqual(self.state(idle), 'sleeping')
        self.assertEqual(runner.sessions[starting].sent, [('Hello', QUEUE)])
        self.assertEqual(runner.sessions[queued].sent, [('Hello', QUEUE)])
        self.assertEqual((self.state(starting), self.state(queued)), ('running', 'running'))
        self.assertEqual(self.state(sleeping), 'sleeping')
        info = self.orchestrator.get(stopping)
        self.assertEqual((info['state'], info['reason']), ('sleeping', None))
        self.assertEqual(self.row(stopping)['stop_requested'], 0)
        self.assertEqual(self.results(stopping), [CUT])
        self.assertEqual(self.continuations(stopping), 0)
        self.assertEqual(self.continuations(running), 1)
        self.assertEqual(len(before.starts), 4)

    async def test_stopped_queued_and_sleeping_threads_have_their_stop_cleared(self) -> None:
        queued = await self.queued()
        await self.stop_unanswered(queued)
        sleeping = await self.sleeping()
        await self.stop_unanswered(sleeping)
        for thread_id in (queued, sleeping):
            self.assertEqual(self.row(thread_id)['stop_requested'], 1)
            self.assertEqual(self.orchestrator.get(thread_id)['reason'], 'stopping')
        runner = await self.stop_server()
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        for thread_id in (queued, sleeping):
            with self.subTest(thread_id=thread_id):
                info = self.orchestrator.get(thread_id)
                self.assertEqual((info['state'], info['reason']), ('sleeping', None))
                self.assertEqual(self.row(thread_id)['stop_requested'], 0)
        self.assertEqual(runner.starts, [])

    async def test_queued_threads_start_oldest_first(self) -> None:
        first = await self.queued()
        second = await self.queued()
        runner = await self.stop_server()
        with mock.patch.object(self.orchestrator.lanes, 'submit') as submit:
            await self.orchestrator.recover()
        self.assertEqual([call.args[0] for call in submit.call_args_list], [first, second])
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        self.assertEqual([spec.id for spec in runner.starts], [first, second])

    async def test_a_restart_counts_as_a_continuation(self) -> None:
        thread_id, _ = await self.running()
        for _ in range(3):
            await self.crash(thread_id)
        self.assertEqual(self.continuations(thread_id), 3)
        self.assertEqual(self.state(thread_id), 'running')
        self.clock.advance(60)
        runner = await self.stop_server()
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        info = self.orchestrator.get(thread_id)
        self.assertEqual((info['state'], info['reason']), ('failed', CRASHED))
        self.assertEqual(self.events(thread_id, EventType.THREAD_FAILED)[-1]['data'],
                         {'reason': CRASHED})
        self.assertEqual(self.results(thread_id)[-1], RESTART)
        self.assertEqual(runner.starts, [])

    async def test_a_child_failing_on_restart_posts_to_its_parent(self) -> None:
        parent_id, _ = await self.idle()
        info, _ = await self.child(parent_id, 'Review it')
        for _ in range(3):
            await self.crash(info['id'])
        runner = await self.stop_server()
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        self.assertEqual(self.state(info['id']), 'failed')
        self.assertEqual(self.started(runner), {parent_id: True})
        self.assertEqual(runner.sessions[parent_id].sent,
                         [(f'Task {info["id"]} (Review it) failed: {CRASHED}.', QUEUE)])

    async def test_a_running_parent_and_child_both_resume(self) -> None:
        parent_id, _ = await self.running()
        info, _ = await self.child(parent_id)
        runner = await self.stop_server()
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        self.assertEqual(self.started(runner), {parent_id: True, info['id']: True})
        for thread_id in (parent_id, info['id']):
            self.assertEqual(runner.sessions[thread_id].sent, [(CONTINUATION, QUEUE)])

    async def test_recover_twice_adds_one_continuation(self) -> None:
        thread_id, _ = await self.running()
        runner = await self.stop_server()
        await self.orchestrator.recover()
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        self.assertEqual(self.continuations(thread_id), 1)
        self.assertEqual(self.results(thread_id), [RESTART])
        self.assertEqual(len(runner.starts), 1)
        self.assertEqual(runner.sessions[thread_id].sent, [(CONTINUATION, QUEUE)])

    async def test_the_restart_resume_takes_a_narrowed_project_mode(self) -> None:
        thread_id, _ = await self.running()
        self.projects.set_mode({'id': self.next_id(), 'project_id': 'loom',
                                'permission_mode': 'approval-required'})
        runner = await self.stop_server()
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        self.assertEqual([spec.permission_mode for spec in runner.starts], ['approval-required'])
        self.assertEqual(self.orchestrator.get(thread_id)['permission_mode'], 'approval-required')

    async def test_nothing_to_recover_writes_nothing(self) -> None:
        await self.sleeping()
        settled, _ = await self.idle()
        await self.orchestrator.settle({'id': self.next_id(), 'thread_id': settled})
        await self.orchestrator.drain()
        self.runner.fail_start = RuntimeError('no binary')
        failed = (await self.launch())['id']
        await self.orchestrator.drain()
        self.assertEqual(self.state(failed), 'failed')
        runner = await self.stop_server()
        before = self.table_rows()
        statements: list[str] = []
        self.store.connection.set_trace_callback(statements.append)
        try:
            await self.orchestrator.recover()
            await self.orchestrator.drain()
        finally:
            self.store.connection.set_trace_callback(None)
        self.assertEqual(self.table_rows(), before)
        self.assertEqual([s for s in statements if not s.startswith('SELECT')], [])
        self.assertEqual(runner.starts, [])


class DispatchIdTest(RestartTest):
    def last_seq(self) -> int:
        return self.store.connection.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'messages'").fetchone()[0]

    async def post(self, thread_id: str, command_id: str, mode: str) -> int:
        await self.orchestrator.send({'id': command_id, 'thread_id': thread_id,
                                      'message': 'more', 'mode': mode})
        return self.last_seq()

    async def test_a_queued_message_carries_its_command_id_and_seq(self) -> None:
        thread_id, session = await self.running()
        seq = await self.post(thread_id, 'queued-1', 'queue')
        await self.orchestrator.drain()
        self.assertEqual(len(session.command_ids), 1)
        await self.finish(thread_id, session)
        self.assertEqual(session.sent[-1], ('more', QUEUE))
        self.assertEqual(session.command_ids[-1], f'queued-1:{seq}')

    async def test_a_steer_carries_its_command_id_and_seq(self) -> None:
        thread_id, session = await self.running()
        seq = await self.post(thread_id, 'steer-1', 'steer')
        await self.orchestrator.drain()
        self.assertEqual(session.sent[-1], ('more', DispatchMode.STEER))
        self.assertEqual(session.command_ids[-1], f'steer-1:{seq}')

    async def test_an_interrupt_and_its_later_send_carry_one_id(self) -> None:
        thread_id, session = await self.running()
        seq = await self.post(thread_id, 'interrupt-1', 'interrupt')
        await wait_for(lambda: len(session.command_ids) == 2)
        await self.orchestrator.drain()
        self.assertEqual(session.interrupt_ids, [f'interrupt-1:{seq}'])
        self.assertEqual(session.sent[-1], ('more', QUEUE))
        self.assertEqual(session.command_ids[-1], f'interrupt-1:{seq}')

    async def test_a_stop_gives_its_id_to_every_target(self) -> None:
        parent_id, parent = await self.running()
        _, child = await self.child(parent_id)
        await self.orchestrator.stop({'id': 'stop-1', 'thread_id': parent_id})
        await self.orchestrator.drain()
        self.assertEqual(parent.interrupt_ids, ['stop-1'])
        self.assertEqual(child.interrupt_ids, ['stop-1'])

    async def test_a_settle_gives_its_id_to_the_interrupt(self) -> None:
        thread_id, session = await self.running()
        await self.orchestrator.settle({'id': 'settle-1', 'thread_id': thread_id})
        await self.orchestrator.drain()
        self.assertEqual(session.interrupt_ids, ['settle-1'])

    async def test_two_rows_with_one_command_id_get_two_ids(self) -> None:
        thread_id, _ = await self.running()
        await self.crash(thread_id)
        first = self.runner.sessions[thread_id]
        self.clock.advance(301)
        await self.crash(thread_id)
        second = self.runner.sessions[thread_id]
        self.assertEqual(self.continuations(thread_id), 1)
        ids = first.command_ids + second.command_ids
        self.assertEqual(len(ids), 2)
        self.assertEqual({sent.rsplit(':', 1)[0] for sent in ids},
                         {f'{thread_id}:continuation:1'})
        self.assertNotEqual(ids[0], ids[1])

    async def test_a_message_sent_again_after_a_crash_keeps_its_id(self) -> None:
        with mock.patch.object(Orchestrator, '_sent', side_effect=asyncio.CancelledError):
            info = await self.launch()
            await self.orchestrator.drain()
        thread_id = info['id']
        rows = self.store.connection.execute(
            'SELECT seq, command_id FROM messages WHERE thread_id = ?', (thread_id,)).fetchall()
        self.assertEqual(len(rows), 1)
        sent = f'{rows[0][1]}:{rows[0][0]}'
        alive = self.orchestrator.lanes.submit(thread_id, lambda: asyncio.sleep(0, 'alive'))
        self.assertEqual(await alive, 'alive')
        before = self.runner.sessions[thread_id]
        runner = await self.stop_server()
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        self.assertEqual(before.command_ids, [sent])
        self.assertEqual(runner.sessions[thread_id].command_ids, [sent])
        self.assertEqual(self.state(thread_id), 'running')
        self.assertEqual(self.pending(thread_id), [])


if __name__ == '__main__':
    unittest.main()
