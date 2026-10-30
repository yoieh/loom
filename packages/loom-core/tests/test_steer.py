import unittest
from unittest import mock

from loom_contracts.events import EventType
from loom_contracts.runners import DispatchMode, MessageEntered

from .fakes import FakeRunner, wait_for
from .test_orchestrator import OrchestratorTest

QUEUE = DispatchMode.QUEUE
STEER = DispatchMode.STEER


class SteerTest(OrchestratorTest):
    async def queued(self, text: str = 'Also this') -> tuple[str, object, str]:
        thread_id, session = await self.running()
        await self.orchestrator.send(
            {'id': 'q1', 'thread_id': thread_id, 'message': text, 'mode': 'queue'})
        await self.orchestrator.drain()
        self.assertEqual(session.sent, [('Hello', QUEUE)])
        return thread_id, session, 'q1'

    async def steer(self, thread_id: str, message_id: str, command_id: str = 's1') -> dict:
        return await self.orchestrator.steer(
            {'id': command_id, 'thread_id': thread_id, 'message_id': message_id})

    async def test_steer_moves_a_queued_message_into_the_running_turn(self) -> None:
        thread_id, session, message_id = await self.queued()
        info = await self.steer(thread_id, message_id)
        self.assertEqual(info['state'], 'running')
        await self.orchestrator.drain()
        self.assertEqual(session.sent, [('Hello', QUEUE), ('Also this', STEER)])
        self.assertEqual(self.pending(thread_id), [])
        session.emit(MessageEntered('Also this'))
        await wait_for(lambda: self.events(thread_id, EventType.MESSAGE_ENTERED))
        entered = self.events(thread_id, EventType.MESSAGE_ENTERED)[-1]
        self.assertEqual(entered['data']['command_id'], message_id)
        await self.finish(thread_id, session)
        self.assertEqual(self.state(thread_id), 'idle')
        self.assertEqual(len(session.sent), 2)

    async def test_steer_skips_messages_queued_before_it(self) -> None:
        thread_id, session = await self.running()
        for command_id, text in (('q1', 'one'), ('q2', 'two')):
            await self.orchestrator.send(
                {'id': command_id, 'thread_id': thread_id, 'message': text, 'mode': 'queue'})
        await self.steer(thread_id, 'q2')
        await self.orchestrator.drain()
        self.assertEqual(session.sent, [('Hello', QUEUE), ('two', STEER)])
        self.assertEqual(self.pending(thread_id), [('one', 'queue')])
        await self.finish(thread_id, session)
        self.assertEqual(session.sent[-1], ('one', QUEUE))

    async def test_retry_returns_the_first_answer_and_steers_once(self) -> None:
        thread_id, session, message_id = await self.queued()
        first = await self.steer(thread_id, message_id)
        await self.orchestrator.drain()
        with mock.patch.object(self.orchestrator.lanes, 'submit') as submit:
            again = await self.steer(thread_id, message_id)
        submit.assert_not_called()
        self.assertEqual(again, first)
        self.assertEqual(session.sent, [('Hello', QUEUE), ('Also this', STEER)])

    async def test_a_message_no_longer_waiting_is_conflict(self) -> None:
        thread_id, session, message_id = await self.queued()
        await self.steer(thread_id, message_id)
        await self.orchestrator.drain()
        await self.assert_error('conflict', self.orchestrator.steer,
                                {'id': 's2', 'thread_id': thread_id, 'message_id': message_id},
                                'not waiting')
        await self.assert_error('conflict', self.orchestrator.steer,
                                {'id': 's3', 'thread_id': thread_id, 'message_id': 'nope'},
                                'not waiting')

    async def test_a_thread_without_a_running_turn_is_conflict(self) -> None:
        thread_id, _ = await self.idle()
        # A message can wait in an idle thread only for a moment, so it is stored directly.
        with self.store.transaction() as tx:
            self.orchestrator._add_message(tx, thread_id, 'w1', 'Later', QUEUE)
        await self.assert_error('conflict', self.orchestrator.steer,
                                {'id': 's1', 'thread_id': thread_id, 'message_id': 'w1'},
                                'no running turn')

    async def test_a_runner_that_cannot_steer_is_conflict(self) -> None:
        await self.use(FakeRunner(capabilities=frozenset({'interrupt', 'resume'})))
        thread_id, session, message_id = await self.queued()
        await self.assert_error('conflict', self.orchestrator.steer,
                                {'id': 's1', 'thread_id': thread_id, 'message_id': message_id},
                                'cannot steer')
        self.assertEqual(self.pending(thread_id), [('Also this', 'queue')])

    async def test_a_turn_that_ends_first_sends_it_as_the_next_turn(self) -> None:
        thread_id, session, message_id = await self.queued()
        with mock.patch.object(self.orchestrator.lanes, 'submit') as submit:
            await self.steer(thread_id, message_id)
        steer_effect = submit.call_args.args[1]
        await self.finish(thread_id, session)
        self.assertEqual(session.sent[-1], ('Also this', QUEUE))
        await steer_effect()
        self.assertEqual(len(session.sent), 2)

    async def test_bad_params_are_invalid_and_unknown_thread_not_found(self) -> None:
        thread_id, _, message_id = await self.queued()
        base = {'id': 's9', 'thread_id': thread_id, 'message_id': message_id}
        for field in ('thread_id', 'message_id'):
            for value in (None, 5):
                with self.subTest(field=field, value=value):
                    await self.assert_error('invalid', self.orchestrator.steer,
                                            {**base, field: value}, field)
            params = dict(base)
            del params[field]
            await self.assert_error('invalid', self.orchestrator.steer, params, field)
        await self.assert_error('not_found', self.orchestrator.steer,
                                {**base, 'thread_id': 'nope'})


if __name__ == '__main__':
    unittest.main()
