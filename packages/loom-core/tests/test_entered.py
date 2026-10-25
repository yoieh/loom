import unittest

from loom_contracts.events import EventType
from loom_contracts.runners import (
    DispatchMode, Exited, MessageEntered, RunnerEvent, TextOut, TurnStarted,
)

from .fakes import FakeSession, wait_for
from .test_concurrency import KeyTest
from .test_orchestrator import done

QUEUE = DispatchMode.QUEUE
STEER = DispatchMode.STEER
CONTINUATION = 'Your last turn was cut off; continue where you were.'
MARK = 'mark'
TURN = (EventType.TURN_STARTED, EventType.MESSAGE_ENTERED, EventType.TURN_RESULT)


class EnteredTest(KeyTest):
    async def emit(self, thread_id: str, session: FakeSession, *events: RunnerEvent) -> None:
        count = len(self.events(thread_id, EventType.TURN_TEXT))
        for event in events:
            session.emit(event)
        session.emit(TextOut(MARK))
        await wait_for(lambda: len(self.events(thread_id, EventType.TURN_TEXT)) > count)
        await self.orchestrator.drain()

    async def send_as(self, thread_id: str, command_id: str, message: str,
                      **extra: object) -> None:
        await self.orchestrator.send(
            {'id': command_id, 'thread_id': thread_id, 'message': message, **extra})
        await self.orchestrator.drain()

    async def entered_launch(self, **extra: object) -> tuple[str, FakeSession]:
        thread_id, session = await self.running(id='c1', **extra)
        await self.emit(thread_id, session, TurnStarted(), MessageEntered('Hello'))
        return thread_id, session

    def entered(self, thread_id: str) -> list[dict]:
        return [event['data'] for event in self.events(thread_id, EventType.MESSAGE_ENTERED)]

    def entered_ids(self, thread_id: str) -> list[str]:
        return [data['command_id'] for data in self.entered(thread_id)]

    def turn(self, thread_id: str) -> list[str]:
        return [event['type'] for event in self.events(thread_id) if event['type'] in TURN]


class EnterTest(EnteredTest):
    async def test_a_launch_logs_its_message_after_turn_started(self) -> None:
        thread_id, _ = await self.entered_launch()
        self.assertEqual(self.turn(thread_id),
                         [EventType.TURN_STARTED, EventType.MESSAGE_ENTERED])
        self.assertEqual(self.entered(thread_id), [{'command_id': 'c1:message', 'text': 'Hello'}])

    async def test_a_steered_message_enters_before_the_turn_result(self) -> None:
        thread_id, session = await self.entered_launch()
        await self.send_as(thread_id, 'c2', 'more')
        self.assertEqual(session.sent[-1], ('more', STEER))
        self.assertEqual(self.entered_ids(thread_id), ['c1:message'])
        await self.emit(thread_id, session, MessageEntered('more'))
        self.assertEqual(self.entered(thread_id)[-1], {'command_id': 'c2', 'text': 'more'})
        self.assertEqual(self.state(thread_id), 'running')
        await self.finish(thread_id, session)
        self.assertEqual(self.turn(thread_id), [
            EventType.TURN_STARTED, EventType.MESSAGE_ENTERED, EventType.MESSAGE_ENTERED,
            EventType.TURN_RESULT,
        ])
        self.assertEqual(self.states(thread_id), ['queued', 'starting', 'running', 'idle'])

    async def test_a_queued_message_enters_when_its_own_turn_starts(self) -> None:
        thread_id, session = await self.entered_launch()
        await self.send_as(thread_id, 'c3', 'later', mode='queue')
        await self.emit(thread_id, session)
        self.assertEqual(self.entered_ids(thread_id), ['c1:message'])
        await self.finish(thread_id, session)
        self.assertEqual(session.sent[-1], ('later', QUEUE))
        self.assertEqual(self.entered_ids(thread_id), ['c1:message'])
        await self.emit(thread_id, session, TurnStarted(), MessageEntered('later'))
        self.assertEqual(self.entered(thread_id)[-1], {'command_id': 'c3', 'text': 'later'})
        self.assertEqual(self.turn(thread_id)[-3:], [
            EventType.TURN_RESULT, EventType.TURN_STARTED, EventType.MESSAGE_ENTERED,
        ])

    async def test_a_delegated_task_and_its_posted_result_log_their_ids(self) -> None:
        parent_id, parent = await self.entered_launch()
        info, child = await self.child(parent_id, id='d1')
        await self.emit(info['id'], child, TurnStarted(), MessageEntered('Review it'))
        self.assertEqual(self.entered(info['id']),
                         [{'command_id': 'd1:message', 'text': 'Review it'}])
        await self.finish(info['id'], child, done(text='Looks fine.'))
        await self.finish(parent_id, parent)
        posted = parent.sent[-1][0]
        self.assertTrue(posted.startswith(f'Task {info["id"]} '))
        self.assertEqual(self.entered_ids(parent_id), ['c1:message'])
        await self.emit(parent_id, parent, TurnStarted(), MessageEntered(posted))
        self.assertEqual(self.entered(parent_id)[-1],
                         {'command_id': f'{info["id"]}:result', 'text': posted})


class MatchTest(EnteredTest):
    async def test_a_later_entry_drops_the_older_messages_it_skips(self) -> None:
        thread_id, session = await self.entered_launch()
        await self.send_as(thread_id, 'a', 'one')
        await self.send_as(thread_id, 'b', 'two')
        await self.emit(thread_id, session, MessageEntered('two'))
        self.assertEqual(self.entered(thread_id)[-1], {'command_id': 'b', 'text': 'two'})
        await self.emit(thread_id, session, MessageEntered('one'))
        self.assertEqual(self.entered_ids(thread_id), ['c1:message', 'b'])

    async def test_an_entry_with_no_matching_text_takes_the_oldest(self) -> None:
        thread_id, session = await self.entered_launch()
        await self.send_as(thread_id, 'a', 'one')
        await self.send_as(thread_id, 'b', 'two')
        await self.emit(thread_id, session, MessageEntered('other'))
        self.assertEqual(self.entered(thread_id)[-1], {'command_id': 'a', 'text': 'other'})
        await self.emit(thread_id, session, MessageEntered('two'))
        self.assertEqual(self.entered_ids(thread_id), ['c1:message', 'a', 'b'])

    async def test_messages_with_the_same_text_enter_in_send_order(self) -> None:
        thread_id, session = await self.entered_launch()
        await self.send_as(thread_id, 'a', 'same')
        await self.send_as(thread_id, 'b', 'same')
        await self.emit(thread_id, session, MessageEntered('same'), MessageEntered('same'))
        self.assertEqual(self.entered_ids(thread_id), ['c1:message', 'a', 'b'])

    async def test_an_entry_with_no_outstanding_message_logs_nothing(self) -> None:
        thread_id, session = await self.entered_launch()
        self.assertEqual(self.entered_ids(thread_id), ['c1:message'])
        states = self.states(thread_id)
        await self.emit(thread_id, session, MessageEntered('Hello'), MessageEntered('again'))
        self.assertEqual(self.entered_ids(thread_id), ['c1:message'])
        self.assertEqual((self.state(thread_id), self.states(thread_id)), ('running', states))
        await self.finish(thread_id, session)
        states = self.states(thread_id)
        turn = self.turn(thread_id)
        await self.emit(thread_id, session, MessageEntered('idle'))
        self.assertEqual(self.entered_ids(thread_id), ['c1:message'])
        self.assertEqual((self.state(thread_id), self.states(thread_id)), ('idle', states))
        self.assertEqual(self.turn(thread_id), turn)
        self.assertEqual(session.sent, [('Hello', QUEUE)])


class SessionTest(EnteredTest):
    async def test_a_late_entry_from_an_exited_session_logs_nothing(self) -> None:
        thread_id, old = await self.running(id='c1')
        await self.emit(thread_id, old, TurnStarted())
        old.emit(Exited(1))
        old.emit(MessageEntered('Hello'))
        await wait_for(lambda: len(self.runner.starts) == 2
                       and old not in self.orchestrator._pumps)
        await self.orchestrator.drain()
        self.assertEqual(self.entered(thread_id), [])
        self.assertNotIn(old, self.orchestrator._outstanding)
        new = self.runner.sessions[thread_id]
        self.assertIsNot(new, old)
        self.assertEqual(new.sent, [(CONTINUATION, QUEUE)])
        await self.emit(thread_id, new, TurnStarted(), MessageEntered(CONTINUATION))
        self.assertEqual(self.entered(thread_id), [
            {'command_id': f'{thread_id}:continuation:1', 'text': CONTINUATION}])

    async def test_the_list_goes_when_its_session_ends(self) -> None:
        thread_id, session = await self.running()
        await self.send_as(thread_id, 's1', 'outstanding')
        self.assertEqual(len(self.orchestrator._outstanding[session]), 2)
        await self.settle(thread_id)
        self.assertNotIn(session, self.orchestrator._outstanding)
        thread_id, session = await self.idle()
        session.exit(0)
        await wait_for(lambda: self.state(thread_id) == 'sleeping')
        await self.orchestrator.drain()
        self.assertNotIn(session, self.orchestrator._outstanding)
        self.assertEqual(self.orchestrator._outstanding, {})


if __name__ == '__main__':
    unittest.main()
