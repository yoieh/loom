import unittest

from loom_contracts.events import EventType
from loom_contracts.runners import DispatchMode
from loom_contracts.states import TurnState

from loom_core.orchestrator import Orchestrator

from .fakes import FakeHost, FakeRunner, ManualClock, wait_for
from .test_concurrency import KeyTest
from .test_orchestrator import START, done

QUEUE = DispatchMode.QUEUE
CONTINUATION = 'Your last turn was cut off; continue where you were.'
ON_RESULT = {'settle': 'on_result'}


class OnResultTest(KeyTest):
    def tail(self, thread_id: str) -> list[tuple[str, dict]]:
        return [(event['type'], event['data']) for event in self.events(thread_id)][-3:]

    def after_settled(self, thread_id: str) -> list[tuple[str, dict]]:
        logged = [(event['type'], event['data']) for event in self.events(thread_id)]
        at = [kind == EventType.THREAD_STATE and data['state'] == 'settled'
              for kind, data in logged].index(True)
        return logged[at:at + 2]

    def settled_events(self, thread_id: str) -> list[dict]:
        return [event['data'] for event in self.events(thread_id, EventType.THREAD_SETTLED)]

    def continuations(self, thread_id: str) -> int:
        return self.store.connection.execute(
            'SELECT count(*) FROM continuations WHERE thread_id = ?', (thread_id,)).fetchone()[0]


class SettleOnResultTest(OnResultTest):
    async def test_a_completed_result_settles_the_thread(self) -> None:
        thread_id, session = await self.running(**ON_RESULT)
        self.assertEqual(self.orchestrator.get(thread_id)['settle'], 'on_result')
        await self.finish(thread_id, session, done(text='All done.'))
        self.assertEqual(self.state(thread_id), 'settled')
        self.assertEqual(self.states(thread_id), ['queued', 'starting', 'running', 'settled'])
        self.assertTrue(session.closed)
        self.assertNotIn(thread_id, self.orchestrator._slots)
        self.assertEqual(self.tail(thread_id), [
            (EventType.TURN_RESULT, {'state': 'completed', 'text': 'All done.'}),
            (EventType.THREAD_STATE, {'state': 'settled', 'waiting': False}),
            (EventType.THREAD_SETTLED, {'result': 'All done.'}),
        ])

    async def test_a_failed_result_settles_the_thread(self) -> None:
        thread_id, session = await self.running(**ON_RESULT)
        await self.finish(thread_id, session,
                          done(TurnState.FAILED, text='Gave up.', reason='error'))
        self.assertEqual(self.state(thread_id), 'settled')
        self.assertTrue(session.closed)
        self.assertEqual(self.settled_events(thread_id), [{'result': 'Gave up.'}])

    async def test_a_stopped_result_does_not_settle_it(self) -> None:
        thread_id, session = await self.running(**ON_RESULT)
        await self.finish(thread_id, session, done(TurnState.STOPPED, text=None))
        self.assertEqual(self.state(thread_id), 'idle')
        self.assertFalse(session.closed)
        self.assertEqual(self.settled_events(thread_id), [])

    async def test_a_result_while_a_stop_is_requested_does_not_settle_it(self) -> None:
        await self.use(FakeRunner(stop_on_interrupt=False))
        thread_id, session = await self.running(**ON_RESULT)
        await self.orchestrator.stop({'id': self.next_id(), 'thread_id': thread_id})
        await wait_for(lambda: session.interrupts == 1)
        await self.finish(thread_id, session, done(text='Late.'))
        info = self.orchestrator.get(thread_id)
        self.assertEqual((info['state'], info['reason']), ('idle', None))
        self.assertEqual(self.settled_events(thread_id), [])

    async def test_a_thread_without_settle_goes_idle(self) -> None:
        thread_id, session = await self.running()
        await self.finish(thread_id, session)
        self.assertEqual(self.state(thread_id), 'idle')
        self.assertFalse(session.closed)
        self.assertEqual(self.settled_events(thread_id), [])

    async def test_queued_messages_and_continuations_are_dropped(self) -> None:
        self.store.clock = ManualClock(START)
        thread_id, session = await self.running(**ON_RESULT)
        session.exit(1)
        await wait_for(lambda: len(self.runner.starts) == 2)
        await self.orchestrator.drain()
        session = self.runner.sessions[thread_id]
        self.assertEqual(session.sent, [(CONTINUATION, QUEUE)])
        self.assertEqual(self.continuations(thread_id), 1)
        await self.send(thread_id, 'after the turn', mode='queue')
        await self.orchestrator.drain()
        self.assertEqual(self.pending(thread_id), [('after the turn', 'queue')])
        await self.finish(thread_id, session)
        self.assertEqual(self.state(thread_id), 'settled')
        self.assertEqual(self.pending(thread_id), [])
        self.assertEqual(self.continuations(thread_id), 0)
        self.assertEqual(session.sent, [(CONTINUATION, QUEUE)])

    async def test_the_slot_is_given_back_to_a_queued_start(self) -> None:
        await self.orchestrator.close()
        self.orchestrator = Orchestrator(self.store, self.projects, {'claude': self.runner},
                                         FakeHost(str(self.data_dir)), agents_max=1)
        thread_id, session = await self.running(**ON_RESULT)
        other = await self.launch()
        await self.orchestrator.drain()
        self.assertEqual(self.state(other['id']), 'queued')
        await self.finish(thread_id, session)
        self.assertEqual(self.state(thread_id), 'settled')
        self.assertNotIn('sleeping', self.states(thread_id))
        self.assertEqual(self.state(other['id']), 'running')

    async def test_settling_on_the_result_frees_the_key(self) -> None:
        thread_id, session = await self.holder(**ON_RESULT)
        waiting = await self.keyed()
        await self.finish(thread_id, session)
        self.assertEqual(self.state(thread_id), 'settled')
        self.assertEqual(self.key_state(thread_id), 'freed')
        self.assertEqual(self.state(waiting['id']), 'running')
        self.assertEqual(self.holders(), [waiting['id']])


class SettledEventTest(OnResultTest):
    async def result_of(self, text: str | None) -> dict:
        thread_id, session = await self.running(**ON_RESULT)
        await self.finish(thread_id, session, done(text=text))
        found = self.settled_events(thread_id)
        self.assertEqual(len(found), 1)
        return found[0]

    async def test_a_result_of_2000_characters_is_kept_whole(self) -> None:
        text = 'a' * 1999 + 'b'
        self.assertEqual(await self.result_of(text), {'result': text})

    async def test_a_result_of_2001_characters_is_cut_to_2000(self) -> None:
        text = 'a' * 2000 + 'b'
        self.assertEqual(await self.result_of(text), {'result': 'a' * 2000})

    async def test_a_result_without_text_gives_an_empty_result(self) -> None:
        self.assertEqual(await self.result_of(None), {'result': ''})

    async def test_threads_settle_sends_it_without_a_result(self) -> None:
        for extra in ({}, ON_RESULT):
            with self.subTest(extra=extra):
                thread_id, _ = await self.running(**extra)
                await self.settle(thread_id)
                self.assertEqual(self.after_settled(thread_id), [
                    (EventType.THREAD_STATE, {'state': 'settled', 'waiting': False}),
                    (EventType.THREAD_SETTLED, {}),
                ])
                self.assertEqual(self.settled_events(thread_id), [{}])

    async def test_settling_a_failed_thread_sends_it_once(self) -> None:
        await self.use(FakeRunner(fail_start=RuntimeError('claude is not installed')))
        thread_id = (await self.launch(**ON_RESULT))['id']
        await self.orchestrator.drain()
        self.assertEqual(self.settled_events(thread_id), [])
        await self.settle(thread_id)
        await self.settle(thread_id)
        self.assertEqual([event['type'] for event in self.events(thread_id)][-2:],
                         [EventType.THREAD_STATE, EventType.THREAD_SETTLED])
        self.assertEqual(self.settled_events(thread_id), [{}])


if __name__ == '__main__':
    unittest.main()
