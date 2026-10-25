import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from loom_contracts.events import EventType

from loom_core import causes, threads
from loom_core.errors import LoomError
from loom_core.service import Caller
from loom_core.store import DB_NAME, Store

from .test_service import AGENT, PERSON, UNKNOWN, ServiceTest
from .test_store import OLD_MESSAGES, OLD_THREADS

NOT_AN_EVENT_ID = 'cause must be an event id'
NO_STORED_EVENT = 'cause names no stored event'
BAD_CAUSES = [
    (None, 'cause must not be null'),
    (True, NOT_AN_EVENT_ID),
    (False, NOT_AN_EVENT_ID),
    ('5', NOT_AN_EVENT_ID),
    (0, NOT_AN_EVENT_ID),
    (-3, NOT_AN_EVENT_ID),
    (1.0, NOT_AN_EVENT_ID),
    ([5], NOT_AN_EVENT_ID),
    (999_999, NO_STORED_EVENT),
    (2**63, NO_STORED_EVENT),
]


def scheduled(thread_id: str) -> tuple[Caller, ...]:
    return (Caller(thread_id=thread_id, schedule_id='s1'),)


class CauseTest(ServiceTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.patcher = mock.patch.object(self.orchestrator.lanes, 'submit')
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def add_event(self, data: dict, thread_id: str | None = None,
                  kind: EventType = EventType.SCHEDULE_FIRED) -> int:
        with self.store.transaction() as tx:
            return self.store.append_event(tx, kind, thread_id, data)

    def fired(self, hops: object = None, thread_id: str | None = None) -> int:
        data = {'schedule_id': 's1', 'action': 'notify', 'text': 'Go'}
        if hops is not None:
            data['hops'] = hops
        return self.add_event(data, thread_id)

    def set_chain(self, thread_id: str, cause: int | None, hops: int) -> None:
        with self.store.transaction() as tx:
            tx.execute('UPDATE threads SET cause = ?, hops = ? WHERE id = ?',
                       (cause, hops, thread_id))

    def chain(self, thread_id: str) -> tuple[int | None, int]:
        info = self.orchestrator.get(thread_id)
        return info['cause'], info['hops']

    def message_chains(self, thread_id: str) -> list[tuple[int | None, int]]:
        return self.store.connection.execute(
            'SELECT cause, hops FROM messages WHERE thread_id = ? ORDER BY seq',
            (thread_id,)).fetchall()

    async def thread(self, cause: int | None = None, hops: int = 0, **extra: object) -> str:
        info = await self.call('threads.launch', self.launch_params(**extra))
        if cause is not None or hops:
            self.set_chain(info['id'], cause, hops)
        return info['id']

    async def launched(self, caller: Caller = PERSON, **extra: object) -> dict:
        return await self.call('threads.launch', self.launch_params(**extra), caller)

    async def sent(self, thread_id: str, caller: Caller = PERSON, **extra: object) -> dict:
        return await self.call('threads.send', {'id': self.next_id(), 'thread_id': thread_id,
                                                'message': 'More', **extra}, caller)

    def hops_of(self, event_id: int) -> int:
        return causes.event_hops(self.store.connection, event_id)


class EventHopsTest(CauseTest):
    async def test_data_hops_that_is_an_int_wins(self) -> None:
        thread_id = await self.thread(hops=6)
        for hops in (0, 3, 9):
            with self.subTest(hops=hops):
                self.assertEqual(self.hops_of(self.fired(hops)), hops)
                self.assertEqual(self.hops_of(self.fired(hops, thread_id)), hops)

    async def test_data_hops_that_is_not_an_int_falls_back_to_the_thread(self) -> None:
        thread_id = await self.thread(hops=4)
        for hops in (True, False, '7', 2.0, [1], {'n': 1}):
            with self.subTest(hops=hops):
                self.assertEqual(self.hops_of(self.fired(hops, thread_id)), 4)
                self.assertEqual(self.hops_of(self.fired(hops)), 0)

    async def test_missing_data_hops_reads_the_threads_hops_now(self) -> None:
        thread_id = await self.thread(hops=2)
        event_id = self.add_event({'text': 'x'}, thread_id, EventType.TURN_TEXT)
        self.assertEqual(self.hops_of(event_id), 2)
        self.set_chain(thread_id, None, 5)
        self.assertEqual(self.hops_of(event_id), 5)

    async def test_missing_data_hops_and_no_thread_is_zero(self) -> None:
        self.assertEqual(self.hops_of(self.fired()), 0)
        gone = self.add_event({'text': 'x'}, 'no-such-thread', EventType.TURN_TEXT)
        self.assertEqual(self.hops_of(gone), 0)

    async def test_an_unknown_event_is_refused(self) -> None:
        for event_id in (999_999, 2**63, 2**80):
            with self.subTest(event_id=event_id), self.assertRaises(LoomError) as caught:
                self.hops_of(event_id)
            self.assertEqual((caught.exception.code, caught.exception.message),
                             ('invalid', NO_STORED_EVENT))

    async def test_a_launch_counts_one_more_than_each_kind_of_cause(self) -> None:
        thread_id = await self.thread(hops=4)
        for event_id, expected in (
            (self.fired(3), 4),
            (self.fired(True, thread_id), 5),
            (self.fired('7', thread_id), 5),
            (self.add_event({'text': 'x'}, thread_id, EventType.TURN_TEXT), 5),
            (self.fired(), 1),
        ):
            with self.subTest(event_id=event_id):
                info = await self.launched(cause=event_id)
                self.assertEqual((info['cause'], info['hops']), (event_id, expected))


class LaunchTest(CauseTest):
    async def test_a_persons_launch_has_no_cause_and_no_hops(self) -> None:
        for caller in (PERSON, UNKNOWN, AGENT):
            with self.subTest(caller=caller):
                info = await self.launched(caller)
                self.assertEqual((info['cause'], info['hops']), (None, 0))
                self.assertEqual(self.chain(info['id']), (None, 0))
                self.assertEqual(self.message_chains(info['id']), [(None, 0)])

    async def test_a_launch_with_a_cause_records_it_and_one_more_hop(self) -> None:
        event_id = self.fired(2)
        info = await self.launched(cause=event_id)
        self.assertEqual((info['cause'], info['hops']), (event_id, 3))
        self.assertEqual(self.chain(info['id']), (event_id, 3))
        self.assertEqual(self.message_chains(info['id']), [(event_id, 3)])

    async def test_an_agents_launch_without_a_cause_takes_its_threads(self) -> None:
        cause = self.fired(4)
        own = await self.thread(cause, 5)
        info = await self.launched(Caller(thread_id=own))
        self.assertEqual(self.chain(info['id']), (cause, 5))

    async def test_an_agents_launch_with_a_cause_takes_the_higher_count(self) -> None:
        own = await self.thread(self.fired(1), 3)
        high = self.fired(6)
        info = await self.launched(Caller(thread_id=own), cause=high)
        self.assertEqual(self.chain(info['id']), (high, 7))
        low = self.fired(0)
        info = await self.launched(Caller(thread_id=own), cause=low)
        self.assertEqual(self.chain(info['id']), (low, 3))

    async def test_a_schedules_launch_ignores_its_maker_thread(self) -> None:
        own = await self.thread(self.fired(1), 6)
        event_id = self.fired(2)
        for caller in scheduled(own):
            with self.subTest(caller=caller):
                info = await self.launched(caller)
                self.assertEqual(self.chain(info['id']), (None, 0))
                info = await self.launched(caller, cause=event_id)
                self.assertEqual(self.chain(info['id']), (event_id, 3))

    async def test_a_retry_answers_the_first_chain(self) -> None:
        params = self.launch_params(cause=self.fired(1))
        first = await self.call('threads.launch', params)
        again = await self.call('threads.launch', params)
        self.assertEqual(again, first)
        self.assertEqual((first['cause'], first['hops']), (params['cause'], 2))


class DelegateTest(CauseTest):
    async def test_a_child_gets_its_parents_cause_and_hops(self) -> None:
        cause = self.fired(2)
        parent = await self.thread(cause, 3)
        for caller in (PERSON, Caller(thread_id=parent), *scheduled(parent)):
            with self.subTest(caller=caller):
                child = await self.call('threads.delegate', {
                    'id': self.next_id(), 'parent_id': parent, 'task': 'Review'}, caller)
                self.assertEqual((child['cause'], child['hops']), (cause, 3))
                self.assertEqual(self.chain(child['id']), (cause, 3))
                self.assertEqual(self.message_chains(child['id']), [(cause, 3)])

    async def test_an_agent_cannot_lower_its_hops_by_delegating_under_another_parent(
            self) -> None:
        parent = await self.thread()
        cause = self.fired(6)
        own = await self.thread(cause, 7)
        child = await self.call('threads.delegate', {
            'id': self.next_id(), 'parent_id': parent, 'task': 'Review'}, Caller(thread_id=own))
        self.assertEqual(self.chain(child['id']), (cause, 7))


class SendTest(CauseTest):
    async def test_a_persons_send_without_a_cause_records_nothing(self) -> None:
        thread_id = await self.thread(self.fired(1), 2)
        before = self.chain(thread_id)
        info = await self.sent(thread_id)
        self.assertEqual(self.message_chains(thread_id)[-1], (None, 0))
        self.assertEqual(self.chain(thread_id), before)
        self.assertEqual((info['cause'], info['hops']), before)

    async def test_a_send_with_more_hops_raises_its_threads(self) -> None:
        thread_id = await self.thread()
        event_id = self.fired(4)
        info = await self.sent(thread_id, cause=event_id)
        self.assertEqual(self.message_chains(thread_id)[-1], (event_id, 5))
        self.assertEqual(self.chain(thread_id), (event_id, 5))
        self.assertEqual((info['cause'], info['hops']), (event_id, 5))

    async def test_a_send_with_fewer_or_equal_hops_leaves_its_thread(self) -> None:
        cause = self.fired(4)
        thread_id = await self.thread(cause, 5)
        for hops in (0, 4):
            with self.subTest(hops=hops):
                event_id = self.fired(hops)
                info = await self.sent(thread_id, cause=event_id)
                self.assertEqual(self.message_chains(thread_id)[-1], (event_id, hops + 1))
                self.assertEqual(self.chain(thread_id), (cause, 5))
                self.assertEqual((info['cause'], info['hops']), (cause, 5))

    async def test_an_agents_send_carries_its_threads_chain(self) -> None:
        cause = self.fired(2)
        own = await self.thread(cause, 3)
        target = await self.thread()
        await self.sent(target, Caller(thread_id=own))
        self.assertEqual(self.message_chains(target)[-1], (cause, 3))
        self.assertEqual(self.chain(target), (cause, 3))
        low = self.fired(0)
        await self.sent(target, Caller(thread_id=own), cause=low)
        self.assertEqual(self.message_chains(target)[-1], (low, 3))
        self.assertEqual(self.chain(target), (cause, 3))

    async def test_an_agent_cannot_lower_its_own_hops_by_a_send(self) -> None:
        cause = self.fired(5)
        own = await self.thread(cause, 6)
        await self.sent(own, Caller(thread_id=own), cause=self.fired(0))
        await self.sent(own, PERSON, cause=self.fired(0))
        self.assertEqual(self.chain(own), (cause, 6))

    async def test_a_schedules_send_ignores_its_maker_thread(self) -> None:
        own = await self.thread(self.fired(1), 6)
        target = await self.thread()
        event_id = self.fired(1)
        for caller in scheduled(own):
            with self.subTest(caller=caller):
                await self.sent(target, caller)
                self.assertEqual(self.message_chains(target)[-1], (None, 0))
                await self.sent(target, caller, cause=event_id)
                self.assertEqual(self.message_chains(target)[-1], (event_id, 2))
        self.assertEqual(self.chain(target), (event_id, 2))


class JoinTest(CauseTest):
    async def coalesce(self, caller: Caller = PERSON, **extra: object) -> dict:
        return await self.launched(caller, concurrency_key='k', on_busy='coalesce', **extra)

    async def waiting(self) -> str:
        await self.thread(concurrency_key='k')
        waiting = await self.coalesce()
        self.assertEqual(threads.fetch(self.store.connection, waiting['id'])['key_state'],
                         'waiting')
        return waiting['id']

    async def test_a_join_with_more_hops_raises_the_waiting_thread(self) -> None:
        waiting = await self.waiting()
        event_id = self.fired(2)
        joined = await self.coalesce(cause=event_id)
        self.assertEqual(joined['id'], waiting)
        self.assertEqual((joined['cause'], joined['hops']), (event_id, 3))
        self.assertEqual(self.chain(waiting), (event_id, 3))

    async def test_a_join_with_fewer_hops_leaves_the_waiting_thread(self) -> None:
        waiting = await self.waiting()
        cause = self.fired(4)
        self.set_chain(waiting, cause, 5)
        joined = await self.coalesce(cause=self.fired(1))
        self.assertEqual((joined['id'], joined['cause'], joined['hops']), (waiting, cause, 5))
        joined = await self.coalesce()
        self.assertEqual(self.chain(waiting), (cause, 5))

    async def test_an_agents_join_raises_the_waiting_thread_to_its_own(self) -> None:
        waiting = await self.waiting()
        cause = self.fired(3)
        own = await self.thread(cause, 4)
        await self.coalesce(Caller(thread_id=own))
        self.assertEqual(self.chain(waiting), (cause, 4))

    async def test_a_bad_cause_on_a_join_changes_nothing(self) -> None:
        waiting = await self.waiting()
        before = self.counts()
        message = await self.refused('threads.launch', self.launch_params(
            concurrency_key='k', on_busy='coalesce', cause=999_999))
        self.assertEqual(message, NO_STORED_EVENT)
        self.assertEqual(self.counts(), before)
        self.assertEqual(self.chain(waiting), (None, 0))


class RefusalTest(CauseTest):
    async def test_launch_refuses_a_bad_cause_and_writes_nothing(self) -> None:
        self.fired(1)
        for value, expected in BAD_CAUSES:
            with self.subTest(cause=value):
                params = self.launch_params(cause=value)
                before = self.counts()
                self.assertEqual(await self.refused('threads.launch', params), expected)
                self.assertEqual(self.counts(), before)
                self.assertEqual(self.receipts(params['id']), 0)

    async def test_send_refuses_a_bad_cause_and_writes_nothing(self) -> None:
        thread_id = await self.thread()
        for value, expected in BAD_CAUSES:
            with self.subTest(cause=value):
                params = {'id': self.next_id(), 'thread_id': thread_id, 'message': 'More',
                          'cause': value}
                before = self.counts()
                self.assertEqual(await self.refused('threads.send', params), expected)
                self.assertEqual(self.counts(), before)
                self.assertEqual(self.chain(thread_id), (None, 0))


class MigrationTest(unittest.TestCase):
    def test_an_old_store_opens_and_its_rows_have_no_cause_and_no_hops(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / 'data'
            data_dir.mkdir(mode=0o700)
            db = sqlite3.connect(data_dir / DB_NAME)
            db.execute(OLD_THREADS)
            db.execute(OLD_MESSAGES)
            db.execute("INSERT INTO threads (id, title, runner, project_id, workspace_strategy, "
                       "workspace_path, permission_mode, requests, state, created_at, "
                       "updated_at) VALUES ('t1', 'Work', 'claude', 'loom', 'root', '/repo', "
                       "'auto', 'any', 'idle', 'now', 'now')")
            db.execute("INSERT INTO messages (thread_id, command_id, text, mode, created_at) "
                       "VALUES ('t1', 'c1', 'held', 'steer', 'now')")
            db.commit()
            db.close()
            store = Store.open(data_dir)
            try:
                info = threads.thread_info(threads.fetch(store.connection, 't1'))
                self.assertEqual((info['cause'], info['hops']), (None, 0))
                self.assertEqual(store.connection.execute(
                    'SELECT cause, hops FROM messages').fetchall(), [(None, 0)])
                self.assertEqual(causes.thread_chain(store.connection, 't1'), causes.Chain())
            finally:
                store.close()


if __name__ == '__main__':
    unittest.main()
