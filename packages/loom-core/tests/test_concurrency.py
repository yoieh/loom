import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from loom_contracts.events import EventType
from loom_contracts.runners import DispatchMode, ThreadSpec

from loom_core import threads
from loom_core.errors import LoomError
from loom_core.orchestrator import Orchestrator
from loom_core.projects import Projects
from loom_core.store import DB_NAME, Store

from .fakes import FakeHost, FakeRunner, FakeSession, ManualClock, wait_for
from .test_delegate import DelegateTest
from .test_orchestrator import START
from .test_store import OLD_THREADS
from .test_workspaces import make_repo

QUEUE = DispatchMode.QUEUE
CONTINUATION = 'Your last turn was cut off; continue where you were.'
CRASHED = 'exited mid-turn after 3 continuations in 5 minutes'
WAITING_ELSEWHERE = 'concurrency key k is waiting in another project or under another parent'
KEY_LENGTH = 'concurrency_key must be a string of 1 to 200 characters'
KEY_ROWS = [
    ({'concurrency_key': None}, 'concurrency_key must not be null'),
    ({'concurrency_key': 7}, KEY_LENGTH),
    ({'concurrency_key': ['k']}, KEY_LENGTH),
    ({'concurrency_key': ''}, KEY_LENGTH),
    ({'concurrency_key': 'k' * 201}, KEY_LENGTH),
    ({'concurrency_key': 'k', 'on_busy': None}, 'on_busy must not be null'),
    ({'concurrency_key': 'k', 'on_busy': 'drop'}, 'on_busy must be one of queue, coalesce'),
    ({'concurrency_key': 'k', 'on_busy': 1}, 'on_busy must be one of queue, coalesce'),
    ({'concurrency_key': 'k', 'on_busy': ['queue']}, 'on_busy must be one of queue, coalesce'),
    ({'on_busy': 'queue'}, 'on_busy needs concurrency_key'),
    ({'on_busy': 'coalesce'}, 'on_busy needs concurrency_key'),
]
SETTLE_ROWS = [
    ({'settle': None}, 'settle must not be null'),
    ({'settle': 'on_idle'}, 'settle must be one of on_result'),
    ({'settle': True}, 'settle must be one of on_result'),
]


class FailOnce(FakeRunner):
    async def start(self, thread: ThreadSpec, host: object) -> FakeSession:
        try:
            return await super().start(thread, host)
        finally:
            self.fail_start = None


class KeyTest(DelegateTest):
    async def keyed(self, key: str = 'k', **extra: object) -> dict:
        info = await self.launch(concurrency_key=key, **extra)
        await self.orchestrator.drain()
        return info

    async def holder(self, key: str = 'k', **extra: object) -> tuple[str, FakeSession]:
        thread_id, session = await self.running(concurrency_key=key, **extra)
        return thread_id, session

    async def settle(self, thread_id: str) -> dict:
        info = await self.orchestrator.settle({'id': self.next_id(), 'thread_id': thread_id})
        await self.orchestrator.drain()
        return info

    async def stop(self, thread_id: str) -> dict:
        info = await self.orchestrator.stop({'id': self.next_id(), 'thread_id': thread_id})
        await self.orchestrator.drain()
        return info

    def key_state(self, thread_id: str) -> str | None:
        return self.row(thread_id)['key_state']

    def holders(self, key: str = 'k') -> list[str]:
        return [row[0] for row in self.store.connection.execute(
            "SELECT id FROM threads WHERE concurrency_key = ? AND key_state = 'held'", (key,))]

    def started(self) -> list[str]:
        return [spec.id for spec in self.runner.starts]

    def thread_count(self) -> int:
        return self.store.connection.execute('SELECT count(*) FROM threads').fetchone()[0]

    def first_message(self, thread_id: str) -> str:
        return self.store.connection.execute(
            'SELECT text FROM messages WHERE thread_id = ? ORDER BY seq LIMIT 1',
            (thread_id,)).fetchone()[0]

    async def restart(self) -> None:
        await self.orchestrator.close()
        self.runner = FakeRunner()
        self.orchestrator = self.make(self.runner)
        await self.orchestrator.recover()
        await self.orchestrator.drain()


class HoldTest(KeyTest):
    async def test_a_launch_on_a_free_key_takes_it_and_starts(self) -> None:
        info = await self.launch(concurrency_key='k')
        self.assertEqual((info['state'], info['concurrency_key']), ('queued', 'k'))
        await self.orchestrator.drain()
        self.assertEqual(self.state(info['id']), 'running')
        self.assertEqual(self.runner.sessions[info['id']].sent, [('Hello', QUEUE)])
        self.assertEqual(self.holders(), [info['id']])

    async def test_a_delegate_on_a_free_key_takes_it_and_starts(self) -> None:
        parent_id, _ = await self.running()
        info, session = await self.child(parent_id, concurrency_key='k')
        self.assertEqual(info['concurrency_key'], 'k')
        self.assertEqual(session.sent, [('Review it', QUEUE)])
        self.assertEqual(self.holders(), [info['id']])

    async def test_three_launches_on_one_key_give_one_holder_and_two_queued(self) -> None:
        first, _ = await self.holder()
        second = await self.keyed()
        third = await self.keyed()
        self.assertEqual(self.holders(), [first])
        self.assertEqual([self.state(second['id']), self.state(third['id'])],
                         ['queued', 'queued'])
        self.assertEqual((second['state'], third['state']), ('queued', 'queued'))
        self.assertEqual(self.started(), [first])
        self.assertEqual(self.states(second['id']), ['queued'])

    async def test_another_key_is_not_held_by_this_one(self) -> None:
        first, _ = await self.holder('k')
        other, _ = await self.holder('k2')
        self.assertEqual((self.holders('k'), self.holders('k2')), ([first], [other]))

    async def test_going_idle_or_sleeping_keeps_the_key(self) -> None:
        first, session = await self.holder()
        waiting = await self.keyed()
        await self.finish(first, session)
        self.assertEqual(self.state(first), 'idle')
        self.assertEqual(self.state(waiting['id']), 'queued')
        session.exit(0)
        await wait_for(lambda: self.state(first) == 'sleeping')
        await self.orchestrator.drain()
        self.assertEqual(self.holders(), [first])
        self.assertEqual(self.state(waiting['id']), 'queued')
        self.assertEqual(self.started(), [first])

    async def test_sending_to_a_waiting_thread_does_not_start_it(self) -> None:
        await self.holder()
        waiting = await self.keyed()
        await self.send(waiting['id'], 'also this')
        await self.orchestrator.drain()
        self.assertEqual(self.state(waiting['id']), 'queued')
        self.assertNotIn(waiting['id'], self.started())


class QueueTest(KeyTest):
    async def test_settling_the_holder_starts_the_oldest_waiting_thread(self) -> None:
        first, _ = await self.holder()
        second = await self.keyed()
        third = await self.keyed()
        await self.settle(first)
        self.assertEqual(self.state(second['id']), 'running')
        self.assertEqual(self.runner.sessions[second['id']].sent, [('Hello', QUEUE)])
        self.assertEqual(self.state(third['id']), 'queued')
        self.assertEqual(self.holders(), [second['id']])
        self.assertEqual(self.key_state(first), 'freed')
        await self.settle(second['id'])
        self.assertEqual(self.state(third['id']), 'running')

    async def test_a_tie_on_created_at_goes_to_the_lower_id(self) -> None:
        self.store.clock = ManualClock(START)
        first, _ = await self.holder()
        tied = sorted([(await self.keyed())['id'], (await self.keyed())['id']])
        await self.settle(first)
        self.assertEqual(self.holders(), [tied[0]])
        self.assertEqual(self.state(tied[1]), 'queued')

    async def test_a_failed_holder_frees_the_key(self) -> None:
        first, session = await self.holder()
        waiting = await self.keyed()
        await self.finish(first, session)
        session.fail_send = RuntimeError('pipe closed')
        await self.send(first)
        await self.orchestrator.drain()
        self.assertEqual(self.state(first), 'failed')
        self.assertEqual(self.state(waiting['id']), 'running')
        self.assertEqual(self.holders(), [waiting['id']])

    async def test_a_holder_that_fails_to_boot_frees_the_key(self) -> None:
        await self.use(FailOnce(fail_start=RuntimeError('claude is not installed')))
        first = await self.launch(concurrency_key='k')
        waiting = await self.launch(concurrency_key='k')
        await self.orchestrator.drain()
        self.assertEqual(self.state(first['id']), 'failed')
        self.assertEqual(self.state(waiting['id']), 'running')
        self.assertEqual(self.holders(), [waiting['id']])

    async def test_a_stop_frees_the_key_at_once(self) -> None:
        await self.use(FakeRunner(stop_on_interrupt=False))
        first, session = await self.holder()
        second = await self.keyed()
        third = await self.keyed()
        await self.orchestrator.stop({'id': self.next_id(), 'thread_id': first})
        await wait_for(lambda: session.interrupts == 1)
        await wait_for(lambda: self.state(second['id']) == 'running')
        self.assertEqual(self.orchestrator.get(first)['reason'], 'stopping')
        self.assertEqual(self.state(third['id']), 'queued')
        self.assertEqual(self.holders(), [second['id']])
        await self.orchestrator.close()

    async def test_a_stop_on_an_ancestor_frees_a_childs_key(self) -> None:
        parent_id, _ = await self.running()
        child, _ = await self.child(parent_id, concurrency_key='k')
        waiting = await self.keyed()
        self.assertEqual(self.state(waiting['id']), 'queued')
        await self.stop(parent_id)
        self.assertEqual(self.key_state(child['id']), 'freed')
        self.assertEqual(self.state(waiting['id']), 'running')
        self.assertEqual(self.holders(), [waiting['id']])

    async def test_a_delegate_on_a_held_key_waits_queued(self) -> None:
        await self.holder()
        parent_id, _ = await self.running()
        info = await self.delegate(parent_id, concurrency_key='k')
        await self.orchestrator.drain()
        self.assertEqual(self.state(info['id']), 'queued')
        self.assertNotIn(info['id'], self.started())

    async def test_a_waiting_thread_settled_or_stopped_leaves_the_wait(self) -> None:
        first, _ = await self.holder()
        settled = await self.keyed()
        stopped = await self.keyed()
        await self.settle(settled['id'])
        await self.stop(stopped['id'])
        self.assertEqual(self.state(settled['id']), 'settled')
        self.assertEqual(self.state(stopped['id']), 'sleeping')
        self.assertEqual((self.key_state(settled['id']), self.key_state(stopped['id'])),
                         ('freed', 'freed'))
        await self.settle(first)
        self.assertEqual(self.holders(), [])
        self.assertEqual(self.started(), [first])
        later, _ = await self.holder()
        self.assertEqual(self.holders(), [later])
        await self.send(stopped['id'])
        await self.orchestrator.drain()
        self.assertEqual(self.state(stopped['id']), 'running')
        self.assertEqual(self.holders(), [later])

    async def test_a_missing_on_busy_queues(self) -> None:
        await self.holder()
        second = await self.keyed()
        third = await self.keyed()
        self.assertNotEqual(second['id'], third['id'])
        self.assertEqual(self.first_message(second['id']), 'Hello')


class KeyRestartTest(KeyTest):
    async def test_a_waiting_thread_stays_queued_after_a_restart(self) -> None:
        first, _ = await self.holder()
        waiting = await self.keyed()
        await self.restart()
        self.assertEqual(self.state(first), 'running')
        self.assertEqual(self.runner.sessions[first].sent, [(CONTINUATION, QUEUE)])
        self.assertEqual(self.holders(), [first])
        self.assertEqual(self.state(waiting['id']), 'queued')
        self.assertEqual(self.started(), [first])
        await self.settle(first)
        self.assertEqual(self.state(waiting['id']), 'running')
        self.assertEqual(self.started(), [first, waiting['id']])

    async def test_a_thread_handed_the_key_before_a_restart_starts_on_recover(self) -> None:
        first, _ = await self.holder()
        waiting = await self.keyed()
        with mock.patch.object(self.orchestrator.lanes, 'submit'):
            await self.orchestrator.settle({'id': self.next_id(), 'thread_id': first})
        self.assertEqual(self.holders(), [waiting['id']])
        self.assertEqual(self.state(waiting['id']), 'queued')
        await self.restart()
        self.assertEqual(self.state(waiting['id']), 'running')
        self.assertEqual(self.runner.sessions[waiting['id']].sent, [('Hello', QUEUE)])


class CoalesceTest(KeyTest):
    async def coalesce(self, message: str, **extra: object) -> dict:
        info = await self.launch(concurrency_key='k', on_busy='coalesce', message=message,
                                 **extra)
        await self.orchestrator.drain()
        return info

    async def test_with_no_thread_waiting_a_coalescing_launch_waits(self) -> None:
        first, _ = await self.holder()
        waiting = await self.coalesce('first')
        self.assertNotEqual(waiting['id'], first)
        self.assertEqual(self.state(waiting['id']), 'queued')
        self.assertEqual(self.started(), [first])

    async def test_a_coalescing_launch_on_a_free_key_takes_it(self) -> None:
        info = await self.coalesce('first')
        self.assertEqual(self.state(info['id']), 'running')
        self.assertEqual(self.holders(), [info['id']])

    async def test_joins_add_to_the_first_message_in_order(self) -> None:
        first, _ = await self.holder()
        waiting = await self.coalesce('first')
        count = self.thread_count()
        events = len(self.events())
        second = await self.coalesce('second')
        third = await self.coalesce('third')
        self.assertEqual(second, self.orchestrator.get(waiting['id']))
        self.assertEqual(third['id'], waiting['id'])
        self.assertEqual(self.thread_count(), count)
        self.assertEqual(len(self.events()), events)
        self.assertEqual(self.pending(waiting['id']), [('first\n\nsecond\n\nthird', 'steer')])
        await self.settle(first)
        self.assertEqual(self.runner.sessions[waiting['id']].sent,
                         [('first\n\nsecond\n\nthird', QUEUE)])

    async def test_a_join_goes_to_the_newest_waiting_thread(self) -> None:
        await self.holder()
        older = await self.keyed()
        newer = await self.keyed()
        joined = await self.coalesce('more')
        self.assertEqual(joined['id'], newer['id'])
        self.assertEqual(self.first_message(newer['id']), 'Hello\n\nmore')
        self.assertEqual(self.first_message(older['id']), 'Hello')

    async def test_a_coalescing_delegate_joins_a_sibling(self) -> None:
        await self.holder()
        parent_id, _ = await self.running()
        waiting = await self.delegate(parent_id, 'first', concurrency_key='k')
        joined = await self.delegate(parent_id, 'second', concurrency_key='k',
                                     on_busy='coalesce')
        self.assertEqual(joined['id'], waiting['id'])
        self.assertEqual(self.first_message(waiting['id']), 'first\n\nsecond')

    async def assert_conflict(self, call, params: dict) -> None:
        tables = self.snapshot()
        with self.assertRaises(LoomError) as raised:
            await call(params)
        self.assertEqual((raised.exception.code, raised.exception.message),
                         ('conflict', WAITING_ELSEWHERE))
        self.assertEqual(self.snapshot(), tables)

    def snapshot(self) -> tuple:
        return tuple(
            self.store.connection.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall()
            for table in ('threads', 'messages', 'events', 'commands', 'starts'))

    async def test_a_waiting_thread_in_another_project_is_a_conflict(self) -> None:
        self.projects.add({'id': 'p2', 'title': 'Other',
                           'path': str(make_repo(self.base / 'other'))})
        await self.holder()
        await self.keyed()
        await self.assert_conflict(self.orchestrator.launch, self.launch_params(
            project_id='other', concurrency_key='k', on_busy='coalesce'))

    async def test_a_waiting_thread_under_another_parent_is_a_conflict(self) -> None:
        await self.holder()
        parent_id, _ = await self.running()
        other_id, _ = await self.running()
        await self.delegate(parent_id, concurrency_key='k')
        await self.assert_conflict(self.orchestrator.delegate, self.delegate_params(
            other_id, concurrency_key='k', on_busy='coalesce'))
        await self.assert_conflict(self.orchestrator.launch, self.launch_params(
            concurrency_key='k', on_busy='coalesce'))

    async def test_a_waiting_launch_is_under_no_parent_for_a_delegate(self) -> None:
        await self.holder()
        parent_id, _ = await self.running()
        await self.keyed()
        await self.assert_conflict(self.orchestrator.delegate, self.delegate_params(
            parent_id, concurrency_key='k', on_busy='coalesce'))

    async def test_a_joining_calls_other_fields_do_not_change_the_waiting_thread(self) -> None:
        codex = FakeRunner('codex')
        await self.orchestrator.close()
        self.orchestrator = self.make(self.runner, codex)
        await self.holder()
        waiting = await self.coalesce('first')
        row = tuple(self.row(waiting['id']))
        joined = await self.coalesce(
            'second', title='Other', workspace={'strategy': 'worktree'}, runner='codex',
            model='opus', requests='caller', origin='voice', audience='person',
            settle='on_result')
        self.assertEqual(joined, waiting)
        self.assertEqual(tuple(self.row(waiting['id'])), row)

    async def test_a_retried_join_answers_the_same_thread_and_adds_nothing(self) -> None:
        await self.holder()
        waiting = await self.coalesce('first')
        params = self.launch_params(concurrency_key='k', on_busy='coalesce', message='second')
        joined = await self.orchestrator.launch(params)
        again = await self.orchestrator.launch(params)
        self.assertEqual((joined['id'], again), (waiting['id'], joined))
        self.assertEqual(self.first_message(waiting['id']), 'first\n\nsecond')

    async def test_with_only_coalescing_calls_at_most_one_thread_waits(self) -> None:
        held = await self.coalesce('zero')
        answers = {(await self.coalesce(f'call {n}'))['id'] for n in range(5)}
        self.assertEqual(len(answers), 1)
        self.assertNotIn(held['id'], answers)
        waiting = self.store.connection.execute(
            "SELECT count(*) FROM threads WHERE key_state = 'waiting'").fetchone()[0]
        self.assertEqual((waiting, self.thread_count()), (1, 2))


class KeyCrashTest(KeyTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.clock = ManualClock(START)
        self.store.clock = self.clock

    async def crash(self, thread_id: str) -> None:
        count = len(self.events(thread_id, EventType.TURN_RESULT))
        self.runner.sessions[thread_id].exit(1)
        await wait_for(lambda: len(self.events(thread_id, EventType.TURN_RESULT)) > count)
        await self.orchestrator.drain()

    async def test_a_crash_continuation_keeps_the_key(self) -> None:
        for settle in ({}, {'settle': 'on_result'}):
            with self.subTest(settle=settle):
                first, _ = await self.holder(f'k{len(settle)}', **settle)
                waiting = await self.keyed(f'k{len(settle)}')
                for _ in range(3):
                    await self.crash(first)
                    self.assertEqual(self.state(first), 'running')
                    self.assertEqual(self.runner.sessions[first].sent, [(CONTINUATION, QUEUE)])
                    self.assertEqual(self.state(waiting['id']), 'queued')
                    self.assertEqual(self.holders(f'k{len(settle)}'), [first])
                    self.clock.advance(60)
                await self.crash(first)
                info = self.orchestrator.get(first)
                self.assertEqual((info['state'], info['reason']), ('failed', CRASHED))
                self.assertEqual(self.state(waiting['id']), 'running')
                self.assertEqual(self.holders(f'k{len(settle)}'), [waiting['id']])


class ParamsTest(KeyTest):
    async def assert_invalid(self, call, params: dict, message: str) -> None:
        count = self.thread_count()
        with self.assertRaises(LoomError) as raised:
            await call(params)
        self.assertEqual((raised.exception.code, raised.exception.message),
                         ('invalid', message))
        self.assertEqual(self.thread_count(), count)

    async def test_each_bad_key_or_on_busy_is_invalid_on_launch(self) -> None:
        for extra, message in KEY_ROWS + SETTLE_ROWS:
            with self.subTest(extra=extra):
                await self.assert_invalid(self.orchestrator.launch,
                                          self.launch_params(**extra), message)

    async def test_each_bad_key_or_on_busy_is_invalid_on_delegate(self) -> None:
        parent_id, _ = await self.running()
        for extra, message in KEY_ROWS:
            with self.subTest(extra=extra):
                await self.assert_invalid(self.orchestrator.delegate,
                                          self.delegate_params(parent_id, **extra), message)

    async def test_a_key_of_200_characters_is_taken_and_201_refused(self) -> None:
        parent_id, _ = await self.running()
        launched = await self.launch(concurrency_key='l' * 200)
        delegated = await self.delegate(parent_id, concurrency_key='d' * 200)
        self.assertEqual((launched['concurrency_key'], delegated['concurrency_key']),
                         ('l' * 200, 'd' * 200))
        await self.assert_invalid(self.orchestrator.launch,
                                  self.launch_params(concurrency_key='l' * 201), KEY_LENGTH)
        await self.assert_invalid(self.orchestrator.delegate,
                                  self.delegate_params(parent_id, concurrency_key='d' * 201),
                                  KEY_LENGTH)

    async def test_missing_fields_take_their_defaults(self) -> None:
        info = await self.launch()
        parent_id = info['id']
        child = await self.delegate(parent_id)
        for thread in (info, child):
            self.assertEqual((thread['concurrency_key'], thread['settle']), (None, None))
            self.assertIsNone(self.key_state(thread['id']))
        await self.orchestrator.drain()
        self.assertEqual(self.state(child['id']), 'running')

    async def test_get_and_list_show_the_key_and_settle(self) -> None:
        keyed = await self.launch(concurrency_key='k', settle='on_result')
        plain = await self.launch()
        await self.orchestrator.drain()
        got = self.orchestrator.get(keyed['id'])
        self.assertEqual((got['concurrency_key'], got['settle']), ('k', 'on_result'))
        listed = {info['id']: (info['concurrency_key'], info['settle'])
                  for info in self.orchestrator.list()}
        self.assertEqual(listed, {keyed['id']: ('k', 'on_result'), plain['id']: (None, None)})
        names = list(got)
        self.assertEqual(names[names.index('audience') + 1:names.index('audience') + 3],
                         ['concurrency_key', 'settle'])


class OldStoreTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_store_made_before_keys_shows_no_key_and_no_settle(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        data_dir = Path(tmp.name) / 'data'
        data_dir.mkdir(mode=0o700)
        db = sqlite3.connect(data_dir / DB_NAME)
        db.execute(OLD_THREADS)
        db.execute("INSERT INTO threads (id, title, runner, project_id, workspace_strategy, "
                   "workspace_path, permission_mode, requests, state, created_at, updated_at) "
                   "VALUES ('t1', 'Work', 'claude', 'loom', 'root', '/repo', 'auto', 'any', "
                   "'sleeping', 'now', 'now')")
        db.commit()
        db.close()
        store = Store.open(data_dir)
        self.addCleanup(store.close)
        orchestrator = Orchestrator(store, Projects(store), {'claude': FakeRunner()},
                                    FakeHost(str(data_dir)))
        got = orchestrator.get('t1')
        self.assertEqual((got['concurrency_key'], got['settle']), (None, None))
        self.assertEqual([(info['concurrency_key'], info['settle'])
                          for info in orchestrator.list()], [(None, None)])
        self.assertIsNone(threads.fetch(store.connection, 't1')['key_state'])


if __name__ == '__main__':
    unittest.main()
