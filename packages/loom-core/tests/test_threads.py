import tempfile
import unittest
import uuid
from pathlib import Path

from loom_contracts.calls import ThreadInfo
from loom_contracts.events import EventType
from loom_contracts.states import ThreadState

from loom_core import threads
from loom_core.errors import LoomError
from loom_core.store import Store
from loom_core.threads import ALLOWED, ENDED, LIVE, thread_info, transition

S = ThreadState
EXPECTED = {
    S.QUEUED: {S.STARTING, S.SLEEPING, S.SETTLED, S.FAILED},
    S.STARTING: {S.RUNNING, S.SLEEPING, S.SETTLED, S.FAILED},
    S.RUNNING: {S.IDLE, S.SLEEPING, S.SETTLED, S.FAILED},
    S.IDLE: {S.RUNNING, S.SLEEPING, S.SETTLED, S.FAILED},
    S.SLEEPING: {S.STARTING, S.SETTLED, S.FAILED},
    S.SETTLED: set(),
    S.FAILED: {S.SETTLED},
}


def fields(state: ThreadState = S.QUEUED, **extra: object) -> dict:
    return {
        'id': str(uuid.uuid4()), 'title': 'Work', 'runner': 'claude', 'project_id': 'loom',
        'workspace_strategy': 'root', 'workspace_path': '/repo', 'branch': 'main',
        'base': None, 'model': None, 'permission_mode': 'auto', 'requests': 'any',
        'state': state, **extra,
    }


class ThreadsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(Path(self.tmp.name) / 'data')
        self.addCleanup(self.store.close)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def insert(self, state: ThreadState = S.QUEUED, **extra: object) -> str:
        with self.store.transaction() as tx:
            return threads.insert(self.store, tx, fields(state, **extra))['id']

    def move(self, thread_id: str, state: ThreadState, reason: str | None = None,
             **extra: object) -> None:
        with self.store.transaction() as tx:
            transition(self.store, tx, thread_id, state, reason, **extra)

    def row(self, thread_id: str):
        return threads.fetch(self.store.connection, thread_id)


class AllowedTest(ThreadsTest):
    def test_allowed_is_the_designs_table(self) -> None:
        self.assertEqual({state: set(moves) for state, moves in ALLOWED.items()}, EXPECTED)
        self.assertEqual(ENDED, {S.SETTLED, S.FAILED})
        self.assertEqual(LIVE | ENDED, set(ThreadState))

    def test_every_allowed_move_is_accepted_and_logged(self) -> None:
        for start, moves in ALLOWED.items():
            for state in moves:
                with self.subTest(start=start, state=state):
                    thread_id = self.insert(start)
                    self.move(thread_id, state, 'why')
                    self.assertEqual(self.row(thread_id)['state'], state)
                    logged = self.store.events(thread_id=thread_id)
                    self.assertEqual(logged[1]['type'], EventType.THREAD_STATE)
                    self.assertEqual(logged[1]['data'],
                                     {'state': state, 'waiting': False, 'reason': 'why'})
                    if state is S.FAILED:
                        self.assertEqual(logged[2]['type'], EventType.THREAD_FAILED)
                        self.assertEqual(logged[2]['data'], {'reason': 'why'})
                        self.assertEqual(len(logged), 3)
                    else:
                        self.assertEqual(len(logged), 2)

    def test_moves_not_allowed_are_conflict_and_write_nothing(self) -> None:
        for start in ThreadState:
            for state in ThreadState:
                if state in ALLOWED[start] or (state == start and start in LIVE):
                    continue
                with self.subTest(start=start, state=state):
                    thread_id = self.insert(start)
                    before = self.store.events()
                    with self.assertRaises(LoomError) as caught:
                        self.move(thread_id, state)
                    self.assertEqual(caught.exception.code, 'conflict')
                    self.assertEqual(self.row(thread_id)['state'], start)
                    self.assertEqual(self.store.events(), before)

    def test_named_moves_are_conflict(self) -> None:
        for start, state in ((S.SETTLED, S.RUNNING), (S.SLEEPING, S.RUNNING),
                             (S.FAILED, S.IDLE), (S.SETTLED, S.SETTLED), (S.QUEUED, S.RUNNING),
                             (S.FAILED, S.FAILED), (S.SETTLED, S.FAILED)):
            with self.subTest(start=start, state=state):
                thread_id = self.insert(start)
                with self.assertRaises(LoomError) as caught:
                    self.move(thread_id, state)
                self.assertEqual(caught.exception.code, 'conflict')

    def test_unknown_thread_is_not_found(self) -> None:
        with self.assertRaises(LoomError) as caught:
            self.move(str(uuid.uuid4()), S.STARTING)
        self.assertEqual(caught.exception.code, 'not_found')


class TransitionTest(ThreadsTest):
    def test_insert_writes_the_queued_event(self) -> None:
        thread_id = self.insert()
        self.assertEqual([(e['type'], e['data']) for e in self.store.events()],
                         [(EventType.THREAD_STATE, {'state': 'queued', 'waiting': False})])
        self.assertEqual(self.row(thread_id)['has_session'], 0)

    def test_same_state_only_updates_waiting_and_reason(self) -> None:
        thread_id = self.insert(S.RUNNING)
        self.move(thread_id, S.RUNNING, 'asking', waiting=True)
        row = self.row(thread_id)
        self.assertEqual((row['state'], row['waiting'], row['reason']), ('running', 1, 'asking'))
        self.move(thread_id, S.RUNNING, 'asking')
        self.assertEqual(self.row(thread_id)['waiting'], 1)
        self.assertEqual(self.store.events()[-1]['data'],
                         {'state': 'running', 'waiting': True, 'reason': 'asking'})

    def test_leaving_running_clears_waiting(self) -> None:
        thread_id = self.insert(S.RUNNING, waiting=1)
        self.move(thread_id, S.IDLE)
        row = self.row(thread_id)
        self.assertEqual((row['state'], row['waiting'], row['reason']), ('idle', 0, None))

    def test_waiting_is_only_kept_while_running(self) -> None:
        thread_id = self.insert(S.IDLE)
        self.move(thread_id, S.IDLE, waiting=True)
        self.assertEqual(self.row(thread_id)['waiting'], 0)

    def test_failed_without_a_reason_logs_an_empty_reason(self) -> None:
        thread_id = self.insert()
        self.move(thread_id, S.FAILED)
        self.assertEqual(self.store.events()[-1]['data'], {'reason': ''})

    def test_move_updates_updated_at(self) -> None:
        thread_id = self.insert()
        before = self.row(thread_id)['updated_at']
        self.move(thread_id, S.STARTING)
        self.assertGreaterEqual(self.row(thread_id)['updated_at'], before)
        self.assertEqual(self.row(thread_id)['created_at'], before)


class NarrowerTest(unittest.TestCase):
    def test_the_narrower_mode_wins_in_either_order(self) -> None:
        order = ['approval-required', 'auto-accept-edits', 'auto', 'full-access']
        for i, first in enumerate(order):
            for j, second in enumerate(order):
                with self.subTest(first=first, second=second):
                    self.assertEqual(threads.narrower(first, second), order[min(i, j)])

    def test_an_unknown_mode_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            threads.narrower('sudo', 'auto')


class ThreadInfoTest(ThreadsTest):
    def test_info_has_the_contract_keys_and_the_mode(self) -> None:
        thread_id = self.insert(S.RUNNING, waiting=1, parent_id='p', origin='voice',
                                model='opus', reason='r')
        info = thread_info(self.row(thread_id))
        self.assertEqual(set(info), set(ThreadInfo.__annotations__) | {'permission_mode'})
        self.assertNotIn('project', info)
        self.assertNotIn('parent', info)
        self.assertEqual(info['id'], thread_id)
        self.assertEqual(info['project_id'], 'loom')
        self.assertEqual(info['parent_id'], 'p')
        self.assertIs(info['waiting'], True)
        self.assertIs(info['state'], S.RUNNING)
        self.assertEqual((info['runner'], info['permission_mode'], info['branch'],
                          info['model'], info['reason'], info['origin']),
                         ('claude', 'auto', 'main', 'opus', 'r', 'voice'))

    def test_select_orders_by_created_at_then_id_and_filters(self) -> None:
        first = self.insert(created_at='2026-10-24T10:00:00.000+00:00')
        tied = sorted([self.insert(created_at='2026-10-24T09:00:00.000+00:00'),
                       self.insert(created_at='2026-10-24T09:00:00.000+00:00')])
        other = self.insert(project_id='other', created_at='2026-10-24T11:00:00.000+00:00')
        ids = [row['id'] for row in threads.select(self.store.connection)]
        self.assertEqual(ids, [*tied, first, other])
        self.assertEqual([row['id'] for row in threads.select(self.store.connection, 'other')],
                         [other])


if __name__ == '__main__':
    unittest.main()
