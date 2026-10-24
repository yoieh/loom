import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from loom_contracts.events import EventType

from loom_core.errors import LoomError
from loom_core.store import EVENT_DAYS, Store

START = datetime(2026, 10, 24, 13, 8, 27, tzinfo=UTC)


class PinnedClock:
    def __init__(self, now: datetime = START) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class EventsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name) / 'data'
        self.clock = PinnedClock()
        self.store = self.open_store()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def open_store(self) -> Store:
        store = Store.open(self.data_dir, self.clock)
        self.addCleanup(store.close)
        return store

    def append(self, thread_id: str | None = 't1', text: str = 'hi') -> int:
        with self.store.transaction() as tx:
            return self.store.append_event(tx, EventType.TURN_TEXT, thread_id, {'text': text})

    def test_append_and_read(self) -> None:
        event_id = self.append(text='hello')
        self.assertEqual(self.store.events(), [{
            'id': event_id,
            'type': 'turn.text',
            'at': '2026-10-24T13:08:27.000+00:00',
            'thread_id': 't1',
            'data': {'text': 'hello'},
        }])
        self.assertIs(self.store.events()[0]['type'], EventType.TURN_TEXT)

    def test_unknown_type_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            with self.store.transaction() as tx:
                self.store.append_event(tx, 'thread.exploded', None, {})
        self.assertEqual(self.store.events(), [])

    def test_ids_grow(self) -> None:
        ids = [self.append() for _ in range(3)]
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(len(set(ids)), 3)

    def test_ids_grow_after_a_prune_removes_every_event(self) -> None:
        ids = [self.append() for _ in range(3)]
        self.clock.now = START + timedelta(days=EVENT_DAYS, milliseconds=1)
        self.assertEqual(self.store.prune_events(), 3)
        self.assertEqual(self.store.events(), [])
        self.assertGreater(self.append(), max(ids))

    def test_after_returns_later_events_in_order(self) -> None:
        ids = [self.append(text=str(n)) for n in range(5)]
        events = self.store.events(after=ids[1])
        self.assertEqual([e['id'] for e in events], ids[2:])
        self.assertEqual([e['data']['text'] for e in events], ['2', '3', '4'])

    def test_thread_filter(self) -> None:
        first = self.append('t1')
        self.append('t2')
        third = self.append('t1')
        self.append(None)
        self.assertEqual([e['id'] for e in self.store.events(thread_id='t1')], [first, third])
        self.assertEqual(len(self.store.events(thread_id=None)), 4)
        self.assertEqual(self.store.events(thread_id='t9'), [])

    def test_limit_caps_the_count(self) -> None:
        ids = [self.append() for _ in range(4)]
        self.assertEqual([e['id'] for e in self.store.events(limit=2)], ids[:2])
        self.assertEqual([e['id'] for e in self.store.events(after=ids[1], limit=1)], [ids[2]])

    def test_limit_is_at_most_1000(self) -> None:
        with self.store.transaction() as tx:
            for _ in range(1001):
                self.store.append_event(tx, EventType.TURN_STARTED, None, {})
        self.assertEqual(len(self.store.events(limit=5000)), 1000)
        self.assertEqual(len(self.store.events()), 1000)

    def test_after_defaults_to_zero(self) -> None:
        ids = [self.append() for _ in range(2)]
        self.assertEqual([e['id'] for e in self.store.events()], ids)
        self.assertEqual(self.store.events(), self.store.events(after=0))

    def test_bad_arguments_are_invalid(self) -> None:
        self.append()
        cases = [
            {'after': None}, {'after': '3'}, {'after': True}, {'after': -1}, {'after': 1.0},
            {'after': 2**63}, {'thread_id': 5}, {'thread_id': ['t1']},
            {'limit': 0}, {'limit': True}, {'limit': '5'},
        ]
        for kwargs in cases:
            with self.subTest(**{k: repr(v) for k, v in kwargs.items()}):
                with self.assertRaises(LoomError) as caught:
                    self.store.events(**kwargs)
                self.assertEqual(caught.exception.code, 'invalid')
                self.assertIn(next(iter(kwargs)), caught.exception.message)

    def test_prune_keeps_an_event_exactly_30_days_old(self) -> None:
        self.append()
        self.clock.now = START + timedelta(days=EVENT_DAYS)
        self.assertEqual(self.store.prune_events(), 0)
        self.assertEqual(len(self.store.events()), 1)

    def test_prune_drops_an_event_30_days_and_1_ms_old(self) -> None:
        self.append()
        self.clock.now = START + timedelta(days=EVENT_DAYS, milliseconds=1)
        self.assertEqual(self.store.prune_events(), 1)
        self.assertEqual(self.store.events(), [])

    def test_prune_keeps_newer_events(self) -> None:
        self.append(text='old')
        self.clock.now = START + timedelta(days=1)
        self.append(text='new')
        self.clock.now = START + timedelta(days=EVENT_DAYS, seconds=1)
        self.assertEqual(self.store.prune_events(), 1)
        self.assertEqual([e['data']['text'] for e in self.store.events()], ['new'])

    def test_open_prunes_old_events(self) -> None:
        self.append(text='old')
        self.clock.now = START + timedelta(days=2)
        self.append(text='new')
        self.store.close()
        self.clock.now = START + timedelta(days=EVENT_DAYS, hours=1)
        store = self.open_store()
        self.assertEqual([e['data']['text'] for e in store.events()], ['new'])


if __name__ == '__main__':
    unittest.main()
