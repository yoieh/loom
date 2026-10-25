import unittest
from enum import StrEnum
from typing import get_args, get_type_hints

from loom_contracts.calls import Audience
from loom_contracts.events import EVENT_DATA, Event, EventData, EventType, ThreadSettledData
from loom_contracts.runners import Decision
from loom_contracts.states import ThreadState, TurnState
from loom_contracts.triggers import ScheduleAction

DATA: dict[str, tuple[set[str], set[str]]] = {
    'thread.state': ({'state'}, {'waiting', 'reason'}),
    'thread.failed': ({'reason'}, set()),
    'thread.settled': (set(), {'result'}),
    'thread.updated': ({'audience', 'previous_audience'}, set()),
    'turn.started': (set(), set()),
    'turn.text': ({'text'}, set()),
    'turn.tool': ({'tool_use_id', 'name', 'input'}, set()),
    'turn.result': ({'state'}, {'text', 'reason'}),
    'request.opened': ({'request_id', 'tool_name', 'input'}, {'description'}),
    'request.closed': ({'request_id', 'decision'}, set()),
    'schedule.fired': ({'schedule_id', 'action', 'text'}, {'by', 'late'}),
}


class EventTypeTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertTrue(issubclass(EventType, StrEnum))
        self.assertEqual({kind.value for kind in EventType}, set(DATA))


class ThreadSettledTest(unittest.TestCase):
    def test_the_type_and_its_data(self) -> None:
        self.assertEqual(EventType.THREAD_SETTLED, 'thread.settled')
        self.assertIs(EVENT_DATA[EventType.THREAD_SETTLED], ThreadSettledData)
        self.assertIn(ThreadSettledData, get_args(EventData))
        self.assertEqual(ThreadSettledData.__required_keys__, frozenset())
        self.assertEqual(ThreadSettledData.__optional_keys__, frozenset({'result'}))


class EventEnvelopeTest(unittest.TestCase):
    def test_keys(self) -> None:
        self.assertEqual(
            Event.__required_keys__, frozenset({'id', 'type', 'at', 'thread_id', 'data'}),
        )
        self.assertEqual(Event.__optional_keys__, frozenset())

    def test_types(self) -> None:
        hints = get_type_hints(Event)
        self.assertIs(hints['id'], int)
        self.assertIs(hints['type'], EventType)
        self.assertIs(hints['at'], str)
        self.assertEqual(hints['thread_id'], str | None)
        self.assertEqual(hints['data'], EventData)

    def test_data_is_the_union_of_the_data_types(self) -> None:
        self.assertEqual(set(get_args(EventData)), set(EVENT_DATA.values()))


class EventDataTest(unittest.TestCase):
    def test_every_event_type_has_data_and_no_other_key(self) -> None:
        self.assertEqual(set(EVENT_DATA), set(EventType))

    def test_keys(self) -> None:
        for kind, (required, optional) in DATA.items():
            with self.subTest(kind=kind):
                data = EVENT_DATA[EventType(kind)]
                self.assertEqual(data.__required_keys__, frozenset(required))
                self.assertEqual(data.__optional_keys__, frozenset(optional))

    def test_field_types(self) -> None:
        expected = {
            ('thread.state', 'state'): ThreadState,
            ('thread.state', 'waiting'): bool,
            ('thread.updated', 'audience'): Audience,
            ('thread.updated', 'previous_audience'): Audience,
            ('thread.settled', 'result'): str,
            ('turn.result', 'state'): TurnState,
            ('request.closed', 'decision'): Decision,
            ('schedule.fired', 'action'): ScheduleAction,
            ('schedule.fired', 'late'): bool,
        }
        for (kind, key), field_type in expected.items():
            with self.subTest(kind=kind, key=key):
                self.assertIs(get_type_hints(EVENT_DATA[EventType(kind)])[key], field_type)


if __name__ == '__main__':
    unittest.main()
