import unittest
from enum import StrEnum
from typing import get_args, get_type_hints

from loom_contracts.calls import Audience
from loom_contracts.events import (
    EVENT_DATA, Event, EventData, EventType, MessageEnteredData, SignalData, SkipReason,
    ThreadSettledData, TriggerSkippedData,
)
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
    'request.opened': ({'request_id', 'tool_name', 'input'}, {'description', 'expires_at'}),
    'request.closed': ({'request_id', 'decision'}, {'reason'}),
    'schedule.fired': ({'schedule_id', 'action', 'text'}, {'by', 'late', 'cause', 'hops'}),
    'signal': ({'topic', 'payload', 'hops'}, {'project_id', 'key', 'cause', 'by'}),
    'trigger.skipped': ({'schedule_id', 'event_id', 'reason'}, set()),
    'message.entered': ({'command_id', 'text'}, set()),
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


class MessageEnteredTest(unittest.TestCase):
    def test_the_type_and_its_data(self) -> None:
        self.assertEqual(EventType.MESSAGE_ENTERED, 'message.entered')
        self.assertIs(EVENT_DATA[EventType.MESSAGE_ENTERED], MessageEnteredData)
        self.assertIn(MessageEnteredData, get_args(EventData))
        self.assertEqual(MessageEnteredData.__required_keys__, frozenset({'command_id', 'text'}))
        self.assertEqual(MessageEnteredData.__optional_keys__, frozenset())


class SignalTest(unittest.TestCase):
    def test_the_types_and_their_data(self) -> None:
        self.assertEqual((EventType.SIGNAL, EventType.TRIGGER_SKIPPED),
                         ('signal', 'trigger.skipped'))
        self.assertIs(EVENT_DATA[EventType.SIGNAL], SignalData)
        self.assertIs(EVENT_DATA[EventType.TRIGGER_SKIPPED], TriggerSkippedData)
        self.assertIn(SignalData, get_args(EventData))
        self.assertIn(TriggerSkippedData, get_args(EventData))

    def test_skip_reasons(self) -> None:
        self.assertTrue(issubclass(SkipReason, StrEnum))
        self.assertEqual([item.value for item in SkipReason], ['own', 'hops', 'rate'])


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
            ('request.opened', 'expires_at'): str,
            ('request.closed', 'decision'): Decision,
            ('request.closed', 'reason'): str,
            ('schedule.fired', 'action'): ScheduleAction,
            ('schedule.fired', 'late'): bool,
            ('schedule.fired', 'cause'): int,
            ('schedule.fired', 'hops'): int,
            ('signal', 'topic'): str,
            ('signal', 'hops'): int,
            ('signal', 'project_id'): str,
            ('signal', 'key'): str,
            ('signal', 'cause'): int,
            ('signal', 'by'): str,
            ('trigger.skipped', 'schedule_id'): str,
            ('trigger.skipped', 'event_id'): int,
            ('trigger.skipped', 'reason'): SkipReason,
            ('message.entered', 'command_id'): str,
            ('message.entered', 'text'): str,
        }
        for (kind, key), field_type in expected.items():
            with self.subTest(kind=kind, key=key):
                self.assertIs(get_type_hints(EVENT_DATA[EventType(kind)])[key], field_type)

    def test_a_signals_payload_is_an_object(self) -> None:
        self.assertEqual(get_type_hints(SignalData)['payload'], dict[str, object])


if __name__ == '__main__':
    unittest.main()
