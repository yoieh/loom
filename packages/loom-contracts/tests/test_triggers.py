import unittest
from enum import StrEnum
from typing import get_args, get_type_hints

from loom_contracts.events import EventType
from loom_contracts.triggers import (
    HOPS_MAX,
    MAX_MATCH,
    MAX_MATCH_FIELDS,
    MIN_INTERVAL_MS,
    RATE_MAX,
    TRIGGER_EVENT_TYPES,
    AtTrigger,
    EventTrigger,
    FixedTimeTrigger,
    IntervalTrigger,
    ScheduleAction,
    Trigger,
    TriggerType,
    WebhookTrigger,
)


class TriggerTypeTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertTrue(issubclass(TriggerType, StrEnum))
        self.assertEqual(
            {kind.value for kind in TriggerType},
            {'at', 'interval', 'fixed_time', 'webhook', 'event'},
        )


class TriggerShapeTest(unittest.TestCase):
    def assert_keys(self, shape: type, required: set[str], optional: set[str]) -> None:
        self.assertEqual(shape.__required_keys__, frozenset(required))
        self.assertEqual(shape.__optional_keys__, frozenset(optional))

    def test_at(self) -> None:
        self.assert_keys(AtTrigger, {'type', 'at'}, set())

    def test_interval(self) -> None:
        self.assert_keys(IntervalTrigger, {'type', 'everyMs'}, set())

    def test_fixed_time(self) -> None:
        self.assert_keys(FixedTimeTrigger, {'type', 'timeOfDay'}, {'weekdays'})

    def test_webhook(self) -> None:
        self.assert_keys(WebhookTrigger, {'type'}, set())

    def test_event(self) -> None:
        self.assert_keys(EventTrigger, {'type', 'topic'},
                         {'project_id', 'match', 'hops_max', 'rate_max'})
        hints = get_type_hints(EventTrigger)
        self.assertEqual((hints['topic'], hints['project_id'], hints['match'],
                          hints['hops_max'], hints['rate_max']),
                         (str, str, dict[str, object], int, int))

    def test_each_shape_names_its_type(self) -> None:
        shapes = {
            TriggerType.AT: AtTrigger,
            TriggerType.INTERVAL: IntervalTrigger,
            TriggerType.FIXED_TIME: FixedTimeTrigger,
            TriggerType.WEBHOOK: WebhookTrigger,
            TriggerType.EVENT: EventTrigger,
        }
        for kind, shape in shapes.items():
            with self.subTest(kind=kind):
                self.assertEqual(get_args(get_type_hints(shape)['type']), (kind.value,))

    def test_trigger_is_the_union_of_the_shapes(self) -> None:
        self.assertEqual(
            set(get_args(Trigger)),
            {AtTrigger, IntervalTrigger, FixedTimeTrigger, WebhookTrigger, EventTrigger},
        )


class ScheduleActionTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertTrue(issubclass(ScheduleAction, StrEnum))
        self.assertEqual({action.value for action in ScheduleAction}, {'post', 'launch', 'notify'})


class MinIntervalTest(unittest.TestCase):
    def test_min_interval_is_60_seconds(self) -> None:
        self.assertEqual(MIN_INTERVAL_MS, 60_000)


class EventTriggerLimitsTest(unittest.TestCase):
    def test_limits(self) -> None:
        self.assertEqual((HOPS_MAX, RATE_MAX, MAX_MATCH, MAX_MATCH_FIELDS), (8, 30, 4_096, 20))

    def test_trigger_event_types_are_loom_event_types(self) -> None:
        self.assertEqual(TRIGGER_EVENT_TYPES, ('thread.settled', 'thread.failed',
                                               'request.opened', 'schedule.fired'))
        self.assertTrue(set(TRIGGER_EVENT_TYPES) <= {kind.value for kind in EventType})


if __name__ == '__main__':
    unittest.main()
