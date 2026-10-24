import unittest
from enum import StrEnum
from typing import get_args, get_type_hints

from loom_contracts.triggers import (
    MIN_INTERVAL_MS,
    AtTrigger,
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
            {kind.value for kind in TriggerType}, {'at', 'interval', 'fixed_time', 'webhook'},
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

    def test_each_shape_names_its_type(self) -> None:
        shapes = {
            TriggerType.AT: AtTrigger,
            TriggerType.INTERVAL: IntervalTrigger,
            TriggerType.FIXED_TIME: FixedTimeTrigger,
            TriggerType.WEBHOOK: WebhookTrigger,
        }
        for kind, shape in shapes.items():
            with self.subTest(kind=kind):
                self.assertEqual(get_args(get_type_hints(shape)['type']), (kind.value,))

    def test_trigger_is_the_union_of_the_shapes(self) -> None:
        self.assertEqual(
            set(get_args(Trigger)),
            {AtTrigger, IntervalTrigger, FixedTimeTrigger, WebhookTrigger},
        )


class ScheduleActionTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertTrue(issubclass(ScheduleAction, StrEnum))
        self.assertEqual({action.value for action in ScheduleAction}, {'post', 'launch', 'notify'})


class MinIntervalTest(unittest.TestCase):
    def test_min_interval_is_60_seconds(self) -> None:
        self.assertEqual(MIN_INTERVAL_MS, 60_000)


if __name__ == '__main__':
    unittest.main()
