import unittest
from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from loom_scheduler import ScheduleError
from loom_scheduler.timing import (
    check_trigger, first_run, is_late, is_missed, iso, next_fixed_time, run_after,
)

PLUS_TWO = timezone(timedelta(hours=2))
FRIDAY = datetime(2026, 10, 23, 7, 30, tzinfo=UTC)

EVERY_MS = 'trigger.everyMs must be an integer from 60000 to 31536000000'
TIME_OF_DAY = 'trigger.timeOfDay must be H:MM or HH:MM, from 0:00 to 23:59'
WEEKDAYS = 'trigger.weekdays must be a list of integers from 0 (Sunday) to 6 (Saturday)'
AT = 'trigger.at must be an ISO 8601 date and time'
TYPES = 'trigger.type must be one of at, interval, fixed_time, event'


def stockholm() -> ZoneInfo | None:
    try:
        return ZoneInfo('Europe/Stockholm')
    except ZoneInfoNotFoundError:
        return None


class TriggerCheckTest(unittest.TestCase):
    def refused(self, trigger: object) -> str:
        with self.assertRaises(ScheduleError) as caught:
            check_trigger(trigger, PLUS_TWO)
        self.assertEqual(caught.exception.code, 'invalid')
        return caught.exception.message

    def assert_refused(self, cases: list[tuple[object, str]]) -> None:
        for trigger, message in cases:
            with self.subTest(trigger=trigger):
                self.assertEqual(self.refused(trigger), message)

    def test_trigger_must_be_an_object(self) -> None:
        self.assert_refused([
            (None, 'trigger is required'),
            ([], 'trigger must be an object'),
            ('at', 'trigger must be an object'),
        ])

    def test_type_is_checked(self) -> None:
        self.assert_refused([
            ({}, 'trigger.type is required'),
            ({'type': None}, 'trigger.type is required'),
            ({'type': 1}, TYPES),
            ({'type': 'cron'}, TYPES),
            ({'type': 'webhook'}, 'webhook triggers are not served yet'),
        ])

    def test_at_is_checked(self) -> None:
        self.assert_refused([
            ({'type': 'at'}, 'trigger.at is required'),
            ({'type': 'at', 'at': None}, 'trigger.at is required'),
            ({'type': 'at', 'at': 1793000000}, AT),
            ({'type': 'at', 'at': 'tomorrow'}, AT),
        ])

    def test_at_without_offset_is_in_the_scheduler_zone(self) -> None:
        trigger = check_trigger({'type': 'at', 'at': '2026-10-25T09:00'}, PLUS_TWO)
        self.assertEqual(first_run(trigger, FRIDAY, PLUS_TWO),
                         datetime(2026, 10, 25, 7, 0, tzinfo=UTC))

    def test_at_with_offset_keeps_it(self) -> None:
        trigger = check_trigger({'type': 'at', 'at': '2026-10-25T09:00+00:00'}, PLUS_TWO)
        self.assertEqual(first_run(trigger, FRIDAY, PLUS_TWO),
                         datetime(2026, 10, 25, 9, 0, tzinfo=UTC))

    def test_at_in_the_past_is_due_at_that_time(self) -> None:
        trigger = check_trigger({'type': 'at', 'at': '2026-10-01T09:00+00:00'}, PLUS_TWO)
        self.assertEqual(first_run(trigger, FRIDAY, PLUS_TWO),
                         datetime(2026, 10, 1, 9, 0, tzinfo=UTC))
        self.assertIsNone(run_after(trigger, FRIDAY, PLUS_TWO))

    def test_every_ms_is_checked(self) -> None:
        self.assert_refused([
            ({'type': 'interval'}, 'trigger.everyMs is required'),
            ({'type': 'interval', 'everyMs': None}, 'trigger.everyMs is required'),
            ({'type': 'interval', 'everyMs': True}, EVERY_MS),
            ({'type': 'interval', 'everyMs': 60000.0}, EVERY_MS),
            ({'type': 'interval', 'everyMs': '60000'}, EVERY_MS),
            ({'type': 'interval', 'everyMs': 59_999}, EVERY_MS),
            ({'type': 'interval', 'everyMs': 31_536_000_001}, EVERY_MS),
        ])

    def test_every_ms_bounds_are_accepted(self) -> None:
        for every in (60_000, 31_536_000_000):
            with self.subTest(every=every):
                trigger = {'type': 'interval', 'everyMs': every}
                self.assertEqual(check_trigger(trigger, PLUS_TWO), trigger)

    def test_time_of_day_is_checked(self) -> None:
        self.assert_refused([
            ({'type': 'fixed_time'}, 'trigger.timeOfDay is required'),
            ({'type': 'fixed_time', 'timeOfDay': None}, 'trigger.timeOfDay is required'),
            ({'type': 'fixed_time', 'timeOfDay': 900}, TIME_OF_DAY),
            ({'type': 'fixed_time', 'timeOfDay': '24:00'}, TIME_OF_DAY),
            ({'type': 'fixed_time', 'timeOfDay': '9:5'}, TIME_OF_DAY),
            ({'type': 'fixed_time', 'timeOfDay': '09:60'}, TIME_OF_DAY),
            ({'type': 'fixed_time', 'timeOfDay': '٠٩:07'}, TIME_OF_DAY),
        ])

    def test_time_of_day_forms_are_accepted_as_given(self) -> None:
        for given in ('9:07', '09:07', ' 09:07 '):
            with self.subTest(given=given):
                trigger = {'type': 'fixed_time', 'timeOfDay': given}
                self.assertEqual(check_trigger(trigger, PLUS_TWO), trigger)
                self.assertEqual(first_run(trigger, FRIDAY, PLUS_TWO),
                                 datetime(2026, 10, 24, 7, 7, tzinfo=UTC))

    def test_weekdays_are_checked(self) -> None:
        base = {'type': 'fixed_time', 'timeOfDay': '09:07'}
        self.assert_refused([
            ({**base, 'weekdays': None}, 'trigger.weekdays must not be null'),
            ({**base, 'weekdays': '1'}, WEEKDAYS),
            ({**base, 'weekdays': [7]}, WEEKDAYS),
            ({**base, 'weekdays': [-1]}, WEEKDAYS),
            ({**base, 'weekdays': [True]}, WEEKDAYS),
            ({**base, 'weekdays': [1.0]}, WEEKDAYS),
        ])

    def test_weekdays_missing_or_empty_mean_every_day(self) -> None:
        base = {'type': 'fixed_time', 'timeOfDay': '09:07'}
        saturday = datetime(2026, 10, 24, 7, 7, tzinfo=UTC)
        for trigger in (base, {**base, 'weekdays': []}):
            with self.subTest(trigger=trigger):
                self.assertEqual(check_trigger(trigger, PLUS_TWO), trigger)
                self.assertEqual(first_run(trigger, FRIDAY, PLUS_TWO), saturday)

    def test_repeated_weekdays_are_accepted(self) -> None:
        trigger = {'type': 'fixed_time', 'timeOfDay': '09:07', 'weekdays': [1, 1, 5]}
        self.assertEqual(check_trigger(trigger, PLUS_TWO), trigger)

    def test_stored_trigger_keeps_only_its_known_keys(self) -> None:
        self.assertEqual(
            check_trigger({'type': 'interval', 'everyMs': 60_000, 'at': 'x', 'extra': 1},
                          PLUS_TWO),
            {'type': 'interval', 'everyMs': 60_000},
        )
        self.assertEqual(
            check_trigger({'type': 'at', 'at': '2026-10-25T09:00', 'everyMs': 5}, PLUS_TWO),
            {'type': 'at', 'at': '2026-10-25T09:00'},
        )


class NextRunTest(unittest.TestCase):
    def test_weekday_schedule_added_friday_after_the_time_is_next_due_monday(self) -> None:
        trigger = {'type': 'fixed_time', 'timeOfDay': '09:07', 'weekdays': [1, 2, 3, 4, 5]}
        added = datetime(2026, 10, 23, 7, 8, tzinfo=UTC)
        due = first_run(trigger, added, PLUS_TWO)
        self.assertEqual(due, datetime(2026, 10, 26, 7, 7, tzinfo=UTC))
        local = due.astimezone(PLUS_TWO)
        self.assertEqual((local.strftime('%A'), local.hour, local.minute), ('Monday', 9, 7))

    def test_weekday_zero_is_sunday(self) -> None:
        trigger = {'type': 'fixed_time', 'timeOfDay': '09:07', 'weekdays': [0]}
        due = first_run(trigger, FRIDAY, PLUS_TWO)
        self.assertEqual(due.astimezone(PLUS_TWO).strftime('%A'), 'Sunday')
        self.assertEqual(due, datetime(2026, 10, 25, 7, 7, tzinfo=UTC))

    def test_fixed_time_is_strictly_after_the_reference(self) -> None:
        trigger = {'type': 'fixed_time', 'timeOfDay': '09:07'}
        at_the_time = datetime(2026, 10, 23, 7, 7, tzinfo=UTC)
        self.assertEqual(next_fixed_time(trigger, at_the_time, PLUS_TWO),
                         datetime(2026, 10, 24, 7, 7, tzinfo=UTC))
        just_before = at_the_time - timedelta(milliseconds=1)
        self.assertEqual(next_fixed_time(trigger, just_before, PLUS_TWO), at_the_time)

    def test_fixed_time_without_a_zone_uses_the_local_zone(self) -> None:
        trigger = {'type': 'fixed_time', 'timeOfDay': '09:07'}
        due = first_run(trigger, FRIDAY, None)
        local = due.astimezone()
        self.assertEqual((local.hour, local.minute), (9, 7))
        self.assertGreater(due, FRIDAY)
        self.assertLessEqual(due - FRIDAY, timedelta(days=1, hours=1))

    def test_interval_runs_every_ms_after_added_and_after_each_pass(self) -> None:
        trigger = {'type': 'interval', 'everyMs': 90_000}
        self.assertEqual(first_run(trigger, FRIDAY, PLUS_TWO), FRIDAY + timedelta(seconds=90))
        later = FRIDAY + timedelta(hours=5)
        self.assertEqual(run_after(trigger, later, PLUS_TWO), later + timedelta(seconds=90))

    def test_iso_is_utc_with_milliseconds(self) -> None:
        at = datetime(2026, 10, 25, 9, 0, 0, 123456, tzinfo=PLUS_TWO)
        self.assertEqual(iso(at), '2026-10-25T07:00:00.123+00:00')


class LateAndMissedTest(unittest.TestCase):
    due = datetime(2026, 10, 25, 7, 0, tzinfo=UTC)

    def test_exactly_sixty_seconds_is_on_time(self) -> None:
        self.assertFalse(is_late(self.due, self.due + timedelta(seconds=60)))
        self.assertTrue(is_late(self.due, self.due + timedelta(seconds=60, milliseconds=1)))

    def test_fixed_time_is_missed_after_ten_minutes(self) -> None:
        trigger = {'type': 'fixed_time', 'timeOfDay': '09:00'}
        self.assertFalse(is_missed(trigger, self.due, self.due + timedelta(minutes=10)))
        self.assertTrue(
            is_missed(trigger, self.due, self.due + timedelta(minutes=10, milliseconds=1)),
        )

    def test_at_and_interval_are_never_missed(self) -> None:
        for trigger in ({'type': 'at', 'at': '2026-10-25T07:00+00:00'},
                        {'type': 'interval', 'everyMs': 60_000}):
            with self.subTest(trigger=trigger):
                self.assertFalse(is_missed(trigger, self.due, self.due + timedelta(days=3)))


@unittest.skipIf(stockholm() is None, 'Europe/Stockholm is not on this machine')
class DaylightSavingTest(unittest.TestCase):
    def runs(self, time_of_day: str, start: datetime, count: int) -> list[datetime]:
        tz = stockholm()
        trigger = {'type': 'fixed_time', 'timeOfDay': time_of_day}
        found = [first_run(trigger, start, tz)]
        while len(found) < count:
            found.append(run_after(trigger, found[-1] + timedelta(seconds=1), tz))
        return found

    def assert_once_a_day_at(self, runs: list[datetime], hour: int, minute: int) -> None:
        tz = stockholm()
        local = [run.astimezone(tz) for run in runs]
        for run in local:
            self.assertEqual((run.hour, run.minute), (hour, minute))
        days = [run.date() for run in local]
        self.assertEqual(len(days), len(set(days)))
        for before, after in zip(days, days[1:]):
            self.assertEqual(after - before, timedelta(days=1))

    def test_nine_o_clock_around_the_autumn_change(self) -> None:
        runs = self.runs('09:00', datetime(2026, 10, 23, 22, 0, tzinfo=UTC), 3)
        self.assertEqual(runs, [
            datetime(2026, 10, 24, 7, 0, tzinfo=UTC),
            datetime(2026, 10, 25, 8, 0, tzinfo=UTC),
            datetime(2026, 10, 26, 8, 0, tzinfo=UTC),
        ])
        self.assert_once_a_day_at(runs, 9, 0)

    def test_nine_o_clock_around_the_spring_change(self) -> None:
        runs = self.runs('09:00', datetime(2026, 3, 27, 23, 0, tzinfo=UTC), 3)
        self.assertEqual(runs, [
            datetime(2026, 3, 28, 8, 0, tzinfo=UTC),
            datetime(2026, 3, 29, 7, 0, tzinfo=UTC),
            datetime(2026, 3, 30, 7, 0, tzinfo=UTC),
        ])
        self.assert_once_a_day_at(runs, 9, 0)

    def test_repeated_hour_runs_once(self) -> None:
        runs = self.runs('02:30', datetime(2026, 10, 24, 12, 0, tzinfo=UTC), 2)
        self.assertEqual(runs, [
            datetime(2026, 10, 25, 0, 30, tzinfo=UTC),
            datetime(2026, 10, 26, 1, 30, tzinfo=UTC),
        ])


if __name__ == '__main__':
    unittest.main()
