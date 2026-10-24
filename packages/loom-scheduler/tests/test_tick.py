import asyncio
import unittest
from datetime import UTC, datetime, timedelta

from loom_contracts.calls import CallName

from loom_scheduler import Tick

from .fakes import START, FakeClock, SchedulerCase


class FakeSource:
    def __init__(self, wake: datetime | None = None, error: Exception | None = None) -> None:
        self.wake = wake
        self.error = error
        self.passes: list[datetime] = []
        self.ran = asyncio.Event()

    def next_wake(self) -> datetime | None:
        return self.wake

    async def run_due(self, now: datetime) -> None:
        self.passes.append(now)
        self.ran.set()
        if self.error is not None:
            raise self.error


class RecordingTick(Tick):
    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock)
        self.timeouts: list[float] = []
        self.waiting = asyncio.Event()

    async def wait(self, timeout: float) -> None:
        self.timeouts.append(timeout)
        self.waiting.set()
        await super().wait(timeout)
        self.waiting.clear()


class DelayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.tick = Tick(self.clock)

    def test_no_source_waits_thirty_seconds(self) -> None:
        self.assertEqual(self.tick.delay(), 30.0)

    def test_source_without_a_next_run_waits_thirty_seconds(self) -> None:
        self.tick.register(FakeSource(None))
        self.assertEqual(self.tick.delay(), 30.0)

    def test_delay_is_the_time_to_the_first_run_capped_at_thirty(self) -> None:
        for seconds, expected in ((10, 10.0), (30, 30.0), (45, 30.0), (-5, 0.0), (0, 0.0)):
            with self.subTest(seconds=seconds):
                tick = Tick(self.clock)
                tick.register(FakeSource(START + timedelta(seconds=seconds)))
                tick.register(FakeSource(START + timedelta(seconds=50)))
                self.assertEqual(tick.delay(), expected)


class RunOnceTest(unittest.IsolatedAsyncioTestCase):
    async def test_only_due_sources_run_each_once(self) -> None:
        clock = FakeClock()
        tick = Tick(clock)
        due = FakeSource(START - timedelta(seconds=1))
        now = FakeSource(START)
        later = FakeSource(START + timedelta(milliseconds=1))
        idle = FakeSource(None)
        for source in (due, now, later, idle):
            tick.register(source)
        await tick.run_once()
        self.assertEqual(due.passes, [START])
        self.assertEqual(now.passes, [START])
        self.assertEqual(later.passes, [])
        self.assertEqual(idle.passes, [])

    async def test_a_failing_source_does_not_stop_the_pass_or_the_loop(self) -> None:
        clock = FakeClock()
        tick = RecordingTick(clock)
        broken = FakeSource(START, RuntimeError('boom'))
        healthy = FakeSource(START)
        tick.register(broken)
        tick.register(healthy)
        loop = asyncio.create_task(tick.run())
        try:
            await asyncio.wait_for(tick.waiting.wait(), 1.0)
            self.assertEqual(len(broken.passes), 1)
            self.assertEqual(len(healthy.passes), 1)
            broken.ran.clear()
            clock.advance(seconds=1)
            tick.wake()
            await asyncio.wait_for(broken.ran.wait(), 1.0)
            self.assertEqual(len(broken.passes), 2)
            self.assertFalse(loop.done())
        finally:
            loop.cancel()
            await asyncio.gather(loop, return_exceptions=True)


class WakeTest(unittest.IsolatedAsyncioTestCase):
    async def test_wake_starts_a_pass_without_waiting_out_the_delay(self) -> None:
        clock = FakeClock()
        tick = RecordingTick(clock)
        source = FakeSource(START + timedelta(seconds=30))
        tick.register(source)
        loop = asyncio.create_task(tick.run())
        try:
            await asyncio.wait_for(tick.waiting.wait(), 1.0)
            self.assertEqual(source.passes, [])
            source.wake = START
            tick.wake()
            await asyncio.wait_for(source.ran.wait(), 0.1)
            self.assertEqual(source.passes, [START])
            await asyncio.wait_for(tick.waiting.wait(), 0.1)
            self.assertEqual(tick.timeouts[0], 30.0)
            self.assertTrue(all(0 <= timeout <= 30.0 for timeout in tick.timeouts))
        finally:
            loop.cancel()
            await asyncio.gather(loop, return_exceptions=True)

    async def test_wait_ends_after_its_timeout(self) -> None:
        tick = Tick(FakeClock())
        await asyncio.wait_for(tick.wait(0.01), 0.1)


class ClockJumpTest(SchedulerCase):
    async def add_notify(self, command_id: str, trigger: dict) -> dict:
        return await self.add(command_id, trigger=trigger, action='notify', text=command_id)

    async def test_runs_due_in_a_jump_follow_the_missed_run_rules(self) -> None:
        interval = await self.add_notify('interval', {'type': 'interval', 'everyMs': 600_000})
        fixed = await self.add_notify('fixed', {'type': 'fixed_time', 'timeOfDay': '10:00'})
        once = await self.add_notify('once', {'type': 'at', 'at': '2026-10-23T07:30+00:00'})
        later = await self.add_notify('later', {'type': 'at', 'at': '2026-10-23T12:00+00:00'})
        self.assertEqual(fixed['next_run'], '2026-10-23T08:00:00.000+00:00')
        self.assertEqual(self.scheduler.tick.delay(), 30.0)
        self.clock.advance(hours=3)
        self.assertEqual(self.scheduler.tick.delay(), 0.0)
        await self.scheduler.tick.run_once()
        runs = {row[0]: row[1:] for row in self.host.rows(
            'SELECT schedule_id, outcome, late, COUNT(*) FROM schedule_runs '
            'GROUP BY schedule_id')}
        self.assertEqual(runs, {
            interval['id']: ('fired', 1, 1),
            fixed['id']: ('skipped_late', 1, 1),
            once['id']: ('fired', 1, 1),
        })
        now = datetime(2026, 10, 23, 10, 0, tzinfo=UTC)
        next_runs = dict(self.host.rows('SELECT id, next_run FROM schedules'))
        self.assertEqual(next_runs, {
            interval['id']: '2026-10-23T10:10:00.000+00:00',
            fixed['id']: '2026-10-24T08:00:00.000+00:00',
            once['id']: None,
            later['id']: '2026-10-23T12:00:00.000+00:00',
        })
        for next_run in next_runs.values():
            if next_run is not None:
                self.assertGreater(datetime.fromisoformat(next_run), now)
        self.assertEqual(self.host.calls_of(CallName.THREADS_SEND), [])
        self.assertEqual(len(self.host.events), 2)


if __name__ == '__main__':
    unittest.main()
