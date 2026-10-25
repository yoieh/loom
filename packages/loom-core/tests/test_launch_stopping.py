import unittest

from loom_core import threads
from loom_core.errors import LoomError
from loom_core.service import Caller, Service

from .fakes import FakeRunner, FakeSession, wait_for
from .test_stop import StopTest


class LaunchStoppingTest(StopTest):
    grace = 0.05

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.use(FakeRunner(stop_on_interrupt=False))

    async def use(self, runner: FakeRunner) -> None:
        await super().use(runner)
        self.service = Service(self.store, self.projects, self.orchestrator)

    async def call(self, params: object, caller: Caller) -> dict:
        return await self.service.method('threads.launch')(params, caller)

    async def refused(self, params: object, caller: Caller) -> tuple[str, str]:
        before = self.counts()
        with self.assertRaises(LoomError) as caught:
            await self.call(params, caller)
        self.assertEqual(self.counts(), before)
        return caught.exception.code, caught.exception.message

    def counts(self) -> tuple[int, ...]:
        starts = self.store.connection.execute('SELECT count(*) FROM starts').fetchone()[0]
        return (*super().counts(), starts)

    async def stopping(self) -> tuple[str, FakeSession]:
        thread_id, session = await self.running()
        await self.stop(thread_id)
        await wait_for(lambda: session.interrupts == 1)
        self.assertEqual(self.reason(thread_id), 'stopping')
        return thread_id, session

    def started(self) -> list[str]:
        return [spec.id for spec in self.runner.starts]

    async def assert_launches(self, caller: Caller) -> dict:
        info = await self.call(self.launch_params(), caller)
        self.assertEqual(info['state'], 'queued')
        await self.orchestrator.drain()
        self.assertEqual(self.started()[-1], info['id'])
        return info


class StoppingCallerTest(LaunchStoppingTest):
    async def test_a_stopping_agent_cannot_launch_until_its_stop_ends(self) -> None:
        thread_id, _ = await self.stopping()
        caller = Caller(thread_id=thread_id)
        self.assertEqual(await self.refused(self.launch_params(), caller),
                         ('conflict', f'thread {thread_id} is stopping'))
        await self.orchestrator.drain()
        self.assertEqual(self.started(), [thread_id])
        self.assertEqual([info['id'] for info in self.orchestrator.list()], [thread_id])
        self.assertEqual(self.state(thread_id), 'sleeping')
        self.assertEqual(threads.fetch(self.store.connection, thread_id)['stop_requested'], 0)
        await self.assert_launches(caller)

    async def test_an_agent_whose_stop_left_it_idle_can_launch(self) -> None:
        await self.use(FakeRunner())
        thread_id, _ = await self.running()
        await self.stop(thread_id)
        await self.orchestrator.drain()
        self.assertEqual((self.state(thread_id), self.reason(thread_id)), ('idle', None))
        await self.assert_launches(Caller(thread_id=thread_id))

    async def test_a_running_agent_without_a_stop_can_launch(self) -> None:
        thread_id, _ = await self.running()
        await self.assert_launches(Caller(thread_id=thread_id))

    async def test_an_unknown_caller_thread_can_launch(self) -> None:
        await self.stopping()
        await self.assert_launches(Caller(thread_id='no-such-thread'))

    async def test_a_person_and_an_unverified_caller_can_launch_during_a_stop(self) -> None:
        await self.stopping()
        for caller in (Caller(), Caller(verified=False)):
            with self.subTest(caller=caller):
                await self.assert_launches(caller)

    async def test_a_schedule_of_a_stopping_thread_can_launch(self) -> None:
        thread_id, _ = await self.stopping()
        await self.assert_launches(Caller(thread_id=thread_id, schedule_id='s1'))


class StoppingRetryTest(LaunchStoppingTest):
    async def test_a_retry_from_before_the_stop_returns_the_first_answer(self) -> None:
        thread_id, session = await self.running()
        caller = Caller(thread_id=thread_id)
        params = self.launch_params()
        first = await self.call(params, caller)
        await self.orchestrator.drain()
        await self.stop(thread_id)
        await wait_for(lambda: session.interrupts == 1)
        started = self.started()
        before = self.counts()
        self.assertEqual(await self.call(dict(params), caller), first)
        self.assertEqual(self.counts(), before)
        self.assertEqual(await self.refused(self.launch_params(), caller),
                         ('conflict', f'thread {thread_id} is stopping'))
        await self.orchestrator.drain()
        self.assertEqual(self.started(), started)

    async def test_an_id_used_for_another_call_still_answers_its_conflict(self) -> None:
        thread_id, session = await self.running()
        await self.stop(thread_id, 'stop-1')
        await wait_for(lambda: session.interrupts == 1)
        self.assertEqual(await self.refused(self.launch_params(id='stop-1'),
                                            Caller(thread_id=thread_id)),
                         ('conflict', 'id was already used for threads.stop'))

    async def test_params_that_are_not_an_object_stay_invalid(self) -> None:
        thread_id, _ = await self.stopping()
        for params in (None, 'launch', ['id']):
            with self.subTest(params=params):
                self.assertEqual(await self.refused(params, Caller(thread_id=thread_id)),
                                 ('invalid', 'params must be an object'))


if __name__ == '__main__':
    unittest.main()
