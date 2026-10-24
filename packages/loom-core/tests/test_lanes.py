import asyncio
import unittest

from loom_core.lanes import Lanes


class LanesTest(unittest.IsolatedAsyncioTestCase):
    async def test_effects_on_one_key_never_overlap_and_finish_in_order(self) -> None:
        lanes = Lanes()
        running = 0
        most = 0
        finished: list[int] = []
        gate = asyncio.Event()

        def effect(number: int):
            async def run() -> int:
                nonlocal running, most
                running += 1
                most = max(most, running)
                if number == 0:
                    await gate.wait()
                await asyncio.sleep(0)
                running -= 1
                finished.append(number)
                return number
            return run

        futures = [lanes.submit('a', effect(number)) for number in range(5)]
        await asyncio.sleep(0.01)
        self.assertEqual(finished, [])
        gate.set()
        self.assertEqual(await asyncio.gather(*futures), [0, 1, 2, 3, 4])
        self.assertEqual(finished, [0, 1, 2, 3, 4])
        self.assertEqual(most, 1)

    async def test_effects_on_two_keys_overlap(self) -> None:
        lanes = Lanes()
        reached = {'a': asyncio.Event(), 'b': asyncio.Event()}

        def effect(mine: str, other: str):
            async def run() -> str:
                reached[mine].set()
                await reached[other].wait()
                return mine
            return run

        both = asyncio.gather(lanes.submit('a', effect('a', 'b')),
                              lanes.submit('b', effect('b', 'a')))
        self.assertEqual(await asyncio.wait_for(both, 2.0), ['a', 'b'])

    async def test_a_raising_effect_fails_only_its_own_future(self) -> None:
        lanes = Lanes()
        ran: list[str] = []

        async def boom() -> None:
            ran.append('boom')
            raise RuntimeError('boom')

        async def after() -> str:
            ran.append('after')
            return 'after'

        failing = lanes.submit('a', boom)
        following = lanes.submit('a', after)
        with self.assertRaisesRegex(RuntimeError, 'boom'):
            await failing
        self.assertEqual(await following, 'after')
        self.assertEqual(ran, ['boom', 'after'])
        self.assertEqual(await lanes.submit('a', after), 'after')

    async def test_drain_returns_once_every_lane_is_empty(self) -> None:
        lanes = Lanes()
        done: list[str] = []

        def effect(name: str, then: str | None = None):
            async def run() -> None:
                await asyncio.sleep(0.01)
                done.append(name)
                if then:
                    lanes.submit(name, effect(then))
            return run

        lanes.submit('a', effect('a', 'a2'))
        lanes.submit('b', effect('b'))
        lanes.submit('a', effect('a1'))
        await asyncio.wait_for(lanes.drain(), 2.0)
        self.assertEqual(sorted(done), ['a', 'a1', 'a2', 'b'])
        self.assertLess(done.index('a1'), done.index('a2'))

    async def test_drain_with_nothing_submitted_returns(self) -> None:
        await asyncio.wait_for(Lanes().drain(), 0.5)

    async def test_a_cancelled_effect_does_not_block_the_lane(self) -> None:
        lanes = Lanes()
        gate = asyncio.Event()

        async def stuck() -> None:
            await gate.wait()

        async def after() -> str:
            return 'after'

        first = lanes.submit('a', stuck)
        second = lanes.submit('a', after)
        await asyncio.sleep(0)
        first.cancel()
        self.assertEqual(await asyncio.wait_for(second, 2.0), 'after')


if __name__ == '__main__':
    unittest.main()
