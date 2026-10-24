import unittest
from enum import StrEnum

from loom_contracts.states import ThreadState, TurnState


class ThreadStateTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertTrue(issubclass(ThreadState, StrEnum))
        self.assertEqual(
            {state.value for state in ThreadState},
            {'queued', 'starting', 'running', 'idle', 'sleeping', 'settled', 'failed'},
        )

    def test_waiting_is_no_thread_state(self) -> None:
        self.assertNotIn('waiting', {state.value for state in ThreadState})

    def test_value_is_its_wire_string(self) -> None:
        self.assertEqual(ThreadState.SLEEPING, 'sleeping')
        self.assertEqual(ThreadState('settled'), ThreadState.SETTLED)


class TurnStateTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertTrue(issubclass(TurnState, StrEnum))
        self.assertEqual(
            {state.value for state in TurnState},
            {'queued', 'running', 'waiting', 'completed', 'failed', 'stopped'},
        )


if __name__ == '__main__':
    unittest.main()
