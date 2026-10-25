import dataclasses
import unittest
from collections.abc import AsyncIterator
from enum import StrEnum
from typing import get_args

from loom_contracts.runners import (
    Capability,
    Decision,
    DispatchMode,
    Exited,
    MessageEntered,
    PermissionMode,
    PermissionRequest,
    RequestAnswer,
    Runner,
    RunnerEvent,
    RunnerHost,
    RunnerName,
    RunnerSession,
    TextOut,
    ThreadSpec,
    ToolUse,
    TurnResult,
    TurnStarted,
)
from loom_contracts.states import TurnState


def make_spec() -> ThreadSpec:
    return ThreadSpec(
        id='77f465be-0000-4000-8000-000000000000',
        runner=RunnerName.CLAUDE,
        project_id='loom',
        workspace_path='/tmp/loom/worktrees/loom/77f465be',
        title='Add the schedule heap',
        permission_mode=PermissionMode.AUTO,
    )


class EnumTest(unittest.TestCase):
    def test_values(self) -> None:
        expected = {
            RunnerName: {'claude', 'codex', 't3'},
            Capability: {'steer', 'interrupt', 'resume', 'permission_requests'},
            DispatchMode: {'steer', 'queue', 'interrupt'},
            PermissionMode: {'approval-required', 'auto-accept-edits', 'auto', 'full-access'},
            Decision: {'allow', 'deny'},
        }
        for enum, values in expected.items():
            with self.subTest(enum=enum.__name__):
                self.assertTrue(issubclass(enum, StrEnum))
                self.assertEqual({member.value for member in enum}, values)


class ThreadSpecTest(unittest.TestCase):
    def test_fields(self) -> None:
        self.assertEqual(
            [field.name for field in dataclasses.fields(ThreadSpec)],
            [
                'id', 'runner', 'project_id', 'workspace_path', 'title', 'permission_mode',
                'model', 'resume', 'parent_id', 'origin',
            ],
        )

    def test_defaults(self) -> None:
        spec = make_spec()
        self.assertIsNone(spec.model)
        self.assertIs(spec.resume, False)
        self.assertIsNone(spec.parent_id)
        self.assertIsNone(spec.origin)

    def test_frozen(self) -> None:
        spec = make_spec()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            spec.title = 'Something else'


class RequestAnswerTest(unittest.TestCase):
    def test_fields_and_default(self) -> None:
        self.assertEqual(
            [field.name for field in dataclasses.fields(RequestAnswer)], ['decision', 'message'],
        )
        self.assertIsNone(RequestAnswer(Decision.ALLOW).message)

    def test_frozen(self) -> None:
        answer = RequestAnswer(Decision.DENY, 'Not in this repo.')
        with self.assertRaises(dataclasses.FrozenInstanceError):
            answer.decision = Decision.ALLOW


class RunnerEventTest(unittest.TestCase):
    def setUp(self) -> None:
        self.events = {
            TurnStarted: (TurnStarted(), []),
            MessageEntered: (MessageEntered('Reply with the word second'), ['text']),
            TextOut: (TextOut('heron'), ['text']),
            ToolUse: (
                ToolUse('toolu_016V', 'Bash', {'command': 'echo hi'}),
                ['tool_use_id', 'name', 'input'],
            ),
            PermissionRequest: (
                PermissionRequest('2038e810', 'Bash', {'command': 'echo hi > a.txt'}, None),
                ['request_id', 'tool_name', 'input', 'description'],
            ),
            TurnResult: (
                TurnResult(TurnState.COMPLETED, 'OK', None), ['state', 'text', 'reason'],
            ),
            Exited: (Exited(143), ['code']),
        }

    def test_runner_event_is_the_union_of_the_events(self) -> None:
        self.assertEqual(set(get_args(RunnerEvent)), set(self.events))

    def test_fields(self) -> None:
        for cls, (_, names) in self.events.items():
            with self.subTest(event=cls.__name__):
                self.assertEqual([field.name for field in dataclasses.fields(cls)], names)

    def test_frozen(self) -> None:
        for cls, (event, names) in self.events.items():
            with self.subTest(event=cls.__name__):
                self.assertTrue(dataclasses.is_dataclass(event))
                self.assertIsInstance(event, RunnerEvent)
                with self.assertRaises(dataclasses.FrozenInstanceError):
                    setattr(event, names[0] if names else 'text', 'changed')


class FullHost:
    data_dir = '/tmp/loom'

    def setting(self, key: str) -> str | None:
        return None

    def environment(self, thread: ThreadSpec) -> dict[str, str]:
        return {}

    def secret(self, name: str) -> str | None:
        return None


class HostWithoutSecret:
    data_dir = '/tmp/loom'

    def setting(self, key: str) -> str | None:
        return None

    def environment(self, thread: ThreadSpec) -> dict[str, str]:
        return {}


class NoEvents:
    def __aiter__(self) -> 'NoEvents':
        return self

    async def __anext__(self) -> RunnerEvent:
        raise StopAsyncIteration


class FullSession:
    def __init__(self) -> None:
        self.events: AsyncIterator[RunnerEvent] = NoEvents()

    async def send(self, message: str, mode: DispatchMode) -> None:
        pass

    async def respond(self, request_id: str, answer: RequestAnswer) -> None:
        pass

    async def interrupt(self) -> None:
        pass

    async def close(self) -> None:
        pass


class SessionWithoutInterrupt:
    def __init__(self) -> None:
        self.events: AsyncIterator[RunnerEvent] = NoEvents()

    async def send(self, message: str, mode: DispatchMode) -> None:
        pass

    async def respond(self, request_id: str, answer: RequestAnswer) -> None:
        pass

    async def close(self) -> None:
        pass


class FullRunner:
    name = 'claude'
    capabilities = frozenset({Capability.STEER, Capability.INTERRUPT})

    async def start(self, thread: ThreadSpec, host: RunnerHost) -> RunnerSession:
        return FullSession()


class RunnerWithoutStart:
    name = 'claude'
    capabilities = frozenset({Capability.STEER})


class ProtocolTest(unittest.IsolatedAsyncioTestCase):
    def test_host(self) -> None:
        self.assertIsInstance(FullHost(), RunnerHost)
        self.assertNotIsInstance(HostWithoutSecret(), RunnerHost)

    def test_session(self) -> None:
        self.assertIsInstance(FullSession(), RunnerSession)
        self.assertNotIsInstance(SessionWithoutInterrupt(), RunnerSession)

    async def test_runner(self) -> None:
        runner = FullRunner()
        self.assertIsInstance(runner, Runner)
        self.assertNotIsInstance(RunnerWithoutStart(), Runner)
        session = await runner.start(make_spec(), FullHost())
        self.assertIsInstance(session, RunnerSession)
        self.assertEqual([event async for event in session.events], [])


if __name__ == '__main__':
    unittest.main()
