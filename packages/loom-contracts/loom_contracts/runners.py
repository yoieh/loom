from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from .states import TurnState


class RunnerName(StrEnum):
    CLAUDE = 'claude'
    CODEX = 'codex'
    T3 = 't3'


class Capability(StrEnum):
    STEER = 'steer'
    INTERRUPT = 'interrupt'
    RESUME = 'resume'
    PERMISSION_REQUESTS = 'permission_requests'


class DispatchMode(StrEnum):
    STEER = 'steer'
    QUEUE = 'queue'
    INTERRUPT = 'interrupt'


class PermissionMode(StrEnum):
    APPROVAL_REQUIRED = 'approval-required'
    AUTO_ACCEPT_EDITS = 'auto-accept-edits'
    AUTO = 'auto'
    FULL_ACCESS = 'full-access'


class Decision(StrEnum):
    ALLOW = 'allow'
    DENY = 'deny'


@dataclass(frozen=True)
class ThreadSpec:
    id: str
    runner: RunnerName
    project_id: str
    workspace_path: str
    title: str
    permission_mode: PermissionMode
    model: str | None = None
    resume: bool = False
    parent_id: str | None = None
    origin: str | None = None


@dataclass(frozen=True)
class RequestAnswer:
    decision: Decision
    message: str | None = None


@dataclass(frozen=True)
class TurnStarted:
    pass


@dataclass(frozen=True)
class MessageEntered:
    text: str


@dataclass(frozen=True)
class TextOut:
    text: str


@dataclass(frozen=True)
class ToolUse:
    tool_use_id: str
    name: str
    input: dict[str, object]


@dataclass(frozen=True)
class PermissionRequest:
    request_id: str
    tool_name: str
    input: dict[str, object]
    description: str | None


@dataclass(frozen=True)
class TurnResult:
    state: TurnState
    text: str | None
    reason: str | None


@dataclass(frozen=True)
class Exited:
    code: int | None


RunnerEvent = (TurnStarted | MessageEntered | TextOut | ToolUse | PermissionRequest
               | TurnResult | Exited)


@runtime_checkable
class RunnerHost(Protocol):
    data_dir: str

    def setting(self, key: str) -> str | None: ...

    def environment(self, thread: ThreadSpec) -> dict[str, str]: ...

    def secret(self, name: str) -> str | None: ...


@runtime_checkable
class RunnerSession(Protocol):
    events: AsyncIterator[RunnerEvent]

    async def send(self, message: str, mode: DispatchMode) -> None: ...

    async def respond(self, request_id: str, answer: RequestAnswer) -> None: ...

    async def interrupt(self) -> None: ...

    async def close(self) -> None: ...


@runtime_checkable
class Runner(Protocol):
    name: str
    capabilities: frozenset[str]

    async def start(self, thread: ThreadSpec, host: RunnerHost) -> RunnerSession: ...
