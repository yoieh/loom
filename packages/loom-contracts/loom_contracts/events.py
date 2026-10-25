from enum import StrEnum
from typing import NotRequired, TypedDict

from .calls import Audience
from .runners import Decision
from .states import ThreadState, TurnState
from .triggers import ScheduleAction


class EventType(StrEnum):
    THREAD_STATE = 'thread.state'
    THREAD_FAILED = 'thread.failed'
    THREAD_UPDATED = 'thread.updated'
    TURN_STARTED = 'turn.started'
    TURN_TEXT = 'turn.text'
    TURN_TOOL = 'turn.tool'
    TURN_RESULT = 'turn.result'
    REQUEST_OPENED = 'request.opened'
    REQUEST_CLOSED = 'request.closed'
    SCHEDULE_FIRED = 'schedule.fired'


class ThreadStateData(TypedDict):
    state: ThreadState
    waiting: NotRequired[bool]
    reason: NotRequired[str]


class ThreadFailedData(TypedDict):
    reason: str


class ThreadUpdatedData(TypedDict):
    audience: Audience
    previous_audience: Audience


class TurnStartedData(TypedDict):
    pass


class TurnTextData(TypedDict):
    text: str


class TurnToolData(TypedDict):
    tool_use_id: str
    name: str
    input: dict[str, object]


class TurnResultData(TypedDict):
    state: TurnState
    text: NotRequired[str]
    reason: NotRequired[str]


class RequestOpenedData(TypedDict):
    request_id: str
    tool_name: str
    input: dict[str, object]
    description: NotRequired[str]


class RequestClosedData(TypedDict):
    request_id: str
    decision: Decision


class ScheduleFiredData(TypedDict):
    schedule_id: str
    action: ScheduleAction
    text: str
    by: NotRequired[str]
    late: NotRequired[bool]


EventData = (
    ThreadStateData
    | ThreadFailedData
    | ThreadUpdatedData
    | TurnStartedData
    | TurnTextData
    | TurnToolData
    | TurnResultData
    | RequestOpenedData
    | RequestClosedData
    | ScheduleFiredData
)


class Event(TypedDict):
    id: int
    type: EventType
    at: str
    thread_id: str | None
    data: EventData


EVENT_DATA: dict[EventType, type] = {
    EventType.THREAD_STATE: ThreadStateData,
    EventType.THREAD_FAILED: ThreadFailedData,
    EventType.THREAD_UPDATED: ThreadUpdatedData,
    EventType.TURN_STARTED: TurnStartedData,
    EventType.TURN_TEXT: TurnTextData,
    EventType.TURN_TOOL: TurnToolData,
    EventType.TURN_RESULT: TurnResultData,
    EventType.REQUEST_OPENED: RequestOpenedData,
    EventType.REQUEST_CLOSED: RequestClosedData,
    EventType.SCHEDULE_FIRED: ScheduleFiredData,
}
