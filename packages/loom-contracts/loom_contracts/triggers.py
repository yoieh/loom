from enum import StrEnum
from typing import Literal, NotRequired, TypedDict

MIN_INTERVAL_MS = 60_000
HOPS_MAX = 8
RATE_MAX = 30
TRIGGER_EVENT_TYPES = ('thread.settled', 'thread.failed', 'request.opened', 'schedule.fired')
MAX_MATCH = 4_096
MAX_MATCH_FIELDS = 20


class TriggerType(StrEnum):
    AT = 'at'
    INTERVAL = 'interval'
    FIXED_TIME = 'fixed_time'
    WEBHOOK = 'webhook'
    EVENT = 'event'


class AtTrigger(TypedDict):
    type: Literal['at']
    at: str


class IntervalTrigger(TypedDict):
    type: Literal['interval']
    everyMs: int


class FixedTimeTrigger(TypedDict):
    type: Literal['fixed_time']
    timeOfDay: str
    weekdays: NotRequired[list[int]]


class WebhookTrigger(TypedDict):
    type: Literal['webhook']


class EventTrigger(TypedDict):
    type: Literal['event']
    topic: str
    project_id: NotRequired[str]
    match: NotRequired[dict[str, object]]
    hops_max: NotRequired[int]
    rate_max: NotRequired[int]


Trigger = AtTrigger | IntervalTrigger | FixedTimeTrigger | WebhookTrigger | EventTrigger


class ScheduleAction(StrEnum):
    POST = 'post'
    LAUNCH = 'launch'
    NOTIFY = 'notify'
