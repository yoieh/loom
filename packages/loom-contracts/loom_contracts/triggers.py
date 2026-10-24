from enum import StrEnum
from typing import Literal, NotRequired, TypedDict

MIN_INTERVAL_MS = 60_000


class TriggerType(StrEnum):
    AT = 'at'
    INTERVAL = 'interval'
    FIXED_TIME = 'fixed_time'
    WEBHOOK = 'webhook'


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


Trigger = AtTrigger | IntervalTrigger | FixedTimeTrigger | WebhookTrigger


class ScheduleAction(StrEnum):
    POST = 'post'
    LAUNCH = 'launch'
    NOTIFY = 'notify'
