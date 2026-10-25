import json
import re
from datetime import UTC, date, datetime, timedelta, tzinfo

from loom_contracts.calls import MAX_TOPIC, TOPIC_PATTERN
from loom_contracts.events import EventType
from loom_contracts.triggers import (
    HOPS_MAX, MAX_MATCH, MAX_MATCH_FIELDS, MIN_INTERVAL_MS, RATE_MAX, TRIGGER_EVENT_TYPES,
    TriggerType,
)

from .host import ScheduleError

MAX_INTERVAL_MS = 365 * 24 * 60 * 60 * 1000
LATE_AFTER = timedelta(seconds=60)
SKIP_AFTER = timedelta(minutes=10)
SEARCH_DAYS = 8
SERVED_TYPES = (TriggerType.AT, TriggerType.INTERVAL, TriggerType.FIXED_TIME, TriggerType.EVENT)
EVENT_LIMITS = (('hops_max', HOPS_MAX), ('rate_max', RATE_MAX))

_TIME_OF_DAY = re.compile(r'([01]?[0-9]|2[0-3]):([0-5][0-9])')
_TOPIC = re.compile(TOPIC_PATTERN)
_TOPIC_MESSAGE = ('trigger.topic must be a signal topic or one of '
                  f'{", ".join(TRIGGER_EVENT_TYPES)}')


def iso(at: datetime) -> str:
    return at.astimezone(UTC).isoformat(timespec='milliseconds')


def parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


def _invalid(message: str) -> ScheduleError:
    return ScheduleError('invalid', message)


def _required(trigger: dict, field: str) -> object:
    value = trigger.get(field)
    if value is None:
        raise _invalid(f'trigger.{field} is required')
    return value


def _instant(wall: datetime, tz: tzinfo | None) -> datetime:
    if wall.tzinfo is None:
        wall = wall.replace(tzinfo=tz) if tz is not None else wall.astimezone()
    return wall.astimezone(UTC)


def parse_at(value: object, tz: tzinfo | None) -> datetime:
    message = 'trigger.at must be an ISO 8601 date and time'
    if not isinstance(value, str):
        raise _invalid(message)
    try:
        return _instant(datetime.fromisoformat(value), tz)
    except (ValueError, OverflowError):
        raise _invalid(message) from None


def _every_ms(value: object) -> int:
    if (isinstance(value, bool) or not isinstance(value, int)
            or not MIN_INTERVAL_MS <= value <= MAX_INTERVAL_MS):
        raise _invalid(
            f'trigger.everyMs must be an integer from {MIN_INTERVAL_MS} to {MAX_INTERVAL_MS}',
        )
    return value


def time_of_day(value: object) -> tuple[int, int]:
    match = _TIME_OF_DAY.fullmatch(value.strip()) if isinstance(value, str) else None
    if match is None:
        raise _invalid('trigger.timeOfDay must be H:MM or HH:MM, from 0:00 to 23:59')
    return int(match.group(1)), int(match.group(2))


def _weekdays(value: object) -> list[int]:
    if value is None:
        raise _invalid('trigger.weekdays must not be null')
    if not isinstance(value, list) or not all(
        isinstance(day, int) and not isinstance(day, bool) and 0 <= day <= 6 for day in value
    ):
        raise _invalid(
            'trigger.weekdays must be a list of integers from 0 (Sunday) to 6 (Saturday)',
        )
    return value


def _not_null(trigger: dict, field: str) -> object:
    value = trigger[field]
    if value is None:
        raise _invalid(f'trigger.{field} must not be null')
    return value


def _topic(value: object) -> str:
    if not isinstance(value, str):
        raise _invalid(_TOPIC_MESSAGE)
    if value in TRIGGER_EVENT_TYPES:
        return value
    if (not 1 <= len(value) <= MAX_TOPIC or _TOPIC.fullmatch(value) is None
            or value in tuple(EventType)):
        raise _invalid(_TOPIC_MESSAGE)
    return value


def _match(value: object) -> dict:
    if not isinstance(value, dict):
        raise _invalid('trigger.match must be a JSON object')
    if len(value) > MAX_MATCH_FIELDS:
        raise _invalid(f'trigger.match must have at most {MAX_MATCH_FIELDS} fields')
    try:
        size = len(json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode())
    except (TypeError, ValueError, RecursionError):
        raise _invalid('trigger.match must be a JSON object') from None
    if size > MAX_MATCH:
        raise _invalid(f'trigger.match must be at most {MAX_MATCH} bytes as JSON')
    return value


def _limit(value: object, field: str, most: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= most:
        raise _invalid(f'trigger.{field} must be an integer from 1 to {most}')
    return value


def _event_trigger(trigger: dict) -> dict:
    stored: dict[str, object] = {
        'type': TriggerType.EVENT.value, 'topic': _topic(_required(trigger, 'topic')),
    }
    if 'project_id' in trigger:
        project_id = _not_null(trigger, 'project_id')
        if not isinstance(project_id, str):
            raise _invalid('trigger.project_id must be a string')
        stored['project_id'] = project_id
    if 'match' in trigger:
        stored['match'] = _match(_not_null(trigger, 'match'))
    for field, most in EVENT_LIMITS:
        if field in trigger:
            stored[field] = _limit(_not_null(trigger, field), field, most)
    return stored


def check_trigger(trigger: object, tz: tzinfo | None) -> dict:
    if trigger is None:
        raise _invalid('trigger is required')
    if not isinstance(trigger, dict):
        raise _invalid('trigger must be an object')
    kind = _required(trigger, 'type')
    if kind == TriggerType.WEBHOOK:
        raise _invalid('webhook triggers are not served yet')
    if not isinstance(kind, str) or kind not in SERVED_TYPES:
        raise _invalid(f'trigger.type must be one of {", ".join(SERVED_TYPES)}')
    kind = TriggerType(kind)
    if kind is TriggerType.EVENT:
        return _event_trigger(trigger)
    if kind is TriggerType.AT:
        at = _required(trigger, 'at')
        parse_at(at, tz)
        return {'type': kind.value, 'at': at}
    if kind is TriggerType.INTERVAL:
        return {'type': kind.value, 'everyMs': _every_ms(_required(trigger, 'everyMs'))}
    clock_time = _required(trigger, 'timeOfDay')
    time_of_day(clock_time)
    stored = {'type': kind.value, 'timeOfDay': clock_time}
    if 'weekdays' in trigger:
        stored['weekdays'] = _weekdays(trigger['weekdays'])
    return stored


def _weekday(day: date) -> int:
    return (day.weekday() + 1) % 7


def next_fixed_time(trigger: dict, after: datetime, tz: tzinfo | None) -> datetime | None:
    hour, minute = time_of_day(trigger['timeOfDay'])
    allowed = set(trigger.get('weekdays') or range(7))
    local = after.astimezone(tz) if tz is not None else after.astimezone()
    for offset in range(SEARCH_DAYS + 1):
        day = local.date() + timedelta(days=offset)
        if _weekday(day) not in allowed:
            continue
        at = _instant(datetime(day.year, day.month, day.day, hour, minute), tz)
        if at > after:
            return at
    return None


def first_run(trigger: dict, added: datetime, tz: tzinfo | None) -> datetime | None:
    kind = trigger['type']
    if kind == TriggerType.EVENT:
        return None
    if kind == TriggerType.AT:
        return parse_at(trigger['at'], tz)
    if kind == TriggerType.INTERVAL:
        return added + timedelta(milliseconds=trigger['everyMs'])
    return next_fixed_time(trigger, added, tz)


def run_after(trigger: dict, now: datetime, tz: tzinfo | None) -> datetime | None:
    kind = trigger['type']
    if kind in (TriggerType.AT, TriggerType.EVENT):
        return None
    if kind == TriggerType.INTERVAL:
        return now + timedelta(milliseconds=trigger['everyMs'])
    return next_fixed_time(trigger, now, tz)


def is_late(due: datetime, now: datetime) -> bool:
    return now - due > LATE_AFTER


def is_missed(trigger: dict, due: datetime, now: datetime) -> bool:
    return trigger['type'] == TriggerType.FIXED_TIME and now - due > SKIP_AFTER
