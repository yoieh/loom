import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from loom_contracts.calls import CallName
from loom_contracts.events import EventType, SkipReason
from loom_contracts.triggers import (
    HOPS_MAX, RATE_MAX, TRIGGER_EVENT_TYPES, ScheduleAction, TriggerType,
)

from .checks import Stopped, checked_text
from .fill import fill
from .host import Maker, ScheduleError, SchedulerHost
from .table import (
    FAILED, FIRED, MAX_REASON, RETRY_AFTER, SELECT, launch_options, title, to_row,
)
from .timing import iso

PAGE = 1_000
RATE_WINDOW = timedelta(hours=1)
NOW = datetime.min.replace(tzinfo=UTC)
READER = Maker(None, False, None)


def skipped(reason: SkipReason) -> str:
    return f'skipped_{reason.value}'


def _whole(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _canon(value: object) -> str:
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False, sort_keys=True)


def matches(match: dict, payload: dict) -> bool:
    return all(field in payload and _canon(payload[field]) == _canon(value)
               for field, value in match.items())


@dataclass
class Watch:
    row: dict
    trigger: dict
    cursor: int
    saved: int
    active: bool = True
    halted: bool = False

    @property
    def id(self) -> str:
        return self.row['id']


class Seen:
    def __init__(self, event: dict) -> None:
        self.event = event
        self.id: int = event['id']
        self.type = event.get('type')
        data = event.get('data')
        self.data: dict = data if isinstance(data, dict) else {}
        self.signal = self.type == EventType.SIGNAL
        self.thread: dict | None = None
        self.looked = False

    def thread_id(self) -> str | None:
        thread_id = self.event.get('thread_id')
        if thread_id is None and self.signal:
            thread_id = self.data.get('by')
        return thread_id if isinstance(thread_id, str) else None

    def has_topic(self, topic: str) -> bool:
        if topic in TRIGGER_EVENT_TYPES:
            return self.type == topic
        return self.signal and self.data.get('topic') == topic

    def payload(self) -> dict:
        if not self.signal:
            return self.data
        payload = self.data.get('payload')
        return payload if isinstance(payload, dict) else {}

    def hops(self) -> int:
        hops = self.data.get('hops')
        if _whole(hops):
            return hops
        if self.event.get('thread_id') is not None and self.thread is not None:
            hops = self.thread.get('hops')
            return hops if _whole(hops) else 0
        return 0


class EventSource:
    def __init__(self, host: SchedulerHost, clock: Callable[[], datetime],
                 wake: Callable[[], None]) -> None:
        self.host = host
        self.clock = clock
        self.wake = wake
        self._pending = False
        self._watching = False
        self._retry: datetime | None = None
        self._held: dict[str, datetime] = {}
        self._unlisten: Callable[[], None] | None = None

    def open(self) -> None:
        if self._unlisten is None:
            self._unlisten = self.host.listen(self._woken)
        with self.host.transaction() as tx:
            self._watching = tx.execute(
                'SELECT COUNT(*) FROM schedules WHERE paused = 0 AND event_after IS NOT NULL',
            ).fetchone()[0] > 0
        self._woken()

    def watch(self) -> None:
        self._watching = True

    def close(self) -> None:
        unlisten, self._unlisten = self._unlisten, None
        if unlisten is not None:
            unlisten()

    def _woken(self) -> None:
        self._pending = True
        if self._watching:
            self.wake()

    def next_wake(self) -> datetime | None:
        if self._pending and self._watching:
            return NOW
        waits = [*self._held.values(), *([] if self._retry is None else [self._retry])]
        return min(waits) if waits else None

    async def run_due(self, now: datetime) -> None:
        self._pending = False
        self._retry = None
        try:
            await self._pass(now)
        except Exception:
            self._retry = now + RETRY_AFTER

    def _load(self) -> list[Watch]:
        with self.host.transaction() as tx:
            rows = tx.execute(
                f'{SELECT} WHERE paused = 0 AND event_after IS NOT NULL ORDER BY created_at, id',
            ).fetchall()
        watches = []
        for found in rows:
            schedule = to_row(found)
            trigger = json.loads(schedule['trigger'])
            if trigger.get('type') == TriggerType.EVENT:
                cursor = schedule['event_after']
                watches.append(Watch(schedule, trigger, cursor, cursor))
        return watches

    async def _pass(self, now: datetime) -> None:
        watches = self._load()
        self._watching = bool(watches)
        held, self._held = self._held, {}
        for watch in watches:
            until = held.get(watch.id)
            if until is not None and until > now:
                watch.halted = True
                self._held[watch.id] = until
        reading = [watch for watch in watches if not watch.halted]
        if not reading:
            return
        after = min(watch.cursor for watch in reading)
        try:
            while True:
                page = await self.host.call(CallName.THREADS_EVENTS, {'after': after}, READER)
                for event in page:
                    await self._event(Seen(event), reading)
                if len(page) < PAGE or page[-1]['id'] <= after:
                    break
                after = page[-1]['id']
        finally:
            self._save(reading)
            for watch in reading:
                if watch.halted:
                    self._held[watch.id] = now + RETRY_AFTER

    async def _event(self, seen: Seen, watches: list[Watch]) -> None:
        for watch in watches:
            if not watch.active or watch.halted or watch.cursor >= seen.id:
                continue
            try:
                await self._handle(watch, seen)
            except Exception:
                watch.halted = True

    async def _handle(self, watch: Watch, seen: Seen) -> None:
        trigger = watch.trigger
        if (not seen.has_topic(trigger['topic'])
                or 'match' in trigger and not matches(trigger['match'], seen.payload())):
            watch.cursor = seen.id
            return
        await self._look_up(seen)
        if 'project_id' in trigger and self._project(seen) != trigger['project_id']:
            watch.cursor = seen.id
            return
        now = self.clock()
        hops = seen.hops()
        reason = self._skip(watch, seen, hops, now)
        if reason is not None:
            self._record(watch, seen, now, skipped(reason), hops, skip=reason)
            return
        if not self._active(watch):
            return
        text = fill(watch.row['text'], seen.id, seen.payload())
        outcome, thread_id, failure, text = await self._fire(watch, seen, text)
        self._record(watch, seen, self.clock(), outcome, hops, text=text, thread_id=thread_id,
                     failure=failure)

    async def _look_up(self, seen: Seen) -> None:
        if seen.looked:
            return
        thread_id = seen.thread_id()
        thread = None
        if thread_id is not None:
            try:
                thread = await self.host.call(CallName.THREADS_GET, {'thread_id': thread_id},
                                              READER)
            except ScheduleError as error:
                if error.code != 'not_found':
                    raise
        seen.thread, seen.looked = thread, True

    def _project(self, seen: Seen) -> str | None:
        if seen.signal and isinstance(seen.data.get('project_id'), str):
            return seen.data['project_id']
        if seen.thread is not None:
            return seen.thread.get('project_id')
        if seen.type == EventType.SCHEDULE_FIRED:
            with self.host.transaction() as tx:
                found = tx.execute('SELECT project_id FROM schedules WHERE id = ?',
                                   (seen.data.get('schedule_id'),)).fetchone()
            return None if found is None else found[0]
        return None

    def _skip(self, watch: Watch, seen: Seen, hops: int, now: datetime) -> SkipReason | None:
        origin = None if seen.thread is None else seen.thread.get('origin')
        if (origin == f'schedule:{watch.id}' or seen.type == EventType.SCHEDULE_FIRED
                and seen.data.get('schedule_id') == watch.id):
            return SkipReason.OWN
        if hops >= watch.trigger.get('hops_max', HOPS_MAX):
            return SkipReason.HOPS
        if self._runs_lately(watch.id, FIRED, now) >= watch.trigger.get('rate_max', RATE_MAX):
            return SkipReason.RATE
        return None

    def _runs_lately(self, schedule_id: str, outcome: str, now: datetime) -> int:
        with self.host.transaction() as tx:
            return tx.execute(
                'SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = ? AND outcome = ? '
                'AND at > ?', (schedule_id, outcome, iso(now - RATE_WINDOW)),
            ).fetchone()[0]

    def _active(self, watch: Watch) -> bool:
        with self.host.transaction() as tx:
            current = tx.execute('SELECT paused FROM schedules WHERE id = ?',
                                 (watch.id,)).fetchone()
        watch.active = current is not None and not current[0]
        return watch.active

    async def _fire(self, watch: Watch, seen: Seen,
                    text: str) -> tuple[str, str | None, str | None, str]:
        schedule = watch.row
        maker = Maker(schedule['by'], bool(schedule['verified']), watch.id)
        action = ScheduleAction(schedule['action'])
        command_id = f'schedule:{watch.id}:event:{seen.id}'
        try:
            if action is ScheduleAction.NOTIFY:
                return FIRED, None, None, text
            if action is ScheduleAction.POST:
                await self.host.call(CallName.THREADS_SEND, {
                    'id': command_id, 'thread_id': schedule['thread_id'], 'message': text,
                    'mode': 'queue', 'cause': seen.id,
                }, maker)
                return FIRED, schedule['thread_id'], None, text
            if schedule['check_argv'] is not None:
                text = await checked_text(self.host, schedule, maker, text)
            params = {
                'id': command_id, 'title': title(schedule['title'], text), 'message': text,
                'project_id': schedule['project_id'], 'origin': f'schedule:{watch.id}',
                'cause': seen.id,
            }
            params.update(launch_options(schedule))
            thread = await self.host.call(CallName.THREADS_LAUNCH, params, maker)
            return FIRED, thread['id'], None, text
        except Stopped as stopped:
            return stopped.outcome, None, stopped.reason, text
        except ScheduleError as error:
            return FAILED, None, error.message[:MAX_REASON], text

    def _fired_data(self, watch: Watch, seen: Seen, action: str, text: str,
                    hops: int) -> dict[str, object]:
        data: dict[str, object] = {
            'schedule_id': watch.id, 'action': action, 'text': text, 'late': False,
        }
        if watch.row['by'] is not None:
            data['by'] = watch.row['by']
        data['cause'] = seen.id
        data['hops'] = hops + 1
        return data

    def _record(self, watch: Watch, seen: Seen, now: datetime, outcome: str, hops: int, *,
                skip: SkipReason | None = None, text: str = '', thread_id: str | None = None,
                failure: str | None = None) -> None:
        notify = skip is SkipReason.RATE and not self._runs_lately(
            watch.id, skipped(SkipReason.RATE), now)
        schedule = watch.row
        with self.host.transaction() as tx:
            current = tx.execute('SELECT paused FROM schedules WHERE id = ?',
                                 (watch.id,)).fetchone()
            if current is None or current[0]:
                watch.active = False
                return
            tx.execute(
                'INSERT INTO schedule_runs (schedule_id, due, at, outcome, late, thread_id, '
                'reason, event_id) VALUES (?, ?, ?, ?, 0, ?, ?, ?)',
                (watch.id, seen.event.get('at'), iso(now), outcome, thread_id, failure, seen.id),
            )
            tx.execute('UPDATE schedules SET event_after = ? WHERE id = ?', (seen.id, watch.id))
            if outcome == FIRED:
                event_thread = schedule['thread_id'] if thread_id is None else thread_id
                self.host.append_event(tx, EventType.SCHEDULE_FIRED, event_thread,
                                       self._fired_data(watch, seen, schedule['action'], text,
                                                        hops))
            if skip is not None:
                self.host.append_event(tx, EventType.TRIGGER_SKIPPED, None, {
                    'schedule_id': watch.id, 'event_id': seen.id, 'reason': skip.value,
                })
            if notify:
                limit = watch.trigger.get('rate_max', RATE_MAX)
                message = (f'Schedule {watch.id} fired {limit} times in the last hour, '
                           f'so it skipped event {seen.id}.')
                self.host.append_event(tx, EventType.SCHEDULE_FIRED, schedule['thread_id'],
                                       self._fired_data(watch, seen, ScheduleAction.NOTIFY.value,
                                                        message, hops))
        watch.cursor = watch.saved = seen.id

    def _save(self, watches: list[Watch]) -> None:
        moved = [watch for watch in watches if watch.active and watch.cursor > watch.saved]
        if not moved:
            return
        with self.host.transaction() as tx:
            for watch in moved:
                tx.execute(
                    'UPDATE schedules SET event_after = ? WHERE id = ? AND paused = 0 '
                    'AND event_after < ?', (watch.cursor, watch.id, watch.cursor),
                )
        for watch in moved:
            watch.saved = watch.cursor
