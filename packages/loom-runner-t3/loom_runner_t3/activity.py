import dataclasses
import json

from loom_contracts.runners import (
    MessageEntered,
    RunnerEvent,
    TextOut,
    ToolUse,
    TurnResult,
    TurnStarted,
)
from loom_contracts.states import TurnState

from .reasons import failure, reason

ENDED = {
    'completed': TurnState.COMPLETED,
    'failed': TurnState.FAILED,
    'interrupted': TurnState.STOPPED,
}
BUSY = frozenset({'running', 'waiting', 'starting', 'queued'})
INTERRUPTED = 'interrupted'
EXPECT_POLLS = 30


def _int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


@dataclasses.dataclass(frozen=True)
class Activity:
    status: str
    active_run: str | None
    latest_run: str | None
    pending: int
    archived: bool
    runs: dict[str, str]
    items: list[dict]
    next_position: int | None
    has_more: bool


def _runs(value: object) -> dict[str, str]:
    runs: dict[str, str] = {}
    if not isinstance(value, list):
        return runs
    for run in value:
        if isinstance(run, dict):
            run_id, status = _text(run.get('runId')), _text(run.get('status'))
            if run_id is not None and status is not None:
                runs.setdefault(run_id, status)
    return runs


def parse_activity(answer: object) -> Activity:
    if not isinstance(answer, dict) or not isinstance(answer.get('thread'), dict):
        raise failure('protocol', 'the activity answer has no thread')
    thread = answer['thread']
    status = thread.get('status')
    if not isinstance(status, str) or not status:
        raise failure('protocol', 'the activity answer has no thread status')
    items = answer.get('items')
    if not isinstance(items, list):
        raise failure('protocol', 'the activity answer has no items')
    if 'nextPosition' not in answer:
        raise failure('protocol', 'the activity answer has no position')
    position = answer['nextPosition']
    if position is None:
        if items:
            raise failure('protocol', 'the activity answer has items but no position')
    elif not _int(position) or position < 0:
        raise failure('protocol', 'the activity answer has a bad position')
    pending = thread.get('pendingRequestCount')
    return Activity(
        status=status,
        active_run=_text(thread.get('activeRunId')),
        latest_run=_text(thread.get('latestRunId')),
        pending=pending if _int(pending) and pending > 0 else 0,
        archived=thread.get('archived') is True,
        runs=_runs(answer.get('recentRuns')),
        items=[item for item in items if isinstance(item, dict)],
        next_position=position,
        has_more=answer.get('hasMore') is True,
    )


def _tool(item: dict) -> ToolUse | None:
    item_id, text = _text(item.get('itemId')), item.get('text')
    if item_id is None or not isinstance(text, str):
        return None
    try:
        data = json.loads(text)
    except (ValueError, RecursionError):
        return None
    if not isinstance(data, dict):
        return None
    name, tool_input = _text(data.get('toolName')), data.get('input')
    if name is None or not isinstance(tool_input, dict):
        return None
    return ToolUse(item_id, name, tool_input)


def map_item(item: dict) -> RunnerEvent | None:
    kind, text = item.get('type'), item.get('text')
    if kind == 'user_message':
        return MessageEntered(text) if isinstance(text, str) else None
    if kind == 'assistant_message':
        if item.get('status') != 'completed' or not isinstance(text, str):
            return None
        return TextOut(text)
    if kind == 'dynamic_tool':
        return _tool(item)
    return None


class Tracker:
    def __init__(self, secrets: tuple[str, ...] = ()) -> None:
        self.secrets = secrets
        self.position: int | None = None
        self.turn: str | None = None
        self.text: str | None = None
        self.error: str | None = None
        self.known: set[str] = set()
        self.status: str | None = None
        self.active: str | None = None
        self.expect = 0

    def baseline(self, activity: Activity) -> None:
        self._move(activity.next_position)
        self.known.update(run for run in activity.runs if run != activity.active_run)
        self.status = activity.status
        self.active = activity.active_run

    def busy(self) -> bool:
        return (self.turn is not None or self.expect > 0 or self.active is not None
                or self.status in BUSY)

    def expect_turn(self) -> None:
        self.expect = EXPECT_POLLS

    def in_turn(self) -> bool:
        return self.turn is not None or self.expect > 0

    def lost(self) -> None:
        self.status = None
        self.active = None

    def abandon(self, why: str) -> list[RunnerEvent]:
        self.lost()
        if not self.in_turn():
            return []
        self.turn = None
        self.expect = 0
        return [TurnResult(TurnState.FAILED, None, why)]

    def apply(self, activity: Activity) -> list[RunnerEvent]:
        events: list[RunnerEvent] = []
        for item in sorted(self._new_items(activity), key=lambda item: item['position']):
            self.position = item['position']
            run = item.get('runId')
            if run is not None and _text(run) is None:
                continue
            if run is not None and run != self.turn and run not in self.known:
                self._begin(run, activity, events)
            if run is not None and run != self.turn:
                continue
            if item.get('type') == 'error' and isinstance(item.get('text'), str):
                self.error = item['text']
            event = map_item(item)
            if isinstance(event, TextOut):
                self.text = event.text
            if event is not None:
                events.append(event)
        self._move(activity.next_position)
        active = activity.active_run
        if active is not None and active != self.turn and active not in self.known:
            self._begin(active, activity, events)
        self.status = activity.status
        self.active = active
        ended = self._end(activity, events)
        if ended:
            self.expect = 0
        elif self.expect > 0 and not (self.turn is not None or active is not None
                                      or activity.status in BUSY):
            self.expect -= 1
        return events

    def _new_items(self, activity: Activity) -> list[dict]:
        return [
            item for item in activity.items
            if _int(item.get('position')) and item['position'] >= 0
            and (self.position is None or item['position'] > self.position)
        ]

    def _move(self, position: int | None) -> None:
        if position is not None and (self.position is None or position > self.position):
            self.position = position

    def _begin(self, run: str, activity: Activity, events: list[RunnerEvent]) -> None:
        if self.turn is not None:
            state = ENDED.get(activity.runs.get(self.turn, ''), TurnState.COMPLETED)
            events.append(self._result(state))
        self.turn = run
        self.known.add(run)
        self.text = None
        self.error = None
        events.append(TurnStarted())

    def _end(self, activity: Activity, events: list[RunnerEvent]) -> bool:
        turn = self.turn
        if turn is None or activity.pending > 0 or activity.active_run == turn:
            return False
        status = activity.runs.get(turn)
        if status is None and activity.latest_run == turn:
            status = activity.status
        state = ENDED.get(status or '')
        if state is None:
            return False
        events.append(self._result(state))
        return True

    def _result(self, state: TurnState) -> TurnResult:
        self.turn = None
        if state is TurnState.COMPLETED:
            return TurnResult(state, self.text, None)
        if state is TurnState.STOPPED:
            return TurnResult(state, self.text, INTERRUPTED)
        why = self.error or 'the run failed'
        return TurnResult(state, self.text, reason('failed', why, self.secrets))
