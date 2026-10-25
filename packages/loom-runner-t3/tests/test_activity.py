import copy
import json
import unittest

from loom_contracts.runners import MessageEntered, TextOut, ToolUse, TurnResult, TurnStarted
from loom_contracts.states import TurnState
from loom_runner_t3.activity import Tracker, map_item, parse_activity
from loom_runner_t3.reasons import T3Error

from .replay import TOKEN, stream

RUN = 'run:thread:mcp%3A00000000-0000-4000-8000-000000000001:ordinal:1'
RUN_2 = 'run:thread:mcp%3A00000000-0000-4000-8000-000000000001:ordinal:2'


def answer(status: str = 'running', items: list | None = None, position: object = ...,
           active: str | None = RUN, runs: list | None = None, pending: int = 0) -> dict:
    items = items or []
    if position is ...:
        position = items[-1]['position'] if items else None
    if runs is None:
        runs = [{'runId': RUN, 'status': status}]
    return {
        'thread': {'status': status, 'activeRunId': active, 'latestRunId': RUN,
                   'pendingRequestCount': pending, 'archived': False},
        'recentRuns': runs,
        'items': items,
        'nextPosition': position,
        'hasMore': False,
    }


def item(position: int, kind: str, text: object, run: object = RUN,
         status: str = 'completed', **extra: object) -> dict:
    data = {'position': position, 'itemId': f'item-{position}', 'runId': run, 'type': kind,
            'status': status, 'title': None, 'text': text}
    data.update(extra)
    return data


def tool_text(name: object = 'Read', tool_input: object = None) -> str:
    return json.dumps({'toolName': name, 'input': {'file_path': '/x'} if tool_input is None
                       else tool_input, 'output': 'line'})


def play(tracker: Tracker, *answers: dict) -> list:
    events = []
    for data in answers:
        events.extend(tracker.apply(parse_activity(data)))
    return events


class ParseTest(unittest.TestCase):
    def test_every_recorded_read_parses(self) -> None:
        for case in ('idle-launch', 'turn', 'tool', 'interrupt', 'steer', 'failed'):
            for line in stream(case):
                with self.subTest(case=case, position=line['nextPosition']):
                    parsed = parse_activity(line)
                    self.assertEqual(parsed.status, line['thread']['status'])

    def test_a_null_missing_or_wrong_position_fails(self) -> None:
        good = answer(items=[item(0, 'user_message', 'hi')])
        cases = {
            'null with items': None,
            'missing': ...,
            'a string': '0',
            'a bool': True,
            'a float': 1.0,
            'negative': -1,
        }
        for name, value in cases.items():
            with self.subTest(name):
                data = copy.deepcopy(good)
                if value is ...:
                    del data['nextPosition']
                else:
                    data['nextPosition'] = value
                with self.assertRaises(T3Error) as caught:
                    parse_activity(data)
                self.assertTrue(str(caught.exception).startswith('t3: protocol: '))

    def test_a_null_position_with_no_items_is_kept(self) -> None:
        self.assertIsNone(parse_activity(answer()).next_position)

    def test_a_null_missing_or_wrong_status_fails(self) -> None:
        for name, value in (('null', None), ('missing', ...), ('a number', 1), ('empty', '')):
            with self.subTest(name):
                data = answer()
                if value is ...:
                    del data['thread']['status']
                else:
                    data['thread']['status'] = value
                with self.assertRaisesRegex(T3Error, '^t3: protocol: '):
                    parse_activity(data)

    def test_a_missing_thread_or_items_fails(self) -> None:
        for name, change in (('no thread', lambda d: d.pop('thread')),
                             ('null thread', lambda d: d.update(thread=None)),
                             ('no items', lambda d: d.pop('items')),
                             ('items not a list', lambda d: d.update(items={}))):
            with self.subTest(name):
                data = answer()
                change(data)
                with self.assertRaisesRegex(T3Error, '^t3: protocol: '):
                    parse_activity(data)
        with self.assertRaisesRegex(T3Error, '^t3: protocol: '):
            parse_activity(['not', 'an', 'object'])


class MapTest(unittest.TestCase):
    def test_the_item_kinds(self) -> None:
        self.assertEqual(map_item(item(0, 'user_message', 'hi')), MessageEntered('hi'))
        self.assertEqual(map_item(item(1, 'assistant_message', 'ok')), TextOut('ok'))
        self.assertEqual(map_item(item(2, 'dynamic_tool', tool_text())),
                         ToolUse('item-2', 'Read', {'file_path': '/x'}))
        for kind in ('reasoning', 'checkpoint', 'run_interrupt_request',
                     'run_interrupt_result', 'error', 'unknown'):
            with self.subTest(kind=kind):
                self.assertIsNone(map_item(item(3, kind, 'text')))

    def test_an_assistant_message_that_is_not_completed_is_not_text(self) -> None:
        self.assertIsNone(map_item(item(1, 'assistant_message', 'o', status='running')))


class TrackerTest(unittest.TestCase):
    def test_a_turn(self) -> None:
        tracker = Tracker()
        events = play(tracker,
                      answer(items=[item(0, 'user_message', 'hi')]),
                      answer(items=[item(1, 'assistant_message', 'ok')]),
                      answer('completed', active=None,
                             items=[item(2, 'checkpoint', '[]')]))
        self.assertEqual(events, [TurnStarted(), MessageEntered('hi'), TextOut('ok'),
                                  TurnResult(TurnState.COMPLETED, 'ok', None)])
        self.assertIsNone(tracker.turn)
        self.assertEqual(tracker.position, 2)

    def test_items_at_or_before_the_position_are_not_reported(self) -> None:
        tracker = Tracker()
        tracker.baseline(parse_activity(answer('completed', active=None, position=5,
                                               items=[item(5, 'user_message', 'old')])))
        events = play(tracker, answer(position=6, items=[
            item(4, 'user_message', 'older', run=RUN_2),
            item(5, 'user_message', 'old', run=RUN_2),
            item(6, 'user_message', 'new', run=RUN_2),
        ], active=RUN_2, runs=[{'runId': RUN_2, 'status': 'running'},
                               {'runId': RUN, 'status': 'completed'}]))
        self.assertEqual(events, [TurnStarted(), MessageEntered('new')])

    def test_a_run_that_was_over_at_start_is_not_a_turn(self) -> None:
        tracker = Tracker()
        tracker.baseline(parse_activity(answer('completed', active=None)))
        self.assertEqual(play(tracker, answer('completed', active=None)), [])

    def test_a_run_going_at_start_is_reported_from_the_next_read(self) -> None:
        tracker = Tracker()
        tracker.baseline(parse_activity(answer(position=3, items=[item(3, 'reasoning', 'x')])))
        events = play(tracker, answer('completed', active=None,
                                      items=[item(4, 'assistant_message', 'done')]))
        self.assertEqual(events, [TurnStarted(), TextOut('done'),
                                  TurnResult(TurnState.COMPLETED, 'done', None)])

    def test_no_result_while_a_request_is_pending(self) -> None:
        tracker = Tracker()
        events = play(tracker,
                      answer(items=[item(0, 'user_message', 'hi')]),
                      answer('completed', active=None, pending=1),
                      answer('completed', active=None, pending=2))
        self.assertEqual(events, [TurnStarted(), MessageEntered('hi')])
        self.assertTrue(tracker.busy())
        events = play(tracker, answer('completed', active=None, pending=0))
        self.assertEqual(events, [TurnResult(TurnState.COMPLETED, None, None)])

    def test_the_run_states(self) -> None:
        for status, state, why in (('completed', TurnState.COMPLETED, None),
                                   ('interrupted', TurnState.STOPPED, 'interrupted'),
                                   ('failed', TurnState.FAILED, 't3: failed: the run failed')):
            with self.subTest(status=status):
                tracker = Tracker()
                events = play(tracker, answer(items=[item(0, 'user_message', 'hi')]),
                              answer(status, active=None))
                self.assertEqual(events[-1], TurnResult(state, None, why))

    def test_a_failed_run_gives_the_error_item_as_its_reason(self) -> None:
        tracker = Tracker((TOKEN,))
        events = play(tracker, answer(items=[item(0, 'user_message', 'hi')]),
                      answer('failed', active=None,
                             items=[item(1, 'error', f'bad {TOKEN}\nmore', status='failed')]))
        self.assertEqual(events[-1], TurnResult(TurnState.FAILED, None,
                                                't3: failed: bad [redacted] more'))

    def test_a_run_that_starts_and_ends_between_reads(self) -> None:
        tracker = Tracker()
        events = play(tracker, answer('completed', active=None, items=[
            item(0, 'user_message', 'hi'), item(1, 'assistant_message', 'ok')]))
        self.assertEqual(events, [TurnStarted(), MessageEntered('hi'), TextOut('ok'),
                                  TurnResult(TurnState.COMPLETED, 'ok', None)])

    def test_a_new_run_ends_the_one_before(self) -> None:
        tracker = Tracker()
        play(tracker, answer(items=[item(0, 'user_message', 'one')]))
        events = play(tracker, answer(active=RUN_2, items=[
            item(1, 'assistant_message', 'first'),
            item(2, 'user_message', 'two', run=RUN_2),
        ], runs=[{'runId': RUN_2, 'status': 'running'}, {'runId': RUN, 'status': 'completed'}]))
        self.assertEqual(events, [TextOut('first'),
                                  TurnResult(TurnState.COMPLETED, 'first', None),
                                  TurnStarted(), MessageEntered('two')])

    def test_items_with_a_null_field_are_skipped(self) -> None:
        self.check_skipped(None)

    def test_items_with_a_missing_field_are_skipped(self) -> None:
        self.check_skipped(...)

    def test_items_with_a_wrong_typed_field_are_skipped(self) -> None:
        self.check_skipped([7])

    def check_skipped(self, value: object) -> None:
        def broken(data: dict, field: str) -> dict:
            data = copy.deepcopy(data)
            if value is ...:
                data.pop(field)
            else:
                data[field] = value
            return data

        def broken_tool(field: str) -> dict:
            body = json.loads(tool_text())
            if value is ...:
                body.pop(field)
            else:
                body[field] = value
            return item(2, 'dynamic_tool', json.dumps(body))

        cases = {
            'user text': broken(item(2, 'user_message', 'x'), 'text'),
            'assistant text': broken(item(2, 'assistant_message', 'x'), 'text'),
            'assistant status': broken(item(2, 'assistant_message', 'x'), 'status'),
            'type': broken(item(2, 'user_message', 'x'), 'type'),
            'position': broken(item(2, 'user_message', 'x'), 'position'),
            'run id': broken(item(2, 'user_message', 'x'), 'runId')
            if value is not ... and value is not None else None,
            'tool item id': broken(item(2, 'dynamic_tool', tool_text()), 'itemId'),
            'tool text': broken(item(2, 'dynamic_tool', tool_text()), 'text'),
            'tool name': broken_tool('toolName'),
            'tool input': broken_tool('input'),
        }
        for name, bad in cases.items():
            if bad is None:
                continue
            with self.subTest(name):
                tracker = Tracker()
                events = play(tracker,
                              answer(items=[item(1, 'user_message', 'before')]),
                              answer(position=3, items=[bad, item(3, 'assistant_message',
                                                                  'after')]))
                self.assertEqual(events, [TurnStarted(), MessageEntered('before'),
                                          TextOut('after')])
                self.assertEqual(tracker.position, 3)

    def test_a_tool_text_that_is_not_json_is_skipped(self) -> None:
        tracker = Tracker()
        events = play(tracker, answer(items=[item(0, 'dynamic_tool', '{cut'),
                                             item(1, 'dynamic_tool', '[1]'),
                                             item(2, 'dynamic_tool', tool_text())]))
        self.assertEqual(events, [TurnStarted(), ToolUse('item-2', 'Read', {'file_path': '/x'})])

    def test_abandon_ends_a_turn_once(self) -> None:
        tracker = Tracker()
        play(tracker, answer(items=[item(0, 'user_message', 'hi')]))
        self.assertEqual(tracker.abandon('t3: x: y'),
                         [TurnResult(TurnState.FAILED, None, 't3: x: y')])
        self.assertEqual(tracker.abandon('t3: x: y'), [])
        self.assertFalse(tracker.busy())
        self.assertEqual(play(tracker, answer(items=[item(1, 'assistant_message', 'late')])),
                         [])

    def test_abandon_after_a_send_ends_the_expected_turn(self) -> None:
        tracker = Tracker()
        tracker.expect_turn()
        self.assertTrue(tracker.busy())
        self.assertEqual(tracker.abandon('t3: x: y'),
                         [TurnResult(TurnState.FAILED, None, 't3: x: y')])
        self.assertFalse(tracker.busy())

    def test_abandon_while_idle_gives_nothing(self) -> None:
        self.assertEqual(Tracker().abandon('t3: x: y'), [])

    def test_busy_follows_the_thread(self) -> None:
        tracker = Tracker()
        self.assertFalse(tracker.busy())
        tracker.baseline(parse_activity(answer('completed', active=None)))
        self.assertFalse(tracker.busy())
        for status in ('running', 'waiting', 'queued', 'starting'):
            with self.subTest(status=status):
                tracker = Tracker()
                tracker.baseline(parse_activity(answer(status, active=None)))
                self.assertTrue(tracker.busy())

    def test_an_expected_turn_stops_being_busy_after_its_polls(self) -> None:
        tracker = Tracker()
        tracker.expect_turn()
        for _ in range(30):
            self.assertTrue(tracker.busy())
            play(tracker, answer('completed', active=None))
        self.assertFalse(tracker.busy())


if __name__ == '__main__':
    unittest.main()
