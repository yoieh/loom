import unittest

from loom_contracts.runners import TextOut, ToolUse, TurnStarted
from loom_runner_codex.stream import ErrorLine, ThreadStarted, TurnEnded, parse_line

from .replay import CODEX_ID, line, stream_lines

MISSING = object()

WRONG_VALUES = {'null': None, 'number': 7, 'missing': MISSING}


def with_key(data: dict, key: str, value: object) -> dict:
    data = dict(data)
    if value is MISSING:
        data.pop(key, None)
    else:
        data[key] = value
    return data


def item_line(kind: str, item: dict) -> bytes:
    return line({'type': kind, 'item': item})


COMMAND = {
    'id': 'item_1',
    'type': 'command_execution',
    'command': "/bin/bash -lc 'echo spike-233'",
    'aggregated_output': '',
    'exit_code': None,
    'status': 'in_progress',
}


class RecordedLinesTest(unittest.TestCase):
    def test_first_turn(self) -> None:
        command = "/bin/bash -lc 'echo spike-233'"
        self.assertEqual([parse_line(raw) for raw in stream_lines('first-turn')], [
            ThreadStarted(CODEX_ID),
            TurnStarted(),
            TextOut('I’ll run the command in the specified folder and remember “lynx.”'),
            ToolUse('item_1', 'command_execution', {'command': command}),
            ToolUse('item_1', 'command_execution', {'command': command}),
            TextOut('OK'),
            TurnEnded(failed=False, reason=None),
        ])

    def test_resume(self) -> None:
        self.assertEqual([parse_line(raw) for raw in stream_lines('resume')], [
            ThreadStarted(CODEX_ID),
            TurnStarted(),
            TextOut('lynx'),
            TurnEnded(failed=False, reason=None),
        ])


class ThreadStartedTest(unittest.TestCase):
    def test_a_canonical_uuid_is_kept(self) -> None:
        raw = line({'type': 'thread.started', 'thread_id': CODEX_ID})
        self.assertEqual(parse_line(raw), ThreadStarted(CODEX_ID))

    def test_a_bad_thread_id_is_ignored(self) -> None:
        bad = {**WRONG_VALUES, 'help flag': '--help', 'path': '../x', 'upper': CODEX_ID.upper(),
               'braced': f'{{{CODEX_ID}}}', 'empty': '', 'list': [CODEX_ID]}
        for name, value in bad.items():
            with self.subTest(case=name):
                data = with_key({'type': 'thread.started'}, 'thread_id', value)
                self.assertIsNone(parse_line(line(data)))


class TurnLinesTest(unittest.TestCase):
    def test_turn_started(self) -> None:
        self.assertEqual(parse_line(b'{"type":"turn.started"}'), TurnStarted())

    def test_turn_completed(self) -> None:
        raw = line({'type': 'turn.completed', 'usage': {'input_tokens': 1}})
        self.assertEqual(parse_line(raw), TurnEnded(failed=False, reason=None))

    def test_turn_failed_with_a_message(self) -> None:
        raw = line({'type': 'turn.failed', 'error': {'message': 'quota exceeded'}})
        self.assertEqual(parse_line(raw), TurnEnded(failed=True, reason='quota exceeded'))

    def test_turn_failed_without_a_usable_message(self) -> None:
        cases = {f'error {name}': with_key({'type': 'turn.failed'}, 'error', value)
                 for name, value in WRONG_VALUES.items()}
        cases.update({
            f'message {name}': {'type': 'turn.failed',
                                'error': with_key({}, 'message', value)}
            for name, value in WRONG_VALUES.items()
        })
        cases['error string'] = {'type': 'turn.failed', 'error': 'boom'}
        for name, data in cases.items():
            with self.subTest(case=name):
                self.assertEqual(parse_line(line(data)), TurnEnded(failed=True,
                                                                   reason='turn failed'))

    def test_error_line(self) -> None:
        raw = line({'type': 'error', 'message': 'Reconnecting... 1/5'})
        self.assertEqual(parse_line(raw), ErrorLine('Reconnecting... 1/5'))

    def test_error_line_without_a_string_message_is_ignored(self) -> None:
        for name, value in WRONG_VALUES.items():
            with self.subTest(case=name):
                self.assertIsNone(parse_line(line(with_key({'type': 'error'}, 'message', value))))


class ItemTest(unittest.TestCase):
    def test_agent_message_on_completed(self) -> None:
        raw = item_line('item.completed', {'id': 'item_0', 'type': 'agent_message', 'text': 'OK'})
        self.assertEqual(parse_line(raw), TextOut('OK'))

    def test_agent_message_on_started_is_ignored(self) -> None:
        raw = item_line('item.started', {'id': 'item_0', 'type': 'agent_message', 'text': 'OK'})
        self.assertIsNone(parse_line(raw))

    def test_agent_message_without_string_text_is_ignored(self) -> None:
        for name, value in WRONG_VALUES.items():
            with self.subTest(case=name):
                item = with_key({'id': 'item_0', 'type': 'agent_message'}, 'text', value)
                self.assertIsNone(parse_line(item_line('item.completed', item)))

    def test_silent_items(self) -> None:
        for kind in ('reasoning', 'todo_list', 'error'):
            for event in ('item.started', 'item.completed'):
                with self.subTest(kind=kind, event=event):
                    item = {'id': 'item_3', 'type': kind, 'text': 'thinking', 'message': 'x'}
                    self.assertIsNone(parse_line(item_line(event, item)))

    def test_tool_items_drop_the_bookkeeping_keys(self) -> None:
        for event in ('item.started', 'item.completed'):
            with self.subTest(event=event):
                self.assertEqual(parse_line(item_line(event, COMMAND)), ToolUse(
                    'item_1', 'command_execution', {'command': "/bin/bash -lc 'echo spike-233'"},
                ))

    def test_other_item_types_are_tools(self) -> None:
        item = {'id': 'item_4', 'type': 'file_change', 'status': 'completed',
                'changes': [{'path': 'a.txt', 'kind': 'add'}]}
        self.assertEqual(parse_line(item_line('item.completed', item)), ToolUse(
            'item_4', 'file_change', {'changes': [{'path': 'a.txt', 'kind': 'add'}]},
        ))
        item = {'id': 'item_5', 'type': 'mcp_tool_call', 'server': 'loom', 'tool': 'speak',
                'arguments': {'text': 'hi'}}
        self.assertEqual(parse_line(item_line('item.started', item)), ToolUse(
            'item_5', 'mcp_tool_call', {'server': 'loom', 'tool': 'speak',
                                        'arguments': {'text': 'hi'}},
        ))

    def test_item_that_is_not_an_object_is_ignored(self) -> None:
        cases = {**WRONG_VALUES, 'string': 'item_1', 'list': [COMMAND]}
        for event in ('item.started', 'item.completed'):
            for name, value in cases.items():
                with self.subTest(event=event, case=name):
                    self.assertIsNone(parse_line(line(with_key({'type': event}, 'item', value))))

    def test_item_id_or_type_that_is_not_a_string_is_ignored(self) -> None:
        for event in ('item.started', 'item.completed'):
            for key in ('id', 'type'):
                for name, value in WRONG_VALUES.items():
                    with self.subTest(event=event, key=key, case=name):
                        message = {'id': 'item_0', 'type': 'agent_message', 'text': 'OK'}
                        for item in (with_key(COMMAND, key, value), with_key(message, key, value)):
                            self.assertIsNone(parse_line(item_line(event, item)))


class UnreadableLineTest(unittest.TestCase):
    def test_unreadable_lines_are_ignored(self) -> None:
        cases = {
            'not json': b'not json',
            'not utf-8': b'{"type":"turn.started","x":"\xff"}',
            'array': b'[{"type":"turn.started"}]',
            'string': b'"turn.started"',
            'number': b'7',
            'null': b'null',
            'type number': b'{"type":7}',
            'type null': b'{"type":null}',
            'type list': b'{"type":["turn.started"]}',
            'no type': b'{"thread_id":"x"}',
            'unknown type': b'{"type":"turn.whatever"}',
            'empty': b'',
            'deep': b'[' * 100_000 + b']' * 100_000,
        }
        for name, raw in cases.items():
            with self.subTest(case=name):
                self.assertIsNone(parse_line(raw))

    def test_synthetic_malformed_stream_keeps_its_good_lines(self) -> None:
        parsed = [item for raw in stream_lines('synthetic-malformed')
                  if (item := parse_line(raw)) is not None]
        self.assertEqual(parsed, [
            ThreadStarted(CODEX_ID),
            TurnStarted(),
            TextOut('OK'),
            TurnEnded(failed=False, reason=None),
        ])


if __name__ == '__main__':
    unittest.main()
