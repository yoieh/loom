import copy
import json
import unittest
from pathlib import Path

from loom_contracts.runners import MessageEntered, PermissionRequest, TextOut, ToolUse, TurnResult
from loom_contracts.states import TurnState
from loom_runner_claude.stream import (
    INTERRUPT_MARKER,
    BadRequest,
    Init,
    Result,
    TaskEnded,
    TaskStarted,
    allow_line,
    deny_line,
    interrupt_line,
    parse_line,
    turn_result,
    user_line,
)

STREAMS = Path(__file__).resolve().parent / 'streams'

TURNS = '77f465be-2f52-4311-84a5-e19e6d1e339b'
TOOLS = '7c5e51f4-cabd-4c0e-a4f8-def985af92a1'
STEER = '332ac17d-0a9f-4e6c-8922-7a001f90871a'
INTERRUPT = 'f8b0d246-bc5a-47cf-84aa-0e7429e35bd0'
BACKGROUND = '96d8dc7e-6fdd-493b-a431-d49f186b2bae'
PERM = '0d01d654-7a63-489d-ad44-894f8ee1a17e'
BADMODEL = '0667ac49-2d45-40a3-b2f3-0d4c8c29808f'
RESUME = '2bb550db-78f6-461c-aa96-47a3900296d9'

STEER_FIRST = (
    '[Test session for a spike, in the folder /tmp/loom-replay/repo. Change no file and run no '
    'command outside that folder unless this message says so. Do not call the speak, ask or '
    'remind tools or any scheduling tool.] Run `sleep 20` with Bash, then reply with the word '
    'first'
)
TOOLS_TEXT = (
    "I ran `echo spike-233`, which printed `spike-233`. There's no `README` in the repo, "
    'so there was nothing to read.\n\ndone'
)
PERM_A_TEXT = 'Done. I ran `echo spike-233 > a.txt` in the repo, and it finished without errors.'
PERM_B_TEXT = (
    "I didn't create `b.txt`: the spike-233 test denied the command, so it never ran. "
    "I haven't retried it."
)
SLEEP_60 = {
    'command': 'sleep 60 && echo spike-233',
    'description': 'Wait 60 seconds then print marker',
}
WRITE_A = {'command': 'echo spike-233 > a.txt', 'description': 'Write spike-233 to a.txt'}
WRITE_B = {'command': 'echo spike-233 > b.txt', 'description': 'Write spike-233 to b.txt'}

EXPECTED: dict[str, list[object]] = {
    'turns': [
        Init(TURNS), TextOut('OK'), Result(False, 'OK', 'completed'),
        Init(TURNS), TextOut('heron'), Result(False, 'heron', 'completed'),
        Init(TURNS), TextOut('OK'), Result(False, 'OK', 'completed'),
    ],
    'tools': [
        Init(TOOLS),
        ToolUse('toolu_016VjNSHj8Ui9nzGremDhu9E', 'Bash', {
            'command': 'echo spike-233 && ls -a && (cat README 2>/dev/null || echo "NO README")',
            'description': 'Echo marker, list repo files, and print README if present',
        }),
        TextOut(TOOLS_TEXT),
        Result(False, TOOLS_TEXT, 'completed'),
    ],
    'steer-replay': [
        Init(STEER),
        MessageEntered(STEER_FIRST),
        ToolUse('toolu_01SteerReplaySleep20Bash', 'Bash', {
            'command': 'sleep 20', 'description': 'Wait 20 seconds',
        }),
        MessageEntered('Reply with the word second'),
        TextOut('first\n\nsecond'),
        Result(False, 'first\n\nsecond', 'completed'),
    ],
    'interrupt': [
        Init(INTERRUPT),
        ToolUse('toolu_01InterruptSleep60Bash', 'Bash', SLEEP_60),
        Result(True, None, 'aborted_streaming'),
    ],
    'interrupt-background': [
        Init(BACKGROUND),
        ToolUse('toolu_01WGGTKZh9Wj1Ef6KP7H4Wqt', 'Bash', {**SLEEP_60, 'run_in_background': True}),
        TaskStarted('b1ajgv20g'),
        Result(True, None, 'aborted_streaming'),
        TaskEnded('b1ajgv20g'),
    ],
    'perm-tool': [
        Init(PERM),
        ToolUse('toolu_01PLXzP7s7QnwHdxuXJZwZBJ', 'Bash', WRITE_A),
        PermissionRequest(
            '2038e810-2aac-41ec-94f3-f181dcdbc712', 'Bash', WRITE_A, 'Write spike-233 to a.txt',
        ),
        TextOut(PERM_A_TEXT),
        Result(False, PERM_A_TEXT, 'completed'),
        Init(PERM),
        ToolUse('toolu_01EnqH5KJV8xxQbVoxQWQf1g', 'Bash', WRITE_B),
        PermissionRequest(
            'ac794f47-7556-4f5f-a8a6-2cec64a05505', 'Bash', WRITE_B, 'Write spike-233 to b.txt',
        ),
        TextOut(PERM_B_TEXT),
        Result(False, PERM_B_TEXT, 'completed'),
    ],
    'badmodel': [
        Init(BADMODEL),
        Result(
            True, 'API Error: 400 unknown provider for model spike-233-no-such-model',
            'api_error',
        ),
    ],
    'resume': [
        Init(RESUME), TextOut('otter'), Result(False, 'otter', 'completed'),
    ],
    'malformed': [MessageEntered('Reply with the word second')],
}


def stream_lines(case: str) -> list[bytes]:
    return (STREAMS / f'{case}.jsonl').read_bytes().split(b'\n')


def replay(case: str) -> list[object]:
    return [item for line in stream_lines(case) for item in parse_line(line)]


def encode(obj: object) -> bytes:
    return json.dumps(obj).encode('utf-8')


class ReplayTest(unittest.TestCase):
    def test_every_stream_file_has_a_case(self) -> None:
        self.assertEqual(sorted(p.stem for p in STREAMS.glob('*.jsonl')), sorted(EXPECTED))

    def test_replay_gives_the_expected_items(self) -> None:
        for case, expected in EXPECTED.items():
            with self.subTest(case=case):
                self.assertEqual(replay(case), expected)

    def test_turns_has_three_turns_of_one_session(self) -> None:
        items = replay('turns')
        triples = [tuple(type(item) for item in items[i:i + 3]) for i in range(0, len(items), 3)]
        self.assertEqual(triples, [(Init, TextOut, Result)] * 3)
        self.assertEqual({item.session_id for item in items if isinstance(item, Init)}, {TURNS})

    def test_steer_replay_has_one_result_for_both_messages(self) -> None:
        results = [item for item in replay('steer-replay') if isinstance(item, Result)]
        self.assertEqual(results, [Result(False, 'first\n\nsecond', 'completed')])
        replays = [
            json.loads(line) for line in stream_lines('steer-replay')
            if line and json.loads(line).get('isReplay') is True
        ]
        self.assertEqual(len(replays), 2)

    def test_perm_tool_requests_carry_ids_inputs_and_descriptions(self) -> None:
        requests = [item for item in replay('perm-tool') if isinstance(item, PermissionRequest)]
        self.assertEqual([r.request_id for r in requests], [
            '2038e810-2aac-41ec-94f3-f181dcdbc712', 'ac794f47-7556-4f5f-a8a6-2cec64a05505',
        ])
        self.assertEqual([r.input for r in requests], [WRITE_A, WRITE_B])
        self.assertEqual([r.description for r in requests], [
            'Write spike-233 to a.txt', 'Write spike-233 to b.txt',
        ])

    def test_badmodel_result_is_an_error_despite_success_subtype(self) -> None:
        lines = [json.loads(line) for line in stream_lines('badmodel') if line]
        self.assertEqual(lines[-1]['subtype'], 'success')
        self.assertTrue(replay('badmodel')[-1].is_error)

    def test_background_task_starts_and_ends_with_one_id(self) -> None:
        items = replay('interrupt-background')
        started = [item.task_id for item in items if isinstance(item, TaskStarted)]
        ended = [item.task_id for item in items if isinstance(item, TaskEnded)]
        self.assertEqual(started, ['b1ajgv20g'])
        self.assertEqual(ended, started)

    def test_malformed_lines_give_nothing_but_the_replayed_message(self) -> None:
        lines = stream_lines('malformed')
        self.assertGreaterEqual(len(lines), 10)
        for number, line in enumerate(lines, 1):
            expected = [MessageEntered('Reply with the word second')] if number == 7 else []
            with self.subTest(line=number):
                self.assertEqual(parse_line(line), expected)

    def test_streams_hold_no_home_paths(self) -> None:
        for path in STREAMS.glob('*.jsonl'):
            with self.subTest(case=path.stem):
                self.assertNotIn(b'/home/', path.read_bytes())


INIT = {'type': 'system', 'subtype': 'init', 'session_id': TURNS}
TASK_STARTED = {
    'type': 'system', 'subtype': 'task_started', 'task_id': 'b1ajgv20g', 'is_backgrounded': True,
}
TASK_ENDED = {'type': 'system', 'subtype': 'task_notification', 'task_id': 'b1ajgv20g'}
ASSISTANT = {
    'type': 'assistant',
    'message': {
        'role': 'assistant',
        'content': [
            {'type': 'text', 'text': 'heron'},
            {'type': 'tool_use', 'id': 'toolu_1', 'name': 'Bash', 'input': {'command': 'ls'}},
            {'type': 'text', 'text': 'after'},
        ],
    },
}
CAN_USE_TOOL = {
    'type': 'control_request',
    'request_id': 'req-1',
    'request': {
        'subtype': 'can_use_tool',
        'tool_name': 'Bash',
        'input': {'command': 'ls'},
        'description': 'List files',
    },
}
RESULT = {
    'type': 'result', 'subtype': 'success', 'is_error': False, 'result': 'OK',
    'terminal_reason': 'completed',
}

HERON = TextOut('heron')
LS = ToolUse('toolu_1', 'Bash', {'command': 'ls'})
AFTER = TextOut('after')

FIELD_RULES: list[tuple[str, dict, tuple[object, ...], tuple[object, ...], list[object]]] = [
    ('result type', RESULT, ('type',), (7, ['result']), []),
    ('init type', INIT, ('type',), (7, {}), []),
    ('assistant type', ASSISTANT, ('type',), (7,), []),
    ('control_request type', CAN_USE_TOOL, ('type',), (7,), []),
    ('init subtype', INIT, ('subtype',), (7, ['init']), []),
    ('task_started subtype', TASK_STARTED, ('subtype',), (7,), []),
    ('init session_id', INIT, ('session_id',), (7, {'id': TURNS}), [Init(None)]),
    ('task_started task_id', TASK_STARTED, ('task_id',), (7, ['b1ajgv20g']), []),
    ('task_started is_backgrounded', TASK_STARTED, ('is_backgrounded',), ('true', 1), []),
    ('task_notification task_id', TASK_ENDED, ('task_id',), (7, ['b1ajgv20g']), []),
    ('assistant message', ASSISTANT, ('message',), ('heron', ['heron']), []),
    ('assistant message.content', ASSISTANT, ('message', 'content'), ('heron', {}), []),
    ('block type', ASSISTANT, ('message', 'content', 1, 'type'), (7, ['tool_use']), [HERON, AFTER]),
    ('text block text', ASSISTANT, ('message', 'content', 0, 'text'), (7, ['heron']), [LS, AFTER]),
    ('tool_use block id', ASSISTANT, ('message', 'content', 1, 'id'), (7,), [HERON, AFTER]),
    ('tool_use block name', ASSISTANT, ('message', 'content', 1, 'name'), (7,), [HERON, AFTER]),
    (
        'tool_use block input', ASSISTANT, ('message', 'content', 1, 'input'), ('ls', ['ls']),
        [HERON, AFTER],
    ),
    ('control_request request_id', CAN_USE_TOOL, ('request_id',), (7, ['req-1']), []),
    ('control_request request', CAN_USE_TOOL, ('request',), ('can_use_tool', []), []),
    ('control_request request.subtype', CAN_USE_TOOL, ('request', 'subtype'), (7,), []),
    (
        'can_use_tool tool_name', CAN_USE_TOOL, ('request', 'tool_name'), (7, ['Bash']),
        [BadRequest('req-1')],
    ),
    (
        'can_use_tool input', CAN_USE_TOOL, ('request', 'input'), ('ls', ['ls']),
        [BadRequest('req-1')],
    ),
    (
        'can_use_tool description', CAN_USE_TOOL, ('request', 'description'), (7, ['List']),
        [PermissionRequest('req-1', 'Bash', {'command': 'ls'}, None)],
    ),
    (
        'result is_error', RESULT, ('is_error',), ('false', 0, 'true'),
        [Result(True, 'OK', 'completed')],
    ),
    ('result result', RESULT, ('result',), (7, ['OK']), [Result(False, None, 'completed')]),
    (
        'result terminal_reason', RESULT, ('terminal_reason',), (7, ['completed']),
        [Result(False, 'OK', None)],
    ),
]

FULL_ITEMS = {
    'init': (INIT, [Init(TURNS)]),
    'task_started': (TASK_STARTED, [TaskStarted('b1ajgv20g')]),
    'task_notification': (TASK_ENDED, [TaskEnded('b1ajgv20g')]),
    'assistant': (ASSISTANT, [HERON, LS, AFTER]),
    'can_use_tool': (
        CAN_USE_TOOL, [PermissionRequest('req-1', 'Bash', {'command': 'ls'}, 'List files')],
    ),
    'result': (RESULT, [Result(False, 'OK', 'completed')]),
}


def field_cases(
    base: dict, path: tuple[object, ...], wrongs: tuple[object, ...],
) -> list[tuple[str, dict]]:
    cases = []
    variants = [('null', None), ('missing', KeyError), *[(f'wrong {w!r}', w) for w in wrongs]]
    for name, value in variants:
        line = copy.deepcopy(base)
        parent = line
        for key in path[:-1]:
            parent = parent[key]
        if value is KeyError:
            del parent[path[-1]]
        else:
            parent[path[-1]] = value
        cases.append((name, line))
    return cases


def all_field_lines() -> list[bytes]:
    return [
        encode(line)
        for _, base, path, wrongs, _ in FIELD_RULES
        for _, line in field_cases(base, path, wrongs)
    ]


class FieldRulesTest(unittest.TestCase):
    def test_base_lines_parse_in_full(self) -> None:
        for name, (line, expected) in FULL_ITEMS.items():
            with self.subTest(line=name):
                self.assertEqual(parse_line(encode(line)), expected)

    def test_each_field_rule(self) -> None:
        for row, base, path, wrongs, expected in FIELD_RULES:
            for case, line in field_cases(base, path, wrongs):
                with self.subTest(row=row, case=case):
                    self.assertEqual(parse_line(encode(line)), expected)

    def test_every_case_has_null_missing_and_wrong_type(self) -> None:
        for row, base, path, wrongs, _ in FIELD_RULES:
            with self.subTest(row=row):
                names = [name for name, _ in field_cases(base, path, wrongs)]
                self.assertEqual(names[:2], ['null', 'missing'])
                self.assertGreaterEqual(len(names), 3)

    def test_unreadable_blocks_are_skipped(self) -> None:
        line = copy.deepcopy(ASSISTANT)
        unreadable = [7, None, 'text', ['text'], {'type': 'thinking', 'thinking': 'x'}]
        line['message']['content'][1:1] = unreadable
        self.assertEqual(parse_line(encode(line)), [HERON, LS, AFTER])

    def test_other_control_request_subtypes_give_nothing(self) -> None:
        for subtype in ('interrupt', 'initialize', 'mcp_message', 'can_use_tool '):
            with self.subTest(subtype=subtype):
                line = copy.deepcopy(CAN_USE_TOOL)
                line['request']['subtype'] = subtype
                self.assertEqual(parse_line(encode(line)), [])

    def test_task_started_in_the_foreground_gives_nothing(self) -> None:
        line = {**TASK_STARTED, 'is_backgrounded': False}
        self.assertEqual(parse_line(encode(line)), [])


REPLAY = {
    'type': 'user',
    'message': {'role': 'user', 'content': 'Reply with the word second'},
    'isReplay': True,
}
SECOND = MessageEntered('Reply with the word second')


def replay_of(content: object) -> bytes:
    return encode({**REPLAY, 'message': {'role': 'user', 'content': content}})


def text_block(text: object) -> dict[str, object]:
    return {'type': 'text', 'text': text}


class ReplayedUserTest(unittest.TestCase):
    def assert_gives_nothing(self, cases: dict[str, bytes]) -> None:
        self.assertEqual(parse_line(encode(REPLAY)), [SECOND])
        for case, line in cases.items():
            with self.subTest(case=case):
                self.assertEqual(parse_line(line), [])

    def test_a_replayed_string_gives_its_text(self) -> None:
        self.assertEqual(parse_line(encode(REPLAY)), [SECOND])

    def test_a_replay_of_text_blocks_joins_their_texts_with_no_separator(self) -> None:
        line = replay_of([text_block('Reply with '), text_block('the word'), text_block(' second')])
        self.assertEqual(parse_line(line), [SECOND])

    def test_a_replay_with_other_items_gives_the_text_blocks_only(self) -> None:
        line = replay_of([
            {'type': 'image', 'source': {'type': 'base64', 'data': 'AAAA'}},
            text_block('Reply with the word '),
            {'tool_use_id': 'toolu_1', 'type': 'tool_result', 'content': 'done'},
            7,
            None,
            'loose text',
            ['text'],
            text_block('second'),
        ])
        self.assertEqual(parse_line(line), [SECOND])

    def test_a_replayed_interrupt_marker_gives_nothing(self) -> None:
        cases = {}
        for marker in ('[Request interrupted by user]',
                       '[Request interrupted by user for tool use]'):
            cases[f'{marker} as a string'] = replay_of(marker)
            cases[f'{marker} as a text block'] = replay_of([text_block(marker)])
        self.assert_gives_nothing(cases)

    def test_a_replay_that_only_quotes_the_interrupt_marker_gives_its_text(self) -> None:
        text = f'What does {INTERRUPT_MARKER}] mean?'
        self.assertEqual(parse_line(replay_of(text)), [MessageEntered(text)])

    def test_a_replay_with_no_text_gives_nothing(self) -> None:
        self.assert_gives_nothing({
            'empty string': replay_of(''),
            'empty list': replay_of([]),
            'empty text block': replay_of([text_block('')]),
            'no text block': replay_of([
                {'tool_use_id': 'toolu_1', 'type': 'tool_result', 'content': 'done'},
                {'type': 'image', 'source': {'type': 'base64', 'data': 'AAAA'}},
            ]),
            'unreadable text blocks': replay_of([text_block(None), text_block(7)]),
        })

    def test_a_replay_without_is_replay_true_gives_nothing(self) -> None:
        cases = {'missing': encode({k: v for k, v in REPLAY.items() if k != 'isReplay'})}
        for value in (None, False, 'true', 1):
            cases[repr(value)] = encode({**REPLAY, 'isReplay': value})
        self.assert_gives_nothing(cases)

    def test_a_replay_without_a_message_object_gives_nothing(self) -> None:
        cases = {'missing': encode({k: v for k, v in REPLAY.items() if k != 'message'})}
        for value in (None, 'Reply with the word second', ['Reply with the word second']):
            cases[repr(value)] = encode({**REPLAY, 'message': value})
        self.assert_gives_nothing(cases)

    def test_a_replay_without_string_or_list_content_gives_nothing(self) -> None:
        cases = {'missing': encode({**REPLAY, 'message': {'role': 'user'}})}
        for value in (None, 7, {'type': 'text', 'text': 'Reply with the word second'}):
            cases[repr(value)] = replay_of(value)
        self.assert_gives_nothing(cases)

    def test_a_text_item_without_a_string_text_adds_nothing(self) -> None:
        for case, text in (('missing', KeyError), ('null', None), ('number', 7)):
            item = {'type': 'text'} if text is KeyError else text_block(text)
            with self.subTest(case=case):
                line = replay_of([text_block('Reply with the word '), item, text_block('second')])
                self.assertEqual(parse_line(line), [SECOND])

    def test_an_item_that_is_not_a_text_block_adds_nothing(self) -> None:
        cases = [
            ('missing', {'text': 'ignored'}),
            ('null', {'type': None, 'text': 'ignored'}),
            ('tool_result', {'type': 'tool_result', 'text': 'ignored'}),
            ('number', {'type': 7, 'text': 'ignored'}),
            ('Text', {'type': 'Text', 'text': 'ignored'}),
        ]
        for case, item in cases:
            with self.subTest(case=case):
                line = replay_of([text_block('Reply with the word '), item, text_block('second')])
                self.assertEqual(parse_line(line), [SECOND])


class ParseLineTest(unittest.TestCase):
    def test_line_endings(self) -> None:
        for name, (line, expected) in FULL_ITEMS.items():
            raw = json.dumps(line, separators=(',', ':')).encode('utf-8')
            for ending in (b'', b'\n', b'\r\n'):
                with self.subTest(line=name, ending=ending):
                    self.assertEqual(parse_line(raw + ending), expected)

    def test_stream_lines_with_crlf(self) -> None:
        for case, expected in EXPECTED.items():
            with self.subTest(case=case):
                data = (STREAMS / f'{case}.jsonl').read_bytes().replace(b'\n', b'\r\n')
                items = [item for line in data.split(b'\n') for item in parse_line(line)]
                self.assertEqual(items, expected)

    def test_bytearray_and_memoryview(self) -> None:
        raw = encode(RESULT)
        self.assertEqual(parse_line(bytearray(raw)), [Result(False, 'OK', 'completed')])
        self.assertEqual(parse_line(memoryview(raw)), [Result(False, 'OK', 'completed')])

    def test_never_raises(self) -> None:
        lines = [line for case in EXPECTED for line in stream_lines(case)]
        lines += all_field_lines()
        lines += [
            b'[' * 100_000,
            b'{"a":' * 100_000,
            b'{"type":"result","is_error":' + b'9' * 10_000 + b'}',
            b'\xef\xbb\xbf' + encode(RESULT),
            encode(RESULT).decode('utf-8').encode('utf-16'),
            b'{"type":"result"} trailing',
            b'NaN',
            b'"result"',
            b'null',
            b'\x00',
            b'\n',
            b'\r\n',
        ]
        for number, line in enumerate(lines):
            with self.subTest(line=number):
                self.assertIsInstance(parse_line(line), list)


class TurnResultTest(unittest.TestCase):
    def test_completed(self) -> None:
        self.assertEqual(
            turn_result(Result(False, 'OK', 'completed'), False),
            TurnResult(TurnState.COMPLETED, 'OK', None),
        )
        self.assertEqual(
            turn_result(Result(False, 'OK', 'completed'), True),
            TurnResult(TurnState.COMPLETED, 'OK', None),
        )

    def test_badmodel_fails_with_api_error(self) -> None:
        result = replay('badmodel')[-1]
        text = 'API Error: 400 unknown provider for model spike-233-no-such-model'
        for interrupted in (False, True):
            with self.subTest(interrupted=interrupted):
                self.assertEqual(
                    turn_result(result, interrupted),
                    TurnResult(TurnState.FAILED, text, 'api_error'),
                )

    def test_interrupt_is_stopped_only_after_a_stop(self) -> None:
        result = replay('interrupt')[-1]
        self.assertEqual(
            turn_result(result, True), TurnResult(TurnState.STOPPED, None, 'aborted_streaming'),
        )
        self.assertEqual(
            turn_result(result, False), TurnResult(TurnState.FAILED, None, 'aborted_streaming'),
        )

    def test_error_without_an_aborted_reason_fails(self) -> None:
        for reason in (None, 'api_error', 'aborted', 'completed'):
            with self.subTest(reason=reason):
                self.assertEqual(
                    turn_result(Result(True, None, reason), True),
                    TurnResult(TurnState.FAILED, None, reason),
                )

    def test_aborted_tools_is_stopped(self) -> None:
        self.assertEqual(
            turn_result(Result(True, None, 'aborted_tools'), True),
            TurnResult(TurnState.STOPPED, None, 'aborted_tools'),
        )


AWKWARD = [
    'Reply with OK',
    'line one\nline two',
    'carriage\rreturn',
    'say "hi"',
    'smörgås',
    'line separator paragraph',
    'all\r\n"of"ä them\\',
    '',
]


class EncoderTest(unittest.TestCase):
    def assert_one_line(self, data: bytes, expected: object) -> None:
        self.assertIsInstance(data, bytes)
        self.assertTrue(data.endswith(b'\n'))
        self.assertFalse(data.endswith(b'\n\n'))
        self.assertEqual(data.count(b'\n'), 1)
        self.assertNotIn(b'\r', data)
        self.assertNotIn(' '.encode('utf-8'), data)
        self.assertNotIn(' '.encode('utf-8'), data)
        self.assertEqual(json.loads(data), expected)

    def test_user_line(self) -> None:
        for message in AWKWARD:
            with self.subTest(message=message):
                self.assert_one_line(
                    user_line(message),
                    {'type': 'user', 'message': {'role': 'user', 'content': message}},
                )

    def test_interrupt_line(self) -> None:
        for request_id in ('spike-int-1', *AWKWARD):
            with self.subTest(request_id=request_id):
                self.assert_one_line(interrupt_line(request_id), {
                    'type': 'control_request', 'request_id': request_id,
                    'request': {'subtype': 'interrupt'},
                })

    def test_allow_line(self) -> None:
        for message in AWKWARD:
            updated = {
                'command': message, 'description': message,
                'nested': {'list': [1, 2.5, None, True, message]},
            }
            with self.subTest(message=message):
                data = allow_line('2038e810-2aac-41ec-94f3-f181dcdbc712', updated)
                self.assert_one_line(data, {
                    'type': 'control_response',
                    'response': {
                        'subtype': 'success',
                        'request_id': '2038e810-2aac-41ec-94f3-f181dcdbc712',
                        'response': {'behavior': 'allow', 'updatedInput': updated},
                    },
                })
                self.assertEqual(json.loads(data)['response']['response']['updatedInput'], updated)

    def test_deny_line(self) -> None:
        for message in AWKWARD:
            with self.subTest(message=message):
                data = deny_line('ac794f47-7556-4f5f-a8a6-2cec64a05505', message)
                self.assert_one_line(data, {
                    'type': 'control_response',
                    'response': {
                        'subtype': 'success',
                        'request_id': 'ac794f47-7556-4f5f-a8a6-2cec64a05505',
                        'response': {'behavior': 'deny', 'message': message},
                    },
                })

    def test_lines_match_the_ones_claude_accepted(self) -> None:
        accepted = [
            (user_line('Reply with OK'),
             '{"type": "user", "message": {"role": "user", "content": "Reply with OK"}}'),
            (interrupt_line('spike-int-1'),
             '{"type": "control_request", "request_id": "spike-int-1", '
             '"request": {"subtype": "interrupt"}}'),
            (allow_line('2038e810-2aac-41ec-94f3-f181dcdbc712', WRITE_A),
             '{"type": "control_response", "response": {"subtype": "success", '
             '"request_id": "2038e810-2aac-41ec-94f3-f181dcdbc712", "response": '
             '{"behavior": "allow", "updatedInput": {"command": "echo spike-233 > a.txt", '
             '"description": "Write spike-233 to a.txt"}}}}'),
            (deny_line('ac794f47-7556-4f5f-a8a6-2cec64a05505', 'Denied by the spike-233 test.'),
             '{"type": "control_response", "response": {"subtype": "success", '
             '"request_id": "ac794f47-7556-4f5f-a8a6-2cec64a05505", "response": '
             '{"behavior": "deny", "message": "Denied by the spike-233 test."}}}'),
        ]
        for data, line in accepted:
            with self.subTest(line=line[:40]):
                self.assertEqual(json.loads(data), json.loads(line))


if __name__ == '__main__':
    unittest.main()
