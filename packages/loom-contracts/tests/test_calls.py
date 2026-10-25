import dataclasses
import unittest
from enum import StrEnum
from types import ModuleType
from typing import get_args, get_type_hints

import loom_contracts
from loom_contracts.calls import (
    CALL_PARAMS,
    MAX_CONCURRENCY_KEY,
    MAX_PAYLOAD,
    MAX_SIGNAL_KEY,
    MAX_TOPIC,
    TOPIC_PATTERN,
    ApiError,
    Audience,
    CallName,
    OnBusy,
    ProjectInfo,
    ScheduleInfo,
    Settle,
    ThreadInfo,
)
from loom_contracts.events import EVENT_DATA, Event
from loom_contracts.runners import (
    Decision,
    DispatchMode,
    PermissionMode,
    RunnerName,
    ThreadSpec,
)
from loom_contracts.triggers import ScheduleAction, Trigger
from loom_contracts.workspaces import Workspace

PARAMS: dict[str, tuple[set[str], set[str]]] = {
    'threads.launch': (
        {'id', 'title', 'message', 'project_id'},
        {'workspace', 'runner', 'model', 'requests', 'origin', 'audience', 'concurrency_key',
         'on_busy', 'settle', 'cause'},
    ),
    'threads.delegate': (
        {'id', 'parent_id', 'task'},
        {'workspace', 'runner', 'model', 'concurrency_key', 'on_busy'},
    ),
    'threads.send': ({'id', 'thread_id', 'message'}, {'mode', 'cause'}),
    'threads.list': (set(), {'project_id', 'audience'}),
    'threads.get': ({'thread_id'}, set()),
    'threads.stop': ({'id', 'thread_id'}, set()),
    'threads.settle': ({'id', 'thread_id'}, set()),
    'threads.update': ({'id', 'thread_id', 'audience'}, set()),
    'threads.events': (set(), {'after', 'thread_id', 'follow', 'audience'}),
    'requests.respond': ({'id', 'request_id', 'decision'}, {'message'}),
    'signals.emit': ({'id', 'topic', 'payload'}, {'project_id', 'key', 'cause'}),
    'schedules.add': (
        {'id', 'trigger', 'text'},
        {'action', 'thread_id', 'project_id', 'title', 'concurrency_key', 'on_busy', 'settle'},
    ),
    'schedules.list': (set(), {'thread_id', 'project_id'}),
    'schedules.pause': ({'id', 'schedule_id'}, set()),
    'schedules.remove': ({'id', 'schedule_id'}, set()),
    'projects.add': ({'id', 'title', 'path'}, {'permission_mode'}),
    'projects.set_mode': ({'id', 'project_id', 'permission_mode'}, set()),
    'projects.remove': ({'id', 'project_id'}, set()),
    'projects.list': (set(), set()),
}

READ_CALLS = {'threads.list', 'threads.get', 'threads.events', 'schedules.list', 'projects.list'}


class CallNameTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertTrue(issubclass(CallName, StrEnum))
        self.assertEqual({call.value for call in CallName}, set(PARAMS))


class AudienceTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertTrue(issubclass(Audience, StrEnum))
        self.assertEqual([item.value for item in Audience], ['person', 'agent'])


class ConcurrencyTest(unittest.TestCase):
    def test_on_busy_values(self) -> None:
        self.assertTrue(issubclass(OnBusy, StrEnum))
        self.assertEqual([item.value for item in OnBusy], ['queue', 'coalesce'])

    def test_settle_values(self) -> None:
        self.assertTrue(issubclass(Settle, StrEnum))
        self.assertEqual([item.value for item in Settle], ['on_result'])

    def test_max_concurrency_key_is_200(self) -> None:
        self.assertEqual(MAX_CONCURRENCY_KEY, 200)


class SignalLimitsTest(unittest.TestCase):
    def test_the_signal_limits(self) -> None:
        self.assertEqual((MAX_TOPIC, MAX_SIGNAL_KEY, MAX_PAYLOAD), (200, 200, 16_384))

    def test_the_topic_pattern_is_a_string(self) -> None:
        self.assertEqual(TOPIC_PATTERN, r'[A-Za-z0-9][A-Za-z0-9._:-]*')

    def test_signals_emit_is_a_call(self) -> None:
        self.assertEqual(CallName.SIGNALS_EMIT, 'signals.emit')


class CallParamsTest(unittest.TestCase):
    def test_every_call_has_params_and_no_other_key(self) -> None:
        self.assertEqual(set(CALL_PARAMS), set(CallName))

    def test_keys(self) -> None:
        for call, (required, optional) in PARAMS.items():
            with self.subTest(call=call):
                params = CALL_PARAMS[CallName(call)]
                self.assertEqual(params.__required_keys__, frozenset(required))
                self.assertEqual(params.__optional_keys__, frozenset(optional))

    def test_changing_calls_require_an_id_and_read_calls_have_none(self) -> None:
        for call, params in CALL_PARAMS.items():
            with self.subTest(call=call):
                if call in READ_CALLS:
                    self.assertNotIn('id', params.__required_keys__ | params.__optional_keys__)
                else:
                    self.assertIn('id', params.__required_keys__)

    def test_field_types(self) -> None:
        expected = {
            ('threads.launch', 'project_id'): str,
            ('threads.launch', 'workspace'): Workspace,
            ('threads.launch', 'runner'): RunnerName,
            ('threads.launch', 'audience'): Audience,
            ('threads.launch', 'concurrency_key'): str,
            ('threads.launch', 'on_busy'): OnBusy,
            ('threads.launch', 'settle'): Settle,
            ('threads.launch', 'cause'): int,
            ('threads.delegate', 'parent_id'): str,
            ('threads.delegate', 'workspace'): Workspace,
            ('threads.delegate', 'runner'): RunnerName,
            ('threads.delegate', 'concurrency_key'): str,
            ('threads.delegate', 'on_busy'): OnBusy,
            ('threads.send', 'mode'): DispatchMode,
            ('threads.send', 'cause'): int,
            ('threads.list', 'project_id'): str,
            ('threads.list', 'audience'): Audience,
            ('threads.update', 'thread_id'): str,
            ('threads.update', 'audience'): Audience,
            ('threads.events', 'after'): int,
            ('threads.events', 'audience'): Audience,
            ('requests.respond', 'decision'): Decision,
            ('signals.emit', 'topic'): str,
            ('signals.emit', 'payload'): dict[str, object],
            ('signals.emit', 'project_id'): str,
            ('signals.emit', 'key'): str,
            ('signals.emit', 'cause'): int,
            ('schedules.add', 'trigger'): Trigger,
            ('schedules.add', 'action'): ScheduleAction,
            ('schedules.add', 'project_id'): str,
            ('schedules.add', 'concurrency_key'): str,
            ('schedules.add', 'on_busy'): OnBusy,
            ('schedules.add', 'settle'): Settle,
            ('schedules.list', 'project_id'): str,
            ('projects.add', 'permission_mode'): PermissionMode,
            ('projects.set_mode', 'permission_mode'): PermissionMode,
        }
        for (call, key), kind in expected.items():
            with self.subTest(call=call, key=key):
                self.assertEqual(get_type_hints(CALL_PARAMS[CallName(call)])[key], kind)

    def test_launch_requests_is_caller_or_any(self) -> None:
        hints = get_type_hints(CALL_PARAMS[CallName.THREADS_LAUNCH])
        self.assertEqual(set(get_args(hints['requests'])), {'caller', 'any'})

    def test_no_permission_mode_on_thread_calls(self) -> None:
        for call, params in CALL_PARAMS.items():
            if call.startswith('threads.'):
                with self.subTest(call=call):
                    keys = params.__required_keys__ | params.__optional_keys__
                    self.assertNotIn('permission_mode', keys)


class RecordTest(unittest.TestCase):
    def test_project_info_has_the_five_project_keys(self) -> None:
        self.assertEqual(
            ProjectInfo.__required_keys__,
            frozenset({'id', 'title', 'workspace_root', 'permission_mode', 'created_at'}),
        )
        self.assertEqual(ProjectInfo.__optional_keys__, frozenset())

    def test_thread_info_keys(self) -> None:
        self.assertEqual(
            ThreadInfo.__required_keys__,
            frozenset({
                'id', 'title', 'runner', 'project_id', 'workspace_path', 'branch', 'model',
                'permission_mode', 'state', 'waiting', 'reason', 'parent_id', 'origin',
                'audience', 'concurrency_key', 'settle', 'cause', 'hops', 'created_at',
            }),
        )
        self.assertEqual(ThreadInfo.__optional_keys__, frozenset())
        self.assertIs(get_type_hints(ThreadInfo)['permission_mode'], PermissionMode)
        self.assertIs(get_type_hints(ThreadInfo)['audience'], Audience)
        self.assertEqual(get_type_hints(ThreadInfo)['concurrency_key'], str | None)
        self.assertEqual(get_type_hints(ThreadInfo)['settle'], Settle | None)
        self.assertEqual(get_type_hints(ThreadInfo)['cause'], int | None)
        self.assertIs(get_type_hints(ThreadInfo)['hops'], int)

    def test_thread_info_has_audience_right_after_origin(self) -> None:
        names = list(ThreadInfo.__annotations__)
        self.assertEqual(names[names.index('origin') + 1], 'audience')

    def test_thread_info_has_concurrency_key_and_settle_right_after_audience(self) -> None:
        names = list(ThreadInfo.__annotations__)
        at = names.index('audience')
        self.assertEqual(names[at + 1:at + 3], ['concurrency_key', 'settle'])

    def test_thread_info_has_cause_and_hops_right_after_settle(self) -> None:
        names = list(ThreadInfo.__annotations__)
        at = names.index('settle')
        self.assertEqual(names[at + 1:], ['cause', 'hops', 'created_at'])

    def test_schedule_info_keys(self) -> None:
        self.assertEqual(
            ScheduleInfo.__required_keys__,
            frozenset({
                'id', 'trigger', 'action', 'text', 'thread_id', 'project_id', 'title',
                'concurrency_key', 'on_busy', 'settle', 'by', 'paused', 'next_run',
                'created_at',
            }),
        )
        self.assertEqual(ScheduleInfo.__optional_keys__, frozenset())
        hints = get_type_hints(ScheduleInfo)
        self.assertEqual(hints['concurrency_key'], str | None)
        self.assertEqual(hints['on_busy'], OnBusy | None)
        self.assertEqual(hints['settle'], Settle | None)

    def test_schedule_info_has_the_launch_options_right_after_title(self) -> None:
        names = list(ScheduleInfo.__annotations__)
        at = names.index('title')
        self.assertEqual(names[at + 1:at + 5], ['concurrency_key', 'on_busy', 'settle', 'by'])

    def test_api_error_keys(self) -> None:
        self.assertEqual(ApiError.__required_keys__, frozenset({'code', 'message'}))
        self.assertEqual(ApiError.__optional_keys__, frozenset())


class ApiVersionTest(unittest.TestCase):
    def test_api_version_is_v1(self) -> None:
        self.assertEqual(loom_contracts.API_VERSION, 'v1')

    def test_package_re_exports_nothing(self) -> None:
        public = {
            name for name, value in vars(loom_contracts).items()
            if not name.startswith('_') and not isinstance(value, ModuleType)
        }
        self.assertEqual(public, {'API_VERSION'})


class ReferenceNamesTest(unittest.TestCase):
    def test_no_bare_project_or_parent_key(self) -> None:
        shapes: dict[str, set[str]] = {}
        typed_dicts = [
            *CALL_PARAMS.values(), ThreadInfo, ProjectInfo, ScheduleInfo, ApiError, Event,
            *EVENT_DATA.values(),
        ]
        for shape in typed_dicts:
            shapes[shape.__name__] = set(shape.__required_keys__ | shape.__optional_keys__)
        shapes[ThreadSpec.__name__] = {field.name for field in dataclasses.fields(ThreadSpec)}
        for name, keys in shapes.items():
            with self.subTest(shape=name):
                self.assertFalse(keys & {'project', 'parent'})


if __name__ == '__main__':
    unittest.main()
