import dataclasses
import unittest
from enum import StrEnum
from types import ModuleType
from typing import get_args, get_type_hints

import loom_contracts
from loom_contracts.calls import (
    CALL_PARAMS,
    ApiError,
    CallName,
    ProjectInfo,
    ScheduleInfo,
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
        {'workspace', 'runner', 'model', 'requests', 'origin'},
    ),
    'threads.delegate': ({'id', 'parent_id', 'task'}, {'workspace', 'runner', 'model'}),
    'threads.send': ({'id', 'thread_id', 'message'}, {'mode'}),
    'threads.list': (set(), {'project_id'}),
    'threads.get': ({'thread_id'}, set()),
    'threads.stop': ({'id', 'thread_id'}, set()),
    'threads.settle': ({'id', 'thread_id'}, set()),
    'threads.events': (set(), {'after', 'thread_id', 'follow'}),
    'requests.respond': ({'id', 'request_id', 'decision'}, {'message'}),
    'schedules.add': (
        {'id', 'trigger', 'text'}, {'action', 'thread_id', 'project_id', 'title'},
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
            ('threads.delegate', 'parent_id'): str,
            ('threads.delegate', 'workspace'): Workspace,
            ('threads.delegate', 'runner'): RunnerName,
            ('threads.send', 'mode'): DispatchMode,
            ('threads.list', 'project_id'): str,
            ('threads.events', 'after'): int,
            ('requests.respond', 'decision'): Decision,
            ('schedules.add', 'trigger'): Trigger,
            ('schedules.add', 'action'): ScheduleAction,
            ('schedules.add', 'project_id'): str,
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
                'state', 'waiting', 'reason', 'parent_id', 'origin', 'created_at',
            }),
        )
        self.assertEqual(ThreadInfo.__optional_keys__, frozenset())

    def test_schedule_info_keys(self) -> None:
        self.assertEqual(
            ScheduleInfo.__required_keys__,
            frozenset({
                'id', 'trigger', 'action', 'text', 'thread_id', 'project_id', 'title', 'by',
                'paused', 'next_run', 'created_at',
            }),
        )
        self.assertEqual(ScheduleInfo.__optional_keys__, frozenset())

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
