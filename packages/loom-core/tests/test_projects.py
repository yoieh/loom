import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from loom_contracts.runners import PermissionMode

from loom_core.errors import LoomError
from loom_core.projects import PROJECT_ID, Projects
from loom_core.store import Store

START = datetime(2026, 10, 24, 13, 8, 27, tzinfo=UTC)


class SteppingClock:
    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> datetime:
        self.now += timedelta(seconds=1)
        return self.now


class ProjectsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = Store.open(self.root / 'data', SteppingClock())
        self.addCleanup(self.store.close)
        self.projects = Projects(self.store)
        self.calls = 0

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def directory(self, name: str = 'repo') -> str:
        path = self.root / name
        path.mkdir(exist_ok=True)
        return str(path)

    def next_id(self) -> str:
        self.calls += 1
        return f'cmd-{self.calls}'

    def add(self, title: str = 'Loom', path: str | None = None, **extra: object) -> dict:
        params = {'id': self.next_id(), 'title': title,
                  'path': path or self.directory(title.lower()), **extra}
        return self.projects.add(params)

    def count(self, table: str) -> int:
        return self.store.connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]

    def assert_error(self, code: str, call, params: object, field: str | None = None) -> None:
        before = (self.count('projects'), self.count('commands'), self.projects.list())
        with self.assertRaises(LoomError) as caught:
            call(params)
        self.assertEqual(caught.exception.code, code)
        if field:
            self.assertIn(field, caught.exception.message)
        self.assertEqual((self.count('projects'), self.count('commands'), self.projects.list()),
                         before)


class AddTest(ProjectsTest):
    def test_add_returns_the_project(self) -> None:
        path = self.directory()
        info = self.projects.add({'id': 'c1', 'title': 'Loom', 'path': path,
                                  'permission_mode': 'approval-required'})
        self.assertEqual(info, {
            'id': 'loom',
            'title': 'Loom',
            'workspace_root': path,
            'permission_mode': 'approval-required',
            'created_at': '2026-10-24T13:08:29.000+00:00',
        })
        self.assertEqual(self.projects.list(), [info])
        self.assertEqual(self.store.events(), [])

    def test_add_is_one_transaction(self) -> None:
        seen: list[str] = []
        self.store.connection.set_trace_callback(seen.append)
        self.projects.add({'id': 'c1', 'title': 'Loom', 'path': self.directory()})
        self.store.connection.set_trace_callback(None)
        self.assertEqual([s for s in seen if s.strip().upper() == 'BEGIN IMMEDIATE'],
                         ['BEGIN IMMEDIATE'])
        self.assertEqual([s for s in seen if s.strip().upper() == 'COMMIT'], ['COMMIT'])
        self.assertEqual(self.count('projects'), 1)
        self.assertEqual(
            self.store.connection.execute('SELECT id, call FROM commands').fetchall(),
            [('c1', 'projects.add')],
        )

    def test_id_is_the_slug_of_the_title(self) -> None:
        self.assertEqual(self.add('Loom')['id'], 'loom')
        self.assertEqual(self.add('Övning Två!')['id'], 'ovning-tva')
        self.assertEqual(self.add('Loom', self.directory('second'))['id'], 'loom-2')
        self.assertEqual(self.add('Loom', self.directory('third'))['id'], 'loom-3')
        self.assertEqual(self.add('  Agent -- Fleet ', self.directory('fleet'))['id'],
                         'agent-fleet')

    def test_long_title_is_cut_to_40(self) -> None:
        title = 'A very long project title that keeps going on and on and on!'
        self.assertEqual(len(title), 60)
        first = self.add(title, self.directory('long1'))['id']
        second = self.add(title, self.directory('long2'))['id']
        for project_id in (first, second):
            self.assertLessEqual(len(project_id), 40)
            self.assertRegex(project_id, PROJECT_ID)
        self.assertEqual(first, 'a-very-long-project-title-that-keeps-goi')
        self.assertEqual(second, 'a-very-long-project-title-that-keeps-g-2')

    def test_cut_does_not_end_in_a_dash(self) -> None:
        project_id = self.add('x' * 39 + ' yz', self.directory('dash'))['id']
        self.assertEqual(project_id, 'x' * 39)
        self.assertRegex(project_id, PROJECT_ID)

    def test_title_without_a_slug_is_invalid(self) -> None:
        for title in ('!!!', '   ', '', '日本'):
            with self.subTest(title=title):
                self.assert_error('invalid', self.projects.add,
                                  {'id': self.next_id(), 'title': title,
                                   'path': self.directory()}, 'title')

    def test_path_is_stored_resolved(self) -> None:
        real = Path(self.directory('real'))
        (self.root / 'link').symlink_to(real)
        info = self.add('Linked', str(self.root / 'link'))
        self.assertEqual(info['workspace_root'], str(real))
        (real / 'sub').mkdir()
        info = self.add('Dotted', f'{real}/sub/../sub/.')
        self.assertEqual(info['workspace_root'], str(real / 'sub'))

    def test_bad_paths(self) -> None:
        file = self.root / 'file.txt'
        file.write_text('x')
        for path in ('relative/dir', str(self.root / 'missing'), str(file)):
            with self.subTest(path=path):
                self.assert_error('invalid', self.projects.add,
                                  {'id': self.next_id(), 'title': 'Loom', 'path': path}, 'path')

    def test_same_root_is_conflict(self) -> None:
        path = self.directory()
        self.add('Loom', path)
        self.assert_error('conflict', self.projects.add,
                          {'id': self.next_id(), 'title': 'Other', 'path': f'{path}/.'})

    def test_null_fields_are_invalid(self) -> None:
        for field in ('title', 'path', 'permission_mode'):
            params = {'id': self.next_id(), 'title': 'Loom', 'path': self.directory()}
            params[field] = None
            with self.subTest(field=field):
                self.assert_error('invalid', self.projects.add, params, field)
        self.assertEqual(self.count('projects'), 0)
        self.assertEqual(self.count('commands'), 0)

    def test_missing_fields(self) -> None:
        for field in ('title', 'path'):
            params = {'id': self.next_id(), 'title': 'Loom', 'path': self.directory()}
            del params[field]
            with self.subTest(field=field):
                self.assert_error('invalid', self.projects.add, params, field)
        info = self.projects.add({'id': 'c1', 'title': 'Loom', 'path': self.directory()})
        self.assertIsNone(info['permission_mode'])
        self.assertIsNone(self.projects.get('loom')['permission_mode'])

    def test_wrong_types_are_invalid(self) -> None:
        for field, value in (('title', 5), ('path', ['/tmp']), ('permission_mode', 'sudo'),
                             ('permission_mode', 3)):
            params = {'id': self.next_id(), 'title': 'Loom', 'path': self.directory()}
            params[field] = value
            with self.subTest(field=field, value=value):
                self.assert_error('invalid', self.projects.add, params, field)
        for params in ([], [('title', 'Loom')], 'Loom', None):
            with self.subTest(params=params):
                self.assert_error('invalid', self.projects.add, params, 'params')

    def test_command_id_is_checked(self) -> None:
        for command_id in (None, 5, '', 'a' * 201):
            params = {'id': command_id, 'title': 'Loom', 'path': self.directory()}
            with self.subTest(command_id=command_id):
                self.assert_error('invalid', self.projects.add, params, 'id')
        self.assert_error('invalid', self.projects.add,
                          {'title': 'Loom', 'path': self.directory()}, 'id')
        info = self.projects.add({'id': 'a' * 200, 'title': 'Loom', 'path': self.directory()})
        self.assertEqual(info['id'], 'loom')

    def test_retry_returns_the_first_answer(self) -> None:
        params = {'id': 'c1', 'title': 'Loom', 'path': self.directory()}
        first = self.projects.add(params)
        again = self.projects.add(dict(params))
        self.assertEqual(again, first)
        self.assertEqual(self.projects.list(), [first])
        self.assertEqual(self.count('commands'), 1)

    def test_same_id_for_another_call_is_conflict(self) -> None:
        self.projects.add({'id': 'c1', 'title': 'Loom', 'path': self.directory()})
        self.assert_error('conflict', self.projects.set_mode,
                          {'id': 'c1', 'project_id': 'loom', 'permission_mode': 'auto'})
        self.assert_error('conflict', self.projects.remove, {'id': 'c1', 'project_id': 'loom'})


class SetModeTest(ProjectsTest):
    def setUp(self) -> None:
        super().setUp()
        self.project = self.add('Loom')

    def test_set_mode_changes_the_mode(self) -> None:
        info = self.projects.set_mode({'id': 'm1', 'project_id': 'loom',
                                       'permission_mode': PermissionMode.FULL_ACCESS})
        self.assertEqual(info, {**self.project, 'permission_mode': 'full-access'})
        self.assertEqual(self.projects.get('loom'), info)

    def test_retry_changes_nothing(self) -> None:
        params = {'id': 'm1', 'project_id': 'loom', 'permission_mode': 'approval-required'}
        first = self.projects.set_mode(params)
        self.projects.set_mode({'id': 'm2', 'project_id': 'loom', 'permission_mode': 'auto'})
        self.assertEqual(self.projects.set_mode(dict(params)), first)
        self.assertEqual(self.projects.get('loom')['permission_mode'], 'auto')

    def test_unknown_project_is_not_found(self) -> None:
        self.assert_error('not_found', self.projects.set_mode,
                          {'id': self.next_id(), 'project_id': 'nope', 'permission_mode': 'auto'})

    def test_bad_params_are_invalid(self) -> None:
        good = {'project_id': 'loom', 'permission_mode': 'auto'}
        for field in ('project_id', 'permission_mode'):
            for value in (None, 5, 'missing'):
                params = {'id': self.next_id(), **good}
                if value == 'missing':
                    del params[field]
                else:
                    params[field] = value
                with self.subTest(field=field, value=value):
                    self.assert_error('invalid', self.projects.set_mode, params, field)
        params = {'id': self.next_id(), **good, 'permission_mode': 'sudo'}
        self.assert_error('invalid', self.projects.set_mode, params, 'permission_mode')
        self.assert_error('invalid', self.projects.set_mode, ['loom'], 'params')


class RemoveTest(ProjectsTest):
    def setUp(self) -> None:
        super().setUp()
        self.project = self.add('Loom')
        self.other = self.add('Other')

    def test_remove_deletes_and_returns_the_project(self) -> None:
        info = self.projects.remove({'id': 'r1', 'project_id': 'loom'})
        self.assertEqual(info, self.project)
        self.assertEqual(self.projects.list(), [self.other])
        with self.assertRaises(LoomError) as caught:
            self.projects.get('loom')
        self.assertEqual(caught.exception.code, 'not_found')

    def test_retry_after_the_project_is_gone(self) -> None:
        first = self.projects.remove({'id': 'r1', 'project_id': 'loom'})
        self.assertEqual(self.projects.remove({'id': 'r1', 'project_id': 'loom'}), first)
        self.assertEqual(self.projects.list(), [self.other])
        self.assert_error('not_found', self.projects.remove, {'id': 'r2', 'project_id': 'loom'})

    def test_retry_does_not_remove_a_new_project_with_the_same_id(self) -> None:
        first = self.projects.remove({'id': 'r1', 'project_id': 'loom'})
        again = self.add('Loom', self.directory('again'))
        self.assertEqual(self.projects.remove({'id': 'r1', 'project_id': 'loom'}), first)
        self.assertEqual(self.projects.get('loom'), again)

    def test_unknown_project_is_not_found(self) -> None:
        self.assert_error('not_found', self.projects.remove, {'id': 'r1', 'project_id': 'nope'})

    def test_bad_params_are_invalid(self) -> None:
        for value in (None, 5, ['loom'], 'missing'):
            params = {'id': self.next_id(), 'project_id': value}
            if value == 'missing':
                del params['project_id']
            with self.subTest(value=value):
                self.assert_error('invalid', self.projects.remove, params, 'project_id')
        self.assert_error('invalid', self.projects.remove, 'loom', 'params')


class ListGetTest(ProjectsTest):
    def test_list_is_in_creation_order(self) -> None:
        zeta = self.add('Zeta')
        alpha = self.add('Alpha')
        mid = self.add('Mid')
        self.assertEqual(self.projects.list(), [zeta, alpha, mid])
        self.assertEqual(self.projects.get('alpha'), alpha)

    def test_same_created_at_is_ordered_by_id(self) -> None:
        self.store.clock = lambda: START
        self.add('Beta')
        self.add('Alpha')
        self.assertEqual([p['id'] for p in self.projects.list()], ['alpha', 'beta'])

    def test_empty_list(self) -> None:
        self.assertEqual(self.projects.list(), [])

    def test_get_unknown_is_not_found(self) -> None:
        with self.assertRaises(LoomError) as caught:
            self.projects.get('nope')
        self.assertEqual(caught.exception.code, 'not_found')


class EffectiveModeTest(ProjectsTest):
    def test_set_mode_wins(self) -> None:
        for default in PermissionMode:
            projects = Projects(self.store, default)
            for mode in PermissionMode:
                path = self.directory(f'{default.value}-{mode.value}')
                info = projects.add({'id': self.next_id(), 'title': 'P', 'path': path,
                                     'permission_mode': mode})
                with self.subTest(default=default, mode=mode):
                    self.assertIs(projects.effective_mode(info['id']), mode)

    def test_unset_mode_uses_the_default_but_never_full_access(self) -> None:
        project_id = self.add('Loom')['id']
        cases = [
            (Projects(self.store), PermissionMode.AUTO),
            (Projects(self.store, 'approval-required'), PermissionMode.APPROVAL_REQUIRED),
            (Projects(self.store, PermissionMode.AUTO_ACCEPT_EDITS),
             PermissionMode.AUTO_ACCEPT_EDITS),
            (Projects(self.store, PermissionMode.FULL_ACCESS), PermissionMode.AUTO),
            (Projects(self.store, 'full-access'), PermissionMode.AUTO),
        ]
        for projects, expected in cases:
            with self.subTest(default=projects.default_mode):
                self.assertIs(projects.effective_mode(project_id), expected)

    def test_bad_default_is_invalid(self) -> None:
        for default in ('sudo', None, 3, ''):
            with self.subTest(default=default):
                with self.assertRaises(LoomError) as caught:
                    Projects(self.store, default)
                self.assertEqual(caught.exception.code, 'invalid')

    def test_unknown_project_is_not_found(self) -> None:
        with self.assertRaises(LoomError) as caught:
            self.projects.effective_mode('nope')
        self.assertEqual(caught.exception.code, 'not_found')


class TempDirOnlyTest(unittest.TestCase):
    def test_store_has_no_default_data_dir(self) -> None:
        with self.assertRaises(TypeError):
            Store.open()


if __name__ == '__main__':
    unittest.main()
