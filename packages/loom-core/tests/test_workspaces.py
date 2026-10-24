import os
import stat
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from loom_contracts.workspaces import WorkspaceStrategy

from loom_core.errors import LoomError
from loom_core.workspaces import (
    THREAD_ID, WorkspacePlan, make_workspace, plan_workspace, worktree_path,
)

SETUP_ENV = {**os.environ, 'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_CONFIG_NOSYSTEM': '1'}


def git(cwd: Path, *args: str) -> str:
    done = subprocess.run(['git', *args], cwd=cwd, env=SETUP_ENV, check=True,
                          capture_output=True, text=True)
    return done.stdout.strip()


def commit(cwd: Path, message: str) -> str:
    git(cwd, '-c', 'user.name=Test', '-c', 'user.email=test@example.com',
        'commit', '-q', '--allow-empty', '-m', message)
    return git(cwd, 'rev-parse', 'HEAD')


def make_repo(path: Path) -> Path:
    path.mkdir(parents=True)
    git(path, 'init', '-q', '-b', 'main')
    commit(path, 'First')
    return path.resolve()


def mode_of(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def project_for(root: Path, project_id: str = 'loom') -> dict:
    return {'id': project_id, 'title': project_id.title(), 'workspace_root': str(root),
            'permission_mode': None, 'created_at': '2026-10-24T13:08:27.000+00:00'}


def new_thread() -> str:
    return str(uuid.uuid4())


class WorkspaceTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name).resolve()
        home = self.base / 'home'
        home.mkdir()
        patcher = mock.patch.dict(os.environ, {'HOME': str(home)})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.root = make_repo(self.base / 'repo')
        self.data_dir = self.base / 'data'
        self.data_dir.mkdir(mode=0o700)
        self.project = project_for(self.root)
        self.thread = new_thread()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def plan(self, workspace: object, project: dict | None = None,
             thread: str | None = None) -> WorkspacePlan:
        return plan_workspace(workspace, project or self.project, thread or self.thread,
                              self.data_dir)

    async def make(self, workspace: object, project: dict | None = None,
                   thread: str | None = None) -> WorkspacePlan:
        project = project or self.project
        return await make_workspace(self.plan(workspace, project, thread), project,
                                    self.data_dir)

    def assert_invalid(self, workspace: object, field: str | None = None,
                       project: dict | None = None, thread: str | None = None) -> None:
        with self.assertRaises(LoomError) as caught:
            self.plan(workspace, project, thread)
        self.assertEqual(caught.exception.code, 'invalid')
        if field:
            self.assertIn(field, caught.exception.message)

    async def assert_make_error(self, code: str, workspace: object,
                                project: dict | None = None,
                                thread: str | None = None) -> LoomError:
        with self.assertRaises(LoomError) as caught:
            await self.make(workspace, project, thread)
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def worktrees(self) -> list[str]:
        listing = git(self.root, 'worktree', 'list', '--porcelain')
        return [line.split(' ', 1)[1] for line in listing.splitlines()
                if line.startswith('worktree ')]


class RootTest(WorkspaceTest):
    async def test_root_is_the_project_root_on_its_branch(self) -> None:
        plan = self.plan({'strategy': 'root'})
        self.assertEqual(plan, WorkspacePlan(WorkspaceStrategy.ROOT, str(self.root), None, None))
        made = await make_workspace(plan, self.project, self.data_dir)
        self.assertEqual(made, WorkspacePlan(WorkspaceStrategy.ROOT, str(self.root), 'main', None))
        self.assertFalse((self.data_dir / 'worktrees').exists())

    async def test_root_without_git_has_no_branch(self) -> None:
        plain = self.base / 'plain'
        plain.mkdir()
        made = await self.make({'strategy': 'root'}, project_for(plain, 'plain'))
        self.assertEqual(made, WorkspacePlan(WorkspaceStrategy.ROOT, str(plain), None, None))

    async def test_root_with_detached_head_has_no_branch(self) -> None:
        git(self.root, 'checkout', '-q', '--detach')
        made = await self.make({'strategy': 'root'})
        self.assertIsNone(made.branch)


class WorktreeTest(WorkspaceTest):
    async def test_defaults_make_a_worktree_from_the_current_branch(self) -> None:
        plan = self.plan({'strategy': 'worktree'})
        path = self.data_dir / 'worktrees' / 'loom' / self.thread
        self.assertEqual(worktree_path(self.data_dir, 'loom', self.thread), path)
        self.assertEqual(plan, WorkspacePlan(WorkspaceStrategy.WORKTREE, str(path),
                                             f'loom/{self.thread}', None))
        made = await make_workspace(plan, self.project, self.data_dir)
        self.assertEqual(made, WorkspacePlan(WorkspaceStrategy.WORKTREE, str(path),
                                             f'loom/{self.thread}', 'main'))
        self.assertEqual(self.worktrees(), [str(self.root), str(path)])
        self.assertEqual(git(path, 'symbolic-ref', '--short', 'HEAD'), f'loom/{self.thread}')
        self.assertEqual(git(path, 'rev-parse', 'HEAD'), git(self.root, 'rev-parse', 'main'))
        for directory in (path, path.parent, path.parent.parent):
            self.assertEqual(mode_of(directory), 0o700, directory)

    async def test_modes_hold_under_a_wide_umask(self) -> None:
        old = os.umask(0)
        try:
            made = await self.make({'strategy': 'worktree'})
        finally:
            os.umask(old)
        path = Path(made.path)
        for directory in (path, path.parent, path.parent.parent):
            self.assertEqual(mode_of(directory), 0o700, directory)

    async def test_given_branch_and_base_are_used(self) -> None:
        git(self.root, 'checkout', '-q', '-b', 'dev')
        dev = commit(self.root, 'Second')
        git(self.root, 'checkout', '-q', 'main')
        made = await self.make({'strategy': 'worktree', 'branch': 'feature/x', 'base': 'dev'})
        self.assertEqual((made.branch, made.base), ('feature/x', 'dev'))
        self.assertEqual(git(Path(made.path), 'symbolic-ref', '--short', 'HEAD'), 'feature/x')
        self.assertEqual(git(Path(made.path), 'rev-parse', 'HEAD'), dev)

    async def test_detached_root_bases_on_its_head(self) -> None:
        first = git(self.root, 'rev-parse', 'HEAD')
        commit(self.root, 'Second')
        git(self.root, 'checkout', '-q', '--detach', first)
        made = await self.make({'strategy': 'worktree'})
        self.assertEqual(made.base, first)
        self.assertEqual(git(Path(made.path), 'rev-parse', 'HEAD'), first)

    async def test_base_that_is_no_commit_is_invalid(self) -> None:
        for base in ('nope', 'main^{tree}', 'main~5'):
            with self.subTest(base=base):
                error = await self.assert_make_error(
                    'invalid', {'strategy': 'worktree', 'base': base})
                self.assertIn('base', error.message)
        self.assertEqual(self.worktrees(), [str(self.root)])

    async def test_branch_git_refuses_is_invalid(self) -> None:
        git(self.root, 'checkout', '-q', '-b', 'previous')
        git(self.root, 'checkout', '-q', 'main')
        for branch in ('a..b', 'x~1', 'end.lock', 'a b', '@{-1}', 'HEAD'):
            with self.subTest(branch=branch):
                error = await self.assert_make_error(
                    'invalid', {'strategy': 'worktree', 'branch': branch})
                self.assertIn('branch', error.message)
        self.assertEqual(self.worktrees(), [str(self.root)])
        self.assertEqual(git(self.root, 'branch', '--format=%(refname:short)').split(),
                         ['main', 'previous'])

    async def test_retry_returns_the_same_plan_and_one_worktree(self) -> None:
        first = await self.make({'strategy': 'worktree'})
        again = await self.make({'strategy': 'worktree'})
        self.assertEqual(again, first)
        self.assertEqual(self.worktrees(), [str(self.root), first.path])

    async def test_branch_used_by_another_worktree_is_conflict(self) -> None:
        await self.make({'strategy': 'worktree', 'branch': 'shared'})
        other = new_thread()
        error = await self.assert_make_error(
            'conflict', {'strategy': 'worktree', 'branch': 'shared'}, thread=other)
        self.assertIn('shared', error.message)
        self.assertEqual(len(self.worktrees()), 2)
        self.assertFalse(worktree_path(self.data_dir, 'loom', other).exists())

    async def test_existing_branch_is_conflict(self) -> None:
        git(self.root, 'branch', 'taken')
        await self.assert_make_error('conflict', {'strategy': 'worktree', 'branch': 'taken'})
        self.assertEqual(self.worktrees(), [str(self.root)])

    async def test_path_that_is_a_plain_directory_is_conflict(self) -> None:
        path = worktree_path(self.data_dir, 'loom', self.thread)
        path.mkdir(parents=True)
        await self.assert_make_error('conflict', {'strategy': 'worktree'})
        self.assertEqual(self.worktrees(), [str(self.root)])
        self.assertEqual(list(path.iterdir()), [])

    async def test_path_that_is_a_file_is_conflict(self) -> None:
        path = worktree_path(self.data_dir, 'loom', self.thread)
        path.parent.mkdir(parents=True)
        path.write_text('x')
        await self.assert_make_error('conflict', {'strategy': 'worktree'})

    async def test_path_on_another_branch_is_conflict(self) -> None:
        await self.make({'strategy': 'worktree', 'branch': 'one'})
        await self.assert_make_error('conflict', {'strategy': 'worktree', 'branch': 'two'})
        self.assertEqual(len(self.worktrees()), 2)

    async def test_symlinked_project_dir_is_refused(self) -> None:
        elsewhere = self.base / 'elsewhere'
        elsewhere.mkdir()
        (self.data_dir / 'worktrees').mkdir(mode=0o700)
        (self.data_dir / 'worktrees' / 'loom').symlink_to(elsewhere)
        await self.assert_make_error('conflict', {'strategy': 'worktree'})
        self.assertEqual(list(elsewhere.iterdir()), [])
        self.assertEqual(self.worktrees(), [str(self.root)])

    async def test_symlink_at_the_worktree_path_is_conflict(self) -> None:
        target = self.base / 'target'
        path = worktree_path(self.data_dir, 'loom', self.thread)
        path.parent.mkdir(parents=True)
        path.symlink_to(target)
        await self.assert_make_error('conflict', {'strategy': 'worktree'})
        self.assertFalse(target.exists())

    async def test_root_that_is_no_git_repo_is_invalid(self) -> None:
        plain = self.base / 'plain'
        plain.mkdir()
        await self.assert_make_error('invalid', {'strategy': 'worktree'},
                                     project_for(plain, 'plain'))
        self.assertFalse((self.data_dir / 'worktrees').exists())

    async def test_root_inside_a_repo_is_invalid(self) -> None:
        inner = self.root / 'inner'
        inner.mkdir()
        await self.assert_make_error('invalid', {'strategy': 'worktree'},
                                     project_for(inner, 'inner'))
        self.assertEqual(self.worktrees(), [str(self.root)])

    async def test_plan_for_another_project_dir_is_invalid(self) -> None:
        plan = self.plan({'strategy': 'worktree'}, project_for(self.root, 'other'))
        with self.assertRaises(LoomError) as caught:
            await make_workspace(plan, self.project, self.data_dir)
        self.assertEqual(caught.exception.code, 'invalid')
        self.assertEqual(self.worktrees(), [str(self.root)])


class ExistingWorktreeTest(WorkspaceTest):
    async def test_worktree_loom_made_is_accepted(self) -> None:
        made = await self.make({'strategy': 'worktree'})
        workspace = {'strategy': 'existing_worktree', 'path': made.path, 'branch': made.branch}
        child = new_thread()
        plan = self.plan(workspace, thread=child)
        expected = WorkspacePlan(WorkspaceStrategy.EXISTING_WORKTREE, made.path, made.branch, None)
        self.assertEqual(plan, expected)
        self.assertEqual(await make_workspace(plan, self.project, self.data_dir), expected)
        self.assertEqual(len(self.worktrees()), 2)

    async def test_project_root_on_its_branch_is_accepted(self) -> None:
        made = await self.make({'strategy': 'existing_worktree', 'path': str(self.root),
                                'branch': 'main'})
        self.assertEqual(made, WorkspacePlan(WorkspaceStrategy.EXISTING_WORKTREE,
                                             str(self.root), 'main', None))

    async def test_path_with_dot_segments_inside_is_resolved(self) -> None:
        made = await self.make({'strategy': 'worktree'})
        path = f'{self.data_dir}/worktrees/loom/x/../{self.thread}'
        plan = self.plan({'strategy': 'existing_worktree', 'path': path,
                          'branch': made.branch}, thread=new_thread())
        self.assertEqual(plan.path, made.path)

    async def test_paths_outside_are_invalid(self) -> None:
        sibling = self.base / 'sibling'
        git(self.root, 'worktree', 'add', '-q', '-b', 'side', str(sibling))
        other = self.data_dir / 'worktrees' / 'other' / 'x'
        other.parent.mkdir(parents=True)
        git(self.root, 'worktree', 'add', '-q', '-b', 'other', str(other))
        escape = self.base / 'escape'
        escape.mkdir()
        project_dir = self.data_dir / 'worktrees' / 'loom'
        project_dir.mkdir()
        (project_dir / 'link').symlink_to(sibling)
        cases = {
            'sibling': (str(sibling), 'side'),
            'other project': (str(other), 'other'),
            'dot-dot out': (f'{project_dir}/../other/x', 'other'),
            'dot-dot out of root': (f'{self.root}/../escape', 'main'),
            'project dir itself': (str(project_dir), 'main'),
            'symlink out': (str(project_dir / 'link'), 'side'),
            'data dir': (str(self.data_dir), 'main'),
            'filesystem root': ('/', 'main'),
        }
        for name, (path, branch) in cases.items():
            with self.subTest(name):
                self.assert_invalid({'strategy': 'existing_worktree', 'path': path,
                                     'branch': branch}, 'path')

    async def test_branch_that_does_not_match_is_invalid(self) -> None:
        made = await self.make({'strategy': 'worktree'})
        await self.assert_make_error(
            'invalid', {'strategy': 'existing_worktree', 'path': made.path, 'branch': 'main'},
            thread=new_thread())
        await self.assert_make_error(
            'invalid', {'strategy': 'existing_worktree', 'path': str(self.root),
                        'branch': made.branch})

    async def test_worktree_of_another_repo_is_invalid(self) -> None:
        other = make_repo(self.base / 'other-repo')
        path = self.data_dir / 'worktrees' / 'loom' / 'foreign'
        path.parent.mkdir(parents=True)
        git(other, 'worktree', 'add', '-q', '-b', 'foreign', str(path))
        await self.assert_make_error(
            'invalid', {'strategy': 'existing_worktree', 'path': str(path), 'branch': 'foreign'})

    async def test_path_that_is_no_checkout_top_is_invalid(self) -> None:
        made = await self.make({'strategy': 'worktree'})
        inner = Path(made.path) / 'sub'
        inner.mkdir()
        missing = self.data_dir / 'worktrees' / 'loom' / 'missing'
        plain = self.data_dir / 'worktrees' / 'loom' / 'plain'
        plain.mkdir()
        for path in (inner, missing, plain):
            with self.subTest(path=path.name):
                await self.assert_make_error(
                    'invalid', {'strategy': 'existing_worktree', 'path': str(path),
                                'branch': made.branch}, thread=new_thread())

    async def test_root_that_is_no_git_repo_is_invalid(self) -> None:
        plain = self.base / 'plain'
        plain.mkdir()
        await self.assert_make_error(
            'invalid', {'strategy': 'existing_worktree', 'path': str(plain), 'branch': 'main'},
            project_for(plain, 'plain'))


class ShapeTest(WorkspaceTest):
    def test_null_fields_are_invalid(self) -> None:
        cases = [
            ({'strategy': None}, 'strategy'),
            ({'strategy': 'worktree', 'branch': None}, 'branch'),
            ({'strategy': 'worktree', 'base': None}, 'base'),
            ({'strategy': 'existing_worktree', 'path': None, 'branch': 'main'}, 'path'),
            ({'strategy': 'existing_worktree', 'path': str(self.root), 'branch': None},
             'branch'),
        ]
        for workspace, field in cases:
            with self.subTest(workspace=workspace):
                self.assert_invalid(workspace, field)

    def test_missing_fields(self) -> None:
        self.assert_invalid({}, 'strategy')
        self.assert_invalid({'branch': 'main'}, 'strategy')
        self.assert_invalid({'strategy': 'existing_worktree', 'branch': 'main'}, 'path')
        self.assert_invalid({'strategy': 'existing_worktree', 'path': str(self.root)}, 'branch')
        plan = self.plan({'strategy': 'worktree'})
        self.assertEqual((plan.branch, plan.base), (f'loom/{self.thread}', None))

    def test_wrong_types_are_invalid(self) -> None:
        existing = {'strategy': 'existing_worktree', 'path': str(self.root), 'branch': 'main'}
        cases = [
            ('worktree', 'workspace'),
            (['root'], 'workspace'),
            ({'strategy': 'clone'}, 'strategy'),
            ({'strategy': 5}, 'strategy'),
            ({'strategy': 'ROOT'}, 'strategy'),
            ({'strategy': 'worktree', 'branch': 5}, 'branch'),
            ({'strategy': 'worktree', 'branch': ''}, 'branch'),
            ({'strategy': 'worktree', 'branch': '-f'}, 'branch'),
            ({'strategy': 'worktree', 'branch': 'a\x00b'}, 'branch'),
            ({'strategy': 'worktree', 'base': ['main']}, 'base'),
            ({'strategy': 'worktree', 'base': '--orphan'}, 'base'),
            ({'strategy': 'worktree', 'base': ''}, 'base'),
            ({**existing, 'path': 5}, 'path'),
            ({**existing, 'path': 'repo'}, 'path'),
            ({**existing, 'path': ''}, 'path'),
            ({**existing, 'path': f'{self.root}\x00'}, 'path'),
            ({**existing, 'branch': '-f'}, 'branch'),
            ({**existing, 'branch': 5}, 'branch'),
        ]
        for workspace, field in cases:
            with self.subTest(workspace=workspace):
                self.assert_invalid(workspace, field)

    def test_bad_project_id_is_invalid(self) -> None:
        for project_id in ('../x', 'Loom', 'loom\n', '', 'a/b', 5, None):
            with self.subTest(project_id=project_id):
                project = {**self.project, 'id': project_id}
                self.assert_invalid({'strategy': 'root'}, 'project', project=project)
                self.assert_invalid({'strategy': 'worktree'}, 'project', project=project)

    def test_bad_thread_id_is_invalid(self) -> None:
        for thread in ('abc', self.thread.upper(), f'{self.thread}\n', f'../{self.thread}',
                       self.thread.replace('-', ''), 5):
            with self.subTest(thread=thread):
                with self.assertRaises(LoomError) as caught:
                    plan_workspace({'strategy': 'worktree'}, self.project, thread,
                                   self.data_dir)
                self.assertEqual(caught.exception.code, 'invalid')
                self.assertIn('thread', caught.exception.message)

    def test_thread_id_pattern(self) -> None:
        self.assertTrue(THREAD_ID.match(self.thread))
        self.assertFalse(THREAD_ID.match(self.thread.upper()))

    def test_plans_are_frozen(self) -> None:
        plan = self.plan({'strategy': 'root'})
        with self.assertRaises(AttributeError):
            plan.path = '/'


if __name__ == '__main__':
    unittest.main()
