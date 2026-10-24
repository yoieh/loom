import os
import tempfile
import unittest
from pathlib import Path

from loom_contracts.runners import PermissionMode, RunnerHost
from loom_runner_codex.command import (
    ENV_ALLOWLIST,
    SANDBOXES,
    check_thread,
    codex_argv,
    codex_env,
    is_canonical_uuid,
)

from .replay import CODEX_ID, THREAD_ID, WORKSPACE, FakeHost, make_thread

SANDBOX_OF = {
    PermissionMode.APPROVAL_REQUIRED: 'read-only',
    PermissionMode.AUTO_ACCEPT_EDITS: 'workspace-write',
    PermissionMode.AUTO: 'workspace-write',
    PermissionMode.FULL_ACCESS: 'danger-full-access',
}

TOKEN = 'loom-test-token-3f9c2a7e'


def first_argv(sandbox: str, model: list[str] | None = None) -> list[str]:
    return ['codex', 'exec', '--json', '-C', WORKSPACE, *(model or []), '-s', sandbox, '-']


def resume_argv(sandbox: str, model: list[str] | None = None) -> list[str]:
    return [
        'codex', '-C', WORKSPACE, '-s', sandbox, 'exec', 'resume', CODEX_ID, '--json',
        *(model or []), '-',
    ]


def host(**options: object) -> FakeHost:
    return FakeHost('/tmp/loom-replay/data', **options)


class ArgvTest(unittest.TestCase):
    def test_first_turn_in_each_mode(self) -> None:
        for mode, sandbox in SANDBOX_OF.items():
            with self.subTest(mode=mode):
                argv = codex_argv(make_thread(permission_mode=mode), 'codex', None)
                self.assertEqual(argv, first_argv(sandbox))
                self.assertTrue(all(type(item) is str for item in argv))

    def test_resume_in_each_mode(self) -> None:
        for mode, sandbox in SANDBOX_OF.items():
            with self.subTest(mode=mode):
                argv = codex_argv(make_thread(permission_mode=mode), 'codex', CODEX_ID)
                self.assertEqual(argv, resume_argv(sandbox))

    def test_mode_given_as_a_plain_string(self) -> None:
        thread = make_thread(permission_mode='full-access')
        self.assertEqual(codex_argv(thread, 'codex', None), first_argv('danger-full-access'))

    def test_sandbox_table_is_exactly_row_15(self) -> None:
        self.assertEqual(SANDBOXES, SANDBOX_OF)
        self.assertEqual(set(SANDBOXES), set(PermissionMode))

    def test_model_goes_before_the_sandbox_or_the_dash(self) -> None:
        thread = make_thread(model='gpt-6.1-sol', permission_mode=PermissionMode.APPROVAL_REQUIRED)
        model = ['-m', 'gpt-6.1-sol']
        self.assertEqual(codex_argv(thread, 'codex', None), first_argv('read-only', model))
        self.assertEqual(codex_argv(thread, 'codex', CODEX_ID), resume_argv('read-only', model))

    def test_codex_bin_is_the_first_item(self) -> None:
        argv = codex_argv(make_thread(), '/opt/codex/bin/codex', CODEX_ID)
        self.assertEqual(argv[0], '/opt/codex/bin/codex')
        self.assertEqual(argv[1:], resume_argv('workspace-write')[1:])

    def test_no_resume_flag_on_the_first_turn(self) -> None:
        argv = codex_argv(make_thread(resume=True), 'codex', None)
        self.assertEqual(argv, first_argv('workspace-write'))

    def test_a_bad_codex_thread_id_is_refused(self) -> None:
        for bad in ('--help', '../x', CODEX_ID.upper(), f'{{{CODEX_ID}}}', '', 7):
            with self.subTest(codex_id=bad), self.assertRaises(ValueError):
                codex_argv(make_thread(), 'codex', bad)

    def test_empty_codex_bin_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            codex_argv(make_thread(), '', None)


class CheckThreadTest(unittest.TestCase):
    def test_a_good_thread_passes(self) -> None:
        check_thread(make_thread(model='gpt-6.1-sol'))

    def test_bad_threads_raise(self) -> None:
        cases = {
            'uppercase id': {'id': THREAD_ID.upper()},
            'braced id': {'id': f'{{{THREAD_ID}}}'},
            'path id': {'id': '../x'},
            'number id': {'id': 7},
            'unknown mode': {'permission_mode': 'yolo'},
            'mode none': {'permission_mode': None},
            'dash model': {'model': '-x'},
            'spaced model': {'model': 'gpt 6'},
            'number model': {'model': 6},
            'relative workspace': {'workspace_path': 'repo'},
            'workspace none': {'workspace_path': None},
        }
        for name, change in cases.items():
            with self.subTest(case=name), self.assertRaises(ValueError):
                check_thread(make_thread(**change))

    def test_uuid_check(self) -> None:
        self.assertTrue(is_canonical_uuid(CODEX_ID))
        self.assertTrue(is_canonical_uuid(THREAD_ID))
        for bad in (None, 7, '', '--help', CODEX_ID.upper(), CODEX_ID.replace('-', ''),
                    f'urn:uuid:{CODEX_ID}', f' {CODEX_ID}'):
            with self.subTest(value=bad):
                self.assertFalse(is_canonical_uuid(bad))


class EnvTest(unittest.TestCase):
    def test_fake_host_is_a_runner_host(self) -> None:
        self.assertIsInstance(host(), RunnerHost)

    def test_only_the_allowlist_and_loom_thread(self) -> None:
        environment = {key: f'/x/{key}' for key in ENV_ALLOWLIST}
        environment.update({
            'PATH': '/usr/bin:/bin',
            'ANTHROPIC_AUTH_TOKEN': 'host-token',
            'ANTHROPIC_BASE_URL': 'http://x',
            'CLAUDE_CONFIG_DIR': '/x/claude',
            'CLAUDECODE': '1',
            'CODEX_HOME': '/host/codex',
            'CLIPROXY_API_KEY': 'host-key',
            'LOOM_THREAD': 'other',
            'SECRET': 'nope',
            'LANG': 7,
        })
        env = codex_env(make_thread(), host(environment=environment), 'codex')
        expected = {key: f'/x/{key}' for key in ENV_ALLOWLIST if key != 'LANG'}
        expected['PATH'] = '/usr/bin:/bin'
        expected['LOOM_THREAD'] = THREAD_ID
        self.assertEqual(env, expected)
        self.assertFalse([key for key in env if key.startswith(('ANTHROPIC_', 'CLAUDE'))])

    def test_codex_home_setting(self) -> None:
        for value, expected in (('/home/u/.codex', '/home/u/.codex'), ('', None), (None, None),
                                (7, None)):
            with self.subTest(value=value):
                env = codex_env(make_thread(), host(settings={'codex_home': value}), 'codex')
                self.assertEqual(env.get('CODEX_HOME'), expected)

    def test_token_becomes_cliproxy_api_key(self) -> None:
        cases = [
            (f' {TOKEN}\n', TOKEN),
            (TOKEN, TOKEN),
            ('  \n', None),
            ('', None),
            (None, None),
            (7, None),
        ]
        for value, expected in cases:
            with self.subTest(token=value):
                env = codex_env(make_thread(), host(secrets={'agent-token': value}), 'codex')
                self.assertEqual(env.get('CLIPROXY_API_KEY'), expected)
                self.assertFalse([key for key in env if key.startswith(('ANTHROPIC_', 'CLAUDE'))])

    def test_host_is_asked_for_this_thread(self) -> None:
        fake = host()
        thread = make_thread()
        codex_env(thread, fake, 'codex')
        self.assertEqual(fake.asked, [thread])


class NodePathTest(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.bin = self.root / 'nvm' / 'bin'
        self.bin.mkdir(parents=True)
        self.codex = self.bin / 'codex'
        self.executable(self.codex)

    def executable(self, path: Path, mode: int = 0o700) -> None:
        path.write_text('#!/bin/sh\n')
        path.chmod(mode)

    def env_for(self, codex_bin: str, environment: dict[str, str]) -> dict[str, str]:
        return codex_env(make_thread(), host(environment=environment), codex_bin)

    def test_node_dir_goes_on_the_end(self) -> None:
        self.executable(self.bin / 'node')
        env = self.env_for(str(self.codex), {'PATH': '/usr/bin:/bin'})
        self.assertEqual(env['PATH'], f'/usr/bin:/bin:{self.bin}')
        self.assertEqual(env['PATH'].split(':')[-1], str(self.bin))

    def test_no_node_leaves_path_unchanged(self) -> None:
        env = self.env_for(str(self.codex), {'PATH': '/usr/bin:/bin'})
        self.assertEqual(env['PATH'], '/usr/bin:/bin')

    def test_node_that_is_not_executable_is_not_used(self) -> None:
        self.executable(self.bin / 'node', mode=0o600)
        env = self.env_for(str(self.codex), {'PATH': '/usr/bin:/bin'})
        self.assertEqual(env['PATH'], '/usr/bin:/bin')

    def test_node_that_is_a_directory_is_not_used(self) -> None:
        (self.bin / 'node').mkdir()
        env = self.env_for(str(self.codex), {'PATH': '/usr/bin:/bin'})
        self.assertEqual(env['PATH'], '/usr/bin:/bin')

    def test_dir_already_on_path_is_not_added_again(self) -> None:
        self.executable(self.bin / 'node')
        for path in (f'{self.bin}:/usr/bin', f'/usr/bin:{self.bin}', f'/usr/bin:{self.bin}/'):
            with self.subTest(path=path):
                env = self.env_for(str(self.codex), {'PATH': path})
                self.assertEqual(env['PATH'], path)

    def test_no_path_gives_just_the_dir(self) -> None:
        self.executable(self.bin / 'node')
        for environment in ({}, {'PATH': ''}):
            with self.subTest(environment=environment):
                env = self.env_for(str(self.codex), environment)
                self.assertEqual(env['PATH'], str(self.bin))

    def test_no_path_and_no_node_sets_no_path(self) -> None:
        env = self.env_for(str(self.codex), {})
        self.assertNotIn('PATH', env)

    def test_bare_name_is_found_on_the_child_path(self) -> None:
        self.executable(self.bin / 'node')
        path = f'/usr/bin:{self.bin}'
        self.assertEqual(self.env_for('codex', {'PATH': path})['PATH'], path)
        other = self.root / 'other'
        other.mkdir()
        self.executable(other / 'codex')
        self.executable(other / 'node')
        path = f'{other}:/usr/bin'
        self.assertEqual(self.env_for('codex', {'PATH': path})['PATH'], path)

    def test_bare_name_not_on_the_child_path_leaves_it_unchanged(self) -> None:
        self.executable(self.bin / 'node')
        empty = self.root / 'empty'
        empty.mkdir()
        env = self.env_for('codex', {'PATH': str(empty)})
        self.assertEqual(env['PATH'], str(empty))

    def test_bare_name_is_not_looked_up_on_loom_own_path(self) -> None:
        self.executable(self.bin / 'node')
        empty = self.root / 'empty'
        empty.mkdir()
        old = os.environ.get('PATH')
        os.environ['PATH'] = str(self.bin)
        try:
            env = self.env_for('codex', {'PATH': str(empty)})
        finally:
            if old is None:
                del os.environ['PATH']
            else:
                os.environ['PATH'] = old
        self.assertEqual(env['PATH'], str(empty))

    def test_relative_codex_bin_adds_nothing(self) -> None:
        self.executable(self.bin / 'node')
        cwd = os.getcwd()
        os.chdir(self.root)
        try:
            env = self.env_for('nvm/bin/codex', {'PATH': '/usr/bin'})
        finally:
            os.chdir(cwd)
        self.assertEqual(env['PATH'], '/usr/bin')

    def test_symlinked_codex_uses_the_link_dir(self) -> None:
        self.executable(self.bin / 'node')
        target = self.root / 'lib' / 'codex.js'
        target.parent.mkdir()
        self.executable(target)
        link = self.bin / 'codex-link'
        link.symlink_to(target)
        env = self.env_for(str(link), {'PATH': '/usr/bin'})
        self.assertEqual(env['PATH'], f'/usr/bin:{self.bin}')


if __name__ == '__main__':
    unittest.main()
