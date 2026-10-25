import contextlib
import importlib.util
import io
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from pathlib import Path
from unittest import mock

from loom_client import cli, install

FAKE = Path(__file__).with_name('fake_mcp_cli.py')
ROOT = os.path.realpath(tempfile.gettempdir())
PYTHON = [sys.executable, '-m', 'loom_mcp']
ENV_VARS = ['LOOM_THREAD', 'LOOM_SOCKET', 'XDG_RUNTIME_DIR']
CLAUDE_ADD = ['mcp', 'add', '--scope', 'user', 'loom', '--', *PYTHON]
CLAUDE_REMOVE = ['mcp', 'remove', '--scope', 'user', 'loom']
CODEX_ADD = ['mcp', 'add', 'loom', '--', *PYTHON]
CODEX_REMOVE = ['mcp', 'remove', 'loom']
REGISTERED = 'claude: registered loom\ncodex: registered loom\n'
ALREADY = 'claude: loom is already registered\ncodex: loom is already registered\n'
TOML_PYTHON = json.dumps(sys.executable)
GOOD_CODEX = f'[mcp_servers.loom]\ncommand = {TOML_PYTHON}\nargs = ["-m", "loom_mcp"]\n'


def make_fake(directory: Path, agent: str, state: Path) -> Path:
    words = [sys.executable, '-I', str(FAKE), ROOT, str(state), agent]
    path = directory / agent
    path.write_text(f'#!/bin/sh\nexec {shlex.join(words)} "$@"\n')
    path.chmod(0o700)
    return path


class InstallCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(os.path.realpath(tempfile.mkdtemp()))
        self.addCleanup(shutil.rmtree, self.tmp)
        self.home = self.tmp / 'home'
        self.claude_dir = self.tmp / 'claude'
        self.codex_dir = self.tmp / 'codex'
        self.state = self.tmp / 'state'
        self.bin = self.tmp / 'bin'
        for directory in (self.home, self.claude_dir, self.codex_dir, self.state, self.bin):
            directory.mkdir()
        for agent in ('claude', 'codex'):
            make_fake(self.bin, agent, self.state)
        self.environ = {
            'PATH': str(self.bin), 'HOME': str(self.home), 'LANG': 'C.UTF-8',
            'CLAUDE_CONFIG_DIR': str(self.claude_dir), 'CODEX_HOME': str(self.codex_dir),
        }
        self.claude_file = self.claude_dir / '.claude.json'
        self.codex_file = self.codex_dir / 'config.toml'

    def install(self, **options: object) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        options.setdefault('environ', self.environ)
        options.setdefault('timeout', 20.0)
        code = install.install(out=out, err=err, **options)
        return code, out.getvalue(), err.getvalue()

    def records(self) -> list[dict]:
        log = self.state / 'calls.jsonl'
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    def calls(self, agent: str) -> list[list[str]]:
        return [record['argv'] for record in self.records() if record['agent'] == agent]

    def clear_calls(self) -> None:
        (self.state / 'calls.jsonl').unlink(missing_ok=True)

    def reset(self) -> None:
        self.clear_calls()
        self.claude_file.unlink(missing_ok=True)
        self.codex_file.unlink(missing_ok=True)

    def set_failure(self, agent: str, action: str, **mode: object) -> None:
        (self.state / f'{agent}-fail.json').write_text(json.dumps({action: mode}))

    def claude_entry(self, path: Path | None = None) -> object:
        return json.loads((path or self.claude_file).read_text())['mcpServers']['loom']

    def codex_data(self, path: Path | None = None) -> dict:
        return tomllib.loads((path or self.codex_file).read_text())

    def write_claude(self, entry: object, **servers: object) -> None:
        self.claude_file.write_text(json.dumps({'mcpServers': {'loom': entry, **servers}}))


class FirstRunTest(InstallCase):
    def test_registers_with_both_agents(self) -> None:
        self.assertEqual(self.install(), (0, REGISTERED, ''))
        self.assertEqual(self.calls('claude'), [CLAUDE_ADD])
        self.assertEqual(self.calls('codex'), [CODEX_ADD])
        entry = self.claude_entry()
        self.assertEqual((entry['command'], entry['args']), (sys.executable, PYTHON[1:]))
        loom = self.codex_data()['mcp_servers']['loom']
        self.assertEqual(loom, {'command': sys.executable, 'args': PYTHON[1:],
                                'env_vars': ENV_VARS})

    def test_a_second_run_changes_nothing(self) -> None:
        self.assertEqual(self.install()[0], 0)
        claude, codex = self.claude_file.read_bytes(), self.codex_file.read_bytes()
        self.clear_calls()
        self.assertEqual(self.install(), (0, ALREADY, ''))
        self.assertEqual(self.records(), [])
        self.assertEqual(self.claude_file.read_bytes(), claude)
        self.assertEqual(self.codex_file.read_bytes(), codex)

    def test_the_lookup_finds_loom_mcp_and_the_command_is_this_python(self) -> None:
        self.assertIsNotNone(importlib.util.find_spec('loom_mcp'))
        self.assertEqual(install.command(), PYTHON)

    def test_without_the_agent_dirs_the_home_files_are_used(self) -> None:
        del self.environ['CLAUDE_CONFIG_DIR'], self.environ['CODEX_HOME']
        self.assertEqual(self.install(), (0, REGISTERED, ''))
        self.assertEqual(self.claude_entry(self.home / '.claude.json')['command'],
                         sys.executable)
        codex_file = self.home / '.codex' / 'config.toml'
        self.assertEqual(self.codex_data(codex_file)['mcp_servers']['loom']['env_vars'],
                         ENV_VARS)
        self.assertFalse(self.claude_file.exists())
        self.assertFalse(self.codex_file.exists())
        self.clear_calls()
        self.assertEqual(self.install(), (0, ALREADY, ''))
        self.assertEqual(self.records(), [])

    def test_the_paths_follow_the_environment(self) -> None:
        self.assertEqual(install.claude_config({'HOME': '/h', 'CLAUDE_CONFIG_DIR': ''}),
                         '/h/.claude.json')
        self.assertEqual(install.claude_config({'HOME': '/h', 'CLAUDE_CONFIG_DIR': '/c'}),
                         '/c/.claude.json')
        self.assertEqual(install.codex_config({'HOME': '/h', 'CODEX_HOME': ''}),
                         '/h/.codex/config.toml')
        self.assertEqual(install.codex_config({'HOME': '/h', 'CODEX_HOME': '/x'}),
                         '/x/config.toml')


class ClaudeTest(InstallCase):
    def test_an_entry_that_differs_is_removed_before_the_add(self) -> None:
        for entry in (
            {'command': '/other/python', 'args': PYTHON[1:]},
            {'command': sys.executable, 'args': ['-m', 'other']},
            {'command': None, 'args': PYTHON[1:]},
            {'args': PYTHON[1:]},
            {'command': 5, 'args': PYTHON[1:]},
            {'command': sys.executable, 'args': '-m loom_mcp'},
            {'command': sys.executable},
            None,
        ):
            with self.subTest(entry=entry):
                self.reset()
                self.write_claude(entry, other={'command': 'x', 'args': []})
                self.assertEqual(self.install()[:2], (0, REGISTERED))
                self.assertEqual(self.calls('claude'), [CLAUDE_REMOVE, CLAUDE_ADD])
                self.assertEqual(self.claude_entry()['command'], sys.executable)
                servers = json.loads(self.claude_file.read_text())['mcpServers']
                self.assertEqual(servers['other'], {'command': 'x', 'args': []})

    def test_a_missing_entry_is_only_added(self) -> None:
        for text in ('not json{', '[]', '{}', '{"mcpServers": []}',
                     '{"mcpServers": {"other": {}}}', '\xff'):
            with self.subTest(text=text):
                self.reset()
                self.claude_file.write_bytes(text.encode('latin-1'))
                self.assertEqual(self.install()[:2], (0, REGISTERED))
                self.assertEqual(self.calls('claude'), [CLAUDE_ADD])

    def test_install_never_writes_the_claude_file_itself(self) -> None:
        self.write_claude({'command': sys.executable, 'args': PYTHON[1:], 'extra': 1})
        before = self.claude_file.read_bytes()
        self.assertEqual(self.install()[:2], (0, REGISTERED.replace(
            'claude: registered loom', 'claude: loom is already registered')))
        self.assertEqual(self.claude_file.read_bytes(), before)
        self.assertEqual(self.calls('claude'), [])


CODEX_KEEP = f'''# my settings
model = "gpt"

[mcp_servers.other]
command = "other"
args = ["a"]

[mcp_servers.loom]
command = {TOML_PYTHON}
# the server's own args
args = ["-m", "loom_mcp"]
{{env_vars}}
[mcp_servers.loom.env]
KEY = "value"

# trailing comment
[profiles.x]
model = "y"
'''


class CodexEnvVarsTest(InstallCase):
    def test_env_vars_are_set_without_running_codex(self) -> None:
        for env_vars in ('', 'env_vars = ["LOOM_THREAD"]\n',
                         'env_vars = [\n  "LOOM_THREAD",\n  "OTHER",\n]\n',
                         'env_vars = []  # none\n'):
            with self.subTest(env_vars=env_vars):
                self.reset()
                text = CODEX_KEEP.replace('{env_vars}\n', env_vars)
                self.codex_file.write_text(text)
                before = tomllib.loads(text)
                self.assertEqual(self.install(), (0, REGISTERED, ''))
                self.assertEqual(self.calls('codex'), [])
                after = self.codex_data()
                self.assertEqual(after['mcp_servers']['loom']['env_vars'], ENV_VARS)
                self.assertEqual(after['mcp_servers']['other'], before['mcp_servers']['other'])
                self.assertEqual(after['mcp_servers']['loom']['env'], {'KEY': 'value'})
                self.assertEqual(after['profiles'], before['profiles'])
                new = self.codex_file.read_text()
                for comment in ('# my settings', "# the server's own args",
                                '# trailing comment'):
                    self.assertIn(comment, new)
                self.assertLess(new.index('env_vars = '), new.index('[mcp_servers.loom.env]'))
                self.assertEqual(new.count('env_vars'), 1)

    def test_env_vars_go_after_the_last_key_of_the_table(self) -> None:
        for text, expected in (
            (GOOD_CODEX, GOOD_CODEX + 'env_vars = ["LOOM_THREAD", "LOOM_SOCKET", '
                                      '"XDG_RUNTIME_DIR"]\n'),
            (GOOD_CODEX.rstrip('\n'), GOOD_CODEX + 'env_vars = ["LOOM_THREAD", '
                                                   '"LOOM_SOCKET", "XDG_RUNTIME_DIR"]\n'),
            ('[mcp_servers.loom]', '[mcp_servers.loom]\nenv_vars = ["LOOM_THREAD", '
                                   '"LOOM_SOCKET", "XDG_RUNTIME_DIR"]\n'),
        ):
            with self.subTest(text=text):
                self.assertEqual(install.with_env_vars(text), expected)

    def test_a_table_that_cannot_be_found_is_left_alone(self) -> None:
        text = f'[mcp_servers]\nloom = {{ command = {TOML_PYTHON}, args = ["-m", "loom_mcp"] }}\n'
        self.codex_file.write_text(text)
        code, out, err = self.install()
        self.assertEqual((code, out), (1, 'claude: registered loom\n'))
        self.assertEqual(err, f'codex: cannot set env_vars in {self.codex_file}\n')
        self.assertEqual(self.codex_file.read_text(), text)
        self.assertEqual(self.calls('codex'), [])
        self.assertEqual(os.listdir(self.codex_dir), ['config.toml'])

    def test_a_header_missing_after_the_add_is_a_failure(self) -> None:
        self.set_failure('codex', 'add', noop=True)
        code, out, err = self.install()
        self.assertEqual((code, out), (1, 'claude: registered loom\n'))
        self.assertEqual(err, f'codex: cannot set env_vars in {self.codex_file}\n')
        self.assertEqual(self.calls('codex'), [CODEX_ADD])
        self.assertFalse(self.codex_file.exists())

    def test_the_rewrite_keeps_the_permission_bits(self) -> None:
        for mode in (0o600, 0o644):
            with self.subTest(mode=oct(mode)):
                self.reset()
                self.codex_file.write_text(GOOD_CODEX)
                self.codex_file.chmod(mode)
                self.assertEqual(self.install()[0], 0)
                self.assertEqual(self.codex_file.stat().st_mode & 0o7777, mode)
                self.assertEqual(self.codex_data()['mcp_servers']['loom']['env_vars'], ENV_VARS)
                self.assertEqual(os.listdir(self.codex_dir), ['config.toml'])

    def test_a_temp_file_starts_at_0600(self) -> None:
        self.codex_file.write_text(GOOD_CODEX)
        self.codex_file.chmod(0o644)
        modes = []
        real_chmod = os.chmod

        def chmod(path: str, bits: int) -> None:
            modes.append((os.stat(path).st_mode & 0o7777, bits))
            real_chmod(path, bits)

        with mock.patch('os.chmod', chmod):
            self.assertEqual(self.install()[0], 0)
        self.assertEqual(modes, [(0o600, 0o644)])


class CodexCommandTest(InstallCase):
    def test_an_entry_with_another_command_is_removed_and_added(self) -> None:
        for table in (
            'command = "/other/python"\nargs = ["-m", "loom_mcp"]\n',
            f'command = {TOML_PYTHON}\n',
            'command = 5\nargs = ["-m", "loom_mcp"]\n',
            f'command = {TOML_PYTHON}\nargs = "-m loom_mcp"\n'
            'env_vars = ["LOOM_THREAD", "LOOM_SOCKET", "XDG_RUNTIME_DIR"]\n',
        ):
            with self.subTest(table=table):
                self.reset()
                self.codex_file.write_text(
                    f'[mcp_servers.other]\ncommand = "o"\n\n[mcp_servers.loom]\n{table}')
                self.assertEqual(self.install(), (0, REGISTERED, ''))
                self.assertEqual(self.calls('codex'), [CODEX_REMOVE, CODEX_ADD])
                data = self.codex_data()
                self.assertEqual(data['mcp_servers']['loom'], {
                    'command': sys.executable, 'args': PYTHON[1:], 'env_vars': ENV_VARS})
                self.assertEqual(data['mcp_servers']['other'], {'command': 'o'})

    def test_a_config_that_does_not_parse_is_left_alone(self) -> None:
        for raw in (b'[mcp_servers.loom\ncommand = 1\n', b'\xff = 1\n'):
            with self.subTest(raw=raw):
                self.reset()
                self.codex_file.write_bytes(raw)
                code, out, err = self.install()
                self.assertEqual((code, out), (1, 'claude: registered loom\n'))
                self.assertTrue(err.startswith(f'codex: cannot read {self.codex_file}: '), err)
                self.assertEqual(err.count('\n'), 1)
                self.assertEqual(self.calls('codex'), [])
                self.assertEqual(self.codex_file.read_bytes(), raw)
                self.assertEqual(os.listdir(self.codex_dir), ['config.toml'])


class FailureTest(InstallCase):
    def test_a_failed_add_is_reported_and_the_other_agent_still_runs(self) -> None:
        self.set_failure('claude', 'add', exit=1, stderr='Error: no\nnetwork\n')
        code, out, err = self.install()
        self.assertEqual((code, out), (1, 'codex: registered loom\n'))
        self.assertEqual(err, 'claude: claude mcp add failed (exit 1): Error: no network\n')

    def test_stderr_comes_before_stdout_and_both_are_cut_to_500(self) -> None:
        self.set_failure('claude', 'add', exit=3, stderr='err', stdout='out')
        self.assertEqual(self.install()[2], 'claude: claude mcp add failed (exit 3): err out\n')
        self.set_failure('claude', 'add', exit=1, stderr='x' * 800, stdout='y')
        prefix = 'claude: claude mcp add failed (exit 1): '
        self.assertEqual(self.install()[2], prefix + 'x' * 500 + '\n')

    def test_control_characters_are_escaped(self) -> None:
        self.set_failure('codex', 'add', exit=1, stderr='\x1b[31mred\x07')
        self.assertEqual(self.install()[2],
                         'codex: codex mcp add failed (exit 1): \\x1b[31mred\\x07\n')

    def test_a_failed_remove_stops_that_agent(self) -> None:
        self.write_claude({'command': 'old'})
        self.set_failure('claude', 'remove', exit=2, stderr='nope')
        code, out, err = self.install()
        self.assertEqual((code, out), (1, 'codex: registered loom\n'))
        self.assertEqual(err, 'claude: claude mcp remove failed (exit 2): nope\n')
        self.assertEqual(self.calls('claude'), [CLAUDE_REMOVE])

    def test_a_child_past_the_timeout_is_a_failure(self) -> None:
        self.set_failure('claude', 'add', sleep=30)
        started = time.monotonic()
        code, out, err = self.install(timeout=0.5)
        self.assertLess(time.monotonic() - started, 15)
        self.assertEqual((code, out), (1, 'codex: registered loom\n'))
        self.assertEqual(err, 'claude: claude mcp add failed (timed out after 0.5 s)\n')

    def test_a_missing_agent_is_skipped(self) -> None:
        (self.bin / 'codex').unlink()
        self.assertEqual(self.install(), (0, 'claude: registered loom\ncodex: not found, '
                                             'skipped\n', ''))
        self.assertFalse(self.codex_file.exists())
        (self.bin / 'claude').unlink()
        self.claude_file.unlink()
        self.assertEqual(self.install(), (0, 'claude: not found, skipped\ncodex: not found, '
                                             'skipped\n', ''))
        self.assertFalse(self.claude_file.exists())

    def test_without_loom_mcp_nothing_runs(self) -> None:
        looked_up = []

        def find_spec(name: str) -> None:
            looked_up.append(name)

        self.assertEqual(self.install(find_spec=find_spec),
                         (2, '', 'loom: loom-mcp is not installed\n'))
        self.assertEqual(looked_up, ['loom_mcp'])
        self.assertEqual(self.records(), [])


class EnvironmentTest(InstallCase):
    def test_the_children_get_only_the_allowlisted_variables(self) -> None:
        self.environ.update(SECRET_TOKEN='s3cret', LOOM_THREAD='t1', TZ='UTC',
                            ANTHROPIC_API_KEY='key')
        self.assertEqual(self.install()[0], 0)
        records = self.records()
        self.assertEqual(len(records), 2)
        for record in records:
            env = record['env']
            self.assertNotIn('SECRET_TOKEN', env)
            self.assertNotIn('ANTHROPIC_API_KEY', env)
            self.assertNotIn('LOOM_THREAD', env)
            self.assertLessEqual(set(env) - {'PWD'}, set(install.ALLOWED_ENV))
            for key in ('HOME', 'CLAUDE_CONFIG_DIR', 'CODEX_HOME', 'PATH', 'TZ', 'LANG'):
                self.assertEqual(env[key], self.environ[key])
            self.assertNotIn('s3cret', json.dumps(record))

    def test_the_environment_is_not_changed(self) -> None:
        before = dict(os.environ)
        self.assertEqual(self.install()[0], 0)
        self.assertEqual(dict(os.environ), before)

    def test_the_fakes_refuse_a_home_outside_the_temp_dir(self) -> None:
        outside = '/nonexistent-loom-install-test'
        for change in ({'HOME': outside}, {'CLAUDE_CONFIG_DIR': outside},
                       {'CODEX_HOME': outside}):
            with self.subTest(change=change):
                env = {**self.environ, **change}
                done = subprocess.run([str(self.bin / 'claude'), *CLAUDE_ADD], env=env,
                                      capture_output=True, timeout=20)
                self.assertEqual(done.returncode, 97)
                self.assertEqual(self.records(), [])
                self.assertFalse(os.path.exists(outside))
        self.environ['CODEX_HOME'] = outside
        code, out, err = self.install()
        refused = 'fake: refusing a home outside the temp dir\n'
        self.assertEqual((code, out), (1, ''))
        self.assertEqual(err, f'claude: claude mcp add failed (exit 97): {refused}'
                              f'codex: codex mcp add failed (exit 97): {refused}')
        self.assertEqual(self.records(), [])
        self.assertFalse(self.claude_file.exists())
        self.assertFalse(os.path.exists(outside))


class CliTest(InstallCase):
    def run_cli(self, *argv: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, self.environ, clear=True), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_loom_install_registers_and_needs_no_socket(self) -> None:
        self.assertEqual(self.run_cli('install'), (0, REGISTERED, ''))
        self.assertEqual(self.run_cli('--socket', '/nowhere.sock', 'install'),
                         (0, ALREADY, ''))

    def test_install_help_exits_0(self) -> None:
        code, out, _ = self.run_cli('install', '--help')
        self.assertEqual(code, 0)
        self.assertIn('usage: loom install', out)
        self.assertEqual(self.records(), [])

    def test_install_takes_no_options(self) -> None:
        code, _, err = self.run_cli('install', '--force')
        self.assertEqual(code, 2)
        self.assertIn('unrecognized arguments: --force', err)
        self.assertEqual(self.records(), [])


if __name__ == '__main__':
    unittest.main()
