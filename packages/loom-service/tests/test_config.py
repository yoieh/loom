import io
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from loom_service.config import (
    Config, StartError, config_path, data_dir_path, load_config, socket_path,
)
from loom_service.host import Host

THREAD = SimpleNamespace(id='9fdbfa8d-8860-410a-8a2b-68a11a109e08')


class PathsTest(unittest.TestCase):
    def test_socket_option_wins_and_is_made_absolute(self) -> None:
        env = {'XDG_RUNTIME_DIR': '/run/user/1000'}
        self.assertEqual(socket_path('/tmp/x/api.sock', env), Path('/tmp/x/api.sock'))
        self.assertEqual(socket_path('run/api.sock', env), Path(os.getcwd()) / 'run/api.sock')

    def test_socket_defaults_to_the_runtime_dir(self) -> None:
        self.assertEqual(socket_path(None, {'XDG_RUNTIME_DIR': '/run/user/1000'}),
                         Path('/run/user/1000/loom/api.sock'))

    def test_loom_socket_does_not_change_the_socket_path(self) -> None:
        env = {'XDG_RUNTIME_DIR': '/run/user/1000', 'LOOM_SOCKET': '/tmp/other.sock'}
        self.assertEqual(socket_path(None, env), Path('/run/user/1000/loom/api.sock'))

    def test_no_socket_path_is_an_error(self) -> None:
        for env in ({}, {'XDG_RUNTIME_DIR': ''}, {'LOOM_SOCKET': '/tmp/other.sock'}):
            with self.subTest(env=env), self.assertRaises(StartError) as caught:
                socket_path(None, env)
            self.assertIn('XDG_RUNTIME_DIR', str(caught.exception))
        with self.assertRaises(StartError):
            socket_path('', {'XDG_RUNTIME_DIR': '/run/user/1000'})

    def test_data_dir_option_then_xdg_then_home(self) -> None:
        env = {'XDG_DATA_HOME': '/data', 'HOME': '/home/user'}
        self.assertEqual(data_dir_path('/d', env), Path('/d'))
        self.assertEqual(data_dir_path(None, env), Path('/data/loom'))
        self.assertEqual(data_dir_path(None, {'XDG_DATA_HOME': '', 'HOME': '/home/user'}),
                         Path('/home/user/.local/share/loom'))

    def test_config_option_then_xdg_then_home(self) -> None:
        env = {'XDG_CONFIG_HOME': '/conf', 'HOME': '/home/user'}
        self.assertEqual(config_path('/c.toml', env), Path('/c.toml'))
        self.assertEqual(config_path(None, env), Path('/conf/loom/config.toml'))
        self.assertEqual(config_path(None, {'HOME': '/home/user'}),
                         Path('/home/user/.config/loom/config.toml'))


class ConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / 'config.toml'

    def load(self, text: str | bytes) -> Config:
        if isinstance(text, str):
            text = text.encode()
        self.path.write_bytes(text)
        return load_config(self.path)

    def assert_refused(self, text: str | bytes, problem: str) -> None:
        with self.assertRaises(StartError) as caught:
            self.load(text)
        self.assertTrue(str(caught.exception).startswith(f'{self.path}: '))
        self.assertIn(problem, str(caught.exception))

    def test_a_missing_file_gives_the_defaults(self) -> None:
        config = load_config(self.path)
        self.assertEqual(config, Config())
        self.assertEqual(config.permission_mode, 'auto')
        self.assertIsNone(config.claude_bin)

    def test_each_key_set_to_a_string_is_used(self) -> None:
        config = self.load(
            'claude_bin = "/opt/claude"\nclaude_config_dir = "/cfg"\n'
            'agent_base_url = "http://127.0.0.1:9/"\npermission_mode = "approval-required"\n')
        self.assertEqual(config, Config('/opt/claude', '/cfg', 'http://127.0.0.1:9/',
                                        'approval-required'))

    def test_each_mode_is_accepted(self) -> None:
        for mode in ('approval-required', 'auto-accept-edits', 'auto', 'full-access'):
            with self.subTest(mode=mode):
                self.assertEqual(self.load(f'permission_mode = "{mode}"').permission_mode, mode)

    def test_bad_configs_are_refused(self) -> None:
        self.assert_refused('claude_path = "/x"', "unknown key 'claude_path'")
        self.assert_refused('[claude]\nbin = "/x"', "unknown key 'claude'")
        self.assert_refused('claude_bin = 1', 'claude_bin must be a string')
        self.assert_refused('agent_base_url = ["x"]', 'agent_base_url must be a string')
        self.assert_refused('permission_mode = "root"', 'permission_mode must be one of')
        self.assert_refused('claude_bin = ', 'Invalid value')
        self.assert_refused(b'claude_bin = "\xff"', 'codec')

    def test_the_whole_number_keys_load_as_ints(self) -> None:
        config = self.load('agents_max = 4\nidle_after = 60\nclaude_bin = "/opt/claude"\n')
        self.assertEqual((config.agents_max, config.idle_after), (4, 60))
        self.assertIs(type(config.agents_max), int)
        self.assertEqual(config.claude_bin, '/opt/claude')

    def test_missing_whole_number_keys_give_the_defaults(self) -> None:
        config = self.load('claude_bin = "/opt/claude"\n')
        self.assertEqual((config.agents_max, config.idle_after), (None, 1200))
        self.assertEqual((Config().agents_max, Config().idle_after), (None, 1200))

    def test_whole_number_keys_refuse_anything_but_1_or_more(self) -> None:
        cases = ('agents_max = "4"', 'agents_max = 4.0', 'agents_max = true',
                 'agents_max = 0', 'idle_after = -1', 'idle_after = 0', 'idle_after = "60"')
        for text in cases:
            key = text.split(' ')[0]
            with self.subTest(config=text):
                self.assert_refused(text, f'{key} must be a whole number of 1 or more')
        self.assert_refused('claude_bin = 4', 'claude_bin must be a string')
        self.assert_refused('permission_mode = 1', 'permission_mode must be a string')

    def test_request_timeout_defaults_to_600_and_loads_a_value(self) -> None:
        self.assertEqual(Config().request_timeout, 600)
        self.assertEqual(self.load('claude_bin = "/opt/claude"\n').request_timeout, 600)
        config = self.load('request_timeout = 30\n')
        self.assertEqual(config.request_timeout, 30)
        self.assertIs(type(config.request_timeout), int)

    def test_request_timeout_refuses_anything_but_1_or_more(self) -> None:
        for value in ('0', '-1', '"60"', '1.5', 'true'):
            with self.subTest(value=value):
                with self.assertRaises(StartError) as caught:
                    self.load(f'request_timeout = {value}\n')
                self.assertEqual(
                    str(caught.exception),
                    f'{self.path}: request_timeout must be a whole number of 1 or more')

    def test_the_codex_keys_default_to_none_and_load_as_strings(self) -> None:
        self.assertEqual((Config().codex_bin, Config().codex_home), (None, None))
        config = self.load('claude_bin = "/opt/claude"\n')
        self.assertEqual((config.codex_bin, config.codex_home), (None, None))
        config = self.load('codex_bin = "/opt/codex"\ncodex_home = "/codex-home"\n')
        self.assertEqual((config.codex_bin, config.codex_home), ('/opt/codex', '/codex-home'))

    def test_the_codex_keys_refuse_anything_but_a_string(self) -> None:
        for text in ('codex_bin = 1', 'codex_bin = true', 'codex_home = ["/x"]',
                     'codex_home = 1.5'):
            key = text.split(' ')[0]
            with self.subTest(config=text):
                self.assert_refused(text, f'{key} must be a string')

    def test_the_t3_keys_default_to_none_and_load_as_strings(self) -> None:
        self.assertEqual((Config().t3_home, Config().t3_bin), (None, None))
        config = self.load('claude_bin = "/opt/claude"\n')
        self.assertEqual((config.t3_home, config.t3_bin), (None, None))
        config = self.load('t3_home = "/t3-home"\nt3_bin = "/opt/t3"\n')
        self.assertEqual((config.t3_home, config.t3_bin), ('/t3-home', '/opt/t3'))

    def test_the_t3_keys_refuse_anything_but_a_string(self) -> None:
        for text in ('t3_home = 1', 't3_home = true', 't3_bin = ["/x"]', 't3_bin = 1.5'):
            key = text.split(' ')[0]
            with self.subTest(config=text):
                self.assert_refused(text, f'{key} must be a string')

    def test_an_unreadable_path_is_refused(self) -> None:
        self.path.mkdir()
        with self.assertRaises(StartError) as caught:
            load_config(self.path)
        self.assertIn('cannot read', str(caught.exception))


class HostTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data_dir = Path(tmp.name)
        self.log = io.StringIO()
        self.environ = {
            'HOME': '/home/user', 'PATH': '/usr/bin', 'LANG': 'C.UTF-8', 'USER': 'user',
            'LOGNAME': 'user', 'SHELL': '/bin/bash', 'XDG_RUNTIME_DIR': '/run/user/1000',
            'LOOM_THREAD': 'inherited', 'LOOM_SOCKET': '/tmp/other.sock', 'SECRET': 'x',
            'ANTHROPIC_AUTH_TOKEN': 'outer',
        }
        self.host = self.make()

    def make(self, config: Config | None = None) -> Host:
        return Host(self.data_dir, Path('/run/user/1000/loom/api.sock'), config or Config(),
                    self.environ, self.log)

    def write_token(self, data: bytes, mode: int = 0o600) -> None:
        path = self.data_dir / 'agent-token'
        path.write_bytes(data)
        path.chmod(mode)

    def test_data_dir_is_a_string(self) -> None:
        self.assertEqual(self.host.data_dir, str(self.data_dir))

    def test_settings_come_from_the_config(self) -> None:
        host = self.make(Config('/opt/claude', '/cfg', 'http://x/', 'auto'))
        self.assertEqual(host.setting('claude_bin'), '/opt/claude')
        self.assertEqual(host.setting('claude_config_dir'), '/cfg')
        self.assertEqual(host.setting('agent_base_url'), 'http://x/')
        self.assertIsNone(host.setting('permission_mode'))
        self.assertIsNone(host.setting('other'))
        self.assertIsNone(self.host.setting('claude_bin'))

    def test_the_codex_settings_come_from_the_config(self) -> None:
        host = self.make(Config(codex_bin='/opt/codex', codex_home='/codex-home'))
        self.assertEqual(host.setting('codex_bin'), '/opt/codex')
        self.assertEqual(host.setting('codex_home'), '/codex-home')
        self.assertIsNone(self.host.setting('codex_bin'))
        self.assertIsNone(self.host.setting('codex_home'))

    def test_environment_is_the_allowlist_plus_socket_and_thread(self) -> None:
        self.assertEqual(self.host.environment(THREAD), {
            'HOME': '/home/user', 'PATH': '/usr/bin', 'LANG': 'C.UTF-8', 'USER': 'user',
            'LOGNAME': 'user', 'SHELL': '/bin/bash', 'XDG_RUNTIME_DIR': '/run/user/1000',
            'LOOM_SOCKET': '/run/user/1000/loom/api.sock', 'LOOM_THREAD': THREAD.id,
        })

    def test_environment_leaves_out_unset_variables(self) -> None:
        self.environ = {'PATH': '/usr/bin'}
        self.assertEqual(set(self.make().environment(THREAD)),
                         {'PATH', 'LOOM_SOCKET', 'LOOM_THREAD'})

    def test_a_private_token_is_read_and_stripped(self) -> None:
        self.write_token(b'  sk-token\n')
        self.assertEqual(self.host.secret('agent-token'), 'sk-token')
        self.assertEqual(self.log.getvalue(), '')

    def test_a_missing_or_empty_token_is_none(self) -> None:
        self.assertIsNone(self.host.secret('agent-token'))
        self.write_token(b'\n \n')
        self.assertIsNone(self.host.secret('agent-token'))
        self.assertEqual(self.log.getvalue(), '')

    def test_a_token_group_or_others_can_access_is_not_used(self) -> None:
        for mode in (0o640, 0o604, 0o610, 0o601):
            with self.subTest(mode=oct(mode)):
                self.log.seek(0)
                self.log.truncate()
                self.write_token(b'sk-token', mode)
                self.assertIsNone(self.host.secret('agent-token'))
                lines = self.log.getvalue().splitlines()
                self.assertEqual(len(lines), 1)
                self.assertTrue(lines[0].startswith('loom: warning: '))
                self.assertNotIn('sk-token', lines[0])

    def test_a_token_that_is_a_symlink_or_a_directory_is_not_used(self) -> None:
        target = self.data_dir / 'target'
        target.write_text('sk-token')
        target.chmod(0o600)
        (self.data_dir / 'agent-token').symlink_to(target)
        self.assertIsNone(self.host.secret('agent-token'))
        (self.data_dir / 'agent-token').unlink()
        (self.data_dir / 'agent-token').mkdir()
        self.assertIsNone(self.host.secret('agent-token'))
        self.assertEqual(len(self.log.getvalue().splitlines()), 2)
        self.assertNotIn('sk-token', self.log.getvalue())

    def test_other_secret_names_are_none(self) -> None:
        self.write_token(b'sk-token')
        self.assertIsNone(self.host.secret('other'))
        self.assertIsNone(self.host.secret('../agent-token'))
        for name in ('codex-token', 'CLIPROXY_API_KEY', 'codex_home', 'agent-token\n'):
            with self.subTest(name=name):
                self.assertIsNone(self.host.secret(name))
        self.assertEqual(self.host.secret('agent-token'), 'sk-token')


if __name__ == '__main__':
    unittest.main()
