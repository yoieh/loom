import dataclasses
import unittest

from loom_contracts.runners import PermissionMode, RunnerHost, RunnerName, ThreadSpec
from loom_runner_claude.command import CLAUDE_MODES, ENV_ALLOWLIST, claude_argv, claude_env

THREAD_ID = '77f465be-2f52-4311-84a5-e19e6d1e339b'

HEAD = [
    'claude',
    '-p',
    '--input-format', 'stream-json',
    '--output-format', 'stream-json',
    '--verbose',
    '--replay-user-messages',
    '--permission-prompt-tool', 'stdio',
]

MODE_FLAGS = {
    PermissionMode.APPROVAL_REQUIRED: ['--permission-mode', 'manual'],
    PermissionMode.AUTO_ACCEPT_EDITS: ['--permission-mode', 'acceptEdits'],
    PermissionMode.AUTO: ['--permission-mode', 'auto'],
    PermissionMode.FULL_ACCESS: [
        '--permission-mode', 'bypassPermissions', '--allow-dangerously-skip-permissions',
    ],
}


def make_thread(**changes: object) -> ThreadSpec:
    thread = ThreadSpec(
        id=THREAD_ID,
        runner=RunnerName.CLAUDE,
        project_id='loom',
        workspace_path='/tmp/loom-replay/repo',
        title='Replay',
        permission_mode=PermissionMode.AUTO,
    )
    return dataclasses.replace(thread, **changes)


class FakeHost:
    data_dir = '/tmp/loom-replay/data'

    def __init__(
        self,
        environment: dict[str, object] | None = None,
        settings: dict[str, object] | None = None,
        secrets: dict[str, object] | None = None,
    ) -> None:
        self.env = environment or {}
        self.settings = settings or {}
        self.secrets = secrets or {}
        self.asked: list[ThreadSpec] = []

    def setting(self, key: str) -> str | None:
        return self.settings.get(key)

    def environment(self, thread: ThreadSpec) -> dict[str, str]:
        self.asked.append(thread)
        return dict(self.env)

    def secret(self, name: str) -> str | None:
        return self.secrets.get(name)


class ArgvTest(unittest.TestCase):
    def test_new_thread_in_each_mode(self) -> None:
        for mode in PermissionMode:
            with self.subTest(mode=mode):
                argv = claude_argv(make_thread(permission_mode=mode), 'claude')
                self.assertEqual(argv, [
                    *HEAD, *MODE_FLAGS[mode], '--disallowed-tools', 'AskUserQuestion',
                    '--session-id', THREAD_ID,
                ])
                self.assertEqual(argv[-2:], ['--session-id', THREAD_ID])
                self.assertNotIn('--resume', argv)
                self.assertTrue(all(type(item) is str for item in argv))

    def test_modes_table_covers_every_mode(self) -> None:
        self.assertEqual(set(CLAUDE_MODES), set(PermissionMode))
        for mode, flags in CLAUDE_MODES.items():
            self.assertEqual(list(flags), MODE_FLAGS[mode])

    def test_mode_given_as_plain_string_maps_the_same(self) -> None:
        argv = claude_argv(make_thread(permission_mode='auto-accept-edits'), 'claude')
        self.assertIn('acceptEdits', argv)

    def test_resume_replaces_session_id(self) -> None:
        for mode in PermissionMode:
            with self.subTest(mode=mode):
                argv = claude_argv(make_thread(permission_mode=mode, resume=True), 'claude')
                self.assertEqual(argv, [
                    *HEAD, *MODE_FLAGS[mode], '--disallowed-tools', 'AskUserQuestion',
                    '--resume', THREAD_ID,
                ])
                self.assertNotIn('--session-id', argv)
                self.assertEqual(argv.count('--resume'), 1)

    def test_resume_must_be_true_itself(self) -> None:
        argv = claude_argv(make_thread(resume='false'), 'claude')
        self.assertEqual(argv[-2:], ['--session-id', THREAD_ID])
        self.assertNotIn('--resume', argv)

    def test_model_only_when_set(self) -> None:
        without = claude_argv(make_thread(), 'claude')
        self.assertNotIn('--model', without)
        for model in ('claude-opus-5-5', 'opus', 'us.anthropic/claude:1[1m]', 'a' * 200):
            with self.subTest(model=model):
                argv = claude_argv(make_thread(model=model), 'claude')
                self.assertEqual(argv, [
                    *HEAD, '--permission-mode', 'auto', '--model', model,
                    '--disallowed-tools', 'AskUserQuestion', '--session-id', THREAD_ID,
                ])

    def test_claude_bin_is_first(self) -> None:
        argv = claude_argv(make_thread(), '/opt/claude/bin/claude')
        self.assertEqual(argv[0], '/opt/claude/bin/claude')

    def test_invalid_input_raises(self) -> None:
        cases = {
            'non-uuid id': (make_thread(id='thread-1'), 'claude'),
            'empty id': (make_thread(id=''), 'claude'),
            'uppercase uuid': (make_thread(id=THREAD_ID.upper()), 'claude'),
            'braced uuid': (make_thread(id='{' + THREAD_ID + '}'), 'claude'),
            'urn uuid': (make_thread(id='urn:uuid:' + THREAD_ID), 'claude'),
            'hex uuid': (make_thread(id=THREAD_ID.replace('-', '')), 'claude'),
            'id starting with -': (make_thread(id='--dangerously-skip-permissions'), 'claude'),
            'id with a newline': (make_thread(id=THREAD_ID + '\n'), 'claude'),
            'id not a str': (make_thread(id=7), 'claude'),
            'unknown mode': (make_thread(permission_mode='bypassPermissions'), 'claude'),
            'empty mode': (make_thread(permission_mode=''), 'claude'),
            'mode None': (make_thread(permission_mode=None), 'claude'),
            'mode not a str': (make_thread(permission_mode=['auto']), 'claude'),
            'model starting with -': (make_thread(model='--verbose'), 'claude'),
            'model with a space': (make_thread(model='claude opus'), 'claude'),
            'model with a newline': (make_thread(model='opus\n'), 'claude'),
            'empty model': (make_thread(model=''), 'claude'),
            'model too long': (make_thread(model='a' * 201), 'claude'),
            'model not a str': (make_thread(model=5), 'claude'),
            'relative workspace': (make_thread(workspace_path='repo'), 'claude'),
            'empty workspace': (make_thread(workspace_path=''), 'claude'),
            'empty claude_bin': (make_thread(), ''),
            'claude_bin None': (make_thread(), None),
        }
        for name, (thread, claude_bin) in cases.items():
            with self.subTest(case=name):
                with self.assertRaises(ValueError):
                    claude_argv(thread, claude_bin)

    def test_only_full_access_bypasses_permissions(self) -> None:
        for mode in PermissionMode:
            for resume in (False, True):
                with self.subTest(mode=mode, resume=resume):
                    argv = claude_argv(make_thread(permission_mode=mode, resume=resume), 'claude')
                    bypass = 'bypassPermissions' in argv
                    skip = '--allow-dangerously-skip-permissions' in argv
                    self.assertEqual(bypass, mode is PermissionMode.FULL_ACCESS)
                    self.assertEqual(skip, mode is PermissionMode.FULL_ACCESS)


class EnvTest(unittest.TestCase):
    def test_fake_host_is_a_runner_host(self) -> None:
        self.assertIsInstance(FakeHost(), RunnerHost)

    def test_allowlist(self) -> None:
        self.assertEqual(ENV_ALLOWLIST, frozenset({
            'HOME', 'LANG', 'LOGNAME', 'PATH', 'SHELL', 'USER', 'XDG_RUNTIME_DIR', 'LOOM_SOCKET',
        }))

    def test_keeps_only_allowlisted_keys(self) -> None:
        allowed = {key: f'value-{key}' for key in ENV_ALLOWLIST}
        host = FakeHost(environment={
            **allowed,
            'CLAUDE_CODE_SESSION_ID': 'leaked',
            'CLAUDECODE': '1',
            'ANTHROPIC_AUTH_TOKEN': 'host-token',
            'ANTHROPIC_BASE_URL': 'http://host',
            'AWS_SECRET_ACCESS_KEY': 'aws',
            'LOOM_TOKEN': 'loom',
            'PATH_EXTRA': '/bin',
            'path': '/lower',
            'HOME': 7,
        })
        thread = make_thread()
        env = claude_env(thread, host)
        expected = {key: value for key, value in allowed.items() if key != 'HOME'}
        self.assertEqual(env, {
            **expected, 'LOOM_THREAD': THREAD_ID, 'ANTHROPIC_API_KEY': '',
        })
        self.assertEqual(host.asked, [thread])

    def test_loom_thread_overrides_the_host(self) -> None:
        host = FakeHost(environment={'LOOM_THREAD': 'other-thread', 'PATH': '/usr/bin'})
        env = claude_env(make_thread(), host)
        self.assertEqual(env['LOOM_THREAD'], THREAD_ID)
        self.assertEqual(env['PATH'], '/usr/bin')

    def test_host_settings_and_secret(self) -> None:
        host = FakeHost(
            environment={'ANTHROPIC_API_KEY': 'host-key', 'ANTHROPIC_AUTH_TOKEN': 'host-token'},
            settings={
                'claude_config_dir': '/home/loom/.claude-loom',
                'agent_base_url': 'http://127.0.0.1:8317',
            },
            secrets={'agent-token': '  sk-loom-token\n'},
        )
        self.assertEqual(claude_env(make_thread(), host), {
            'LOOM_THREAD': THREAD_ID,
            'CLAUDE_CONFIG_DIR': '/home/loom/.claude-loom',
            'ANTHROPIC_BASE_URL': 'http://127.0.0.1:8317',
            'ANTHROPIC_AUTH_TOKEN': 'sk-loom-token',
            'ANTHROPIC_API_KEY': '',
        })

    def test_missing_host_values_are_left_out(self) -> None:
        for value in (None, '', 5, b'/cfg', ['/cfg']):
            with self.subTest(value=value):
                host = FakeHost(
                    settings={'claude_config_dir': value, 'agent_base_url': value},
                    secrets={'agent-token': value},
                )
                self.assertEqual(claude_env(make_thread(), host), {
                    'LOOM_THREAD': THREAD_ID, 'ANTHROPIC_API_KEY': '',
                })
        self.assertEqual(claude_env(make_thread(), FakeHost()), {
            'LOOM_THREAD': THREAD_ID, 'ANTHROPIC_API_KEY': '',
        })

    def test_whitespace_token_is_left_out(self) -> None:
        for token in (' ', ' \n\t', '\r\n'):
            with self.subTest(token=token):
                env = claude_env(make_thread(), FakeHost(secrets={'agent-token': token}))
                self.assertNotIn('ANTHROPIC_AUTH_TOKEN', env)
                self.assertEqual(env['ANTHROPIC_API_KEY'], '')

    def test_each_host_value_is_asked_by_its_key(self) -> None:
        host = FakeHost(
            settings={'CLAUDE_CONFIG_DIR': '/wrong', 'ANTHROPIC_BASE_URL': 'http://wrong'},
            secrets={'ANTHROPIC_AUTH_TOKEN': 'wrong', 'agent_token': 'wrong'},
        )
        self.assertEqual(claude_env(make_thread(), host), {
            'LOOM_THREAD': THREAD_ID, 'ANTHROPIC_API_KEY': '',
        })


if __name__ == '__main__':
    unittest.main()
