import contextlib
import io
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from loom_runner_t3.__main__ import main
from loom_runner_t3.pairing import run_t3

from .fake_t3 import FakeT3
from .replay import DAY, NOW, ORIGIN, SESSION_ID, TOKEN, FakeClock, FakeHttp, credential_text

CONNECTIONS = "remove 'Loom' in T3 Settings → Connections"


class MainBase(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name).resolve()
        self.user_home = self.base / 'home'
        self.user_home.mkdir()
        self.t3_home = self.user_home / '.t3'
        self.write(self.t3_home / 'userdata' / 'server-runtime.json',
                   f'{{"origin": "{ORIGIN}", "serverVersion": "0.1.2"}}')
        self.t3 = FakeT3(self.t3_home / 'runtime' / 'versions' / '0.1.2')
        self.t3.install()
        self.data_dir = self.user_home / '.local' / 'share' / 'loom'
        self.environ = {'HOME': str(self.user_home), 'PATH': os.environ.get('PATH', '')}
        self.clock = FakeClock()
        self.http = FakeHttp()

    def write(self, path: Path, text: str, mode: int = 0o600) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        path.chmod(mode)

    def store(self, text: str | None = None, data_dir: Path | None = None, **changes) -> Path:
        path = (data_dir or self.data_dir) / 't3-client.json'
        self.write(path, credential_text(**changes) if text is None else text)
        return path

    def run_main(self, *argv: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(err):
            code = main(list(argv), environ=self.environ, http=self.http, run=run_t3,
                        clock=self.clock, out=out, err=err)
        for text in (out.getvalue(), err.getvalue()):
            self.assertNotIn(TOKEN, text)
        return code, out.getvalue(), err.getvalue()


class StatusTest(MainBase):
    def test_not_paired(self) -> None:
        self.assertEqual(self.run_main('status'), (1, 'Not paired. Run loom t3 pair.\n', ''))

    def test_paired(self) -> None:
        self.store(expires_at=NOW + 30 * DAY)
        self.assertEqual(self.run_main('status'),
                         (0, f'Paired with T3 at {ORIGIN}: 30 days left.\n', ''))

    def test_days_are_rounded_down(self) -> None:
        self.store(expires_at=NOW + 10 * DAY - 1)
        self.assertEqual(self.run_main('status')[1],
                         f'Paired with T3 at {ORIGIN}: 9 days left.\n')

    def test_a_warning_under_seven_days(self) -> None:
        self.store(expires_at=NOW + 7 * DAY)
        self.assertEqual(self.run_main('status'),
                         (0, f'Paired with T3 at {ORIGIN}: 7 days left.\n', ''))
        self.store(expires_at=NOW + 7 * DAY - 1)
        self.assertEqual(self.run_main('status'), (0, (
            f'Paired with T3 at {ORIGIN}: 6 days left.\n'
            'Warning: the pairing ends in 6 days; run loom t3 pair to renew it.\n'), ''))

    def test_the_last_second_is_zero_days(self) -> None:
        self.store(expires_at=NOW + 1)
        code, out, _ = self.run_main('status')
        self.assertEqual(code, 0)
        self.assertTrue(out.startswith(f'Paired with T3 at {ORIGIN}: 0 days left.\n'))

    def test_expired(self) -> None:
        for expires_at in (NOW, NOW - 3 * DAY):
            with self.subTest(expires_at=expires_at):
                self.store(expires_at=expires_at)
                day = time.strftime('%Y-%m-%d', time.localtime(expires_at))
                self.assertEqual(self.run_main('status'),
                                 (1, f'The pairing expired on {day}. Run loom t3 pair.\n', ''))

    def test_a_broken_file_fails(self) -> None:
        path = self.store('not json')
        self.assertEqual(self.run_main('status'), (
            1, '', f'loom: t3: {path} is not a usable pairing; run loom t3 pair\n'))
        path.chmod(0o644)
        code, out, err = self.run_main('status')
        self.assertEqual((code, out), (1, ''))
        self.assertEqual(err, f'loom: t3: {path}: its mode is 644, make it 600\n')


class UnpairTest(MainBase):
    def test_not_paired(self) -> None:
        self.assertEqual(self.run_main('unpair'), (0, 'Not paired.\n', ''))
        self.assertEqual(self.t3.calls(), [])

    def test_unpair_revokes_and_deletes(self) -> None:
        path = self.store()
        self.assertEqual(self.run_main('unpair'), (0, 'Unpaired.\n', ''))
        self.assertFalse(path.exists())
        call, = self.t3.calls()
        self.assertEqual(call['argv'], [str(self.t3.path), 'auth', 'session', 'revoke',
                                        '--base-dir', str(self.t3_home), SESSION_ID])
        self.assertNotIn(TOKEN, str(call))
        self.assertEqual(self.http.calls, [])

    def test_a_failed_revoke_still_deletes(self) -> None:
        path = self.store()
        self.t3.exit('session-revoke', 2)
        self.assertEqual(self.run_main('unpair'), (1, '', (
            'loom: t3: could not revoke the session (t3 exited with code 2); '
            f'{CONNECTIONS}\n')))
        self.assertFalse(path.exists())

    def test_a_missing_t3_still_deletes(self) -> None:
        path = self.store()
        self.t3.path.unlink()
        code, out, err = self.run_main('unpair')
        self.assertEqual((code, out), (1, ''))
        self.assertTrue(err.startswith('loom: t3: could not revoke the session (could not '
                                       f'run {self.t3.path}: '), err)
        self.assertTrue(err.endswith(f'{CONNECTIONS}\n'))
        self.assertFalse(path.exists())

    def test_no_usable_session_id_still_deletes(self) -> None:
        for text in ('not json', credential_text(session_id=None),
                     credential_text(session_id='--base-dir')):
            with self.subTest(text=text):
                path = self.store(text)
                self.assertEqual(self.run_main('unpair'), (1, '', (
                    'loom: t3: could not revoke the session (the file has no usable session '
                    f'id); {CONNECTIONS}\n')))
                self.assertFalse(path.exists())
        self.assertEqual(self.t3.calls(), [])

    def test_a_file_with_a_wide_mode_is_deleted_without_reading_it(self) -> None:
        path = self.store()
        path.chmod(0o644)
        code, _, err = self.run_main('unpair')
        self.assertEqual(code, 1)
        self.assertIn('no usable session id', err)
        self.assertFalse(path.exists())


class PathsTest(MainBase):
    def test_the_data_dir_defaults_to_xdg_data_home(self) -> None:
        xdg = self.base / 'xdg-data'
        self.store(data_dir=xdg / 'loom')
        self.environ['XDG_DATA_HOME'] = str(xdg)
        self.assertEqual(self.run_main('status')[0], 0)
        del self.environ['XDG_DATA_HOME']
        self.assertEqual(self.run_main('status')[0], 1)
        self.store()
        self.assertEqual(self.run_main('status')[0], 0)

    def test_data_dir_option(self) -> None:
        other = self.base / 'other'
        self.store(data_dir=other)
        self.assertEqual(self.run_main('status', '--data-dir', str(other))[0], 0)
        cwd = os.getcwd()
        os.chdir(self.base)
        try:
            self.assertEqual(self.run_main('status', '--data-dir', 'other')[0], 0)
        finally:
            os.chdir(cwd)

    def test_the_config_names_t3_home(self) -> None:
        other_home = self.base / 'other-t3'
        self.write(other_home / 'userdata' / 'server-runtime.json',
                   '{"serverVersion": "9.9.9"}')
        other = FakeT3(other_home / 'runtime' / 'versions' / '9.9.9')
        other.install()
        config = self.user_home / '.config' / 'loom' / 'config.toml'
        self.write(config, f't3_home = "{other_home}"\nclaude_bin = 5\n')
        self.store()
        self.assertEqual(self.run_main('unpair')[0], 0)
        self.assertEqual(other.calls()[0]['argv'][-3:],
                         ['--base-dir', str(other_home), SESSION_ID])
        self.assertEqual(self.t3.calls(), [])

    def test_xdg_config_home_and_the_config_option(self) -> None:
        bin_path = self.base / 'bin'
        fake = FakeT3(bin_path)
        fake.install()
        xdg = self.base / 'xdg-config'
        self.write(xdg / 'loom' / 'config.toml', f't3_bin = "{fake.path}"\n')
        self.environ['XDG_CONFIG_HOME'] = str(xdg)
        self.store()
        self.assertEqual(self.run_main('unpair')[0], 0)
        self.assertEqual(len(fake.calls()), 1)
        explicit = self.base / 'explicit.toml'
        self.write(explicit, f't3_bin = "{self.t3.path}"\n')
        self.store()
        self.assertEqual(self.run_main('unpair', '--config', str(explicit))[0], 0)
        self.assertEqual(len(self.t3.calls()), 1)

    def test_a_missing_config_means_the_defaults(self) -> None:
        self.store()
        missing = self.base / 'missing.toml'
        self.assertEqual(self.run_main('unpair', '--config', str(missing))[0], 0)
        self.assertEqual(len(self.t3.calls()), 1)

    def test_a_bad_config_exits_2(self) -> None:
        config = self.base / 'config.toml'
        for text, problem in (('t3_home = ', None),
                              ('t3_home = 5\n', 't3_home must be a string'),
                              ('t3_bin = ["x"]\n', 't3_bin must be a string'),
                              ('t3_bin = true\n', 't3_bin must be a string')):
            with self.subTest(text=text):
                self.write(config, text)
                code, out, err = self.run_main('status', '--config', str(config))
                self.assertEqual((code, out), (2, ''))
                self.assertTrue(err.startswith(f'loom: {config}: '), err)
                if problem is not None:
                    self.assertEqual(err, f'loom: {config}: {problem}\n')

    def test_usage_errors_exit_2(self) -> None:
        for argv in ((), ('bogus',), ('status', '--nope'), ('status', '--data-dir', ''),
                     ('pair', '--config', '')):
            with self.subTest(argv=argv):
                self.assertEqual(self.run_main(*argv)[0], 2)

    def test_pair_goes_through_main(self) -> None:
        (self.t3_home / 'userdata' / 'server-runtime.json').unlink()
        self.assertEqual(self.run_main('pair'), (
            1, '', 'loom: t3: T3 is not running (no origin in its server-runtime.json)\n'))
        self.assertEqual(self.http.calls, [])

    def test_python_m_runs_the_cli(self) -> None:
        env = {'HOME': str(self.user_home), 'PATH': os.environ.get('PATH', '')}
        done = subprocess.run([sys.executable, '-m', 'loom_runner_t3', 'status'], env=env,
                              capture_output=True, text=True, timeout=60)
        self.assertEqual((done.returncode, done.stdout, done.stderr),
                         (1, 'Not paired. Run loom t3 pair.\n', ''))
        done = subprocess.run([sys.executable, '-m', 'loom_runner_t3'], env=env,
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 2)


if __name__ == '__main__':
    unittest.main()
