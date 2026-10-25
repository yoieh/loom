import io
import json
import tempfile
import time
import unittest
from pathlib import Path

from loom_service.config import Config
from loom_service.host import Host

from .support import Connection, ServerCase

TOKEN = 'sk-t3-access-token-1234'
ORIGIN = 'http://127.0.0.1:9'
NOT_PAIRED = 't3: not paired (run loom t3 pair)'
NOT_RUNNING = 't3: T3 is not running at the paired address'


def credential_text(**changes: object) -> str:
    values = {
        'origin': ORIGIN, 'access_token': TOKEN, 'expires_at': int(time.time()) + 30 * 86400,
        'session_id': 'session-1', 'client_id': 'client-1', 'scope': 'orchestration:read',
        **changes,
    }
    return json.dumps(values, indent=2) + '\n'


def write_runtime(t3_home: Path, origin: str = ORIGIN) -> None:
    userdata = t3_home / 'userdata'
    userdata.mkdir(parents=True)
    (userdata / 'server-runtime.json').write_text(json.dumps({'origin': origin}))


class HostTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data_dir = Path(tmp.name)
        self.path = self.data_dir / 't3-client.json'
        self.log = io.StringIO()
        self.host = self.make()

    def make(self, config: Config | None = None) -> Host:
        return Host(self.data_dir, Path('/run/user/1000/loom/api.sock'), config or Config(),
                    {'PATH': '/usr/bin'}, self.log)

    def write(self, data: bytes, mode: int = 0o600) -> None:
        self.path.write_bytes(data)
        self.path.chmod(mode)

    def reset_log(self) -> None:
        self.log.seek(0)
        self.log.truncate()

    def test_the_t3_settings_come_from_the_config(self) -> None:
        host = self.make(Config(t3_home='/t3-home', t3_bin='/opt/t3'))
        self.assertEqual(host.setting('t3_home'), '/t3-home')
        self.assertEqual(host.setting('t3_bin'), '/opt/t3')
        self.assertIsNone(self.host.setting('t3_home'))
        self.assertIsNone(self.host.setting('t3_bin'))

    def test_a_private_t3_client_file_is_read_and_stripped(self) -> None:
        text = credential_text()
        self.write(f'\n {text}\n'.encode())
        self.assertEqual(self.host.secret('t3-client'), text.strip())
        self.assertEqual(self.log.getvalue(), '')

    def test_a_missing_t3_client_file_is_none_with_no_warning(self) -> None:
        self.assertIsNone(self.host.secret('t3-client'))
        self.assertEqual(self.log.getvalue(), '')

    def test_a_t3_client_file_that_breaks_a_rule_is_not_used(self) -> None:
        text = credential_text().encode()
        target = self.data_dir / 'target'
        target.write_bytes(text)
        target.chmod(0o600)

        def wide() -> None:
            self.write(text, 0o644)

        def symlink() -> None:
            self.path.symlink_to(target)

        def directory() -> None:
            self.path.mkdir()
            (self.path / 'inner').write_bytes(text)

        def too_big() -> None:
            self.write(text + b' ' * 65_536)

        def not_utf8() -> None:
            self.write(text + b'\xff\xfe')

        for name, make in (('0644', wide), ('symlink', symlink), ('directory', directory),
                           ('over 64 KiB', too_big), ('not UTF-8', not_utf8)):
            with self.subTest(case=name):
                self.reset_log()
                make()
                self.assertIsNone(self.host.secret('t3-client'))
                lines = self.log.getvalue().splitlines()
                self.assertEqual(len(lines), 1)
                self.assertTrue(lines[0].startswith(f'loom: warning: not using {self.path}: '))
                self.assertNotIn(TOKEN, lines[0])
                self.assertNotIn('session-1', lines[0])
                if self.path.is_dir() and not self.path.is_symlink():
                    (self.path / 'inner').unlink()
                    self.path.rmdir()
                else:
                    self.path.unlink()

    def test_the_agent_token_is_unchanged_and_other_names_are_none(self) -> None:
        token = self.data_dir / 'agent-token'
        token.write_text(' sk-token\n')
        token.chmod(0o600)
        self.write(credential_text().encode())
        self.assertEqual(self.host.secret('agent-token'), 'sk-token')
        for name in ('other', 't3-client.json', 't3_client', 'T3-client', '../t3-client',
                     't3-client\n', None, 5):
            with self.subTest(name=name):
                self.assertIsNone(self.host.secret(name))
        self.assertEqual(self.log.getvalue(), '')


class T3LaunchTest(ServerCase):
    def write_credential(self, mode: int = 0o600) -> None:
        self.data_dir.mkdir(exist_ok=True)
        path = self.data_dir / 't3-client.json'
        path.write_text(credential_text())
        path.chmod(mode)

    async def launch_t3(self, conn: Connection) -> dict:
        await self.add_project(conn)
        thread = await self.launch(conn, 'hello', runner='t3')
        self.assertEqual(thread['runner'], 't3')
        return thread

    async def failed_with(self, conn: Connection, thread: dict) -> str:
        events = await self.wait_events(conn, 'thread.failed', thread['id'])
        got = await conn.ok('threads.get', thread_id=thread['id'])
        self.assertEqual(got['state'], 'failed')
        self.assertEqual(events[0]['data']['reason'], got['reason'])
        return got['reason']

    async def assert_no_token(self, conn: Connection) -> None:
        seen = [json.dumps(await self.all_events(conn)),
                json.dumps(await conn.ok('threads.list', project_id='repo')),
                self.log.getvalue()]
        for text in seen:
            self.assertNotIn(TOKEN, text)

    async def test_with_no_pairing_a_t3_thread_is_served_and_fails_not_paired(self) -> None:
        conn = await self.connect()
        thread = await self.launch_t3(conn)
        self.assertEqual(await self.failed_with(conn, thread), NOT_PAIRED)
        self.assertFalse((self.data_dir / 't3').exists())

    async def test_t3_home_reaches_the_runner(self) -> None:
        self.write_credential()
        write_runtime(self.home / '.t3')
        t3_home = self.base / 't3-home'
        t3_home.mkdir()
        await self.restart(self.config(t3_home=str(t3_home), t3_bin=str(self.base / 't3')))
        conn = await self.connect()
        thread = await self.launch_t3(conn)
        self.assertEqual(await self.failed_with(conn, thread), NOT_RUNNING)
        await self.assert_no_token(conn)

    async def test_no_reason_event_or_log_line_holds_the_token(self) -> None:
        t3_home = self.base / 't3-home'
        t3_home.mkdir()
        await self.restart(self.config(t3_home=str(t3_home)))
        conn = await self.connect()
        await self.add_project(conn)
        reasons = []
        for mode in (0o600, 0o644):
            with self.subTest(mode=oct(mode)):
                self.write_credential(mode)
                thread = await self.launch(conn, 'hello', runner='t3', title=f'T3 {mode:o}')
                reasons.append(await self.failed_with(conn, thread))
        self.assertEqual(reasons, [NOT_RUNNING, NOT_PAIRED])
        warnings = [line for line in self.log.getvalue().splitlines() if 'warning' in line]
        self.assertEqual(len(warnings), 1)
        self.assertIn('t3-client.json', warnings[0])
        await self.assert_no_token(conn)


if __name__ == '__main__':
    unittest.main()
