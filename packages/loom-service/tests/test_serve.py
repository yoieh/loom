import asyncio
import json
import os
import signal
import socket
import stat
import sys
import tempfile
import unittest
from pathlib import Path

from .support import (
    TIMEOUT, Connection, clean_env, connect, make_claude_bin, make_repo, run_client, wait_until,
)

NETWORK_TABLES = ('tcp', 'tcp6', 'udp', 'udp6')


def mode_of(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def network_inodes() -> set[str]:
    inodes = set()
    for name in NETWORK_TABLES:
        try:
            lines = (Path('/proc/net') / name).read_text().splitlines()[1:]
        except FileNotFoundError:
            continue
        inodes.update(fields[9] for fields in map(str.split, lines) if len(fields) > 9)
    return inodes


def socket_inodes(pid: int) -> set[str]:
    found = set()
    for fd in Path(f'/proc/{pid}/fd').iterdir():
        try:
            target = os.readlink(fd)
        except OSError:
            continue
        if target.startswith('socket:['):
            found.add(target[len('socket:['):-1])
    return found


def is_gone(pid: int) -> bool:
    try:
        text = Path(f'/proc/{pid}/stat').read_text()
    except FileNotFoundError:
        return True
    return text[text.rindex(')') + 2] == 'Z'


class ServeCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name).resolve()
        self.home = self.base / 'home'
        self.home.mkdir()
        self.socket = self.base / 'run' / 'api.sock'
        self.data_dir = self.base / 'data'
        self.config = self.base / 'config.toml'
        self.config.write_text(f'claude_bin = "{make_claude_bin(self.base)}"\n')

    def env(self, **extra: str) -> dict[str, str]:
        return clean_env(HOME=str(self.home), OUTSIDE='leak',
                         LOOM_SOCKET=str(self.base / 'elsewhere.sock'), **extra)

    def args(self, socket_path: Path | None = None) -> list[str]:
        return ['--socket', str(socket_path or self.socket), '--data-dir', str(self.data_dir),
                '--config', str(self.config)]

    async def spawn(self, *args: str, env: dict[str, str] | None = None
                    ) -> asyncio.subprocess.Process:
        process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'loom_service', 'serve', *args,
            env=self.env() if env is None else env, cwd=self.base,
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE)
        self.addAsyncCleanup(self.stop, process)
        return process

    async def started(self, *args: str, env: dict[str, str] | None = None
                      ) -> tuple[asyncio.subprocess.Process, str]:
        process = await self.spawn(*(args or self.args()), env=env)
        line = await asyncio.wait_for(process.stderr.readline(), 15)
        return process, line.decode().rstrip('\n')

    async def exits(self, *args: str, env: dict[str, str] | None = None) -> tuple[int, str]:
        process = await self.spawn(*args, env=env)
        _, err = await asyncio.wait_for(process.communicate(), 15)
        return process.returncode, err.decode()

    async def stop(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is None:
            process.kill()
        await process.wait()

    async def signalled(self, process: asyncio.subprocess.Process, signum: int) -> tuple[int, str]:
        process.send_signal(signum)
        rest = await asyncio.wait_for(process.stderr.read(), 15)
        await asyncio.wait_for(process.wait(), TIMEOUT)
        return process.returncode, rest.decode()

    async def connect(self, socket_path: Path | None = None) -> Connection:
        conn = await connect(socket_path or self.socket)
        self.addAsyncCleanup(conn.close)
        return conn

    def assert_one_loom_line(self, err: str, problem: str) -> None:
        lines = err.splitlines()
        self.assertEqual(len(lines), 1, err)
        self.assertTrue(lines[0].startswith('loom: '), err)
        self.assertIn(problem, lines[0])


class StartTest(ServeCase):
    async def test_serve_listens_privately_and_binds_no_network_socket(self) -> None:
        process, line = await self.started()
        self.assertEqual(line, f'loom: serving on {self.socket}')
        self.assertEqual(mode_of(self.socket), 0o600)
        self.assertTrue(stat.S_ISSOCK(os.lstat(self.socket).st_mode))
        self.assertEqual(mode_of(self.socket.parent), 0o700)
        self.assertEqual(mode_of(self.data_dir), 0o700)
        held = socket_inodes(process.pid)
        self.assertTrue(held)
        self.assertEqual(held & network_inodes(), set())
        conn = await self.connect()
        self.assertEqual(await conn.ok('projects.list'), [])
        code, rest = await self.signalled(process, signal.SIGTERM)
        self.assertEqual(code, 0)
        self.assertEqual(rest, 'loom: stopped\n')
        self.assertFalse(os.path.lexists(self.socket))

    async def test_the_default_paths_come_from_the_xdg_dirs(self) -> None:
        runtime = self.base / 'xdg-run'
        runtime.mkdir(mode=0o700)
        env = self.env(XDG_RUNTIME_DIR=str(runtime), XDG_DATA_HOME=str(self.base / 'xdg-data'),
                       XDG_CONFIG_HOME=str(self.base / 'xdg-config'))
        process, line = await self.started('--config', str(self.config), env=env)
        expected = runtime / 'loom' / 'api.sock'
        self.assertEqual(line, f'loom: serving on {expected}')
        self.assertTrue(stat.S_ISSOCK(os.lstat(expected).st_mode))
        self.assertFalse(os.path.lexists(self.base / 'elsewhere.sock'))
        self.assertTrue((self.base / 'xdg-data' / 'loom' / 'loom.db').is_file())
        code, _ = await self.signalled(process, signal.SIGINT)
        self.assertEqual(code, 0)
        self.assertFalse(os.path.lexists(expected))

    async def test_no_runtime_dir_and_no_socket_exits_2(self) -> None:
        rest = ['--data-dir', str(self.data_dir), '--config', str(self.config)]
        for env in (self.env(), self.env(XDG_RUNTIME_DIR='')):
            with self.subTest(runtime=env.get('XDG_RUNTIME_DIR')):
                code, err = await self.exits(*rest, env=env)
                self.assertEqual(code, 2)
                self.assert_one_loom_line(err, 'XDG_RUNTIME_DIR')
        self.assertFalse(self.data_dir.exists())

    async def test_a_second_server_on_an_answering_socket_exits_2(self) -> None:
        first, _ = await self.started()
        code, err = await self.exits(*self.args())
        self.assertEqual(code, 2)
        self.assert_one_loom_line(err, 'another server is already answering')
        self.assertIsNone(first.returncode)
        conn = await self.connect()
        self.assertEqual(await conn.ok('projects.list'), [])

    async def test_a_socket_nobody_answers_on_is_replaced(self) -> None:
        self.socket.parent.mkdir(mode=0o700)
        stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stale.bind(str(self.socket))
        stale.close()
        _, line = await self.started()
        self.assertEqual(line, f'loom: serving on {self.socket}')
        self.assertEqual(mode_of(self.socket), 0o600)
        conn = await self.connect()
        self.assertEqual(await conn.ok('projects.list'), [])

    async def test_a_path_that_is_not_a_socket_exits_2_and_is_left_alone(self) -> None:
        self.socket.parent.mkdir(mode=0o700)
        self.socket.write_text('keep me')
        code, err = await self.exits(*self.args())
        self.assertEqual(code, 2)
        self.assert_one_loom_line(err, 'exists and is not a socket')
        self.assertEqual(self.socket.read_text(), 'keep me')

    async def test_a_bad_config_exits_2_naming_the_problem(self) -> None:
        cases = {
            'colour = "blue"\n': "unknown key 'colour'",
            'claude_bin = 1\n': 'claude_bin must be a string',
            'permission_mode = "root"\n': 'permission_mode must be one of',
            'claude_bin = "x\n': f'{self.config}: ',
        }
        for text, problem in cases.items():
            with self.subTest(config=text):
                self.config.write_text(text)
                code, err = await self.exits(*self.args())
                self.assertEqual(code, 2)
                self.assert_one_loom_line(err, problem)
                self.assertFalse(os.path.lexists(self.socket))


class StopTest(ServeCase):
    async def test_sigterm_ends_streams_closes_agents_and_removes_the_socket(self) -> None:
        await self.stops_cleanly(signal.SIGTERM)

    async def test_sigint_ends_streams_closes_agents_and_removes_the_socket(self) -> None:
        await self.stops_cleanly(signal.SIGINT)

    async def stops_cleanly(self, signum: int) -> None:
        repo = make_repo(self.base / 'repo')
        process, _ = await self.started()
        added = await run_client(self.socket, [{'call': 'projects.add', 'params': {
            'id': 'p1', 'title': 'Repo', 'path': str(repo)}}], clean_env())
        self.assertEqual(added[0]['ok'], True)
        conn = await self.connect()
        thread = await conn.ok('threads.launch', id='l1', title='T', message='hello',
                               project_id='repo', workspace={'strategy': 'root'})
        follower = await self.connect()
        self.assertEqual(await follower.call('threads.events', follow=True),
                         {'ok': True, 'result': {'follow': True}})
        while (await follower.read())['event']['type'] != 'turn.result':
            pass
        spawned = json.loads((repo / 'spawned.json').read_text())
        self.assertEqual(spawned['env']['LOOM_THREAD'], thread['id'])
        self.assertEqual(spawned['env']['LOOM_SOCKET'], str(self.socket))
        self.assertNotIn('OUTSIDE', spawned['env'])
        self.assertFalse(is_gone(spawned['pid']))

        code, rest = await self.signalled(process, signum)
        self.assertEqual(code, 0)
        self.assertEqual(rest, 'loom: stopped\n')
        self.assertTrue(await follower.stream_ends())
        await wait_until(lambda: is_gone(spawned['pid']))
        self.assertFalse(os.path.lexists(self.socket))


if __name__ == '__main__':
    unittest.main()
