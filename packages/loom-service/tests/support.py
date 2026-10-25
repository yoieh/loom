import asyncio
import io
import itertools
import json
import os
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from unittest import mock

from loom_service.config import Config
from loom_service.serve import Serving, start

TIMEOUT = 5.0
PLENTY = 64 * 2**30
FAKE_CLAUDE = Path(__file__).resolve().parent / 'fake_claude.py'
FAKE_CODEX = Path(__file__).resolve().parent / 'fake_codex.py'
SETUP_ENV = {**os.environ, 'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_CONFIG_NOSYSTEM': '1'}

CLIENT = '''
import socket, sys
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.settimeout(10)
sock.connect(sys.argv[1])
file = sock.makefile('rwb')
for request in sys.argv[2:]:
    file.write(request.encode() + b'\\n')
    file.flush()
    sys.stdout.write(file.readline().decode())
'''

UNSETTING_PARENT = '''
import subprocess, sys
sys.exit(subprocess.run(['env', '-u', 'LOOM_THREAD', *sys.argv[1:]]).returncode)
'''


def git(cwd: Path, *args: str) -> str:
    done = subprocess.run(['git', *args], cwd=cwd, env=SETUP_ENV, check=True,
                          capture_output=True, text=True)
    return done.stdout.strip()


def make_repo(path: Path) -> Path:
    path.mkdir(parents=True)
    git(path, 'init', '-q', '-b', 'main')
    git(path, '-c', 'user.name=Test', '-c', 'user.email=test@example.com',
        'commit', '-q', '--allow-empty', '-m', 'First')
    return path.resolve()


def make_claude_bin(directory: Path) -> Path:
    path = directory / 'claude'
    path.write_text(f'#!/bin/sh\nexec {sys.executable} {FAKE_CLAUDE} "$@"\n')
    path.chmod(0o700)
    return path


def make_codex_bin(directory: Path) -> Path:
    path = directory / 'codex'
    path.write_text(f'#!/bin/sh\nexec {sys.executable} {FAKE_CODEX} "$@"\n')
    path.chmod(0o700)
    return path


def clean_env(**extra: str) -> dict[str, str]:
    env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'LANG': 'C.UTF-8'}
    env.update(extra)
    return env


async def run_client(socket: Path, requests: list[object], env: dict[str, str],
                     unset_parent: bool = False) -> list[dict]:
    args = [sys.executable, '-I', '-c', CLIENT, str(socket), *map(json.dumps, requests)]
    if unset_parent:
        args = [sys.executable, '-I', '-c', UNSETTING_PARENT, *args]
    process = await asyncio.create_subprocess_exec(
        *args, env=env, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE)
    try:
        out, _ = await asyncio.wait_for(process.communicate(), TIMEOUT)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    return [json.loads(line) for line in out.splitlines()]


def pad_line(size: int, call: str = 'projects.list') -> bytes:
    head = json.dumps({'call': call, 'params': {'pad': ''}}).encode()
    cut = head.rindex(b'""') + 1
    line = head[:cut] + b'x' * (size - len(head)) + head[cut:]
    assert len(line) == size
    return line


class Connection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.ids = itertools.count(1)

    async def send(self, data: bytes) -> None:
        self.writer.write(data)
        await self.writer.drain()

    async def read(self, timeout: float = TIMEOUT) -> dict:
        line = await asyncio.wait_for(self.reader.readline(), timeout)
        if not line:
            raise EOFError('the server closed the connection')
        return json.loads(line)

    async def request(self, request: object) -> dict:
        await self.send(json.dumps(request).encode() + b'\n')
        return await self.read()

    async def call(self, call: str, **params: object) -> dict:
        return await self.request({'v': 'v1', 'call': call, 'params': params})

    async def ok(self, call: str, **params: object) -> object:
        answer = await self.call(call, **params)
        if answer.get('ok') is not True:
            raise AssertionError(f'{call} answered {answer}')
        return answer['result']

    def new_id(self) -> str:
        return f'c{id(self)}-{next(self.ids)}'

    async def at_eof(self, timeout: float = TIMEOUT) -> bool:
        try:
            rest = await asyncio.wait_for(self.reader.read(), timeout)
        except ConnectionResetError:
            return True
        return rest == b''

    async def stream_ends(self, timeout: float = TIMEOUT) -> bool:
        try:
            rest = await asyncio.wait_for(self.reader.read(), timeout)
        except ConnectionResetError:
            return True
        return all('event' in json.loads(line) for line in rest.splitlines())

    async def close(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except OSError:
            pass


async def connect(socket: Path) -> Connection:
    reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(str(socket)), TIMEOUT)
    return Connection(reader, writer)


async def wait_until(predicate: Callable[[], object], timeout: float = TIMEOUT) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f'condition not met within {timeout} s')
        await asyncio.sleep(0.01)


class ServerCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name).resolve()
        self.home = self.base / 'home'
        self.home.mkdir()
        patcher = mock.patch.dict(os.environ, {'HOME': str(self.home)})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.repo = make_repo(self.base / 'repo')
        self.socket = self.base / 'run' / 'api.sock'
        self.data_dir = self.base / 'data'
        self.claude_bin = make_claude_bin(self.base)
        self.log = io.StringIO()
        self.environ = {
            'HOME': str(self.home), 'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
            'LANG': 'C.UTF-8', 'OUTSIDE': 'leak', 'LOOM_THREAD': 'inherited',
            'LOOM_SOCKET': str(self.base / 'elsewhere.sock'),
        }
        self.serving = await self.serve()

    def config(self, **values: str) -> Config:
        return Config(claude_bin=str(self.claude_bin), **values)

    async def serve(self, config: Config | None = None, socket: Path | None = None,
                    data_dir: Path | None = None, **options: object) -> Serving:
        options.setdefault('memory', lambda: PLENTY)
        serving = await start(socket or self.socket, data_dir or self.data_dir,
                              config or self.config(), environ=self.environ, log=self.log,
                              **options)
        self.addAsyncCleanup(serving.close)
        return serving

    async def restart(self, config: Config) -> None:
        await self.serving.close()
        self.serving = await self.serve(config)

    async def connect(self, socket: Path | None = None) -> Connection:
        conn = await connect(socket or self.socket)
        self.addAsyncCleanup(conn.close)
        return conn

    async def add_project(self, conn: Connection, **params: object) -> dict:
        params = {'title': 'Repo', 'path': str(self.repo), **params}
        return await conn.ok('projects.add', id=conn.new_id(), **params)

    async def launch(self, conn: Connection, message: str = 'hello', **params: object) -> dict:
        params = {'title': 'First', 'project_id': 'repo', 'workspace': {'strategy': 'root'},
                  **params}
        return await conn.ok('threads.launch', id=conn.new_id(), message=message, **params)

    async def all_events(self, conn: Connection, **params: object) -> list[dict]:
        found: list[dict] = []
        after = 0
        while page := await conn.ok('threads.events', after=after, **params):
            found += page
            after = page[-1]['id']
        return found

    async def wait_events(self, conn: Connection, kind: str, thread_id: str,
                          count: int = 1) -> list[dict]:
        deadline = asyncio.get_running_loop().time() + TIMEOUT
        while True:
            events = [event for event in await self.all_events(conn, thread_id=thread_id)
                      if event['type'] == kind]
            if len(events) >= count:
                return events
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f'{count} {kind} events not seen within {TIMEOUT} s')
            await asyncio.sleep(0.02)

    async def spawned(self, thread_id: str, root: Path | None = None) -> dict:
        path = (root or self.repo) / 'spawned.json'

        def ready() -> bool:
            try:
                return json.loads(path.read_text())['env'].get('LOOM_THREAD') == thread_id
            except (OSError, ValueError):
                return False

        await wait_until(ready)
        return json.loads(path.read_text())
