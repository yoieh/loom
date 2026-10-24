import asyncio
import json
import os
import socket
import stat
import sys
import traceback
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TextIO

from loom_core.errors import LoomError
from loom_core.service import Caller, Service

from .config import StartError
from .peer import PROC, identify, peer_credentials

MAX_LINE = 1_048_576
VERSION = 'v1'
FOLLOW_CALL = 'threads.events'
PROBE_TIMEOUT = 1.0

_DISCARD = 65_536


def encode(value: object) -> bytes:
    return json.dumps(value, separators=(',', ':')).encode('ascii') + b'\n'


def error_line(code: str, message: str) -> bytes:
    return encode({'ok': False, 'error': {'code': code, 'message': message}})


def decode(line: bytes) -> dict:
    try:
        text = line.decode('utf-8')
    except UnicodeDecodeError:
        raise LoomError('invalid', 'request is not UTF-8') from None
    try:
        request = json.loads(text)
    except (ValueError, RecursionError):
        raise LoomError('invalid', 'request is not JSON') from None
    if not isinstance(request, dict):
        raise LoomError('invalid', 'request must be an object')
    if request.get('v', VERSION) != VERSION:
        raise LoomError('invalid', f'v must be "{VERSION}"')
    return request


def _make_dirs(path: Path) -> None:
    missing = []
    while not os.path.lexists(path):
        missing.append(path)
        path = path.parent
    for directory in reversed(missing):
        try:
            os.mkdir(directory, 0o700)
        except FileExistsError:
            continue
        except OSError as error:
            raise StartError(f'cannot make {directory}: {error.strerror}') from None
        os.chmod(directory, 0o700)


def _answers(path: Path) -> bool:
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(PROBE_TIMEOUT)
    try:
        probe.connect(str(path))
    except (ConnectionRefusedError, FileNotFoundError):
        return False
    except OSError as error:
        raise StartError(f'cannot check {path}: {error.strerror or error}') from None
    finally:
        probe.close()
    return True


def _clear_stale(path: Path) -> None:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(info.st_mode):
        raise StartError(f'{path} exists and is not a socket')
    if _answers(path):
        raise StartError(f'another server is already answering on {path}')
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


class Server:
    def __init__(self, path: Path, *, max_line: int = MAX_LINE, log: TextIO | None = None,
                 proc: str | Path = PROC) -> None:
        self.path = Path(path)
        self.max_line = max_line
        self.log = sys.stderr if log is None else log
        self.proc = proc
        self.service: Service | None = None
        self._sock: socket.socket | None = None
        self._identity: tuple[int, int] | None = None
        self._server: asyncio.Server | None = None
        self._connections: set[asyncio.Task] = set()
        self._closing = False

    def bind(self) -> None:
        _make_dirs(self.path.parent)
        _clear_stale(self.path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            old = os.umask(0o177)
            try:
                sock.bind(str(self.path))
            finally:
                os.umask(old)
            info = os.lstat(self.path)
            self._identity = (info.st_dev, info.st_ino)
            os.chmod(self.path, 0o600)
            sock.listen(socket.SOMAXCONN)
            sock.setblocking(False)
        except OSError as error:
            sock.close()
            self.unlink()
            raise StartError(f'cannot listen on {self.path}: {error.strerror or error}') from None
        self._sock = sock

    async def start(self, service: Service) -> None:
        self.service = service
        self._server = await asyncio.start_unix_server(
            self._connected, sock=self._sock, limit=self.max_line)

    async def close(self) -> None:
        self._closing = True
        if self._server is not None:
            self._server.close()
        elif self._sock is not None:
            self._sock.close()
        tasks = list(self._connections)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def unlink(self) -> None:
        if self._identity is None:
            return
        try:
            info = os.lstat(self.path)
            if (info.st_dev, info.st_ino) == self._identity:
                os.unlink(self.path)
        except FileNotFoundError:
            pass
        self._identity = None

    def _write_log(self, text: str) -> None:
        self.log.write(text)
        self.log.flush()

    async def _connected(self, reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter) -> None:
        if self._closing:
            writer.close()
            return
        task = asyncio.current_task()
        self._connections.add(task)
        try:
            await self._serve(reader, writer)
        except OSError:
            pass
        finally:
            self._connections.discard(task)
            writer.close()

    def _caller(self, writer: asyncio.StreamWriter) -> Caller:
        try:
            pid, uid, _ = peer_credentials(writer.get_extra_info('socket'))
        except OSError:
            raise LoomError('forbidden', 'cannot tell who the peer is') from None
        return identify(pid, uid, proc=self.proc)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            caller = self._caller(writer)
        except LoomError as error:
            writer.write(error_line(error.code, error.message))
            await writer.drain()
            return
        while True:
            try:
                line = await reader.readuntil(b'\n')
            except asyncio.IncompleteReadError:
                return
            except asyncio.LimitOverrunError:
                writer.write(error_line('invalid', f'request line is over {self.max_line} bytes'))
                await writer.drain()
                return
            answer, events = await self._answer(line[:-1], caller)
            writer.write(answer)
            await writer.drain()
            if events is not None:
                await self._follow(events, reader, writer)
                return

    async def _answer(self, line: bytes,
                      caller: Caller) -> tuple[bytes, AsyncIterator[dict] | None]:
        call = None
        try:
            request = decode(line)
            method = self.service.method(request.get('call'))
            call = request['call']
            params = request.get('params', {})
            if not isinstance(params, dict):
                raise LoomError('invalid', 'params must be an object')
            if call == FOLLOW_CALL and params.get('follow') is True:
                events = self.service.follow(params, caller)
                return encode({'ok': True, 'result': {'follow': True}}), events
            return encode({'ok': True, 'result': await method(params, caller)}), None
        except LoomError as error:
            return error_line(error.code, error.message), None
        except Exception:
            self._write_log(f'loom: internal error in {call or "a request"}\n'
                            f'{traceback.format_exc()}')
            return error_line('failed', 'internal error'), None

    async def _follow(self, events: AsyncIterator[dict], reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        pump = asyncio.ensure_future(self._pump(events, writer))
        watch = asyncio.ensure_future(self._until_eof(reader))
        try:
            await asyncio.wait({pump, watch}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            pump.cancel()
            watch.cancel()
            await asyncio.gather(pump, watch, return_exceptions=True)
            await events.aclose()
        if pump.done() and not pump.cancelled():
            error = pump.exception()
            if error is not None and not isinstance(error, OSError):
                self._write_log('loom: internal error in a follow stream\n'
                                + ''.join(traceback.format_exception(error)))

    async def _pump(self, events: AsyncIterator[dict], writer: asyncio.StreamWriter) -> None:
        async for event in events:
            writer.write(encode({'event': event}))
            await writer.drain()

    async def _until_eof(self, reader: asyncio.StreamReader) -> None:
        while await reader.read(_DISCARD):
            pass
