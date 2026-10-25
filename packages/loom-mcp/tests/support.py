import json
import os
import queue
import socketserver
import subprocess
import sys
import tempfile
import threading
import unittest
from collections.abc import Callable, Iterable
from pathlib import Path

AT = '2026-10-25T12:00:00.000+00:00'
CLOSE = object()
POLL = 0.01
TIMEOUT = 10.0

Reply = Callable[[dict], Iterable[object]]


def line(value: object) -> bytes:
    return json.dumps(value).encode('utf-8') + b'\n'


def ok(result: object) -> list[object]:
    return [line({'ok': True, 'result': result})]


def failed(code: str, message: str) -> list[object]:
    return [line({'ok': False, 'error': {'code': code, 'message': message}})]


def dropped() -> list[object]:
    return [CLOSE]


def thread_info(thread_id: str = 't1', **values: object) -> dict:
    return {'id': thread_id, 'title': 'First', 'runner': 'claude', 'project_id': 'demo',
            'workspace_path': '/tmp/demo', 'branch': None, 'model': None,
            'permission_mode': 'auto', 'state': 'queued', 'waiting': False, 'reason': None,
            'parent_id': None, 'origin': None, 'audience': 'agent', 'created_at': AT,
            **values}


def schedule_info(schedule_id: str = 'sc1', **values: object) -> dict:
    return {'id': schedule_id, 'trigger': {'type': 'at', 'at': '2026-10-26T09:00'},
            'action': 'notify', 'text': 'Stand-up', 'thread_id': None, 'project_id': None,
            'title': None, 'by': None, 'paused': False, 'next_run': AT, 'created_at': AT,
            **values}


def signal_event(event_id: int = 1, **data: object) -> dict:
    return {'id': event_id, 'type': 'signal', 'at': AT, 'thread_id': None,
            'data': {'topic': 'demo.done', 'payload': {'to': 'done'}, 'hops': 0, **data}}


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        fake: FakeLoom = self.server.fake
        for raw in self.rfile:
            request = json.loads(raw)
            fake.requests.append(request)
            for chunk in fake.reply(request):
                if chunk is CLOSE:
                    return
                self.wfile.write(chunk)
                self.wfile.flush()


class _UnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


class FakeLoom:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.requests: list[dict] = []
        self._replies: dict[str, Reply] = {}
        self._server = _UnixServer(str(path), _Handler)
        self._server.fake = self
        self._thread = threading.Thread(target=self._server.serve_forever, args=(POLL,),
                                        daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def reply(self, request: dict) -> Iterable[object]:
        call = request.get('call')
        reply = self._replies.get(call)
        if reply is None:
            return failed('invalid', f'the fake has no answer for {call}')
        return reply(request['params'])

    def on(self, call: str, *answers: list[object]) -> None:
        remaining = list(answers)

        def reply(params: dict) -> list[object]:
            return remaining.pop(0) if len(remaining) > 1 else remaining[0]

        self._replies[call] = reply

    def on_get(self, threads: dict[str, dict | list[object]]) -> None:
        def reply(params: dict) -> list[object]:
            thread_id = params.get('thread_id')
            found = threads.get(thread_id)
            if found is None:
                return failed('not_found', f"no thread with id '{thread_id}'")
            return found if isinstance(found, list) else ok(found)

        self._replies['threads.get'] = reply

    @property
    def calls(self) -> list[tuple[str, dict]]:
        return [(request['call'], request['params']) for request in self.requests]

    def names(self) -> list[str]:
        return [request['call'] for request in self.requests]


class Mcp:
    def __init__(self, env: dict[str, str], cwd: Path) -> None:
        self.process = subprocess.Popen(
            [sys.executable, '-m', 'loom_mcp'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env, cwd=cwd)
        self.lines: queue.Queue[bytes | None] = queue.Queue()
        self.stderr = b''
        self._next_id = 0
        self._readers = [threading.Thread(target=self._read_stdout, daemon=True),
                         threading.Thread(target=self._read_stderr, daemon=True)]
        for reader in self._readers:
            reader.start()

    def _read_stdout(self) -> None:
        for raw in self.process.stdout:
            self.lines.put(raw)
        self.lines.put(None)

    def _read_stderr(self) -> None:
        self.stderr = self.process.stderr.read()

    def kill(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait()
        for reader in self._readers:
            reader.join(TIMEOUT)
        for pipe in (self.process.stdin, self.process.stdout, self.process.stderr):
            try:
                pipe.close()
            except OSError:
                pass

    def send_raw(self, data: bytes) -> None:
        self.process.stdin.write(data)
        self.process.stdin.flush()

    def send(self, message: object) -> None:
        self.send_raw(line(message))

    def read_line(self) -> bytes:
        try:
            raw = self.lines.get(timeout=TIMEOUT)
        except queue.Empty:
            raise AssertionError(f'no answer within {TIMEOUT} s') from None
        if raw is None:
            raise AssertionError(f'loom-mcp closed its stdout: {self.stderr!r}')
        return raw

    def read(self) -> dict:
        return json.loads(self.read_line())

    def request(self, method: str, params: object = None, **extra: object) -> dict:
        self._next_id += 1
        message = {'jsonrpc': '2.0', 'id': self._next_id, 'method': method, **extra}
        if params is not None:
            message['params'] = params
        self.send(message)
        answer = self.read()
        if answer.get('id') != self._next_id:
            raise AssertionError(f'answer to another request: {answer}')
        return answer

    def tool(self, name: str, arguments: object = None) -> tuple[str, bool]:
        params = {'name': name}
        if arguments is not None:
            params['arguments'] = arguments
        answer = self.request('tools/call', params)
        if 'result' not in answer:
            raise AssertionError(f'not a tool result: {answer}')
        result = answer['result']
        return result['content'][0]['text'], result['isError']

    def ok(self, name: str, arguments: object = None) -> object:
        text, is_error = self.tool(name, arguments)
        if is_error:
            raise AssertionError(f'{name} failed: {text}')
        return json.loads(text)

    def close_stdin(self) -> int:
        self.process.stdin.close()
        return self.process.wait(TIMEOUT)

    def rest(self) -> list[bytes]:
        found = []
        while (raw := self.lines.get(timeout=TIMEOUT)) is not None:
            found.append(raw)
        return found


class McpCase(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name).resolve()
        self.loom = FakeLoom(self.base / 'api.sock')
        self.addCleanup(self.loom.close)

    def start(self, **extra: str) -> Mcp:
        env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'HOME': str(self.base),
               'LOOM_SOCKET': str(self.loom.path), **extra}
        mcp = Mcp(env, self.base)
        self.addCleanup(mcp.kill)
        return mcp
