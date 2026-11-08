import json
import socketserver
import tempfile
import threading
import unittest
from collections.abc import Callable, Iterable
from pathlib import Path

AT = '2026-10-24T12:00:00.000+00:00'
CLOSE = object()
POLL = 0.01

Reply = Callable[[object], Iterable[object]]


def line(value: object) -> bytes:
    return json.dumps(value).encode('utf-8') + b'\n'


def ok(result: object) -> list[bytes]:
    return [line({'ok': True, 'result': result})]


def failed(code: str, message: str) -> list[bytes]:
    return [line({'ok': False, 'error': {'code': code, 'message': message}})]


def event(event_id: int, kind: str = 'turn.text', thread_id: str | None = 't1',
          **data: object) -> dict:
    return {'id': event_id, 'type': kind, 'at': AT, 'thread_id': thread_id, 'data': data}


def stream(*events: dict, close: bool = True) -> list[object]:
    chunks: list[object] = [line({'ok': True, 'result': {'follow': True}})]
    chunks += [line({'event': item}) for item in events]
    if close:
        chunks.append(CLOSE)
    return chunks


def thread_info(thread_id: str = 't1', **values: object) -> dict:
    return {'id': thread_id, 'title': 'First', 'runner': 'claude', 'project_id': 'demo',
            'workspace_path': '/tmp/demo', 'branch': None, 'model': None, 'state': 'idle',
            'waiting': False, 'reason': None, 'parent_id': None, 'origin': None,
            'created_at': AT, **values}


def project_info(project_id: str = 'demo', **values: object) -> dict:
    return {'id': project_id, 'title': 'Demo', 'workspace_root': '/tmp/demo',
            'permission_mode': None, 'created_at': AT, **values}


def schedule_info(schedule_id: str = 's1', **values: object) -> dict:
    return {'id': schedule_id, 'trigger': {'type': 'at', 'at': '2026-10-25T09:00'},
            'action': 'notify', 'text': 'Stand-up', 'thread_id': None, 'project_id': None,
            'title': None, 'runner': None, 'model': None, 'workspace': None, 'check': None,
            'by': None,
            'paused': False, 'next_run': AT, 'created_at': AT,
            **values}


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        fake: FakeServer = self.server.fake
        fake.connections += 1
        for raw in self.rfile:
            try:
                request = json.loads(raw)
            except ValueError:
                request = raw
            fake.requests.append(request)
            for chunk in fake.reply(request):
                if chunk is CLOSE:
                    return
                self.wfile.write(chunk)
                self.wfile.flush()


class _UnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


class FakeServer:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.requests: list[object] = []
        self.connections = 0
        self.reply: Reply = lambda request: ok(None)
        self._server = _UnixServer(str(path), _Handler)
        self._server.fake = self
        self._thread = threading.Thread(target=self._server.serve_forever, args=(POLL,),
                                        daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    @property
    def calls(self) -> list[tuple[str, dict]]:
        return [(request['call'], request['params']) for request in self.requests]


class FakeServerCase(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name).resolve()
        self.socket = self.base / 'api.sock'
        self.server = FakeServer(self.socket)
        self.addCleanup(self.server.close)

    def answer(self, chunks: Iterable[object]) -> None:
        chunks = list(chunks)
        self.server.reply = lambda request: chunks

    def answer_with(self, reply: Reply) -> None:
        self.server.reply = reply
