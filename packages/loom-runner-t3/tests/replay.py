import asyncio
import copy
import dataclasses
import json
import os
import time
from collections.abc import Callable
from pathlib import Path

from loom_contracts.runners import PermissionMode, RunnerName, ThreadSpec

HERE = Path(__file__).resolve().parent
ANSWERS = HERE / 'answers'
STREAMS = HERE / 'streams'

TOKEN = 'loom-t3-test-token-5b1e9c7d'
ORIGIN = 'http://127.0.0.1:41773'
MCP_URL = ORIGIN + '/mcp'
MCP_SESSION = '00000000-0000-4000-8003-000000000001'
SESSION_ID = '00000000-0000-4000-8002-000000000001'
CLIENT_ID = 'replay-client-id'
SCOPE = 'orchestration:read orchestration:operate settings:write source-control:write'
NOW = 1_761_400_000
DAY = 86400

THREAD_ID = '6f1b7c55-1d2e-4c1a-9a51-0d0c6e3b2a77'
T3_THREAD = 'mcp:00000000-0000-4000-8000-000000000001'
T3_THREAD_2 = 'mcp:00000000-0000-4000-8000-000000000002'
T3_THREAD_3 = 'mcp:00000000-0000-4000-8000-000000000003'
PROJECT_ID = 'mcp:00000000-0000-4000-8001-000000000001'
PROJECT_ROOT = '/tmp/loom-replay/project'
WORKTREE = '/tmp/loom-replay/worktree'

DEFAULT_MODEL = {
    'instanceId': 'claude-proxy',
    'model': 'claude-opus-5-5',
    'options': [{'id': 'effort', 'value': 'high'}, {'id': 'contextWindow', 'value': '1m'}],
}


def make_thread(**changes: object) -> ThreadSpec:
    thread = ThreadSpec(
        id=THREAD_ID,
        runner=RunnerName.T3,
        project_id='loom',
        workspace_path=PROJECT_ROOT,
        title='Replay',
        permission_mode=PermissionMode.APPROVAL_REQUIRED,
    )
    return dataclasses.replace(thread, **changes)


def recorded(name: str) -> dict:
    return json.loads((ANSWERS / f'{name}.json').read_text(encoding='utf-8'))


def stream(case: str) -> list[dict]:
    text = (STREAMS / f'{case}.jsonl').read_text(encoding='utf-8')
    return [json.loads(line) for line in text.splitlines() if line]


def credential(**changes: object) -> dict:
    data = {
        'origin': ORIGIN,
        'access_token': TOKEN,
        'expires_at': NOW + 30 * DAY,
        'session_id': SESSION_ID,
        'client_id': CLIENT_ID,
        'scope': SCOPE,
    }
    data.update(changes)
    return data


def credential_text(**changes: object) -> str:
    return json.dumps(credential(**changes))


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding='utf-8')


def make_t3_home(base: Path, origin: object = ORIGIN, model: object = DEFAULT_MODEL,
                 version: str = '0.1.2') -> Path:
    home = base / 't3-home'
    write_json(home / 'userdata' / 'server-runtime.json',
               {'origin': origin, 'serverVersion': version, 'pid': 1})
    if model is not ...:
        write_json(home / 'userdata' / 'settings.json', {'defaultModelSelection': model})
    return home


class FakeHost:
    def __init__(self, data_dir: str, t3_home: str | None = None,
                 secret: str | None = None) -> None:
        self.data_dir = data_dir
        self.settings = {'t3_home': t3_home}
        self.secrets = {'t3-client': secret}
        self.asked: list[str] = []

    def setting(self, key: str) -> str | None:
        self.asked.append(f'setting:{key}')
        return self.settings.get(key)

    def environment(self, thread: ThreadSpec) -> dict[str, str]:
        self.asked.append('environment')
        return {}

    def secret(self, name: str) -> str | None:
        self.asked.append(f'secret:{name}')
        return self.secrets.get(name)


@dataclasses.dataclass
class Reply:
    status: int
    headers: dict
    body: object
    sse: bool = False

    def render(self, request_id: object) -> tuple[int, dict, bytes]:
        body = copy.deepcopy(self.body)
        if isinstance(body, dict) and body.get('jsonrpc') == '2.0' and request_id is not None:
            body['id'] = request_id
        if isinstance(body, bytes):
            data = body
        elif isinstance(body, str):
            data = body.encode('utf-8')
        elif body is None:
            data = b''
        else:
            data = json.dumps(body).encode('utf-8')
        headers = dict(self.headers)
        if self.sse:
            data = b'event: message\ndata: ' + data + b'\n\n'
            headers['content-type'] = 'text/event-stream'
        return self.status, headers, data


def from_recording(name: str) -> Reply:
    data = recorded(name)
    return Reply(data['status'], data['headers'], data['body'])


def rpc(result: object, status: int = 200, sse: bool = False) -> Reply:
    return Reply(status, {'content-type': 'application/json'},
                 {'jsonrpc': '2.0', 'id': 0, 'result': result}, sse)


def tool(value: dict, sse: bool = False) -> Reply:
    return rpc({'content': [{'type': 'text', 'text': json.dumps(value)}],
                'structuredContent': value, 'isError': False}, sse=sse)


def tool_error(code: str, message: str) -> Reply:
    text = json.dumps({'_tag': 'OrchestratorMcpFailure', 'code': code, 'message': message})
    return rpc({'content': [{'type': 'text', 'text': text}], 'isError': True})


def rpc_error(status: int, code: int, message: str) -> Reply:
    return Reply(status, {'content-type': 'application/json'},
                 {'jsonrpc': '2.0', 'id': 0, 'error': {'code': code, 'message': message}})


def lost_session() -> Reply:
    return from_recording('no-session')


def unknown_session() -> Reply:
    return from_recording('unknown-session')


def bad_token() -> Reply:
    return from_recording('bad-token')


def projects(root: str = PROJECT_ROOT, *extra: dict) -> Reply:
    value = copy.deepcopy(recorded('project-list')['body']['result']['structuredContent'])
    value['projects'][0]['workspaceRoot'] = root
    value['projects'].extend(extra)
    return tool(value)


def launch(name: str = 'launch-idle') -> Reply:
    return from_recording(name)


@dataclasses.dataclass
class Sent:
    method: str
    url: str
    headers: dict
    body: bytes | None
    timeout: float
    payload: object
    key: str

    @property
    def arguments(self) -> dict:
        return self.payload['params']['arguments']


def _key(url: str, payload: object) -> str:
    path = url.split('?', 1)[0]
    if not path.endswith('/mcp'):
        return path[len(ORIGIN):] if path.startswith(ORIGIN) else path
    if not isinstance(payload, dict):
        return 'mcp'
    method = payload.get('method')
    if method == 'tools/call':
        return payload['params']['name']
    return method


class FakeHttp:
    def __init__(self) -> None:
        self.calls: list[Sent] = []
        self.routes: dict[str, list[object]] = {}
        self.on('initialize', from_recording('initialize'))
        self.on('notifications/initialized', from_recording('initialized'))

    def on(self, key: str, *replies: object) -> None:
        self.routes[key] = list(replies)

    def keys(self) -> list[str]:
        return [call.key for call in self.calls]

    def count(self, key: str) -> int:
        return self.keys().count(key)

    def sent(self, key: str) -> list[Sent]:
        return [call for call in self.calls if call.key == key]

    def __call__(self, method: str, url: str, headers: dict, body: bytes | None,
                 timeout: float) -> tuple[int, dict, bytes]:
        payload = None
        if body is not None and url.split('?', 1)[0].endswith('/mcp'):
            payload = json.loads(body)
        key = _key(url, payload)
        self.calls.append(Sent(method, url, dict(headers), body, timeout, payload, key))
        queue = self.routes.get(key)
        if not queue:
            raise AssertionError(f'no reply for {key}')
        reply = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(reply, BaseException):
            raise reply
        if callable(reply) and not isinstance(reply, Reply):
            reply = reply(self.calls[-1])
        request_id = payload.get('id') if isinstance(payload, dict) else None
        return reply.render(request_id)


def reads(case: str) -> list[Reply]:
    return [tool(line) for line in stream(case)]


class FakeGit:
    def __init__(self, top: str = PROJECT_ROOT, main: str = PROJECT_ROOT,
                 branch: str | None = None) -> None:
        self.top = top
        self.main = main
        self.branch = branch
        self.calls: list[list[str]] = []

    async def __call__(self, args: list[str]) -> tuple[int, str]:
        self.calls.append(list(args))
        if 'rev-parse' in args:
            return 0, f'{self.top}\n{self.main}/.git\n'
        if self.branch is None:
            return 128, ''
        return 0, self.branch + '\n'


class FakeSleep:
    def __init__(self, limit: int = 200) -> None:
        self.calls: list[float] = []
        self.limit = limit
        self.free = False
        self._gate = asyncio.Event()

    def open(self) -> None:
        self.free = True
        self._gate.set()

    def step(self) -> None:
        self._gate.set()

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        if len(self.calls) > self.limit:
            await asyncio.Event().wait()
        await self._gate.wait()
        if not self.free:
            self._gate.clear()
        await asyncio.sleep(0)


class FakeClock:
    def __init__(self, now: float = NOW) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


async def until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError('timed out waiting')
        await asyncio.sleep(0.002)


def mode_of(path: str | os.PathLike) -> int:
    return os.lstat(path).st_mode & 0o7777
