import http.client
import itertools
import json
import os
import threading
import urllib.parse
from collections.abc import Callable

from .credential import Credential
from .reasons import NotPaired, NotRunning, T3Error, failure

PROTOCOL = '2025-06-18'
TIMEOUT = 10.0
MAX_BODY = 16_777_216
MAX_RUNTIME = 1_048_576
CLIENT_INFO = {'name': 'Loom', 'version': '0.1.0'}

HttpCall = Callable[
    [str, str, dict[str, str], bytes | None, float],
    tuple[int, dict[str, str], bytes],
]


def http_call(method: str, url: str, headers: dict[str, str], body: bytes | None,
              timeout: float) -> tuple[int, dict[str, str], bytes]:
    parts = urllib.parse.urlsplit(url)
    if parts.scheme != 'http' or not parts.hostname:
        raise ValueError('only http URLs are allowed')
    path = parts.path or '/'
    if parts.query:
        path += '?' + parts.query
    connection = http.client.HTTPConnection(parts.hostname, parts.port, timeout=timeout)
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        data = response.read(MAX_BODY + 1)
        answer = {key.lower(): value for key, value in response.getheaders()}
        return response.status, answer, data
    finally:
        connection.close()


def runtime_path(t3_home: str) -> str:
    return os.path.join(t3_home, 'userdata', 'server-runtime.json')


def read_runtime(t3_home: str) -> dict | None:
    try:
        with open(runtime_path(t3_home), 'rb') as file:
            data = file.read(MAX_RUNTIME + 1)
    except OSError:
        return None
    if len(data) > MAX_RUNTIME:
        return None
    try:
        value = json.loads(data)
    except (ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def check_origin(t3_home: str, origin: str) -> None:
    runtime = read_runtime(t3_home)
    found = runtime.get('origin') if runtime is not None else None
    if not isinstance(found, str) or found != origin:
        raise NotRunning()


def _content_type(headers: dict[str, str]) -> str:
    for key, value in headers.items():
        if isinstance(key, str) and key.lower() == 'content-type' and isinstance(value, str):
            return value.split(';', 1)[0].strip().lower()
    return ''


def _header(headers: dict[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if isinstance(key, str) and key.lower() == name:
            return value if isinstance(value, str) else None
    return None


def _json(data: bytes) -> object:
    try:
        return json.loads(data.decode('utf-8'))
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise failure('protocol', 'the answer is not JSON') from None


def _sse(data: bytes) -> list[object]:
    try:
        text = data.decode('utf-8')
    except UnicodeDecodeError:
        raise failure('protocol', 'the answer is not UTF-8') from None
    messages = []
    for block in text.replace('\r\n', '\n').split('\n\n'):
        lines = [line[5:].removeprefix(' ') for line in block.split('\n')
                 if line.startswith('data:')]
        if lines:
            messages.append(_json('\n'.join(lines).encode('utf-8')))
    return messages


def decode_body(headers: dict[str, str], data: bytes, request_id: int) -> object:
    kind = _content_type(headers)
    if kind == 'application/json':
        return _json(data)
    if kind == 'text/event-stream':
        for message in _sse(data):
            if isinstance(message, dict) and message.get('id') == request_id:
                return message
        raise failure('protocol', 'the event stream holds no answer to the request')
    raise failure('protocol', f'unexpected content type {kind or "none"!r}')


def _rpc_error(body: object) -> tuple[object, object] | None:
    if not isinstance(body, dict) or not isinstance(body.get('error'), dict):
        return None
    error = body['error']
    return error.get('code'), error.get('message')


def tool_failure(result: dict, secrets: tuple[str, ...]) -> T3Error:
    content = result.get('content')
    text = None
    if isinstance(content, list) and content and isinstance(content[0], dict):
        text = content[0].get('text')
    if not isinstance(text, str):
        return failure('error', 'T3 gave no error text', secrets)
    try:
        data = json.loads(text)
    except (ValueError, RecursionError):
        data = None
    if isinstance(data, dict):
        code, message = data.get('code'), data.get('message')
        if isinstance(code, str) and code and isinstance(message, str):
            return failure(code, message, secrets)
    return failure('error', text, secrets)


def tool_value(result: dict) -> dict:
    structured = result.get('structuredContent')
    if isinstance(structured, dict):
        return structured
    content = result.get('content')
    if isinstance(content, list) and content and isinstance(content[0], dict):
        text = content[0].get('text')
        if isinstance(text, str):
            try:
                value = json.loads(text)
            except (ValueError, RecursionError):
                value = None
            if isinstance(value, dict):
                return value
    raise failure('protocol', 'the tool answer holds no JSON object')


class McpClient:
    def __init__(self, credential: Credential, t3_home: str, http: HttpCall = http_call,
                 timeout: float = TIMEOUT) -> None:
        self._credential = credential
        self._t3_home = t3_home
        self._http = http
        self._timeout = timeout
        self._url = credential.origin + '/mcp'
        self._session_id: str | None = None
        self._ids = itertools.count(1)
        self._lock = threading.Lock()
        self.secrets = (credential.access_token,)

    def call_tool(self, name: str, arguments: dict[str, object]) -> dict:
        result = self.request('tools/call', {'name': name, 'arguments': arguments})
        if result.get('isError') is True:
            raise tool_failure(result, self.secrets)
        return tool_value(result)

    def request(self, method: str, params: dict[str, object]) -> dict:
        with self._lock:
            if self._session_id is None:
                self._initialize()
            payload = {'jsonrpc': '2.0', 'id': next(self._ids), 'method': method,
                       'params': params}
            status, headers, data = self._post(payload)
            if self._lost(status, headers, data):
                self._session_id = None
                self._initialize()
                status, headers, data = self._post(payload)
                if self._lost(status, headers, data):
                    raise failure('protocol', 'T3 dropped the MCP session twice')
            return self._result(status, headers, data, payload['id'])

    def _initialize(self) -> None:
        payload = {'jsonrpc': '2.0', 'id': next(self._ids), 'method': 'initialize',
                   'params': {'protocolVersion': PROTOCOL, 'capabilities': {},
                              'clientInfo': CLIENT_INFO}}
        status, headers, data = self._post(payload)
        self._result(status, headers, data, payload['id'])
        session_id = _header(headers, 'mcp-session-id')
        if not session_id or any(ord(char) < 0x21 or ord(char) > 0x7e for char in session_id):
            raise failure('protocol', 'initialize gave no Mcp-Session-Id')
        self._session_id = session_id
        status, headers, data = self._post({'jsonrpc': '2.0',
                                            'method': 'notifications/initialized'})
        if status == 401:
            raise NotPaired()
        if not 200 <= status < 300:
            self._session_id = None
            raise failure('protocol', f'notifications/initialized answered {status}')

    def _headers(self) -> dict[str, str]:
        headers = {
            'Accept': 'application/json, text/event-stream',
            'Content-Type': 'application/json',
            'MCP-Protocol-Version': PROTOCOL,
            'Authorization': f'Bearer {self._credential.access_token}',
        }
        if self._session_id is not None:
            headers['Mcp-Session-Id'] = self._session_id
        return headers

    def _post(self, payload: dict[str, object]) -> tuple[int, dict[str, str], bytes]:
        check_origin(self._t3_home, self._credential.origin)
        body = json.dumps(payload).encode('utf-8')
        try:
            status, headers, data = self._http('POST', self._url, self._headers(), body,
                                               self._timeout)
        except TimeoutError:
            raise failure('timeout', f'T3 did not answer within {self._timeout:g} s') from None
        except (OSError, http.client.HTTPException, ValueError) as error:
            raise failure('unreachable', type(error).__name__) from None
        if not isinstance(headers, dict):
            headers = {}
        if not isinstance(data, bytes):
            data = b''
        if len(data) > MAX_BODY:
            raise failure('protocol', 'the answer is too long')
        return status, headers, data

    def _lost(self, status: int, headers: dict[str, str], data: bytes) -> bool:
        if status == 404:
            return True
        if status != 400:
            return False
        try:
            body = json.loads(data.decode('utf-8'))
        except (UnicodeDecodeError, ValueError, RecursionError):
            return False
        error = _rpc_error(body)
        return error is not None and error[0] == -32600

    def _result(self, status: int, headers: dict[str, str], data: bytes,
                request_id: int) -> dict:
        if status == 401:
            raise NotPaired()
        body = decode_body(headers, data, request_id) if data else None
        error = _rpc_error(body)
        if error is not None:
            code, message = error
            message = message if isinstance(message, str) else 'no message'
            raise failure(str(code) if isinstance(code, int) else 'rpc', message, self.secrets)
        if not 200 <= status < 300:
            raise failure('http', f'status {status}', self.secrets)
        if not isinstance(body, dict) or body.get('id') != request_id:
            raise failure('protocol', 'the answer is not a JSON-RPC answer to the request')
        result = body.get('result')
        if not isinstance(result, dict):
            raise failure('protocol', 'the answer has no result object')
        return result
