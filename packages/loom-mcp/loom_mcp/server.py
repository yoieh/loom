import json
import os
import sys
import traceback
from collections.abc import Callable, Iterator
from typing import BinaryIO, TextIO

from .identity import Identity
from .tools import BY_NAME, TOOLS, ToolError, Tools

MAX_LINE = 1_048_576
SKIP_CHUNK = 65_536

PROTOCOL_VERSIONS = ('2024-11-05', '2025-03-26', '2025-06-18', '2025-11-25')
LATEST_VERSION = '2025-11-25'
SERVER_INFO = {'name': 'loom', 'version': '0.1.0'}

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

OVER_CAP = None


class RpcError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def read_lines(stream: BinaryIO) -> Iterator[bytes | None]:
    while True:
        line = stream.readline(MAX_LINE + 1)
        if not line:
            return
        if line.endswith(b'\n') or len(line) <= MAX_LINE:
            yield line
            continue
        yield OVER_CAP
        while True:
            rest = stream.readline(SKIP_CHUNK)
            if not rest or rest.endswith(b'\n'):
                break


def _error(request_id: object, code: int, message: str) -> dict:
    return {'jsonrpc': '2.0', 'id': request_id, 'error': {'code': code, 'message': message}}


def _compact(value: object) -> str:
    return json.dumps(value, separators=(',', ':'))


def _tool_result(text: str, is_error: bool) -> dict:
    return {'content': [{'type': 'text', 'text': text}], 'isError': is_error}


class Server:
    def __init__(self, tools: Tools, output: BinaryIO, errors: TextIO) -> None:
        self._tools = tools
        self._output = output
        self._errors = errors
        self._methods: dict[str, Callable[[dict], object]] = {
            'initialize': self._initialize,
            'ping': lambda params: {},
            'tools/list': lambda params: {'tools': [tool.listing() for tool in TOOLS]},
            'tools/call': self._tools_call,
        }

    def run(self, stream: BinaryIO) -> int:
        for line in read_lines(stream):
            reply = self.handle_line(line)
            if reply is not None:
                self._output.write(_compact(reply).encode('utf-8') + b'\n')
                self._output.flush()
        return 0

    def handle_line(self, line: bytes | None) -> dict | None:
        if line is OVER_CAP:
            return _error(None, INVALID_REQUEST, f'request line is over {MAX_LINE} bytes')
        try:
            message = json.loads(line)
        except (ValueError, RecursionError):
            return _error(None, PARSE_ERROR, 'request is not JSON')
        if not isinstance(message, dict):
            return _error(None, INVALID_REQUEST, 'request must be an object')
        if 'id' not in message:
            return None
        request_id = message['id']
        try:
            result = self._dispatch(message)
        except RpcError as error:
            return _error(request_id, error.code, error.message)
        except Exception:
            traceback.print_exc(file=self._errors)
            self._errors.flush()
            return _error(request_id, INTERNAL_ERROR, 'internal error')
        return {'jsonrpc': '2.0', 'id': request_id, 'result': result}

    def _dispatch(self, message: dict) -> object:
        method = message.get('method')
        if not isinstance(method, str):
            raise RpcError(INVALID_REQUEST, 'method must be a string')
        handler = self._methods.get(method)
        if handler is None:
            raise RpcError(METHOD_NOT_FOUND, f'unknown method: {method}')
        params = message.get('params')
        if params is None:
            params = {}
        elif not isinstance(params, dict):
            raise RpcError(INVALID_PARAMS, 'params must be an object')
        return handler(params)

    def _initialize(self, params: dict) -> dict:
        asked = params.get('protocolVersion')
        known = isinstance(asked, str) and asked in PROTOCOL_VERSIONS
        return {'protocolVersion': asked if known else LATEST_VERSION,'capabilities': {'tools': {}},
                'serverInfo': SERVER_INFO}

    def _tools_call(self, params: dict) -> dict:
        name = params.get('name')
        if not isinstance(name, str):
            raise RpcError(INVALID_PARAMS, 'name must be a string')
        if name not in BY_NAME:
            raise RpcError(INVALID_PARAMS, f'unknown tool: {name}')
        arguments = params.get('arguments')
        if arguments is None:
            arguments = {}
        elif not isinstance(arguments, dict):
            raise RpcError(INVALID_PARAMS, 'arguments must be an object')
        try:
            result = self._tools.call(name, arguments)
        except ToolError as error:
            return _tool_result(str(error), True)
        return _tool_result(_compact(result), False)


def main() -> int:
    output = sys.stdout.buffer
    sys.stdout = sys.stderr
    server = Server(Tools(Identity(os.environ)), output, sys.stderr)
    return server.run(sys.stdin.buffer)
