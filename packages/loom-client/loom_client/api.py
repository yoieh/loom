import json
import os
import socket
import uuid
from collections.abc import Iterator
from typing import BinaryIO

from loom_contracts import API_VERSION
from loom_contracts.calls import CallName, ProjectInfo, ScheduleInfo, ThreadInfo
from loom_contracts.events import Event

from .errors import LoomApiError, LoomConnectionError, LoomProtocolError

SOCKET_NAME = 'api.sock'
DEFAULT_TIMEOUT = 30.0


def default_socket() -> str:
    path = os.environ.get('LOOM_SOCKET')
    if path:
        return path
    runtime = os.environ.get('XDG_RUNTIME_DIR')
    if runtime:
        return os.path.join(runtime, 'loom', SOCKET_NAME)
    raise LoomConnectionError(
        'no socket path: LOOM_SOCKET and XDG_RUNTIME_DIR are not set, so give a socket path')


def _reason(error: OSError) -> str:
    return error.strerror or str(error) or type(error).__name__


def _params(**values: object) -> dict:
    return {key: value for key, value in values.items() if value is not None}


def _command_id(value: str | None) -> str:
    return str(uuid.uuid4()) if value is None else value


def _json_line(line: bytes, what: str) -> object:
    if not line.endswith(b'\n'):
        raise LoomProtocolError(f'the connection closed before a full {what} line')
    try:
        return json.loads(line.decode('utf-8'))
    except (ValueError, RecursionError):
        raise LoomProtocolError(f'the {what} is not JSON') from None


def parse_answer(line: bytes) -> object:
    answer = _json_line(line, 'answer')
    if not isinstance(answer, dict):
        raise LoomProtocolError('the answer is not an object')
    ok = answer.get('ok')
    if not isinstance(ok, bool):
        raise LoomProtocolError('the answer has no ok of true or false')
    if ok:
        if 'result' not in answer:
            raise LoomProtocolError('the answer has no result')
        return answer['result']
    error = answer.get('error')
    if not isinstance(error, dict):
        raise LoomProtocolError('the error answer has no error object')
    code = error.get('code')
    message = error.get('message')
    if not isinstance(code, str) or not isinstance(message, str):
        raise LoomProtocolError('the error answer has no code and message strings')
    raise LoomApiError(code, message)


def check_event(value: object) -> Event:
    if not isinstance(value, dict):
        raise LoomProtocolError('an event is not an object')
    event_id = value.get('id')
    if isinstance(event_id, bool) or not isinstance(event_id, int):
        raise LoomProtocolError('an event has no integer id')
    return value


def _stream_event(line: bytes) -> Event:
    value = _json_line(line, 'event')
    if not isinstance(value, dict):
        raise LoomProtocolError('an event line is not an object')
    return check_event(value.get('event'))


def _is_follow_ack(result: object) -> bool:
    return type(result) is dict and len(result) == 1 and result.get('follow') is True


class Client:
    def __init__(self, socket_path: str | None = None,
                 timeout: float | None = DEFAULT_TIMEOUT) -> None:
        self.socket_path = default_socket() if socket_path is None else os.fspath(socket_path)
        self.timeout = timeout

    def _connect(self) -> socket.socket:
        path = self.socket_path
        if not path:
            raise LoomConnectionError('the socket path is empty')
        if '\x00' in path:
            raise LoomConnectionError('the socket path holds a NUL character')
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect(path)
        except OSError as error:
            sock.close()
            raise LoomConnectionError(f'cannot reach Loom at {path}: {_reason(error)}') from None
        return sock

    def _lost(self, error: OSError) -> LoomConnectionError:
        return LoomConnectionError(
            f'lost the connection to Loom at {self.socket_path}: {_reason(error)}')

    def _send(self, sock: socket.socket, file: BinaryIO, name: str, params: dict) -> bytes:
        request = {'v': API_VERSION, 'call': str(name), 'params': params}
        refused = None
        try:
            sock.sendall(json.dumps(request).encode('utf-8') + b'\n')
        except OSError as error:
            refused = error
        line = self._readline(file)
        if not line and refused is not None:
            raise self._lost(refused)
        return line

    def _readline(self, file: BinaryIO) -> bytes:
        try:
            return file.readline()
        except OSError as error:
            raise self._lost(error) from None

    def call(self, name: str, params: dict) -> object:
        with self._connect() as sock, sock.makefile('rb') as file:
            try:
                return parse_answer(self._send(sock, file, name, params))
            except LoomConnectionError as error:
                command_id = params.get('id')
                if isinstance(command_id, str):
                    error.command_id = command_id
                raise

    def follow_events(self, after: int = 0, thread_id: str | None = None, *,
                      audience: str | None = None) -> Iterator[Event]:
        params = _params(after=after, thread_id=thread_id, audience=audience, follow=True)
        last = after
        with self._connect() as sock, sock.makefile('rb') as file:
            try:
                ack = parse_answer(self._send(sock, file, CallName.THREADS_EVENTS, params))
                if not _is_follow_ack(ack):
                    raise LoomProtocolError('the follow answer is not {"follow": true}')
                sock.settimeout(None)
                while True:
                    line = self._readline(file)
                    if not line:
                        raise LoomConnectionError(
                            f'lost the connection to Loom at {self.socket_path} '
                            f'after event {last}')
                    event = _stream_event(line)
                    last = event['id']
                    yield event
            except LoomConnectionError as error:
                error.last_event_id = last
                raise

    def threads_launch(self, project_id: str, title: str, message: str, *,
                       workspace: dict | None = None, runner: str | None = None,
                       model: str | None = None, requests: str | None = None,
                       origin: str | None = None, audience: str | None = None,
                       concurrency_key: str | None = None, on_busy: str | None = None,
                       settle: str | None = None, cause: int | None = None,
                       id: str | None = None) -> ThreadInfo:
        return self.call(CallName.THREADS_LAUNCH, _params(
            id=_command_id(id), project_id=project_id, title=title, message=message,
            workspace=workspace, runner=runner, model=model, requests=requests, origin=origin,
            audience=audience, concurrency_key=concurrency_key, on_busy=on_busy, settle=settle,
            cause=cause))

    def threads_delegate(self, parent_id: str, task: str, *, workspace: dict | None = None,
                         runner: str | None = None, model: str | None = None,
                         concurrency_key: str | None = None, on_busy: str | None = None,
                         id: str | None = None) -> ThreadInfo:
        return self.call(CallName.THREADS_DELEGATE, _params(
            id=_command_id(id), parent_id=parent_id, task=task, workspace=workspace,
            runner=runner, model=model, concurrency_key=concurrency_key, on_busy=on_busy))

    def threads_send(self, thread_id: str, message: str, *, mode: str | None = None,
                     cause: int | None = None, id: str | None = None) -> ThreadInfo:
        return self.call(CallName.THREADS_SEND, _params(
            id=_command_id(id), thread_id=thread_id, message=message, mode=mode, cause=cause))

    def threads_steer(self, thread_id: str, message_id: str, *,
                      id: str | None = None) -> ThreadInfo:
        return self.call(CallName.THREADS_STEER, _params(
            id=_command_id(id), thread_id=thread_id, message_id=message_id))

    def threads_list(self, project_id: str | None = None, *,
                     audience: str | None = None) -> list[ThreadInfo]:
        return self.call(CallName.THREADS_LIST, _params(project_id=project_id, audience=audience))

    def threads_get(self, thread_id: str) -> ThreadInfo:
        return self.call(CallName.THREADS_GET, _params(thread_id=thread_id))

    def threads_stop(self, thread_id: str, *, id: str | None = None) -> ThreadInfo:
        return self.call(CallName.THREADS_STOP, _params(id=_command_id(id), thread_id=thread_id))

    def threads_settle(self, thread_id: str, *, id: str | None = None) -> ThreadInfo:
        return self.call(CallName.THREADS_SETTLE,
                         _params(id=_command_id(id), thread_id=thread_id))

    def threads_update(self, thread_id: str, audience: str, *,
                       id: str | None = None) -> ThreadInfo:
        return self.call(CallName.THREADS_UPDATE,
                         _params(id=_command_id(id), thread_id=thread_id, audience=audience))

    def threads_events(self, after: int = 0, thread_id: str | None = None, *,
                       audience: str | None = None) -> list[Event]:
        page = self.call(CallName.THREADS_EVENTS,
                         _params(after=after, thread_id=thread_id, audience=audience))
        if not isinstance(page, list):
            raise LoomProtocolError('the events answer is not a list')
        for event in page:
            check_event(event)
        return page

    def requests_respond(self, request_id: str, decision: str, *, message: str | None = None,
                         id: str | None = None) -> object:
        return self.call(CallName.REQUESTS_RESPOND, _params(
            id=_command_id(id), request_id=request_id, decision=decision, message=message))

    def signals_emit(self, topic: str, payload: dict, *, project_id: str | None = None,
                     key: str | None = None, cause: int | None = None,
                     id: str | None = None) -> Event:
        return self.call(CallName.SIGNALS_EMIT, _params(
            id=_command_id(id), topic=topic, payload=payload, project_id=project_id, key=key,
            cause=cause))

    def schedules_add(self, trigger: dict, text: str, *, action: str | None = None,
                      thread_id: str | None = None, project_id: str | None = None,
                      title: str | None = None, concurrency_key: str | None = None,
                      on_busy: str | None = None, settle: str | None = None,
                      runner: str | None = None, model: str | None = None,
                      workspace: dict | None = None, check: list[str] | None = None,
                      id: str | None = None) -> ScheduleInfo:
        return self.call(CallName.SCHEDULES_ADD, _params(
            id=_command_id(id), trigger=trigger, text=text, action=action, thread_id=thread_id,
            project_id=project_id, title=title, concurrency_key=concurrency_key,
            on_busy=on_busy, settle=settle, runner=runner, model=model, workspace=workspace,
            check=check))

    def schedules_list(self, thread_id: str | None = None,
                       project_id: str | None = None) -> list[ScheduleInfo]:
        return self.call(CallName.SCHEDULES_LIST,
                         _params(thread_id=thread_id, project_id=project_id))

    def schedules_pause(self, schedule_id: str, *, id: str | None = None) -> ScheduleInfo:
        return self.call(CallName.SCHEDULES_PAUSE,
                         _params(id=_command_id(id), schedule_id=schedule_id))

    def schedules_remove(self, schedule_id: str, *, id: str | None = None) -> ScheduleInfo:
        return self.call(CallName.SCHEDULES_REMOVE,
                         _params(id=_command_id(id), schedule_id=schedule_id))

    def projects_add(self, title: str, path: str, *, permission_mode: str | None = None,
                     id: str | None = None) -> ProjectInfo:
        return self.call(CallName.PROJECTS_ADD, _params(
            id=_command_id(id), title=title, path=path, permission_mode=permission_mode))

    def projects_set_mode(self, project_id: str, permission_mode: str, *,
                          id: str | None = None) -> ProjectInfo:
        return self.call(CallName.PROJECTS_SET_MODE, _params(
            id=_command_id(id), project_id=project_id, permission_mode=permission_mode))

    def projects_remove(self, project_id: str, *, id: str | None = None) -> ProjectInfo:
        return self.call(CallName.PROJECTS_REMOVE,
                         _params(id=_command_id(id), project_id=project_id))

    def projects_list(self) -> list[ProjectInfo]:
        return self.call(CallName.PROJECTS_LIST, {})
