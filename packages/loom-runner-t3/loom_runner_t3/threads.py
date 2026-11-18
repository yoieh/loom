import asyncio
import time
from collections.abc import Callable

from loom_contracts.runners import DispatchMode, RunnerHost

from .credential import Credential, usable_credential
from .mcp import TIMEOUT, HttpCall, McpClient, check_origin, http_call
from .reasons import NotPaired
from .session import SECRET, SEND_MODES, Call, find_project, t3_home_of


def _given(**arguments: object) -> dict[str, object]:
    return {name: value for name, value in arguments.items() if value is not None}


class T3Threads:
    def __init__(self, host: RunnerHost, *, http: HttpCall = http_call,
                 clock: Callable[[], float] = time.time, timeout: float = TIMEOUT) -> None:
        self._host = host
        self._http = http
        self._clock = clock
        self._timeout = timeout
        self._paired: tuple[Credential, str] | None = None
        self._client: McpClient | None = None

    def _connect(self) -> Call:
        credential = usable_credential(self._host.secret(SECRET), self._clock())
        if credential is None:
            raise NotPaired()
        t3_home = t3_home_of(self._host)
        check_origin(t3_home, credential.origin)
        if self._client is None or self._paired != (credential, t3_home):
            self._client = McpClient(credential, t3_home, self._http, self._timeout)
            self._paired = (credential, t3_home)
        client = self._client

        async def call(name: str, arguments: dict[str, object]) -> dict:
            return await asyncio.to_thread(client.call_tool, name, arguments)

        return call

    async def list_threads(self, root: str, *, statuses: list[str] | None = None,
                           title_contains: str | None = None, limit: int | None = None,
                           cursor: int | None = None) -> dict:
        call = self._connect()
        project_id = await find_project(call, root)
        return await call('t3_thread_list', _given(
            projectId=project_id, statuses=statuses, titleContains=title_contains,
            limit=limit, cursor=cursor))

    async def search(self, query: str, *, root: str | None = None,
                     limit: int | None = None) -> dict:
        call = self._connect()
        project_id = None if root is None else await find_project(call, root)
        return await call('t3_thread_search', _given(
            query=query, projectId=project_id, limit=limit))

    async def read(self, thread_id: str, *, view: str | None = None,
                   after_position: int | None = None, limit: int | None = None,
                   item_id: str | None = None, text_offset: int | None = None) -> dict:
        call = self._connect()
        return await call('t3_thread_read', _given(
            threadId=thread_id, view=view, afterPosition=after_position, limit=limit,
            itemId=item_id, textOffset=text_offset))

    async def runtime_mode(self, thread_id: str) -> str | None:
        answer = await self.read(thread_id, view='activity', limit=1)
        thread = answer.get('thread')
        mode = thread.get('runtimeMode') if isinstance(thread, dict) else None
        return mode if isinstance(mode, str) else None

    async def send(self, thread_id: str, message: str, *, mode: DispatchMode,
                   client_request_id: str) -> dict:
        call = self._connect()
        return await call('t3_thread_send', _given(
            threadId=thread_id, message=message, mode=SEND_MODES[mode],
            clientRequestId=client_request_id))

    async def interrupt(self, thread_id: str, *, client_request_id: str,
                        reason: str | None = None) -> dict:
        call = self._connect()
        return await call('t3_thread_interrupt', _given(
            threadId=thread_id, clientRequestId=client_request_id, reason=reason))
