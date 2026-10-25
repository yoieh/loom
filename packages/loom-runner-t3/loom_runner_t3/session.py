import asyncio
import contextlib
import json
import os
import re
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable

from loom_contracts.runners import (
    Capability,
    DispatchMode,
    Exited,
    PermissionMode,
    RequestAnswer,
    RunnerEvent,
    RunnerHost,
    ThreadSpec,
)

from .activity import Activity, Tracker, parse_activity
from .credential import usable_credential
from .mcp import TIMEOUT, HttpCall, McpClient, check_origin, http_call
from .reasons import (
    ANSWERED_IN_T3,
    NotPaired,
    NotRunning,
    T3Error,
    failure,
    safe_reason,
)
from .record import RecordError, is_canonical_uuid, is_t3_thread_id, read_record, write_record

SECRET = 't3-client'
DEFAULT_HOME = '~/.t3'
MAX_PAGES = 50
MAX_SETTINGS = 1_048_576
GIT_ENV_KEYS = ('HOME', 'LANG', 'LC_ALL', 'PATH', 'TMPDIR')

SEND_MODES = {
    DispatchMode.STEER: 'steer',
    DispatchMode.QUEUE: 'queue',
    DispatchMode.INTERRUPT: 'restart',
}

_MODEL = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/\[\]-]{0,199}')

Call = Callable[[str, dict[str, object]], Awaitable[dict]]
Git = Callable[[list[str]], Awaitable[tuple[int, str]]]
Sleep = Callable[[float], Awaitable[None]]


def git_env() -> dict[str, str]:
    env = {key: os.environ[key] for key in GIT_ENV_KEYS if key in os.environ}
    env['GIT_TERMINAL_PROMPT'] = '0'
    return env


async def run_git(args: list[str]) -> tuple[int, str]:
    process = await asyncio.create_subprocess_exec(
        'git', *args,
        env=git_env(),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), TIMEOUT)
    except BaseException:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
        raise
    return process.returncode, stdout.decode('utf-8', 'replace')


def t3_home_of(host: RunnerHost) -> str:
    home = host.setting('t3_home')
    return home if isinstance(home, str) and home else os.path.expanduser(DEFAULT_HOME)


def check_thread(thread: ThreadSpec) -> None:
    if not is_canonical_uuid(thread.id):
        raise failure('invalid', f'thread id {thread.id!r} is not a canonical lowercase uuid')
    if thread.permission_mode not in set(PermissionMode):
        raise failure('invalid', f'unknown permission mode {thread.permission_mode!r}')
    model = thread.model
    if model is not None and (not isinstance(model, str) or not _MODEL.fullmatch(model)):
        raise failure('invalid', f'model {model!r} is not a valid model name')
    workspace = thread.workspace_path
    if not isinstance(workspace, str) or not os.path.isabs(workspace):
        raise failure('invalid', f'workspace path {workspace!r} is not absolute')
    if not isinstance(thread.title, str):
        raise failure('invalid', 'the thread title is not a string')


def _default_model(t3_home: str) -> dict:
    path = os.path.join(t3_home, 'userdata', 'settings.json')
    try:
        with open(path, 'rb') as file:
            data = file.read(MAX_SETTINGS + 1)
        settings = json.loads(data) if len(data) <= MAX_SETTINGS else None
    except (OSError, ValueError, RecursionError):
        settings = None
    if not isinstance(settings, dict):
        raise failure('no model', f'{path} is missing or unreadable')
    selection = settings.get('defaultModelSelection')
    if not isinstance(selection, dict):
        raise failure('no model', f'{path} has no defaultModelSelection object')
    return selection


def model_selection(model: str | None, t3_home: str) -> dict:
    if model is not None and '/' in model:
        instance, name = model.split('/', 1)
        if instance and name:
            return {'instanceId': instance, 'model': name}
        raise failure('no model', f'{model!r} is not <instance>/<model>')
    default = _default_model(t3_home)
    if model is None:
        return default
    instance = default.get('instanceId')
    if not isinstance(instance, str) or not instance:
        raise failure('no model', 'defaultModelSelection has no instanceId')
    return {'instanceId': instance, 'model': model}


async def _git(git: Git, args: list[str]) -> tuple[int, str]:
    try:
        return await git(args)
    except (OSError, TimeoutError) as error:
        why = getattr(error, 'strerror', None) or type(error).__name__
        raise failure('git', f'could not run git: {why}') from None


async def workspace_strategy(git: Git, path: str) -> tuple[str, dict[str, str]]:
    code, out = await _git(git, ['-C', path, 'rev-parse', '--path-format=absolute',
                                 '--show-toplevel', '--git-common-dir'])
    lines = out.splitlines()
    if code != 0 or len(lines) != 2:
        raise failure('git', f'{path} is not a git checkout')
    top, common = lines
    if os.path.basename(common) != '.git':
        raise failure('git', f'{path} has no main checkout')
    main = os.path.dirname(common)
    if top == main:
        return main, {'type': 'root'}
    code, out = await _git(git, ['-C', path, 'symbolic-ref', '--short', 'HEAD'])
    branch = out.strip()
    if code != 0 or not branch:
        raise failure('git', f'the worktree {path} has no branch (detached HEAD)')
    return main, {'type': 'existing_worktree', 'worktreePath': top, 'branch': branch}


async def find_project(call: Call, main: str) -> str:
    target = os.path.realpath(main)
    cursor = None
    for _ in range(MAX_PAGES):
        answer = await call('t3_project_list', {} if cursor is None else {'cursor': cursor})
        projects = answer.get('projects')
        for project in projects if isinstance(projects, list) else []:
            if not isinstance(project, dict):
                continue
            project_id, root = project.get('id'), project.get('workspaceRoot')
            if not isinstance(project_id, str) or not project_id or not isinstance(root, str):
                continue
            if os.path.realpath(root) == target:
                return project_id
        cursor = answer.get('nextCursor')
        if not isinstance(cursor, int) or isinstance(cursor, bool):
            break
    raise T3Error(safe_reason(f't3: no T3 project at {main}', ()))


async def read_pages(call: Call, t3_id: str, tracker: Tracker,
                     take: Callable[[Activity], None]) -> Activity | None:
    last = None
    for _ in range(MAX_PAGES):
        before = tracker.position
        answer = await call('t3_thread_read', {'threadId': t3_id, 'view': 'activity',
                                               'afterPosition': before})
        last = parse_activity(answer)
        take(last)
        if not last.has_more or tracker.position == before:
            break
    return last


class T3Runner:
    name = 't3'
    capabilities = frozenset({Capability.STEER, Capability.INTERRUPT, Capability.RESUME})

    def __init__(
        self,
        http: HttpCall = http_call,
        clock: Callable[[], float] = time.time,
        sleep: Sleep = asyncio.sleep,
        running_interval: float = 1.0,
        idle_interval: float = 15.0,
        timeout: float = TIMEOUT,
        git: Git = run_git,
        new_id: Callable[[], str] = lambda: str(uuid.uuid4()),
    ) -> None:
        self._http = http
        self._clock = clock
        self._sleep = sleep
        self._running_interval = running_interval
        self._idle_interval = idle_interval
        self._timeout = timeout
        self._git = git
        self._new_id = new_id

    async def start(self, thread: ThreadSpec, host: RunnerHost) -> 'T3Session':
        credential = usable_credential(host.secret(SECRET), self._clock())
        if credential is None:
            raise NotPaired()
        t3_home = t3_home_of(host)
        check_origin(t3_home, credential.origin)
        check_thread(thread)
        client = McpClient(credential, t3_home, self._http, self._timeout)

        async def call(name: str, arguments: dict[str, object]) -> dict:
            return await asyncio.to_thread(client.call_tool, name, arguments)

        tracker = Tracker(client.secrets)
        if thread.resume is True:
            t3_id = await self._resume(thread, host, call, tracker)
        else:
            t3_id = await self._launch(thread, host, t3_home, call)
        return T3Session(call, t3_id, tracker, sleep=self._sleep,
                         running_interval=self._running_interval,
                         idle_interval=self._idle_interval, new_id=self._new_id)

    async def _launch(self, thread: ThreadSpec, host: RunnerHost, t3_home: str,
                      call: Call) -> str:
        main, strategy = await workspace_strategy(self._git, thread.workspace_path)
        project_id = await find_project(call, main)
        selection = model_selection(thread.model, t3_home)
        answer = await call('t3_thread_launch', {
            'projectId': project_id,
            'title': thread.title,
            'workspaceStrategy': strategy,
            'runtimeMode': str(PermissionMode(thread.permission_mode)),
            'modelSelection': selection,
        })
        t3_id = answer.get('threadId')
        if not is_t3_thread_id(t3_id):
            raise failure('protocol', 'the launch answer has no T3 thread id')
        try:
            write_record(host.data_dir, thread.id, t3_id)
        except OSError as error:
            why = error.strerror or type(error).__name__
            raise failure('record', f'could not store the T3 thread id: {why}') from None
        return t3_id

    async def _resume(self, thread: ThreadSpec, host: RunnerHost, call: Call,
                      tracker: Tracker) -> str:
        try:
            t3_id = read_record(host.data_dir, thread.id)
        except RecordError as error:
            raise failure('record', f'the T3 thread record for {thread.id} is unreadable: '
                                    f'{error}') from None
        if t3_id is None:
            raise failure('record', f'no T3 thread is stored for {thread.id}')
        last = await read_pages(call, t3_id, tracker, tracker.baseline)
        if last is not None and last.archived:
            raise failure('archived', f'the T3 thread {t3_id} is archived')
        return t3_id


class T3Session:
    def __init__(
        self,
        call: Call,
        t3_id: str,
        tracker: Tracker,
        *,
        sleep: Sleep = asyncio.sleep,
        running_interval: float = 1.0,
        idle_interval: float = 15.0,
        new_id: Callable[[], str] = lambda: str(uuid.uuid4()),
    ) -> None:
        self._call = call
        self.t3_id = t3_id
        self._tracker = tracker
        self._sleep = sleep
        self._running_interval = running_interval
        self._idle_interval = idle_interval
        self._new_id = new_id
        self._loop = asyncio.get_running_loop()
        self._queue: asyncio.Queue[RunnerEvent] = asyncio.Queue()
        self._wake = asyncio.Event()
        self._closed = False
        self._ended = False
        self.events: AsyncIterator[RunnerEvent] = self._iterate()
        self._poller = self._loop.create_task(self._poll())

    async def send(self, message: str, mode: DispatchMode) -> None:
        mode = DispatchMode(mode)
        self._check_open()
        await self._call('t3_thread_send', {
            'threadId': self.t3_id,
            'message': message,
            'mode': SEND_MODES[mode],
            'clientRequestId': self._new_id(),
        })
        self._tracker.expect_turn()
        self._wake.set()

    async def respond(self, request_id: str, answer: RequestAnswer) -> None:
        raise T3Error(ANSWERED_IN_T3)

    async def interrupt(self) -> None:
        if self._closed or self._ended:
            return
        await self._call('t3_thread_interrupt', {
            'threadId': self.t3_id,
            'clientRequestId': self._new_id(),
        })
        self._wake.set()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._poller.cancel()
        await asyncio.wait({self._poller})
        self._finish()

    def _check_open(self) -> None:
        if self._closed or self._ended:
            raise T3Error('t3: the session has ended')

    async def _iterate(self) -> AsyncIterator[RunnerEvent]:
        while True:
            event = await self._queue.get()
            yield event
            if isinstance(event, Exited):
                return

    def _emit(self, events: list[RunnerEvent]) -> None:
        if not self._ended:
            for event in events:
                self._queue.put_nowait(event)

    def _finish(self) -> None:
        if not self._ended:
            self._ended = True
            self._queue.put_nowait(Exited(None))

    def _take(self, activity: Activity) -> None:
        self._emit(self._tracker.apply(activity))

    async def _nap(self, seconds: float) -> bool:
        if self._wake.is_set():
            self._wake.clear()
            return False
        sleeper = self._loop.create_task(self._sleep(seconds))
        waker = self._loop.create_task(self._wake.wait())
        try:
            done, _ = await asyncio.wait({sleeper, waker},
                                         return_when=asyncio.FIRST_COMPLETED)
        finally:
            sleeper.cancel()
            waker.cancel()
        if waker in done:
            self._wake.clear()
        return sleeper in done

    async def _poll(self) -> None:
        while not self._closed and not self._ended:
            busy = self._tracker.busy()
            if not await self._nap(self._running_interval if busy else self._idle_interval):
                continue
            try:
                await read_pages(self._call, self.t3_id, self._tracker, self._take)
            except (NotPaired, NotRunning) as error:
                self._emit(self._tracker.abandon(str(error)))
                self._finish()
                return
            except T3Error as error:
                self._emit(self._tracker.abandon(str(error)))
            except Exception as error:
                why = safe_reason(f't3: internal: {type(error).__name__}', self._tracker.secrets)
                self._emit(self._tracker.abandon(why))
