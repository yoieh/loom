from enum import StrEnum
from typing import Literal, NotRequired, TypedDict

from .runners import Decision, DispatchMode, PermissionMode, RunnerName
from .states import ThreadState
from .triggers import ScheduleAction, Trigger
from .workspaces import Workspace

MAX_CONCURRENCY_KEY = 200
MAX_TOPIC = 200
MAX_SIGNAL_KEY = 200
MAX_PAYLOAD = 16_384
TOPIC_PATTERN = r'[A-Za-z0-9][A-Za-z0-9._:-]*'


class CallName(StrEnum):
    THREADS_LAUNCH = 'threads.launch'
    THREADS_DELEGATE = 'threads.delegate'
    THREADS_SEND = 'threads.send'
    THREADS_STEER = 'threads.steer'
    THREADS_LIST = 'threads.list'
    THREADS_GET = 'threads.get'
    THREADS_STOP = 'threads.stop'
    THREADS_SETTLE = 'threads.settle'
    THREADS_UPDATE = 'threads.update'
    THREADS_EVENTS = 'threads.events'
    REQUESTS_RESPOND = 'requests.respond'
    SIGNALS_EMIT = 'signals.emit'
    SCHEDULES_ADD = 'schedules.add'
    SCHEDULES_LIST = 'schedules.list'
    SCHEDULES_PAUSE = 'schedules.pause'
    SCHEDULES_REMOVE = 'schedules.remove'
    PROJECTS_ADD = 'projects.add'
    PROJECTS_SET_MODE = 'projects.set_mode'
    PROJECTS_REMOVE = 'projects.remove'
    PROJECTS_LIST = 'projects.list'


class Audience(StrEnum):
    PERSON = 'person'
    AGENT = 'agent'


class OnBusy(StrEnum):
    QUEUE = 'queue'
    COALESCE = 'coalesce'


class Settle(StrEnum):
    ON_RESULT = 'on_result'


class ThreadsLaunchParams(TypedDict):
    id: str
    title: str
    message: str
    project_id: str
    workspace: NotRequired[Workspace]
    runner: NotRequired[RunnerName]
    model: NotRequired[str]
    requests: NotRequired[Literal['caller', 'any']]
    origin: NotRequired[str]
    audience: NotRequired[Audience]
    concurrency_key: NotRequired[str]
    on_busy: NotRequired[OnBusy]
    settle: NotRequired[Settle]
    cause: NotRequired[int]


class ThreadsDelegateParams(TypedDict):
    id: str
    parent_id: str
    task: str
    workspace: NotRequired[Workspace]
    runner: NotRequired[RunnerName]
    model: NotRequired[str]
    concurrency_key: NotRequired[str]
    on_busy: NotRequired[OnBusy]


class ThreadsSendParams(TypedDict):
    id: str
    thread_id: str
    message: str
    mode: NotRequired[DispatchMode]
    cause: NotRequired[int]


class ThreadsSteerParams(TypedDict):
    id: str
    thread_id: str
    message_id: str


class ThreadsListParams(TypedDict):
    project_id: NotRequired[str]
    audience: NotRequired[Audience]


class ThreadsGetParams(TypedDict):
    thread_id: str


class ThreadsStopParams(TypedDict):
    id: str
    thread_id: str


class ThreadsSettleParams(TypedDict):
    id: str
    thread_id: str


class ThreadsUpdateParams(TypedDict):
    id: str
    thread_id: str
    audience: Audience


class ThreadsEventsParams(TypedDict):
    after: NotRequired[int]
    thread_id: NotRequired[str]
    follow: NotRequired[bool]
    audience: NotRequired[Audience]


class RequestsRespondParams(TypedDict):
    id: str
    request_id: str
    decision: Decision
    message: NotRequired[str]


class SignalsEmitParams(TypedDict):
    id: str
    topic: str
    payload: dict[str, object]
    project_id: NotRequired[str]
    key: NotRequired[str]
    cause: NotRequired[int]


class SchedulesAddParams(TypedDict):
    id: str
    trigger: Trigger
    text: str
    action: NotRequired[ScheduleAction]
    thread_id: NotRequired[str]
    project_id: NotRequired[str]
    title: NotRequired[str]
    concurrency_key: NotRequired[str]
    on_busy: NotRequired[OnBusy]
    settle: NotRequired[Settle]
    runner: NotRequired[RunnerName]
    model: NotRequired[str]
    workspace: NotRequired[Workspace]


class SchedulesListParams(TypedDict):
    thread_id: NotRequired[str]
    project_id: NotRequired[str]


class SchedulesPauseParams(TypedDict):
    id: str
    schedule_id: str


class SchedulesRemoveParams(TypedDict):
    id: str
    schedule_id: str


class ProjectsAddParams(TypedDict):
    id: str
    title: str
    path: str
    permission_mode: NotRequired[PermissionMode]


class ProjectsSetModeParams(TypedDict):
    id: str
    project_id: str
    permission_mode: PermissionMode


class ProjectsRemoveParams(TypedDict):
    id: str
    project_id: str


class ProjectsListParams(TypedDict):
    pass


CALL_PARAMS: dict[CallName, type] = {
    CallName.THREADS_LAUNCH: ThreadsLaunchParams,
    CallName.THREADS_DELEGATE: ThreadsDelegateParams,
    CallName.THREADS_SEND: ThreadsSendParams,
    CallName.THREADS_STEER: ThreadsSteerParams,
    CallName.THREADS_LIST: ThreadsListParams,
    CallName.THREADS_GET: ThreadsGetParams,
    CallName.THREADS_STOP: ThreadsStopParams,
    CallName.THREADS_SETTLE: ThreadsSettleParams,
    CallName.THREADS_UPDATE: ThreadsUpdateParams,
    CallName.THREADS_EVENTS: ThreadsEventsParams,
    CallName.REQUESTS_RESPOND: RequestsRespondParams,
    CallName.SIGNALS_EMIT: SignalsEmitParams,
    CallName.SCHEDULES_ADD: SchedulesAddParams,
    CallName.SCHEDULES_LIST: SchedulesListParams,
    CallName.SCHEDULES_PAUSE: SchedulesPauseParams,
    CallName.SCHEDULES_REMOVE: SchedulesRemoveParams,
    CallName.PROJECTS_ADD: ProjectsAddParams,
    CallName.PROJECTS_SET_MODE: ProjectsSetModeParams,
    CallName.PROJECTS_REMOVE: ProjectsRemoveParams,
    CallName.PROJECTS_LIST: ProjectsListParams,
}


class ThreadInfo(TypedDict):
    id: str
    title: str
    runner: RunnerName
    project_id: str
    workspace_path: str
    branch: str | None
    model: str | None
    permission_mode: PermissionMode
    state: ThreadState
    waiting: bool
    reason: str | None
    parent_id: str | None
    origin: str | None
    audience: Audience
    concurrency_key: str | None
    settle: Settle | None
    cause: int | None
    hops: int
    created_at: str


class ProjectInfo(TypedDict):
    id: str
    title: str
    workspace_root: str
    permission_mode: PermissionMode | None
    created_at: str


class ScheduleInfo(TypedDict):
    id: str
    trigger: Trigger
    action: ScheduleAction
    text: str
    thread_id: str | None
    project_id: str | None
    title: str | None
    concurrency_key: str | None
    on_busy: OnBusy | None
    settle: Settle | None
    runner: RunnerName | None
    model: str | None
    workspace: Workspace | None
    by: str | None
    paused: bool
    next_run: str | None
    created_at: str


class ApiError(TypedDict):
    code: str
    message: str
