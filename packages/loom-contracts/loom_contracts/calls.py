from enum import StrEnum
from typing import Literal, NotRequired, TypedDict

from .runners import Decision, DispatchMode, PermissionMode, RunnerName
from .states import ThreadState
from .triggers import ScheduleAction, Trigger
from .workspaces import Workspace


class CallName(StrEnum):
    THREADS_LAUNCH = 'threads.launch'
    THREADS_DELEGATE = 'threads.delegate'
    THREADS_SEND = 'threads.send'
    THREADS_LIST = 'threads.list'
    THREADS_GET = 'threads.get'
    THREADS_STOP = 'threads.stop'
    THREADS_SETTLE = 'threads.settle'
    THREADS_EVENTS = 'threads.events'
    REQUESTS_RESPOND = 'requests.respond'
    SCHEDULES_ADD = 'schedules.add'
    SCHEDULES_LIST = 'schedules.list'
    SCHEDULES_PAUSE = 'schedules.pause'
    SCHEDULES_REMOVE = 'schedules.remove'
    PROJECTS_ADD = 'projects.add'
    PROJECTS_SET_MODE = 'projects.set_mode'
    PROJECTS_REMOVE = 'projects.remove'
    PROJECTS_LIST = 'projects.list'


class ThreadsLaunchParams(TypedDict):
    id: str
    title: str
    message: str
    project: str
    workspace: NotRequired[Workspace]
    runner: NotRequired[RunnerName]
    model: NotRequired[str]
    requests: NotRequired[Literal['caller', 'any']]
    origin: NotRequired[str]


class ThreadsDelegateParams(TypedDict):
    id: str
    parent: str
    task: str
    workspace: NotRequired[Workspace]
    runner: NotRequired[RunnerName]
    model: NotRequired[str]


class ThreadsSendParams(TypedDict):
    id: str
    thread_id: str
    message: str
    mode: NotRequired[DispatchMode]


class ThreadsListParams(TypedDict):
    project: NotRequired[str]


class ThreadsGetParams(TypedDict):
    thread_id: str


class ThreadsStopParams(TypedDict):
    id: str
    thread_id: str


class ThreadsSettleParams(TypedDict):
    id: str
    thread_id: str


class ThreadsEventsParams(TypedDict):
    after: NotRequired[int]
    thread_id: NotRequired[str]


class RequestsRespondParams(TypedDict):
    id: str
    request_id: str
    decision: Decision
    message: NotRequired[str]


class SchedulesAddParams(TypedDict):
    id: str
    trigger: Trigger
    text: str
    action: NotRequired[ScheduleAction]
    thread_id: NotRequired[str]
    project: NotRequired[str]
    title: NotRequired[str]


class SchedulesListParams(TypedDict):
    thread_id: NotRequired[str]
    project: NotRequired[str]


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
    CallName.THREADS_LIST: ThreadsListParams,
    CallName.THREADS_GET: ThreadsGetParams,
    CallName.THREADS_STOP: ThreadsStopParams,
    CallName.THREADS_SETTLE: ThreadsSettleParams,
    CallName.THREADS_EVENTS: ThreadsEventsParams,
    CallName.REQUESTS_RESPOND: RequestsRespondParams,
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
    state: ThreadState
    waiting: bool
    reason: str | None
    parent_id: str | None
    origin: str | None
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
    by: str | None
    paused: bool
    next_run: str | None
    created_at: str


class ApiError(TypedDict):
    code: str
    message: str
