import uuid
from collections.abc import Callable
from dataclasses import dataclass

from loom_client import Client, LoomApiError, LoomConnectionError

from .identity import Caller, Identity

STRING = 'string'
OBJECT = 'object'
INTEGER = 'integer'
KINDS = {STRING: (str, 'a string'), OBJECT: (dict, 'an object'), INTEGER: (int, 'an integer')}

WORKSPACE = ('Where the thread works: {"strategy": "root"}, {"strategy": "worktree", '
             '"branch"?, "base"?} or {"strategy": "existing_worktree", "path", "branch"}.')
TRIGGER = ('When it runs: {"type": "at", "at": "2026-10-25T09:00"}, {"type": "interval", '
           '"everyMs": 3600000}, {"type": "fixed_time", "timeOfDay": "9:00", '
           '"weekdays": [1, 2, 3, 4, 5]} (0 is Sunday), or {"type": "event", "topic": '
           '"factory.run.status", "project_id"?, "match"?: {"to": "done"}, "hops_max"?, '
           '"rate_max"?}, which runs once for each matching signal, thread.settled, '
           'thread.failed, request.opened or schedule.fired.')
SCHEDULE_TEXT = ('What it posts, launches with or notifies. With an event trigger, '
                 '{{payload.<field>}} and {{event.id}} in it are filled in from the event.')

NOT_A_LOOM_THREAD = 'this agent is not a Loom thread, so it has no tasks; use launch_thread'
NEEDS_PROJECT = 'project_id is required outside a Loom thread'
UNKNOWN_PROJECT = "cannot tell this thread's project; give project_id"
MAY_HAVE_RUN = 'the call may have run, so check with threads or schedules before calling again'
MAY_HAVE_EMITTED = 'the signal may have been written, and emitting it again writes a second one'

CONCURRENCY_KEY = ('A key, such as pm:demo, that at most one thread holds at a time; the launch '
                   'waits while another thread holds it.')
ON_BUSY = ('With concurrency_key: queue (default) waits, coalesce joins the waiting launch for '
           'the key.')
SETTLE = ('on_result: the thread settles after its first completed or failed result instead of '
          'going idle.')
LAUNCH_ONLY = 'Only with action launch. '


class ToolError(Exception):
    pass


@dataclass(frozen=True)
class Arg:
    name: str
    kind: str
    description: str
    required: bool = False


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    args: tuple[Arg, ...] = ()

    def listing(self) -> dict:
        properties = {arg.name: {'type': arg.kind, 'description': arg.description}
                      for arg in self.args}
        required = [arg.name for arg in self.args if arg.required]
        return {'name': self.name, 'description': self.description,
                'inputSchema': {'type': 'object', 'properties': properties,
                                'required': required}}


def _runner_args() -> tuple[Arg, ...]:
    return (
        Arg('workspace', OBJECT, WORKSPACE),
        Arg('runner', STRING, 'The agent that runs it: claude, codex or t3.'),
        Arg('model', STRING, "The agent's model. Default: the agent's own."),
    )


def _key_args() -> tuple[Arg, ...]:
    return (
        Arg('concurrency_key', STRING, CONCURRENCY_KEY),
        Arg('on_busy', STRING, ON_BUSY),
    )


TOOLS = (
    Tool('launch_thread',
         "Start a new top-level Loom thread with a first message. It runs with its project's "
         'permission mode.', (
             Arg('title', STRING, "The thread's title.", True),
             Arg('message', STRING, 'The first message.', True),
             Arg('project_id', STRING,
                 "The project to start it in. Default: this thread's project."),
             *_runner_args(),
             Arg('audience', STRING, 'Who talks with it: person or agent (default).'),
             *_key_args(),
             Arg('settle', STRING, SETTLE),
         )),
    Tool('delegate',
         "Start a child thread for a task. Its result is posted back to this thread as a "
         'message.', (
             Arg('task', STRING, "The child's first message.", True),
             *_runner_args(),
             *_key_args(),
         )),
    Tool('task_status', "Read one of this thread's tasks.", (
        Arg('task_id', STRING, 'The task_id delegate answered.', True),
    )),
    Tool('task_cancel', "Stop one of this thread's tasks and every thread it started.", (
        Arg('task_id', STRING, 'The task_id delegate answered.', True),
    )),
    Tool('thread_send', 'Send a message into a Loom thread.', (
        Arg('thread_id', STRING, 'The thread.', True),
        Arg('message', STRING, 'The message.', True),
        Arg('mode', STRING,
            'What happens when a turn is running: steer (default), queue or interrupt.'),
    )),
    Tool('threads', 'List Loom threads and their states, oldest first.', (
        Arg('project_id', STRING, "Only this project's threads."),
    )),
    Tool('emit',
         'Emit a signal that event schedules can act on. A signal from a Loom thread records '
         'that thread.', (
             Arg('topic', STRING,
                 "What happened, such as factory.run.status. Not one of Loom's own event types.",
                 True),
             Arg('payload', OBJECT, 'Any JSON object about it, {} for none.', True),
             Arg('project_id', STRING, 'The project the signal is about.'),
             Arg('key', STRING, 'What the signal is about within its topic, such as a run id.'),
             Arg('cause', INTEGER, 'The id of the event behind this signal.'),
         )),
    Tool('schedule',
         'Add a schedule that posts into a thread, launches a thread or notifies, on a clock '
         'trigger or an event trigger.', (
             Arg('trigger', OBJECT, TRIGGER, True),
             Arg('text', STRING, SCHEDULE_TEXT, True),
             Arg('action', STRING, 'post (default), launch or notify.'),
             Arg('thread_id', STRING, 'The thread a post goes into. Default: this thread.'),
             Arg('project_id', STRING, 'The project a launch starts its threads in.'),
             Arg('title', STRING, 'The title of each thread a launch starts.'),
             Arg('concurrency_key', STRING, LAUNCH_ONLY + CONCURRENCY_KEY),
             Arg('on_busy', STRING, LAUNCH_ONLY + ON_BUSY),
             Arg('settle', STRING, LAUNCH_ONLY + SETTLE),
         )),
    Tool('schedules', 'List schedules.'),
    Tool('unschedule', 'Remove a schedule.', (
        Arg('id', STRING, 'The schedule id.', True),
    )),
)

BY_NAME = {tool.name: tool for tool in TOOLS}


def check_arguments(tool: Tool, arguments: dict) -> dict:
    values = {}
    for arg in tool.args:
        value = arguments.get(arg.name)
        if value is None:
            if arg.required:
                raise ToolError(f'{arg.name} is required')
            continue
        kind, article = KINDS[arg.kind]
        if not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
            raise ToolError(f'{arg.name} must be {article}')
        values[arg.name] = value
    return values


def _write(send: Callable[[str], object], text: str = MAY_HAVE_RUN) -> object:
    command_id = str(uuid.uuid4())
    try:
        return send(command_id)
    except LoomConnectionError:
        pass
    try:
        return send(command_id)
    except LoomConnectionError as error:
        raise ToolError(f'{error.message}; {text}') from None


class Tools:
    def __init__(self, identity: Identity, make_client: Callable[[], Client] = Client) -> None:
        self._identity = identity
        self._make_client = make_client
        self._client: Client | None = None
        self._handlers: dict[str, Callable[[Client, dict], object]] = {
            'launch_thread': self._launch_thread,
            'delegate': self._delegate,
            'task_status': self._task_status,
            'task_cancel': self._task_cancel,
            'thread_send': self._thread_send,
            'threads': self._threads,
            'emit': self._emit,
            'schedule': self._schedule,
            'schedules': self._schedules,
            'unschedule': self._unschedule,
        }

    def call(self, name: str, arguments: dict) -> object:
        values = check_arguments(BY_NAME[name], arguments)
        try:
            if self._client is None:
                self._client = self._make_client()
            return self._handlers[name](self._client, values)
        except LoomApiError as error:
            raise ToolError(f'{error.code}: {error.message}') from None
        except LoomConnectionError as error:
            raise ToolError(error.message) from None

    def _loom_thread(self, client: Client) -> str:
        thread_id = self._identity.caller(client).thread_id
        if thread_id is None:
            raise ToolError(NOT_A_LOOM_THREAD)
        return thread_id

    def _caller_project(self, client: Client, caller: Caller) -> str:
        if caller.thread_id is None:
            raise ToolError(NEEDS_PROJECT)
        thread = client.threads_get(caller.thread_id)
        project_id = thread.get('project_id') if isinstance(thread, dict) else None
        if not isinstance(project_id, str):
            raise ToolError(UNKNOWN_PROJECT)
        return project_id

    def _own_task(self, client: Client, task_id: str) -> dict:
        caller_id = self._loom_thread(client)
        thread = client.threads_get(task_id)
        parent_id = thread.get('parent_id') if isinstance(thread, dict) else None
        if not isinstance(parent_id, str) or parent_id != caller_id:
            raise ToolError(f'{task_id} is not a task of this thread')
        return thread

    def _launch_thread(self, client: Client, values: dict) -> object:
        caller = self._identity.caller(client)
        project_id = values.get('project_id')
        if project_id is None:
            project_id = self._caller_project(client, caller)
        return _write(lambda command_id: client.threads_launch(
            project_id, values['title'], values['message'], workspace=values.get('workspace'),
            runner=values.get('runner'), model=values.get('model'), origin=caller.origin,
            audience=values.get('audience'), concurrency_key=values.get('concurrency_key'),
            on_busy=values.get('on_busy'), settle=values.get('settle'), id=command_id))

    def _delegate(self, client: Client, values: dict) -> object:
        parent_id = self._loom_thread(client)
        thread = _write(lambda command_id: client.threads_delegate(
            parent_id, values['task'], workspace=values.get('workspace'),
            runner=values.get('runner'), model=values.get('model'),
            concurrency_key=values.get('concurrency_key'), on_busy=values.get('on_busy'),
            id=command_id))
        task_id = thread.get('id') if isinstance(thread, dict) else None
        return {'task_id': task_id, 'thread': thread}

    def _task_status(self, client: Client, values: dict) -> object:
        return self._own_task(client, values['task_id'])

    def _task_cancel(self, client: Client, values: dict) -> object:
        task_id = values['task_id']
        self._own_task(client, task_id)
        return _write(lambda command_id: client.threads_stop(task_id, id=command_id))

    def _thread_send(self, client: Client, values: dict) -> object:
        return _write(lambda command_id: client.threads_send(
            values['thread_id'], values['message'], mode=values.get('mode'), id=command_id))

    def _threads(self, client: Client, values: dict) -> object:
        return client.threads_list(values.get('project_id'))

    def _emit(self, client: Client, values: dict) -> object:
        return _write(lambda command_id: client.signals_emit(
            values['topic'], values['payload'], project_id=values.get('project_id'),
            key=values.get('key'), cause=values.get('cause'), id=command_id), MAY_HAVE_EMITTED)

    def _schedule(self, client: Client, values: dict) -> object:
        return _write(lambda command_id: client.schedules_add(
            values['trigger'], values['text'], action=values.get('action'),
            thread_id=values.get('thread_id'), project_id=values.get('project_id'),
            title=values.get('title'), concurrency_key=values.get('concurrency_key'),
            on_busy=values.get('on_busy'), settle=values.get('settle'), id=command_id))

    def _schedules(self, client: Client, values: dict) -> object:
        return client.schedules_list()

    def _unschedule(self, client: Client, values: dict) -> object:
        return _write(lambda command_id: client.schedules_remove(values['id'], id=command_id))
