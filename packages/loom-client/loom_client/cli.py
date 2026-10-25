import argparse
import importlib.util
import json
import os
import sys
from collections.abc import Callable
from typing import TextIO

from loom_contracts.calls import Audience, CallName
from loom_contracts.runners import Decision, DispatchMode, PermissionMode, RunnerName
from loom_contracts.triggers import ScheduleAction, TriggerType
from loom_contracts.workspaces import WorkspaceStrategy

from . import install, output
from .api import Client
from .errors import LoomApiError, LoomConnectionError, LoomProtocolError

EXIT_API = 1
EXIT_USAGE = 2
EXIT_CONNECTION = 3
EXIT_INTERRUPT = 130
EXIT_PIPE = 141

PAGE = 1000
STDIN = '-'
SERVICE = 'loom_service'

GROUPS = {
    'threads': ('start, message and follow agent threads', ()),
    'projects': ('manage the projects threads run in', ('project',)),
    'schedules': ('add, list, pause and remove schedules', ('schedule',)),
    'requests': ("answer threads' permission prompts", ('request',)),
    'signals': ('emit signals for event triggers', ('signal',)),
}

WORKSPACE_FIELDS = {
    WorkspaceStrategy.ROOT: (),
    WorkspaceStrategy.WORKTREE: ('branch', 'base'),
    WorkspaceStrategy.EXISTING_WORKTREE: ('path', 'branch'),
}

Run = Callable[[Client, argparse.Namespace], object]


def _values(kind: type) -> list[str]:
    return [item.value for item in kind]


def _event_id(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f'not an event id: {text!r}') from None
    if value < 0:
        raise argparse.ArgumentTypeError(f'not an event id: {text!r}')
    return value


def _cause(text: str) -> int:
    try:
        return int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f'not an event id: {text!r}') from None


def _seconds(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f'not a whole number of seconds: {text!r}') from None
    if value < 1:
        raise argparse.ArgumentTypeError(f'not a whole number of seconds: {text!r}')
    return value


def _weekdays(text: str) -> list[int]:
    try:
        return [int(item) for item in text.split(',')]
    except ValueError:
        raise argparse.ArgumentTypeError(f'not a list of weekday numbers: {text!r}') from None


def _stdin_or(value: str) -> str:
    return sys.stdin.read() if value == STDIN else value


def _payload(args: argparse.Namespace, parser: argparse.ArgumentParser) -> dict:
    text = _stdin_or(args.payload)
    try:
        payload = json.loads(text)
    except (ValueError, RecursionError):
        parser.error('--payload is not JSON')
    if not isinstance(payload, dict):
        parser.error('--payload must be a JSON object')
    return payload


def _workspace(args: argparse.Namespace, parser: argparse.ArgumentParser) -> dict | None:
    given = {key: getattr(args, key) for key in ('branch', 'base', 'path')
             if getattr(args, key) is not None}
    if args.workspace is None:
        if given:
            parser.error(f'--{next(iter(given))} needs --workspace')
        return None
    allowed = WORKSPACE_FIELDS[WorkspaceStrategy(args.workspace)]
    for key in given:
        if key not in allowed:
            parser.error(f'--{key} does not go with --workspace {args.workspace}')
    return {'strategy': args.workspace, **given}


def _trigger(args: argparse.Namespace, parser: argparse.ArgumentParser) -> dict:
    if args.weekdays is not None and args.time is None:
        parser.error('--weekdays needs --time')
    if args.at is not None:
        return {'type': TriggerType.AT.value, 'at': args.at}
    if args.every is not None:
        return {'type': TriggerType.INTERVAL.value, 'everyMs': args.every * 1000}
    trigger = {'type': TriggerType.FIXED_TIME.value, 'timeOfDay': args.time}
    if args.weekdays is not None:
        trigger['weekdays'] = args.weekdays
    return trigger


def _add_id(command: argparse.ArgumentParser) -> None:
    command.add_argument('--id', help='command id, to retry a call safely (default: a new one)')


def _add_cause(command: argparse.ArgumentParser) -> None:
    command.add_argument('--cause', metavar='EVENT_ID', type=_cause,
                         help='id of the event behind this call')


def _add_audience(command: argparse.ArgumentParser, text: str, required: bool = False) -> None:
    command.add_argument('--audience', choices=_values(Audience), required=required, help=text)


def _add_workspace(command: argparse.ArgumentParser) -> None:
    command.add_argument('--workspace', choices=_values(WorkspaceStrategy),
                         help='where the thread works')
    command.add_argument('--branch', help='branch of a worktree')
    command.add_argument('--base', help='branch a new worktree starts from')
    command.add_argument('--path', help='path of an existing worktree')
    command.add_argument('--runner', choices=_values(RunnerName), help='agent that runs it')
    command.add_argument('--model', help='model for the agent')


def _launch_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('project_id')
    command.add_argument('title')
    command.add_argument('message', help='first message, or - to read it from stdin')
    _add_workspace(command)
    command.add_argument('--requests', choices=('caller', 'any'),
                         help='who may answer its permission prompts')
    command.add_argument('--origin', help='where the thread comes from')
    _add_audience(command, 'who talks with the thread (default agent)')
    _add_cause(command)
    _add_id(command)


def _launch(client: Client, args: argparse.Namespace) -> object:
    return client.threads_launch(
        args.project_id, args.title, _stdin_or(args.message), workspace=args.workspace,
        runner=args.runner, model=args.model, requests=args.requests, origin=args.origin,
        audience=args.audience, cause=args.cause, id=args.id)


def _delegate_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('parent_id')
    command.add_argument('task', help='the task, or - to read it from stdin')
    _add_workspace(command)
    _add_id(command)


def _delegate(client: Client, args: argparse.Namespace) -> object:
    return client.threads_delegate(args.parent_id, _stdin_or(args.task), workspace=args.workspace,
                                   runner=args.runner, model=args.model, id=args.id)


def _send_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('thread_id')
    command.add_argument('message', help='the message, or - to read it from stdin')
    command.add_argument('--mode', choices=_values(DispatchMode), help='how to deliver it')
    _add_cause(command)
    _add_id(command)


def _send(client: Client, args: argparse.Namespace) -> object:
    return client.threads_send(args.thread_id, _stdin_or(args.message), mode=args.mode,
                               cause=args.cause, id=args.id)


def _list_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('--project', help='only threads of this project')
    _add_audience(command, 'only threads with this audience')


def _thread_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('thread_id')


def _thread_write_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('thread_id')
    _add_id(command)


def _update_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('thread_id')
    _add_audience(command, 'who talks with the thread', required=True)
    _add_id(command)


def _events_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('--after', type=_event_id, default=0,
                         help='print events after this event id (default 0)')
    command.add_argument('--thread', help='only events of this thread')
    _add_audience(command, 'only events of threads with this audience')
    command.add_argument('--follow', action='store_true', help='go on printing new events')


def _project_add_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('title')
    command.add_argument('path', help="the project's main checkout")
    command.add_argument('--mode', choices=_values(PermissionMode), help='permission mode')
    _add_id(command)


def _project_mode_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('project_id')
    command.add_argument('mode', choices=_values(PermissionMode))
    _add_id(command)


def _project_write_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('project_id')
    _add_id(command)


def _schedule_add_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('text', help='the text, or - to read it from stdin')
    when = command.add_mutually_exclusive_group(required=True)
    when.add_argument('--at', metavar='DATETIME', help='run once at this time')
    when.add_argument('--every', metavar='SECONDS', type=_seconds,
                      help='run every so many seconds')
    when.add_argument('--time', metavar='H:MM', help='run each day at this local time')
    command.add_argument('--weekdays', metavar='LIST', type=_weekdays,
                         help='with --time, only these days, 0 is Sunday (as 1,2,3,4,5)')
    command.add_argument('--action', choices=_values(ScheduleAction),
                         help='what it does (default post)')
    command.add_argument('--thread', help='thread it posts into')
    command.add_argument('--project', help='project it launches in')
    command.add_argument('--title', help='its title, also of a thread it launches')
    _add_id(command)


def _schedule_add(client: Client, args: argparse.Namespace) -> object:
    return client.schedules_add(
        args.trigger, _stdin_or(args.text), action=args.action, thread_id=args.thread,
        project_id=args.project, title=args.title, id=args.id)


def _schedule_list_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('--thread', help='only schedules of this thread')
    command.add_argument('--project', help='only schedules of this project')


def _schedule_write_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('schedule_id')
    _add_id(command)


def _respond_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('request_id')
    command.add_argument('decision', choices=_values(Decision))
    command.add_argument('--message', help='what the agent is told when it is denied')
    _add_id(command)


def _respond(client: Client, args: argparse.Namespace) -> object:
    return client.requests_respond(args.request_id, args.decision, message=args.message,
                                   id=args.id)


def _emit_args(command: argparse.ArgumentParser) -> None:
    command.add_argument('topic')
    command.add_argument('--payload', metavar='JSON', default='{}',
                         help='a JSON object, or - to read it from stdin (default {})')
    command.add_argument('--project', help='project the signal is about')
    command.add_argument('--key', help='what the signal is about, such as a run id')
    _add_cause(command)
    _add_id(command)


def _emit(client: Client, args: argparse.Namespace) -> object:
    return client.signals_emit(args.topic, args.payload, project_id=args.project, key=args.key,
                               cause=args.cause, id=args.id)


def _no_args(command: argparse.ArgumentParser) -> None:
    pass


COMMANDS: dict[CallName, tuple[str, Callable[[argparse.ArgumentParser], None], Run | None]] = {
    CallName.THREADS_LAUNCH: ('start a thread', _launch_args, _launch),
    CallName.THREADS_DELEGATE: ('start a child thread for a task', _delegate_args, _delegate),
    CallName.THREADS_SEND: ('send a message to a thread', _send_args, _send),
    CallName.THREADS_LIST: (
        'list threads', _list_args,
        lambda client, args: client.threads_list(args.project, audience=args.audience)),
    CallName.THREADS_GET: (
        'show a thread', _thread_args, lambda client, args: client.threads_get(args.thread_id)),
    CallName.THREADS_STOP: (
        'stop a thread and its children', _thread_write_args,
        lambda client, args: client.threads_stop(args.thread_id, id=args.id)),
    CallName.THREADS_SETTLE: (
        'settle a thread', _thread_write_args,
        lambda client, args: client.threads_settle(args.thread_id, id=args.id)),
    CallName.THREADS_UPDATE: (
        "change a thread's audience", _update_args,
        lambda client, args: client.threads_update(args.thread_id, args.audience, id=args.id)),
    CallName.THREADS_EVENTS: ('print events', _events_args, None),
    CallName.PROJECTS_ADD: (
        'add a project', _project_add_args,
        lambda client, args: client.projects_add(
            args.title, os.path.abspath(args.path), permission_mode=args.mode, id=args.id)),
    CallName.PROJECTS_SET_MODE: (
        "change a project's permission mode", _project_mode_args,
        lambda client, args: client.projects_set_mode(args.project_id, args.mode, id=args.id)),
    CallName.PROJECTS_REMOVE: (
        'remove a project', _project_write_args,
        lambda client, args: client.projects_remove(args.project_id, id=args.id)),
    CallName.PROJECTS_LIST: (
        'list projects', _no_args, lambda client, args: client.projects_list()),
    CallName.SCHEDULES_ADD: ('add a schedule', _schedule_add_args, _schedule_add),
    CallName.SCHEDULES_LIST: (
        'list schedules', _schedule_list_args,
        lambda client, args: client.schedules_list(args.thread, args.project)),
    CallName.SCHEDULES_PAUSE: (
        'pause a schedule', _schedule_write_args,
        lambda client, args: client.schedules_pause(args.schedule_id, id=args.id)),
    CallName.SCHEDULES_REMOVE: (
        'remove a schedule', _schedule_write_args,
        lambda client, args: client.schedules_remove(args.schedule_id, id=args.id)),
    CallName.REQUESTS_RESPOND: ('answer a permission prompt', _respond_args, _respond),
    CallName.SIGNALS_EMIT: ('emit a signal', _emit_args, _emit),
}


def command_name(call: CallName) -> tuple[str, str]:
    group, action = call.value.split('.')
    return group, action.replace('_', '-')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='loom', description='Run and follow Loom threads.')
    parser.add_argument('--socket', help='socket path (default $LOOM_SOCKET, '
                                         'else $XDG_RUNTIME_DIR/loom/api.sock)')
    parser.add_argument('--json', action='store_true', help='print JSON')
    commands = parser.add_subparsers(dest='command', required=True, metavar='command')
    serve = commands.add_parser('serve', help='start the Loom server')
    serve.add_argument('--socket', dest='serve_socket',
                       help='socket path (default $XDG_RUNTIME_DIR/loom/api.sock)')
    serve.add_argument('--data-dir', help='data dir (default $XDG_DATA_HOME/loom)')
    serve.add_argument('--config', help='config file (default $XDG_CONFIG_HOME/loom/config.toml)')
    commands.add_parser('install', help='register loom-mcp with Claude Code and Codex')
    groups = {}
    for call, (text, add_arguments, _) in COMMANDS.items():
        group, action = command_name(call)
        if group not in groups:
            group_help, aliases = GROUPS[group]
            group_parser = commands.add_parser(group, aliases=list(aliases), help=group_help)
            groups[group] = group_parser.add_subparsers(dest='action', required=True,
                                                        metavar='action')
        command = groups[group].add_parser(action, help=text)
        add_arguments(command)
        command.set_defaults(call=call, parser=command)
    return parser


def _serve(args: argparse.Namespace) -> int:
    if importlib.util.find_spec(SERVICE) is None:
        sys.stderr.write('loom: loom-service is not installed\n')
        return EXIT_USAGE
    options = []
    socket = args.serve_socket if args.serve_socket is not None else args.socket
    for flag, value in (('--socket', socket), ('--data-dir', args.data_dir),
                        ('--config', args.config)):
        if value is not None:
            options += [flag, value]
    sys.stdout.flush()
    sys.stderr.flush()
    argv = [sys.executable, '-m', SERVICE, 'serve', *options]
    try:
        os.execv(sys.executable, argv)
    except OSError as error:
        sys.stderr.write(f'loom: cannot start loom-service: {error.strerror or error}\n')
    return EXIT_USAGE


class Printer:
    def __init__(self, as_json: bool, out: TextIO) -> None:
        self.as_json = as_json
        self.out = out

    def _line(self, text: str) -> None:
        self.out.write(text + '\n')
        self.out.flush()

    def result(self, call: str, result: object) -> None:
        if self.as_json:
            self._line(json.dumps(result))
            return
        for line in output.human(call, result):
            self._line(line)

    def event(self, event: object) -> None:
        self._line(json.dumps(event) if self.as_json else output.event_line(event))

    def error(self, error: LoomApiError) -> None:
        if self.as_json:
            self._line(json.dumps({'error': {'code': error.code, 'message': error.message}}))


def _events(client: Client, args: argparse.Namespace, printer: Printer) -> None:
    if args.follow:
        for event in client.follow_events(args.after, args.thread, audience=args.audience):
            printer.event(event)
        return
    after = args.after
    while True:
        page = client.threads_events(after, args.thread, audience=args.audience)
        for event in page:
            printer.event(event)
        if len(page) < PAGE:
            return
        if page[-1]['id'] <= after:
            raise LoomProtocolError('the events answer did not move past --after')
        after = page[-1]['id']


def _run(args: argparse.Namespace) -> int:
    printer = Printer(args.json, sys.stdout)
    follow = args.call is CallName.THREADS_EVENTS and args.follow
    if 'workspace' in args:
        args.workspace = _workspace(args, args.parser)
    if args.call is CallName.SCHEDULES_ADD:
        args.trigger = _trigger(args, args.parser)
    if args.call is CallName.SIGNALS_EMIT:
        args.payload = _payload(args, args.parser)
    try:
        client = Client(args.socket)
        if args.call is CallName.THREADS_EVENTS:
            _events(client, args, printer)
        else:
            _, _, run = COMMANDS[args.call]
            printer.result(args.call, run(client, args))
    except LoomApiError as error:
        printer.error(error)
        sys.stderr.write(f'loom: {output.escape(error.code)}: {output.escape(error.message)}\n')
        return EXIT_API
    except LoomConnectionError as error:
        message = f'loom: {output.escape(error.message)}'
        if follow and error.last_event_id is not None:
            message += f'; resume with --after {error.last_event_id}'
        command_id = getattr(error, 'command_id', None)
        if command_id is not None:
            message += f'; retry with --id {output.escape(command_id)}'
        sys.stderr.write(message + '\n')
        return EXIT_CONNECTION
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = build_parser().parse_args(argv)
            if args.command == 'serve':
                return _serve(args)
            if args.command == 'install':
                return install.install()
            return _run(args)
        except SystemExit as done:
            if done.code is None or isinstance(done.code, int):
                return done.code or 0
            return EXIT_USAGE
    except KeyboardInterrupt:
        return EXIT_INTERRUPT
    except BrokenPipeError:
        try:
            stdout = sys.stdout.fileno()
        except (OSError, ValueError):
            return EXIT_PIPE
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, stdout)
        os.close(devnull)
        return EXIT_PIPE
