import os
import re
import shutil
import uuid

from loom_contracts.runners import PermissionMode, RunnerHost, ThreadSpec

ENV_ALLOWLIST: frozenset[str] = frozenset({
    'HOME',
    'LANG',
    'LOGNAME',
    'PATH',
    'SHELL',
    'USER',
    'XDG_RUNTIME_DIR',
    'LOOM_SOCKET',
})

SANDBOXES: dict[PermissionMode, str] = {
    PermissionMode.APPROVAL_REQUIRED: 'read-only',
    PermissionMode.AUTO_ACCEPT_EDITS: 'workspace-write',
    PermissionMode.AUTO: 'workspace-write',
    PermissionMode.FULL_ACCESS: 'danger-full-access',
}

_MODEL = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/\[\]-]{0,199}')

DEFAULT_TOKEN_ENV = 'CLIPROXY_API_KEY'
_TOKEN_ENV = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,127}')
# The token must never replace a variable Loom sets, or reach Codex as a Claude variable.
_NOT_TOKEN_ENV = ENV_ALLOWLIST | {'LOOM_THREAD', 'CODEX_HOME'}


def is_canonical_uuid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


def _text(value: object) -> bool:
    return isinstance(value, str) and value != ''


def check_thread(thread: ThreadSpec) -> None:
    if not is_canonical_uuid(thread.id):
        raise ValueError(f'thread id {thread.id!r} is not a canonical lowercase uuid')
    mode = thread.permission_mode
    if not isinstance(mode, str) or mode not in SANDBOXES:
        raise ValueError(f'unknown permission mode {mode!r}')
    model = thread.model
    if model is not None and (not isinstance(model, str) or not _MODEL.fullmatch(model)):
        raise ValueError(f'model {model!r} is not a valid model name')
    workspace = thread.workspace_path
    if not isinstance(workspace, str) or not os.path.isabs(workspace):
        raise ValueError(f'workspace path {workspace!r} is not absolute')


def codex_argv(thread: ThreadSpec, codex_bin: str, codex_thread_id: str | None) -> list[str]:
    if not _text(codex_bin):
        raise ValueError('codex_bin is empty')
    check_thread(thread)
    if codex_thread_id is not None and not is_canonical_uuid(codex_thread_id):
        raise ValueError('the codex thread id is not a canonical lowercase uuid')
    workspace = thread.workspace_path
    sandbox = SANDBOXES[PermissionMode(thread.permission_mode)]
    model = ['-m', thread.model] if thread.model is not None else []
    if codex_thread_id is None:
        return [codex_bin, 'exec', '--json', '-C', workspace, *model, '-s', sandbox, '-']
    return [
        codex_bin, '-C', workspace, '-s', sandbox,
        'exec', 'resume', codex_thread_id, '--json', *model, '-',
    ]


def _bin_dir(codex_bin: str, path: str) -> str | None:
    if '/' in codex_bin:
        directory = os.path.dirname(codex_bin)
    else:
        found = shutil.which(codex_bin, path=path)
        directory = os.path.dirname(found) if found else None
    if not directory or not os.path.isabs(directory):
        return None
    return directory


def _with_node(codex_bin: str, path: str | None) -> str | None:
    directory = _bin_dir(codex_bin, os.defpath if path is None else path)
    if directory is None:
        return path
    node = os.path.join(directory, 'node')
    if not os.path.isfile(node) or not os.access(node, os.X_OK):
        return path
    if not path:
        return directory
    entries = path.split(os.pathsep)
    if os.path.normpath(directory) in {os.path.normpath(entry) for entry in entries if entry}:
        return path
    return os.pathsep.join([*entries, directory])


def is_token_env(name: object) -> bool:
    return (isinstance(name, str) and _TOKEN_ENV.fullmatch(name) is not None
            and name not in _NOT_TOKEN_ENV and not name.startswith(('ANTHROPIC_', 'CLAUDE')))


def token_env(host: RunnerHost) -> str:
    name = host.setting('codex_token_env')
    return name if is_token_env(name) else DEFAULT_TOKEN_ENV


def codex_env(thread: ThreadSpec, host: RunnerHost, codex_bin: str) -> dict[str, str]:
    env = {
        key: value
        for key, value in host.environment(thread).items()
        if key in ENV_ALLOWLIST and isinstance(value, str)
    }
    env['LOOM_THREAD'] = thread.id
    path = _with_node(codex_bin, env.get('PATH'))
    if path is not None:
        env['PATH'] = path
    codex_home = host.setting('codex_home')
    if _text(codex_home):
        env['CODEX_HOME'] = codex_home
    token = host.secret('agent-token')
    if isinstance(token, str) and token.strip():
        env[token_env(host)] = token.strip()
    return env
