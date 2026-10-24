import os
import re
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

CLAUDE_MODES: dict[PermissionMode, tuple[str, ...]] = {
    PermissionMode.APPROVAL_REQUIRED: ('--permission-mode', 'manual'),
    PermissionMode.AUTO_ACCEPT_EDITS: ('--permission-mode', 'acceptEdits'),
    PermissionMode.AUTO: ('--permission-mode', 'auto'),
    PermissionMode.FULL_ACCESS: (
        '--permission-mode',
        'bypassPermissions',
        '--allow-dangerously-skip-permissions',
    ),
}

_MODEL = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/\[\]-]{0,199}')


def _is_canonical_uuid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


def _text(value: object) -> bool:
    return isinstance(value, str) and value != ''


def claude_argv(thread: ThreadSpec, claude_bin: str) -> list[str]:
    if not _text(claude_bin):
        raise ValueError('claude_bin is empty')
    if not _is_canonical_uuid(thread.id):
        raise ValueError(f'thread id {thread.id!r} is not a canonical lowercase uuid')
    mode = thread.permission_mode
    if not isinstance(mode, str) or mode not in CLAUDE_MODES:
        raise ValueError(f'unknown permission mode {mode!r}')
    model = thread.model
    if model is not None and (not isinstance(model, str) or not _MODEL.fullmatch(model)):
        raise ValueError(f'model {model!r} is not a valid model name')
    workspace = thread.workspace_path
    if not isinstance(workspace, str) or not os.path.isabs(workspace):
        raise ValueError(f'workspace path {workspace!r} is not absolute')
    argv = [
        claude_bin,
        '-p',
        '--input-format', 'stream-json',
        '--output-format', 'stream-json',
        '--verbose',
        '--replay-user-messages',
        '--permission-prompt-tool', 'stdio',
        *CLAUDE_MODES[PermissionMode(mode)],
    ]
    if model is not None:
        argv += ['--model', model]
    argv += ['--disallowed-tools', 'AskUserQuestion']
    argv += ['--resume' if thread.resume is True else '--session-id', thread.id]
    return argv


def claude_env(thread: ThreadSpec, host: RunnerHost) -> dict[str, str]:
    env = {
        key: value
        for key, value in host.environment(thread).items()
        if key in ENV_ALLOWLIST and isinstance(value, str)
    }
    env['LOOM_THREAD'] = thread.id
    config_dir = host.setting('claude_config_dir')
    if _text(config_dir):
        env['CLAUDE_CONFIG_DIR'] = config_dir
    base_url = host.setting('agent_base_url')
    if _text(base_url):
        env['ANTHROPIC_BASE_URL'] = base_url
    token = host.secret('agent-token')
    if isinstance(token, str) and token.strip():
        env['ANTHROPIC_AUTH_TOKEN'] = token.strip()
    env['ANTHROPIC_API_KEY'] = ''
    return env
