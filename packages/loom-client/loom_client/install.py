import copy
import importlib.util
import json
import os
import pwd
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Callable, Mapping
from typing import TextIO

from .output import escape

NAME = 'loom'
MODULE = 'loom_mcp'
ENV_VARS = ['LOOM_THREAD', 'LOOM_SOCKET', 'XDG_RUNTIME_DIR']
ALLOWED_ENV = (
    'PATH', 'HOME', 'USER', 'LOGNAME', 'SHELL', 'LANG', 'LC_ALL', 'LC_CTYPE', 'TERM', 'TMPDIR',
    'TZ', 'XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'XDG_CACHE_HOME', 'XDG_RUNTIME_DIR',
    'CLAUDE_CONFIG_DIR', 'CODEX_HOME',
)
TIMEOUT = 60.0
DETAIL = 500
HEADER = f'[mcp_servers.{NAME}]'
ENV_VARS_LINE = 'env_vars = ' + json.dumps(ENV_VARS) + '\n'

EXIT_FAILED = 1
EXIT_MISSING = 2

MISSING = object()

FindSpec = Callable[[str], object]


class Failure(Exception):
    pass


def command() -> list[str]:
    return [sys.executable, '-m', MODULE]


def child_env(environ: Mapping[str, str]) -> dict[str, str]:
    return {key: environ[key] for key in ALLOWED_ENV if key in environ}


def _home(environ: Mapping[str, str]) -> str:
    return environ.get('HOME') or pwd.getpwuid(os.getuid()).pw_dir


def claude_config(environ: Mapping[str, str]) -> str:
    if environ.get('CLAUDE_CONFIG_DIR'):
        return os.path.join(environ['CLAUDE_CONFIG_DIR'], '.claude.json')
    return os.path.join(_home(environ), '.claude.json')


def codex_config(environ: Mapping[str, str]) -> str:
    if environ.get('CODEX_HOME'):
        return os.path.join(environ['CODEX_HOME'], 'config.toml')
    return os.path.join(_home(environ), '.codex', 'config.toml')


def _detail(err: str, out: str) -> str:
    text = ' '.join(' '.join(part.split()) for part in (err, out) if part.strip())
    return escape(text[:DETAIL])


class Agent:
    def __init__(self, name: str, binary: str, env: dict[str, str], timeout: float) -> None:
        self.name = name
        self.binary = binary
        self.env = env
        self.timeout = timeout

    def run(self, *args: str) -> None:
        words = ' '.join((self.name, *args[:2]))
        try:
            process = subprocess.Popen(
                [self.binary, *args], env=self.env, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        except OSError as error:
            raise Failure(f'{words} failed: {escape(error.strerror or str(error))}') from None
        try:
            out, err = process.communicate(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            _kill(process)
            raise Failure(f'{words} failed (timed out after {self.timeout:g} s)') from None
        if process.returncode != 0:
            detail = _detail(err.decode('utf-8', 'replace'), out.decode('utf-8', 'replace'))
            raise Failure(f'{words} failed (exit {process.returncode}): {detail}')


def _kill(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.kill()
    process.wait()
    process.stdout.close()
    process.stderr.close()


def _matches(entry: object, registered: list[str]) -> bool:
    return (isinstance(entry, dict) and entry.get('command') == registered[0]
            and entry.get('args') == registered[1:])


def _claude_entry(path: str) -> object:
    try:
        with open(path, 'rb') as file:
            data = json.load(file)
    except (OSError, ValueError, RecursionError):
        return MISSING
    servers = data.get('mcpServers') if isinstance(data, dict) else None
    if not isinstance(servers, dict):
        return MISSING
    return servers.get(NAME, MISSING)


def install_claude(agent: Agent, environ: Mapping[str, str], registered: list[str]) -> str:
    entry = _claude_entry(claude_config(environ))
    if _matches(entry, registered):
        return 'loom is already registered'
    if entry is not MISSING:
        agent.run('mcp', 'remove', '--scope', 'user', NAME)
    agent.run('mcp', 'add', '--scope', 'user', NAME, '--', *registered)
    return 'registered loom'


def _read_toml(path: str) -> tuple[str | None, dict]:
    try:
        with open(path, 'rb') as file:
            raw = file.read()
    except FileNotFoundError:
        return None, {}
    except OSError as error:
        raise Failure(f'cannot read {path}: {error.strerror or error}') from None
    try:
        text = raw.decode('utf-8')
        return text, tomllib.loads(text)
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, RecursionError) as error:
        raise Failure(f'cannot read {path}: {escape(str(error))}') from None


def _codex_entry(data: dict) -> object:
    servers = data.get('mcp_servers')
    if not isinstance(servers, dict):
        return MISSING
    return servers.get(NAME, MISSING)


def _is_header(line: str) -> bool:
    return line.lstrip().startswith('[')


def _is_key(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith('#')


def _drop_env_vars(lines: list[str]) -> list[str]:
    kept: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.lstrip().startswith('env_vars') and \
                line.lstrip()[len('env_vars'):].lstrip().startswith('='):
            end = index + 1
            while end < len(lines):
                try:
                    tomllib.loads(''.join(lines[index:end]))
                    break
                except tomllib.TOMLDecodeError:
                    end += 1
            index = end
            continue
        kept.append(line)
        index += 1
    return kept


def with_env_vars(text: str) -> str | None:
    lines = text.splitlines(keepends=True)
    starts = [index for index, line in enumerate(lines) if line.strip() == HEADER]
    if len(starts) != 1:
        return None
    start = starts[0] + 1
    end = start
    while end < len(lines) and not _is_header(lines[end]):
        end += 1
    body = _drop_env_vars(lines[start:end])
    keys = [index for index, line in enumerate(body) if _is_key(line)]
    at = keys[-1] + 1 if keys else 0
    before = body[:at]
    if before and not before[-1].endswith('\n'):
        before[-1] += '\n'
    elif not before and not lines[start - 1].endswith('\n'):
        lines[start - 1] += '\n'
    return ''.join(lines[:start] + before + [ENV_VARS_LINE] + body[at:] + lines[end:])


def _without_env_vars(data: dict) -> dict:
    data = copy.deepcopy(data)
    data['mcp_servers'][NAME].pop('env_vars', None)
    return data


def _checked(text: str, data: dict) -> bool:
    try:
        new = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return False
    entry = _codex_entry(new)
    if not isinstance(entry, dict) or entry.get('env_vars') != ENV_VARS:
        return False
    return _without_env_vars(new) == _without_env_vars(data)


def _replace(path: str, text: str) -> None:
    bits = stat.S_IMODE(os.stat(path).st_mode)
    handle, temp = tempfile.mkstemp(dir=os.path.dirname(path), prefix='.config.toml.')
    try:
        with os.fdopen(handle, 'wb') as file:
            file.write(text.encode('utf-8'))
            file.flush()
            os.fsync(file.fileno())
        os.chmod(temp, bits)
        os.replace(temp, path)
    except BaseException:
        try:
            os.unlink(temp)
        except FileNotFoundError:
            pass
        raise


def _set_env_vars(path: str) -> None:
    failure = Failure(f'cannot set env_vars in {path}')
    text, data = _read_toml(path)
    if text is None or not isinstance(_codex_entry(data), dict):
        raise failure
    new = with_env_vars(text)
    if new is None or not _checked(new, data):
        raise failure
    if new != text:
        try:
            _replace(path, new)
        except OSError as error:
            raise Failure(f'cannot set env_vars in {path}: '
                          f'{error.strerror or error}') from None


def install_codex(agent: Agent, environ: Mapping[str, str], registered: list[str]) -> str:
    path = codex_config(environ)
    _, data = _read_toml(path)
    entry = _codex_entry(data)
    command_matches = _matches(entry, registered)
    if command_matches and entry.get('env_vars') == ENV_VARS:
        return 'loom is already registered'
    if entry is not MISSING and not command_matches:
        agent.run('mcp', 'remove', NAME)
    if not command_matches:
        agent.run('mcp', 'add', NAME, '--', *registered)
    _set_env_vars(path)
    return 'registered loom'


def _write(stream: TextIO, line: str) -> None:
    stream.write(line + '\n')
    stream.flush()


INSTALLERS = (('claude', install_claude), ('codex', install_codex))


def install(environ: Mapping[str, str] = os.environ, out: TextIO | None = None,
            err: TextIO | None = None, find_spec: FindSpec = importlib.util.find_spec,
            timeout: float = TIMEOUT) -> int:
    out = sys.stdout if out is None else out
    err = sys.stderr if err is None else err
    if find_spec(MODULE) is None:
        _write(err, 'loom: loom-mcp is not installed')
        return EXIT_MISSING
    registered = command()
    env = child_env(environ)
    search = environ.get('PATH', os.defpath)
    code = 0
    for name, installer in INSTALLERS:
        binary = shutil.which(name, path=search)
        if binary is None:
            _write(out, f'{name}: not found, skipped')
            continue
        try:
            line = installer(Agent(name, binary, env, timeout), environ, registered)
        except Failure as failure:
            _write(err, f'{name}: {failure}')
            code = EXIT_FAILED
            continue
        _write(out, f'{name}: {line}')
    return code
