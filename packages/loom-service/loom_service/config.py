import dataclasses
import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

MODES = ('approval-required', 'auto-accept-edits', 'auto', 'full-access')
DEFAULT_MODE = 'auto'
SOCKET_NAME = 'api.sock'
CONFIG_NAME = 'config.toml'
TOKEN_ENV = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,127}')
# Variables Loom sets itself, which the Codex token must not replace.
NOT_TOKEN_ENV = ('HOME', 'LANG', 'LOGNAME', 'PATH', 'SHELL', 'USER', 'XDG_RUNTIME_DIR',
                 'LOOM_SOCKET', 'LOOM_THREAD', 'CODEX_HOME')


class StartError(Exception):
    pass


@dataclass(frozen=True)
class Config:
    claude_bin: str | None = None
    claude_config_dir: str | None = None
    agent_base_url: str | None = None
    permission_mode: str = DEFAULT_MODE
    agents_max: int | None = None
    idle_after: int = 1200
    request_timeout: int = 600
    codex_bin: str | None = None
    codex_home: str | None = None
    codex_token_env: str | None = None
    t3_home: str | None = None
    t3_bin: str | None = None


KEYS = tuple(field.name for field in dataclasses.fields(Config))
WHOLE_NUMBERS = ('agents_max', 'idle_after', 'request_timeout')


def _option(value: str | None, name: str) -> Path | None:
    if value is None:
        return None
    if not value:
        raise StartError(f'{name} must not be empty')
    return Path(os.path.abspath(value))


def _home(environ: Mapping[str, str]) -> Path:
    home = environ.get('HOME')
    return Path(home) if home else Path.home()


def _xdg(environ: Mapping[str, str], name: str, fallback: str) -> Path:
    value = environ.get(name)
    base = Path(os.path.abspath(value)) if value else _home(environ) / fallback
    return base / 'loom'


def socket_path(option: str | None, environ: Mapping[str, str]) -> Path:
    path = _option(option, '--socket')
    if path is not None:
        return path
    runtime = environ.get('XDG_RUNTIME_DIR')
    if not runtime:
        raise StartError('no socket path: XDG_RUNTIME_DIR is not set, so pass --socket')
    return Path(os.path.abspath(runtime)) / 'loom' / SOCKET_NAME


def data_dir_path(option: str | None, environ: Mapping[str, str]) -> Path:
    path = _option(option, '--data-dir')
    return path if path is not None else _xdg(environ, 'XDG_DATA_HOME', '.local/share')


def config_path(option: str | None, environ: Mapping[str, str]) -> Path:
    path = _option(option, '--config')
    if path is not None:
        return path
    return _xdg(environ, 'XDG_CONFIG_HOME', '.config') / CONFIG_NAME


def load_config(path: Path) -> Config:
    try:
        data = Path(path).read_bytes()
    except FileNotFoundError:
        return Config()
    except OSError as error:
        raise StartError(f'cannot read {path}: {error.strerror or error}') from None
    try:
        values = tomllib.loads(data.decode('utf-8'))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise StartError(f'{path}: {error}') from None
    numbers = {key: values.pop(key) for key in WHOLE_NUMBERS if key in values}
    for key, value in numbers.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise StartError(f'{path}: {key} must be a whole number of 1 or more')
    for key, value in values.items():
        if key not in KEYS:
            raise StartError(f'{path}: unknown key {key!r}')
        if not isinstance(value, str):
            raise StartError(f'{path}: {key} must be a string')
    if values.get('permission_mode', DEFAULT_MODE) not in MODES:
        raise StartError(f'{path}: permission_mode must be one of {", ".join(MODES)}')
    name = values.get('codex_token_env')
    if name is not None and (not TOKEN_ENV.fullmatch(name) or name in NOT_TOKEN_ENV
                             or name.startswith(('ANTHROPIC_', 'CLAUDE'))):
        raise StartError(f'{path}: codex_token_env must be an environment variable name that '
                         'Loom does not set itself')
    values.update(numbers)
    return Config(**values)
