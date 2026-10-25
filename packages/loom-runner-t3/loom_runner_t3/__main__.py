import argparse
import os
import sys
import time
import tomllib
from collections.abc import Callable, Mapping, Sequence
from typing import TextIO

from .mcp import HttpCall, http_call
from .pairing import RunT3, Settings, Tools, pair, run_t3, status, unpair

COMMANDS = {'pair': pair, 'unpair': unpair, 'status': status}
CONFIG_KEYS = ('t3_home', 't3_bin')


class ConfigError(Exception):
    pass


def _home(environ: Mapping[str, str]) -> str:
    return environ.get('HOME') or os.path.expanduser('~')


def _xdg(environ: Mapping[str, str], name: str, fallback: str) -> str:
    value = environ.get(name)
    base = os.path.abspath(value) if value else os.path.join(_home(environ), fallback)
    return os.path.join(base, 'loom')


def data_dir_path(option: str | None, environ: Mapping[str, str]) -> str:
    if option is not None:
        return os.path.abspath(option)
    return _xdg(environ, 'XDG_DATA_HOME', '.local/share')


def config_path(option: str | None, environ: Mapping[str, str]) -> str:
    if option is not None:
        return os.path.abspath(option)
    return os.path.join(_xdg(environ, 'XDG_CONFIG_HOME', '.config'), 'config.toml')


def load_config(path: str) -> dict[str, str | None]:
    try:
        with open(path, 'rb') as file:
            data = file.read()
    except FileNotFoundError:
        return {key: None for key in CONFIG_KEYS}
    except OSError as error:
        raise ConfigError(f'{path}: {error.strerror or type(error).__name__}') from None
    try:
        values = tomllib.loads(data.decode('utf-8'))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ConfigError(f'{path}: {error}') from None
    found: dict[str, str | None] = {}
    for key in CONFIG_KEYS:
        value = values.get(key)
        if value is not None and not isinstance(value, str):
            raise ConfigError(f'{path}: {key} must be a string')
        found[key] = value or None
    return found


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='loom t3', description='Pair Loom with T3.')
    commands = parser.add_subparsers(dest='command', required=True, metavar='command')
    for name, text in (('pair', 'pair Loom with the T3 that is running'),
                       ('unpair', 'revoke the pairing and delete it'),
                       ('status', 'say whether Loom is paired and for how long')):
        command = commands.add_parser(name, help=text)
        command.add_argument('--data-dir', help='data dir (default $XDG_DATA_HOME/loom)')
        command.add_argument('--config',
                             help='config file (default $XDG_CONFIG_HOME/loom/config.toml)')
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    http: HttpCall = http_call,
    run: RunT3 = run_t3,
    clock: Callable[[], float] = time.time,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    environ = os.environ if environ is None else environ
    out = sys.stdout if out is None else out
    err = sys.stderr if err is None else err
    try:
        args = _parser().parse_args(argv)
    except SystemExit as stop:
        return stop.code if isinstance(stop.code, int) else 2
    for option, value in (('--data-dir', args.data_dir), ('--config', args.config)):
        if value is not None and not value:
            err.write(f'loom: {option} must not be empty\n')
            return 2
    path = config_path(args.config, environ)
    try:
        config = load_config(path)
    except ConfigError as error:
        err.write(f'loom: {error}\n')
        return 2
    settings = Settings(
        data_dir=data_dir_path(args.data_dir, environ),
        t3_home=config['t3_home'] or os.path.join(_home(environ), '.t3'),
        t3_bin=config['t3_bin'],
        environ=environ,
    )
    return COMMANDS[args.command](settings, Tools(http, run, clock), out, err)


if __name__ == '__main__':
    raise SystemExit(main())
