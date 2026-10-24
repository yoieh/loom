import argparse
import asyncio
import os
import signal
import sqlite3
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import TextIO

from loom_core.orchestrator import Orchestrator
from loom_core.projects import Projects
from loom_core.service import Service
from loom_core.store import Store
from loom_runner_claude import ClaudeRunner

from .config import Config, StartError, config_path, data_dir_path, load_config, socket_path
from .host import Host
from .peer import PROC
from .server import MAX_LINE, Server


class Serving:
    def __init__(self, server: Server, service: Service, log: TextIO) -> None:
        self.server = server
        self.service = service
        self.log = log
        self._closed = False

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self.server.close()
            await self.service.close()
        finally:
            self.server.unlink()
        self.log.write('loom: stopped\n')
        self.log.flush()


async def start(socket: Path, data_dir: Path, config: Config, *,
                environ: Mapping[str, str] | None = None, log: TextIO | None = None,
                max_line: int = MAX_LINE, proc: str | Path = PROC) -> Serving:
    log = sys.stderr if log is None else log
    server = Server(socket, max_line=max_line, log=log, proc=proc)
    server.bind()
    try:
        store = Store.open(data_dir)
    except (OSError, sqlite3.Error) as error:
        await server.close()
        server.unlink()
        raise StartError(f'cannot open the store in {data_dir}: {error}') from None
    projects = Projects(store, default_mode=config.permission_mode)
    host = Host(data_dir, socket, config, environ, log)
    orchestrator = Orchestrator(store, projects, {'claude': ClaudeRunner()}, host)
    await orchestrator.recover()
    service = Service(store, projects, orchestrator)
    await server.start(service)
    log.write(f'loom: serving on {socket}\n')
    log.flush()
    return Serving(server, service, log)


async def _serve(socket: Path, data_dir: Path, config: Config) -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    serving = await start(socket, data_dir, config)
    try:
        await stop.wait()
    finally:
        await serving.close()
    return 0


def parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='loom')
    commands = parser.add_subparsers(dest='command', required=True)
    serve = commands.add_parser('serve', help='serve the Loom API on a Unix socket')
    serve.add_argument('--socket', help='socket path (default $XDG_RUNTIME_DIR/loom/api.sock)')
    serve.add_argument('--data-dir', help='data dir (default $XDG_DATA_HOME/loom)')
    serve.add_argument('--config', help='config file (default $XDG_CONFIG_HOME/loom/config.toml)')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        socket = socket_path(args.socket, os.environ)
        data_dir = data_dir_path(args.data_dir, os.environ)
        config = load_config(config_path(args.config, os.environ))
        return asyncio.run(_serve(socket, data_dir, config))
    except StartError as error:
        sys.stderr.write(f'loom: {error}\n')
        return 2
