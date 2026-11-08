import os
import stat
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol, TextIO

from .config import Config

ENV_KEYS = ('HOME', 'LANG', 'LOGNAME', 'PATH', 'SHELL', 'USER', 'XDG_RUNTIME_DIR')
SETTINGS = ('claude_bin', 'claude_config_dir', 'agent_base_url', 'codex_bin', 'codex_home',
            'codex_token_env', 't3_home', 't3_bin')
SECRETS = {'agent-token': 'agent-token', 't3-client': 't3-client.json'}
MAX_SECRET = 65_536


class Thread(Protocol):
    id: str


class Host:
    def __init__(self, data_dir: Path, socket: Path, config: Config,
                 environ: Mapping[str, str] | None = None, log: TextIO | None = None) -> None:
        self.data_dir = str(data_dir)
        self.socket = str(socket)
        self.config = config
        self._environ = dict(os.environ if environ is None else environ)
        self._log = sys.stderr if log is None else log

    def setting(self, key: str) -> str | None:
        return getattr(self.config, key) if key in SETTINGS else None

    def check_environment(self) -> dict[str, str]:
        env = {key: self._environ[key] for key in ENV_KEYS if key in self._environ}
        env['LOOM_SOCKET'] = self.socket
        return env

    def environment(self, thread: Thread) -> dict[str, str]:
        env = self.check_environment()
        env['LOOM_THREAD'] = thread.id
        return env

    def secret(self, name: str) -> str | None:
        file_name = SECRETS.get(name) if isinstance(name, str) else None
        if file_name is None:
            return None
        return self._read_token(Path(self.data_dir) / file_name)

    def _warn(self, message: str) -> None:
        self._log.write(f'loom: warning: {message}\n')
        self._log.flush()

    def _read_token(self, path: Path) -> str | None:
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
        try:
            fd = os.open(path, flags)
        except FileNotFoundError:
            return None
        except OSError as error:
            self._warn(f'not using {path}: {error.strerror}')
            return None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                self._warn(f'not using {path}: it is not a regular file')
                return None
            if info.st_mode & 0o077:
                mode = oct(stat.S_IMODE(info.st_mode))[2:]
                self._warn(f'not using {path}: its mode is {mode}, make it 600')
                return None
            data = os.read(fd, MAX_SECRET + 1)
        except OSError as error:
            self._warn(f'not using {path}: {error.strerror}')
            return None
        finally:
            os.close(fd)
        if len(data) > MAX_SECRET:
            self._warn(f'not using {path}: it is over {MAX_SECRET} bytes')
            return None
        try:
            token = data.decode('utf-8').strip()
        except UnicodeDecodeError:
            self._warn(f'not using {path}: it is not UTF-8')
            return None
        return token or None
