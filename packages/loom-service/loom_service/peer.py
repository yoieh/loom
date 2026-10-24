import os
import socket
import struct
from pathlib import Path

from loom_core.errors import LoomError
from loom_core.service import Caller

MAX_CHAIN = 64
PROC = '/proc'
THREAD_ENTRY = b'LOOM_THREAD='

_CREDENTIALS = struct.Struct('3i')


def peer_credentials(sock: socket.socket) -> tuple[int, int, int]:
    data = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, _CREDENTIALS.size)
    return _CREDENTIALS.unpack(data)


def _parent(proc: Path, pid: int) -> int | None:
    try:
        text = (proc / str(pid) / 'stat').read_bytes()
        return int(text[text.rindex(b')') + 1:].split()[1])
    except (OSError, ValueError, IndexError):
        return None


def _environ(proc: Path, pid: int) -> bytes | None:
    try:
        return (proc / str(pid) / 'environ').read_bytes()
    except OSError:
        return None


def _loom_thread(environ: bytes) -> str | None:
    for entry in environ.split(b'\0'):
        if entry.startswith(THREAD_ENTRY) and len(entry) > len(THREAD_ENTRY):
            return entry[len(THREAD_ENTRY):].decode('utf-8', 'replace')
    return None


def caller_of(pid: int, proc: str | Path = PROC, server_pid: int | None = None) -> Caller:
    proc = Path(proc)
    server_pid = os.getpid() if server_pid is None else server_pid
    if pid <= 0:
        return Caller(verified=False)
    parent = _parent(proc, pid)
    own = _environ(proc, pid)
    if parent is None or own is None:
        return Caller(verified=False)
    chain = [pid]
    while parent and parent not in chain and len(chain) < MAX_CHAIN:
        chain.append(parent)
        parent = _parent(proc, parent)
    candidates = chain[:chain.index(server_pid)] if server_pid in chain else chain[:1]
    for candidate in candidates:
        environ = own if candidate == pid else _environ(proc, candidate)
        thread_id = None if environ is None else _loom_thread(environ)
        if thread_id is not None:
            return Caller(thread_id=thread_id)
    return Caller()


def identify(pid: int, uid: int, *, own_uid: int | None = None, proc: str | Path = PROC,
             server_pid: int | None = None) -> Caller:
    if uid != (os.geteuid() if own_uid is None else own_uid):
        raise LoomError('forbidden', 'the peer runs as another user')
    return caller_of(pid, proc, server_pid)
