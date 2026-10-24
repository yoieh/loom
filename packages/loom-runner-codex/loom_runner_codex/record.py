import contextlib
import os
import stat

from .command import is_canonical_uuid

RECORD_DIR = 'codex'

MAX_RECORD = 4096

_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


class RecordError(Exception):
    pass


def record_dir(data_dir: str) -> str:
    return os.path.join(data_dir, RECORD_DIR)


def _read_at_most(fd: int, size: int) -> bytes:
    data = bytearray()
    while len(data) < size and (chunk := os.read(fd, size - len(data))):
        data += chunk
    return bytes(data)


def _read_file(dir_fd: int, thread_id: str) -> bytes | None:
    try:
        fd = os.open(thread_id, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dir_fd)
    except FileNotFoundError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise RecordError('the record is not a regular file')
        return _read_at_most(fd, MAX_RECORD + 1)
    finally:
        os.close(fd)


def read_record(data_dir: str, thread_id: str) -> str | None:
    try:
        dir_fd = os.open(record_dir(data_dir), _DIR_FLAGS)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise RecordError('the record directory is unreadable') from error
    try:
        data = _read_file(dir_fd, thread_id)
    except OSError as error:
        raise RecordError('the record is unreadable') from error
    finally:
        os.close(dir_fd)
    if data is None:
        return None
    if len(data) > MAX_RECORD:
        raise RecordError('the record is too long')
    try:
        codex_id = data.decode('utf-8').strip()
    except UnicodeDecodeError as error:
        raise RecordError('the record is not utf-8') from error
    if not is_canonical_uuid(codex_id):
        raise RecordError('the record holds no codex thread id')
    return codex_id


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def write_record(data_dir: str, thread_id: str, codex_id: str) -> None:
    if not is_canonical_uuid(thread_id) or not is_canonical_uuid(codex_id):
        raise ValueError('a record holds only canonical lowercase uuids')
    directory = record_dir(data_dir)
    with contextlib.suppress(FileExistsError):
        os.mkdir(directory, 0o700)
    dir_fd = os.open(directory, _DIR_FLAGS)
    try:
        if stat.S_IMODE(os.fstat(dir_fd).st_mode) & ~0o700:
            os.fchmod(dir_fd, 0o700)
        temp = f'.{thread_id}.tmp'
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temp, dir_fd=dir_fd)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        fd = os.open(temp, flags, 0o600, dir_fd=dir_fd)
        try:
            try:
                _write_all(fd, f'{codex_id}\n'.encode('ascii'))
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(temp, thread_id, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(temp, dir_fd=dir_fd)
            raise
    finally:
        os.close(dir_fd)
