import contextlib
import os
import re
import stat
import uuid

RECORD_DIR = 't3'

MAX_RECORD = 4096

_T3_ID = re.compile(r'mcp:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})')

_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


class RecordError(Exception):
    pass


def is_canonical_uuid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


def is_t3_thread_id(value: object) -> bool:
    return isinstance(value, str) and _T3_ID.fullmatch(value) is not None


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
    if not is_canonical_uuid(thread_id):
        raise RecordError('the loom thread id is not a canonical lowercase uuid')
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
        t3_id = data.decode('utf-8').strip()
    except UnicodeDecodeError as error:
        raise RecordError('the record is not utf-8') from error
    if not is_t3_thread_id(t3_id):
        raise RecordError('the record holds no T3 thread id')
    return t3_id


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def write_record(data_dir: str, thread_id: str, t3_id: str) -> None:
    if not is_canonical_uuid(thread_id) or not is_t3_thread_id(t3_id):
        raise ValueError('a record holds only a T3 thread id under a canonical loom thread id')
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
                _write_all(fd, f'{t3_id}\n'.encode('ascii'))
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
