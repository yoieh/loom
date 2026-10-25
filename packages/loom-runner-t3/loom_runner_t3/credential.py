import contextlib
import dataclasses
import json
import os
import re
import stat

FILE_NAME = 't3-client.json'
MAX_FILE = 65_536

FIELDS = ('origin', 'access_token', 'expires_at', 'session_id', 'client_id', 'scope')

_LOOPBACK = re.compile(r'http://(?:127\.0\.0\.1|localhost|\[::1\]):([0-9]{1,5})')
_TOKEN = re.compile(r'[\x21-\x7e]{1,4096}')
_SESSION_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,199}')

_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


class CredentialError(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class Credential:
    origin: str
    access_token: str = dataclasses.field(repr=False)
    expires_at: int
    session_id: str
    client_id: str
    scope: str

    def expired(self, now: float) -> bool:
        return now >= self.expires_at

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self), indent=2) + '\n'


def is_loopback_origin(origin: object) -> bool:
    if not isinstance(origin, str):
        return False
    match = _LOOPBACK.fullmatch(origin)
    return match is not None and 1 <= int(match.group(1)) <= 65535


def is_session_id(value: object) -> bool:
    return isinstance(value, str) and _SESSION_ID.fullmatch(value) is not None


def is_token(value: object) -> bool:
    return isinstance(value, str) and _TOKEN.fullmatch(value) is not None


def _object(text: object) -> dict | None:
    if not isinstance(text, str):
        return None
    try:
        data = json.loads(text)
    except (ValueError, RecursionError):
        return None
    return data if isinstance(data, dict) else None


def parse_credential(text: object) -> Credential | None:
    data = _object(text)
    if data is None:
        return None
    values = {name: data.get(name) for name in FIELDS}
    expires_at = values['expires_at']
    if isinstance(expires_at, bool) or not isinstance(expires_at, int):
        return None
    if not all(isinstance(values[name], str) for name in FIELDS if name != 'expires_at'):
        return None
    if not is_loopback_origin(values['origin']) or not is_token(values['access_token']):
        return None
    if not is_session_id(values['session_id']) or not values['client_id']:
        return None
    return Credential(**values)


def usable_credential(text: object, now: float) -> Credential | None:
    credential = parse_credential(text)
    if credential is None or credential.expired(now):
        return None
    return credential


def stored_session_id(text: object) -> str | None:
    data = _object(text)
    session_id = data.get('session_id') if data is not None else None
    return session_id if is_session_id(session_id) else None


def credential_path(data_dir: str) -> str:
    return os.path.join(data_dir, FILE_NAME)


def _read_at_most(fd: int, size: int) -> bytes:
    data = bytearray()
    while len(data) < size and (chunk := os.read(fd, size - len(data))):
        data += chunk
    return bytes(data)


def read_credential_file(data_dir: str) -> str | None:
    path = credential_path(data_dir)
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise CredentialError(f'{path}: {error.strerror or type(error).__name__}') from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise CredentialError(f'{path}: it is not a regular file')
        if info.st_mode & 0o077:
            mode = oct(stat.S_IMODE(info.st_mode))[2:]
            raise CredentialError(f'{path}: its mode is {mode}, make it 600')
        data = _read_at_most(fd, MAX_FILE + 1)
    except OSError as error:
        raise CredentialError(f'{path}: {error.strerror or type(error).__name__}') from None
    finally:
        os.close(fd)
    if len(data) > MAX_FILE:
        raise CredentialError(f'{path}: it is over {MAX_FILE} bytes')
    try:
        return data.decode('utf-8').strip()
    except UnicodeDecodeError:
        raise CredentialError(f'{path}: it is not UTF-8') from None


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def write_credential_file(data_dir: str, credential: Credential) -> None:
    with contextlib.suppress(FileExistsError):
        os.makedirs(data_dir, 0o700)
    dir_fd = os.open(data_dir, _DIR_FLAGS)
    try:
        temp = f'.{FILE_NAME}.tmp'
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temp, dir_fd=dir_fd)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
        fd = os.open(temp, flags, 0o600, dir_fd=dir_fd)
        try:
            try:
                os.fchmod(fd, 0o600)
                _write_all(fd, credential.to_json().encode('utf-8'))
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(temp, FILE_NAME, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(temp, dir_fd=dir_fd)
            raise
    finally:
        os.close(dir_fd)


def delete_credential_file(data_dir: str) -> bool:
    try:
        os.unlink(credential_path(data_dir))
    except FileNotFoundError:
        return False
    return True
