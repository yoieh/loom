import base64
import dataclasses
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import subprocess
import time
import urllib.parse
from collections.abc import Callable, Mapping
from typing import TextIO

from .credential import (
    Credential,
    CredentialError,
    credential_path,
    delete_credential_file,
    is_loopback_origin,
    is_session_id,
    is_token,
    parse_credential,
    read_credential_file,
    stored_session_id,
    write_credential_file,
)
from .mcp import TIMEOUT, HttpCall, read_runtime
from .reasons import safe_reason

CLIENT_NAME = 'Loom'
REDIRECT = 'http://127.0.0.1/loom/callback'
SCOPES = ('orchestration:read', 'orchestration:operate', 'settings:write',
          'source-control:write')
PAIRING_LABEL = 'Loom pairing'
CONNECTIONS = 'T3 Settings → Connections'
CLI_TIMEOUT = 30
MAX_ANSWER = 1_048_576
DAY = 86400
WARN_DAYS = 7
ENV_KEYS = ('HOME', 'PATH', 'LANG', 'USER', 'LOGNAME')
HOME_VARIABLE = 'T3CODE_HOME'

_VERSION = re.compile(r'[0-9A-Za-z][0-9A-Za-z.+_-]{0,63}')

RunT3 = Callable[[list[str], dict[str, str]], tuple[int, bytes]]


class PairingFailed(Exception):
    pass


def run_t3(argv: list[str], env: dict[str, str]) -> tuple[int, bytes]:
    done = subprocess.run(argv, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL, timeout=CLI_TIMEOUT, check=False)
    return done.returncode, done.stdout


@dataclasses.dataclass(frozen=True)
class Settings:
    data_dir: str
    t3_home: str
    t3_bin: str | None
    environ: Mapping[str, str]


@dataclasses.dataclass
class Tools:
    http: HttpCall
    run: RunT3
    clock: Callable[[], float] = time.time


def days_left(expires_at: int, now: float) -> int:
    return math.floor((expires_at - now) / DAY)


def _t3_env(settings: Settings) -> dict[str, str]:
    env = {key: settings.environ[key] for key in ENV_KEYS if key in settings.environ}
    env[HOME_VARIABLE] = settings.t3_home
    return env


def t3_bin(settings: Settings) -> str:
    if settings.t3_bin:
        return settings.t3_bin
    runtime = read_runtime(settings.t3_home)
    version = runtime.get('serverVersion') if runtime is not None else None
    if not isinstance(version, str) or not _VERSION.fullmatch(version) or '..' in version:
        raise PairingFailed('T3 names no server version, so its t3 CLI is unknown; '
                            'set t3_bin')
    return os.path.join(settings.t3_home, 'runtime', 'versions', version, 't3')


def _cli(settings: Settings, tools: Tools, step: str, *args: str) -> object:
    binary = t3_bin(settings)
    try:
        code, out = tools.run([binary, *args], _t3_env(settings))
    except subprocess.TimeoutExpired:
        raise PairingFailed(f'{step}: t3 took longer than {CLI_TIMEOUT} s') from None
    except OSError as error:
        why = error.strerror or type(error).__name__
        raise PairingFailed(f'{step}: could not run {binary}: {why}') from None
    if code != 0:
        raise PairingFailed(f'{step}: t3 exited with code {code}')
    try:
        return json.loads(out.decode('utf-8'))
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise PairingFailed(f'{step}: t3 printed no JSON') from None


def _session_list(settings: Settings, tools: Tools) -> list:
    sessions = _cli(settings, tools, 'session-list', 'auth', 'session', 'list', '--json',
                    '--base-dir', settings.t3_home)
    if not isinstance(sessions, list):
        raise PairingFailed('session-list: t3 printed no list')
    return sessions


def _session_ids(sessions: list) -> set[str]:
    return {entry['sessionId'] for entry in sessions
            if isinstance(entry, dict) and isinstance(entry.get('sessionId'), str)}


def _new_session(sessions: list, before: set[str]) -> str:
    found = []
    for entry in sessions:
        if not isinstance(entry, dict) or not isinstance(entry.get('client'), dict):
            continue
        if entry['client'].get('label') != CLIENT_NAME:
            continue
        session_id = entry.get('sessionId')
        if isinstance(session_id, str) and session_id in before:
            continue
        found.append(session_id)
    if len(found) != 1:
        raise PairingFailed(f'session-list: found {len(found)} new {CLIENT_NAME!r} sessions, '
                            f'not 1; remove them in {CONNECTIONS}')
    if not is_session_id(found[0]):
        raise PairingFailed(f'session-list: the new session has no id; remove it in '
                            f'{CONNECTIONS}')
    return found[0]


def revoke(settings: Settings, tools: Tools, session_id: str) -> None:
    binary = t3_bin(settings)
    argv = [binary, 'auth', 'session', 'revoke', '--base-dir', settings.t3_home, session_id]
    try:
        code, _ = tools.run(argv, _t3_env(settings))
    except subprocess.TimeoutExpired:
        raise PairingFailed(f't3 took longer than {CLI_TIMEOUT} s') from None
    except OSError as error:
        raise PairingFailed(f'could not run {binary}: '
                            f'{error.strerror or type(error).__name__}') from None
    if code != 0:
        raise PairingFailed(f't3 exited with code {code}')


class _Oauth:
    def __init__(self, settings: Settings, tools: Tools, origin: str) -> None:
        self.settings = settings
        self.tools = tools
        self.origin = origin

    def send(self, step: str, method: str, path: str, body: bytes | None,
             content_type: str | None) -> tuple[int, dict, bytes]:
        runtime = read_runtime(self.settings.t3_home)
        if runtime is None or runtime.get('origin') != self.origin:
            raise PairingFailed(f'{step}: T3 is not running at {self.origin}')
        headers = {'Accept': 'application/json'}
        if content_type is not None:
            headers['Content-Type'] = content_type
        try:
            status, answer, data = self.tools.http(method, self.origin + path, headers, body,
                                                   TIMEOUT)
        except TimeoutError:
            raise PairingFailed(f'{step}: T3 did not answer within {TIMEOUT:g} s') from None
        except Exception as error:
            why = f'{step}: T3 is not reachable ({type(error).__name__})'
            raise PairingFailed(why) from None
        if not isinstance(answer, dict):
            answer = {}
        return status, answer, data if isinstance(data, bytes) else b''

    def json(self, step: str, path: str, payload: dict) -> tuple[int, object]:
        status, _, data = self.send(step, 'POST', path, json.dumps(payload).encode('utf-8'),
                                    'application/json')
        return status, _parse(data)

    def form(self, step: str, path: str, fields: dict[str, str]) -> tuple[int, object]:
        body = urllib.parse.urlencode(fields).encode('ascii')
        status, _, data = self.send(step, 'POST', path, body,
                                    'application/x-www-form-urlencoded')
        return status, _parse(data)


def _parse(data: bytes) -> object:
    if len(data) > MAX_ANSWER:
        return None
    try:
        return json.loads(data.decode('utf-8'))
    except (UnicodeDecodeError, ValueError, RecursionError):
        return None


def _field(body: object, name: str) -> object:
    return body.get(name) if isinstance(body, dict) else None


def _status(step: str, status: object, wanted: tuple[int, ...]) -> None:
    if status not in wanted:
        raise PairingFailed(f'{step}: T3 answered {status}')


def _check_redirect(redirect_to: str, state: str, issuer: str) -> str:
    parts = urllib.parse.urlsplit(redirect_to)
    query = urllib.parse.parse_qs(parts.query)
    if f'{parts.scheme}://{parts.netloc}{parts.path}' != REDIRECT:
        raise PairingFailed('decision: the answer goes to another redirect URI')
    got_state = query.get('state', [''])
    if len(got_state) != 1 or not hmac.compare_digest(got_state[0].encode(), state.encode()):
        raise PairingFailed('decision: the answer has another state')
    issuers = query.get('iss', [''])
    if len(issuers) != 1 or issuers[0].rstrip('/') != issuer:
        raise PairingFailed('decision: the answer names another issuer')
    codes = query.get('code', [''])
    if len(codes) != 1 or not codes[0]:
        raise PairingFailed('decision: the answer has no code')
    return codes[0]


@dataclasses.dataclass
class _Secrets:
    values: list[str] = dataclasses.field(default_factory=list)

    def add(self, value: object) -> None:
        if isinstance(value, str) and value:
            self.values.append(value)


def _flow(settings: Settings, tools: Tools, origin: str, kept: _Secrets) -> tuple[dict, str]:
    oauth = _Oauth(settings, tools, origin)
    before = _session_ids(_session_list(settings, tools))
    status, body = oauth.json('register', '/oauth/mcp/register', {
        'client_name': CLIENT_NAME, 'redirect_uris': [REDIRECT],
        'token_endpoint_auth_method': 'none'})
    _status('register', status, (200, 201))
    client_id = _field(body, 'client_id')
    if not isinstance(client_id, str) or not client_id:
        raise PairingFailed('register: the answer has no client_id')
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode('ascii')).digest()).rstrip(b'=').decode('ascii')
    state = secrets.token_urlsafe(24)
    for value in (client_id, verifier, challenge, state):
        kept.add(value)
    authorization = {
        'response_type': 'code', 'client_id': client_id, 'redirect_uri': REDIRECT,
        'code_challenge': challenge, 'code_challenge_method': 'S256', 'state': state,
        'resource': origin + '/mcp'}
    status, headers, _ = oauth.send(
        'authorize', 'GET', '/oauth/mcp/authorize?' + urllib.parse.urlencode(authorization),
        None, None)
    _status('authorize', status, (301, 302, 303, 307, 308))
    location = next((value for key, value in headers.items()
                     if isinstance(key, str) and key.lower() == 'location'), None)
    if not isinstance(location, str) or not location:
        raise PairingFailed('authorize: the answer has no Location')
    status, _ = oauth.json('approval', '/oauth/mcp/approval', authorization)
    _status('approval', status, (200,))
    scopes = [part for scope in SCOPES for part in ('--scope', scope)]
    created = _cli(settings, tools, 'pairing-create', 'auth', 'pairing', 'create', *scopes,
                   '--ttl', '5m', '--label', PAIRING_LABEL, '--json',
                   '--base-dir', settings.t3_home)
    code = _field(created, 'credential')
    if not isinstance(code, str) or not code:
        raise PairingFailed('pairing-create: t3 gave no pairing code')
    kept.add(code)
    status, body = oauth.json('decision', '/oauth/mcp/decision', {
        'authorization': authorization,
        'decision': {'_tag': 'pairing-code', 'access': 'full-access', 'code': code}})
    _status('decision', status, (200,))
    redirect_to = _field(body, 'redirectTo')
    if not isinstance(redirect_to, str) or not redirect_to:
        raise PairingFailed('decision: the answer has no redirectTo')
    kept.add(redirect_to)
    auth_code = _check_redirect(redirect_to, state, origin)
    kept.add(auth_code)
    status, body = oauth.form('token', '/oauth/mcp/token', {
        'grant_type': 'authorization_code', 'code': auth_code, 'redirect_uri': REDIRECT,
        'client_id': client_id, 'code_verifier': verifier, 'resource': origin + '/mcp'})
    _status('token', status, (200,))
    token, expires_in = _field(body, 'access_token'), _field(body, 'expires_in')
    kept.add(token)
    if not is_token(token):
        raise PairingFailed('token: the answer has no usable access_token')
    if isinstance(expires_in, bool) or not isinstance(expires_in, int) or expires_in <= 0:
        raise PairingFailed('token: the answer has no usable expires_in')
    token_type = _field(body, 'token_type')
    if token_type is not None and (not isinstance(token_type, str)
                                   or token_type.lower() != 'bearer'):
        raise PairingFailed('token: the token is not a bearer token')
    scope = _field(body, 'scope')
    session_id = _new_session(_session_list(settings, tools), before)
    return ({'access_token': token, 'expires_in': expires_in, 'client_id': client_id,
             'scope': scope if isinstance(scope, str) else ' '.join(SCOPES)}, session_id)


def pair(settings: Settings, tools: Tools, out: TextIO, err: TextIO) -> int:
    kept = _Secrets()
    try:
        old_text = read_credential_file(settings.data_dir)
    except CredentialError:
        old_text = ''
    runtime = read_runtime(settings.t3_home)
    origin = runtime.get('origin') if runtime is not None else None
    if not isinstance(origin, str):
        err.write('loom: t3: T3 is not running (no origin in its server-runtime.json)\n')
        return 1
    if not is_loopback_origin(origin):
        err.write(safe_reason(f'loom: t3: pairing failed: T3 is at {origin!r}, which is not '
                              f'a loopback address', ()) + '\n')
        return 1
    try:
        token, session_id = _flow(settings, tools, origin, kept)
        credential = Credential(
            origin=origin,
            access_token=token['access_token'],
            expires_at=math.floor(tools.clock()) + token['expires_in'],
            session_id=session_id,
            client_id=token['client_id'],
            scope=token['scope'],
        )
        write_credential_file(settings.data_dir, credential)
    except PairingFailed as error:
        err.write(safe_reason(f'loom: t3: pairing failed: {error}', kept.values) + '\n')
        return 1
    except OSError as error:
        why = error.strerror or type(error).__name__
        err.write(f'loom: t3: pairing failed: could not write '
                  f'{credential_path(settings.data_dir)}: {why}\n')
        return 1
    days = token['expires_in'] // DAY
    out.write(f"Paired as '{CLIENT_NAME}' (full access, {days} days). Remove it with "
              f'loom t3 unpair or in {CONNECTIONS}.\n')
    if old_text is not None:
        _revoke_old(settings, tools, old_text, session_id, err)
    return 0


def _revoke_old(settings: Settings, tools: Tools, old_text: str, session_id: str,
                err: TextIO) -> None:
    old_id = stored_session_id(old_text)
    if old_id == session_id:
        return
    try:
        if old_id is None:
            raise PairingFailed('the old file has no usable session id')
        revoke(settings, tools, old_id)
    except PairingFailed as error:
        err.write(safe_reason(f'loom: warning: could not revoke the old session ({error}); '
                              f"remove the older '{CLIENT_NAME}' in {CONNECTIONS}", ()) + '\n')


def unpair(settings: Settings, tools: Tools, out: TextIO, err: TextIO) -> int:
    path = credential_path(settings.data_dir)
    try:
        text = read_credential_file(settings.data_dir)
    except CredentialError:
        text = ''
    if text is None:
        out.write('Not paired.\n')
        return 0
    session_id = stored_session_id(text)
    why = None
    try:
        if session_id is None:
            raise PairingFailed('the file has no usable session id')
        revoke(settings, tools, session_id)
    except PairingFailed as error:
        why = str(error)
    try:
        delete_credential_file(settings.data_dir)
    except OSError as error:
        err.write(f'loom: t3: could not delete {path}: '
                  f'{error.strerror or type(error).__name__}\n')
        return 1
    if why is not None:
        err.write(safe_reason(f'loom: t3: could not revoke the session ({why}); '
                              f"remove '{CLIENT_NAME}' in {CONNECTIONS}", ()) + '\n')
        return 1
    out.write('Unpaired.\n')
    return 0


def status(settings: Settings, tools: Tools, out: TextIO, err: TextIO) -> int:
    try:
        text = read_credential_file(settings.data_dir)
    except CredentialError as error:
        err.write(f'loom: t3: {error}\n')
        return 1
    if text is None:
        out.write('Not paired. Run loom t3 pair.\n')
        return 1
    credential = parse_credential(text)
    if credential is None:
        err.write(f'loom: t3: {credential_path(settings.data_dir)} is not a usable pairing; '
                  f'run loom t3 pair\n')
        return 1
    now = tools.clock()
    if credential.expired(now):
        day = time.strftime('%Y-%m-%d', time.localtime(credential.expires_at))
        out.write(f'The pairing expired on {day}. Run loom t3 pair.\n')
        return 1
    days = days_left(credential.expires_at, now)
    out.write(f'Paired with T3 at {credential.origin}: {days} days left.\n')
    if days < WARN_DAYS:
        out.write(f'Warning: the pairing ends in {days} days; run loom t3 pair to renew it.\n')
    return 0
