import base64
import copy
import hashlib
import io
import json
import os
import sys
import tempfile
import unittest
import urllib.parse
from pathlib import Path

from loom_runner_t3.credential import parse_credential
from loom_runner_t3.pairing import SCOPES, Settings, Tools, pair, run_t3

from .fake_t3 import FakeT3
from .replay import (
    CLIENT_ID,
    NOW,
    ORIGIN,
    TOKEN,
    FakeClock,
    FakeHttp,
    Reply,
    credential_text,
    from_recording,
    make_t3_home,
    mode_of,
    recorded,
)

NEW_SESSION = '00000000-0000-4000-8002-000000000002'
OLD_SESSION = '00000000-0000-4000-8002-000000000001'
OTHER_SESSION = '00000000-0000-4000-8002-000000000009'
CODE = 'replay-pairing-code'
AUTH_CODE = 'replay-authorization-code'
REDIRECT = 'http://127.0.0.1/loom/callback'

ORDER = ['t3 session-list', '/oauth/mcp/register', '/oauth/mcp/authorize',
         '/oauth/mcp/approval', 't3 pairing-create', '/oauth/mcp/decision', '/oauth/mcp/token',
         't3 session-list']


def session_entry(session_id: object = NEW_SESSION, label: str = 'Loom') -> dict:
    entry = copy.deepcopy(recorded('session-list')['stdout'][0])
    entry['sessionId'] = session_id
    entry['client']['label'] = label
    return entry


def decision(state: str | None = None, iss: str = ORIGIN, redirect: str = REDIRECT,
             code: str = AUTH_CODE):
    def reply(call) -> Reply:
        sent = json.loads(call.body)
        query = urllib.parse.urlencode({'code': code,
                                        'state': state or sent['authorization']['state'],
                                        'iss': iss})
        return Reply(200, {'content-type': 'application/json'},
                     {'redirectTo': f'{redirect}?{query}'})
    return reply


def token_reply(**changes: object) -> Reply:
    body = dict(recorded('oauth-token')['body'])
    body['access_token'] = TOKEN
    body.update(changes)
    return Reply(200, {'content-type': 'application/json'}, body)


class PairingBase(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name).resolve()
        self.home = make_t3_home(self.base)
        self.data_dir = self.base / 'data'
        self.file = self.data_dir / 't3-client.json'
        self.t3 = FakeT3(self.home / 'runtime' / 'versions' / '0.1.2')
        self.t3.install()
        self.t3.output('pairing-create', recorded('pairing-create')['stdout'])
        self.t3.output('session-list', [session_entry(OTHER_SESSION, 'Thread Voice')], call=0)
        self.t3.output('session-list', [session_entry(OTHER_SESSION, 'Thread Voice'),
                                        session_entry()], call=1)
        self.http = FakeHttp()
        self.http.on('/oauth/mcp/register', from_recording('oauth-register'))
        self.http.on('/oauth/mcp/authorize', from_recording('oauth-authorize'))
        self.http.on('/oauth/mcp/approval', from_recording('oauth-approval'))
        self.http.on('/oauth/mcp/decision', decision())
        self.http.on('/oauth/mcp/token', token_reply())
        self.order: list[str] = []
        self.environ = {'HOME': str(self.base), 'PATH': os.environ.get('PATH', ''),
                        'LANG': 'C.UTF-8', 'USER': 'tester', 'LOGNAME': 'tester',
                        'LOOM_SECRET': TOKEN, 'T3_TOKEN': TOKEN}
        self.on_run = None

    def settings(self, t3_bin: str | None = None) -> Settings:
        return Settings(str(self.data_dir), str(self.home), t3_bin, self.environ)

    def tools(self) -> Tools:
        def http(method, url, headers, body, timeout):
            self.order.append(urllib.parse.urlsplit(url).path)
            return self.http(method, url, headers, body, timeout)

        def run(argv, env):
            self.order.append(f't3 {argv[2]}-{argv[3]}')
            if self.on_run is not None:
                self.on_run(argv)
            return run_t3(argv, env)

        return Tools(http, run, FakeClock())

    def pair(self, t3_bin: str | None = None) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        code = pair(self.settings(t3_bin), self.tools(), out, err)
        self.assert_no_secret(out.getvalue() + err.getvalue())
        return code, out.getvalue(), err.getvalue()

    def assert_no_secret(self, text: str) -> None:
        for secret in (TOKEN, CODE, AUTH_CODE, 'fake t3 stderr'):
            self.assertNotIn(secret, text)

    def assert_fails(self, step: str) -> str:
        code, out, err = self.pair()
        self.assertEqual(code, 1)
        self.assertEqual(out, '')
        self.assertTrue(err.startswith(f'loom: t3: pairing failed: {step}: '), err)
        self.assertEqual(err.count('\n'), 1)
        self.assertFalse(self.file.exists())
        return err


class PairTest(PairingBase):
    def test_pair_runs_every_step_in_order_and_writes_the_credential(self) -> None:
        code, out, err = self.pair()
        self.assertEqual((code, err), (0, ''))
        self.assertEqual(out, "Paired as 'Loom' (full access, 30 days). Remove it with loom t3 "
                              'unpair or in T3 Settings → Connections.\n')
        self.assertEqual(self.order, ORDER)
        self.assertEqual(mode_of(self.data_dir), 0o700)
        self.assertEqual(mode_of(self.file), 0o600)
        stored = parse_credential(self.file.read_text())
        self.assertEqual(stored.origin, ORIGIN)
        self.assertEqual(stored.access_token, TOKEN)
        self.assertEqual(stored.expires_at, NOW + 2592000)
        self.assertEqual(stored.session_id, NEW_SESSION)
        self.assertEqual(stored.client_id, CLIENT_ID)
        self.assertEqual(stored.scope, ' '.join(SCOPES))

    def test_the_t3_cli_gets_the_flags_and_an_allowlisted_environment(self) -> None:
        self.pair()
        calls = self.t3.calls()
        self.assertEqual([call['key'] for call in calls],
                         ['session-list', 'pairing-create', 'session-list'])
        binary = str(self.t3.path)
        self.assertEqual(calls[1]['argv'], [
            binary, 'auth', 'pairing', 'create',
            '--scope', 'orchestration:read', '--scope', 'orchestration:operate',
            '--scope', 'settings:write', '--scope', 'source-control:write',
            '--ttl', '5m', '--label', 'Loom pairing', '--json', '--base-dir', str(self.home)])
        self.assertEqual(calls[0]['argv'], [binary, 'auth', 'session', 'list', '--json',
                                            '--base-dir', str(self.home)])
        for call in calls:
            env = {key: value for key, value in call['env'].items()
                   if key not in ('PWD', 'SHLVL', '_', 'LC_CTYPE')}
            self.assertEqual(set(env), {'HOME', 'PATH', 'LANG', 'USER', 'LOGNAME',
                                        'T3CODE_HOME'})
            self.assertEqual(env['T3CODE_HOME'], str(self.home))
            self.assertNotIn(TOKEN, json.dumps(call))

    def test_the_oauth_requests(self) -> None:
        self.pair()
        register, authorize, approval, decided, exchanged = self.http.calls
        self.assertEqual(json.loads(register.body), {
            'client_name': 'Loom', 'redirect_uris': [REDIRECT],
            'token_endpoint_auth_method': 'none'})
        self.assertEqual(authorize.method, 'GET')
        self.assertIsNone(authorize.body)
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(authorize.url).query))
        self.assertEqual(json.loads(approval.body), query)
        self.assertEqual(query['client_id'], CLIENT_ID)
        self.assertEqual(query['redirect_uri'], REDIRECT)
        self.assertEqual(query['code_challenge_method'], 'S256')
        self.assertEqual(query['resource'], ORIGIN + '/mcp')
        self.assertEqual(json.loads(decided.body), {
            'authorization': query,
            'decision': {'_tag': 'pairing-code', 'access': 'full-access', 'code': CODE}})
        form = dict(urllib.parse.parse_qsl(exchanged.body.decode()))
        self.assertEqual(exchanged.headers['Content-Type'], 'application/x-www-form-urlencoded')
        self.assertEqual(form['grant_type'], 'authorization_code')
        self.assertEqual(form['code'], AUTH_CODE)
        self.assertEqual(form['redirect_uri'], REDIRECT)
        self.assertEqual(form['client_id'], CLIENT_ID)
        challenge = base64.urlsafe_b64encode(
            hashlib.sha256(form['code_verifier'].encode()).digest()).rstrip(b'=').decode()
        self.assertEqual(challenge, query['code_challenge'])
        for call in self.http.calls:
            self.assertTrue(call.url.startswith(ORIGIN + '/oauth/mcp/'))
            self.assertNotIn('Authorization', call.headers)

    def test_no_redirect_is_followed(self) -> None:
        location = 'http://198.51.100.7/connect-agent?x=1'
        self.http.on('/oauth/mcp/authorize', Reply(302, {'location': location}, ''))
        self.assertEqual(self.pair()[0], 0)
        paths = [urllib.parse.urlsplit(call.url).path for call in self.http.calls]
        self.assertNotIn('/connect-agent', paths)
        self.assertNotIn('/loom/callback', paths)
        self.assertTrue(all(call.url.startswith(ORIGIN) for call in self.http.calls))

    def test_the_t3_output_is_never_shown(self) -> None:
        stderr = self.base / 'stderr'
        saved = os.dup(2)
        try:
            with open(stderr, 'wb') as file:
                os.dup2(file.fileno(), 2)
                code, out, err = self.pair()
        finally:
            os.dup2(saved, 2)
            os.close(saved)
        self.assertEqual(code, 0)
        self.assertNotIn(b'fake t3 stderr', stderr.read_bytes())
        self.assertNotIn(CODE.encode(), stderr.read_bytes())

    def test_a_decision_for_another_request_fails(self) -> None:
        for name, reply in (('state', decision(state='forged')),
                            ('issuer', decision(iss='http://127.0.0.1:1')),
                            ('redirect URI', decision(redirect='http://127.0.0.1/evil')),
                            ('host', decision(redirect='http://evil/loom/callback'))):
            with self.subTest(name):
                self.setUp()
                self.http.on('/oauth/mcp/decision', reply)
                err = self.assert_fails('decision')
                self.assertNotIn('/oauth/mcp/token', self.order)
                self.assertIn('another', err)

    def test_a_non_loopback_origin_fails_before_any_call(self) -> None:
        for origin in ('http://192.168.1.4:41773', 'https://127.0.0.1:41773',
                       'http://127.0.0.1:41773/x', 'http://localhost.evil:1'):
            with self.subTest(origin=origin):
                self.setUp()
                make_t3_home(self.base, origin=origin)
                code, out, err = self.pair()
                self.assertEqual(code, 1)
                self.assertIn('not a loopback address', err)
                self.assertEqual(self.order, [])
                self.assertEqual(self.http.calls, [])
                self.assertFalse(self.file.exists())

    def test_no_running_t3_fails_before_any_call(self) -> None:
        (self.home / 'userdata' / 'server-runtime.json').unlink()
        code, _, err = self.pair()
        self.assertEqual(code, 1)
        self.assertTrue(err.startswith('loom: t3: T3 is not running'))
        self.assertEqual(self.order, [])

    def answer_cases(self, value: object) -> dict:
        def body(name: str, field: str, status: int | None = None) -> Reply:
            data = copy.deepcopy(recorded(name))
            content = data['body']
            if name == 'oauth-token':
                content['access_token'] = TOKEN
            if value is ...:
                content.pop(field)
            else:
                content[field] = value
            return Reply(data['status'], data['headers'], content)

        location = from_recording('oauth-authorize')
        if value is ...:
            location.headers = {}
        else:
            location.headers = {'location': value}
        created = copy.deepcopy(recorded('pairing-create')['stdout'])
        if value is ...:
            created.pop('credential')
        else:
            created['credential'] = value
        return {
            'register': ('/oauth/mcp/register', body('oauth-register', 'client_id')),
            'authorize': ('/oauth/mcp/authorize', location),
            'decision': ('/oauth/mcp/decision', body('oauth-decision', 'redirectTo')),
            'token': ('/oauth/mcp/token', body('oauth-token', 'access_token')),
            'token ': ('/oauth/mcp/token', body('oauth-token', 'expires_in')),
            'pairing-create': ('t3', created),
            'session-list': ('t3', [session_entry(OTHER_SESSION, 'Thread Voice'),
                                    session_entry(None if value is ... else value)]),
        }

    def check_answer_fields(self, value: object) -> None:
        for step, (where, reply) in self.answer_cases(value).items():
            with self.subTest(step=step):
                self.setUp()
                if where == 't3':
                    if step == 'session-list':
                        if value is ...:
                            reply[1].pop('sessionId')
                        self.t3.output('session-list', reply, call=1)
                    else:
                        self.t3.output(step, reply)
                else:
                    self.http.on(where, reply)
                self.assert_fails(step.strip())

    def test_a_null_answer_field_fails(self) -> None:
        self.check_answer_fields(None)

    def test_a_missing_answer_field_fails(self) -> None:
        self.check_answer_fields(...)

    def test_a_wrong_typed_answer_field_fails(self) -> None:
        self.check_answer_fields(['x'])

    def test_a_bool_or_zero_expires_in_fails(self) -> None:
        for value in (True, 0, -5, '2592000', 2592000.5):
            with self.subTest(value=value):
                self.setUp()
                self.http.on('/oauth/mcp/token', token_reply(expires_in=value))
                self.assert_fails('token')

    def test_a_failing_status_fails_the_step(self) -> None:
        for path, step in (('/oauth/mcp/register', 'register'),
                           ('/oauth/mcp/authorize', 'authorize'),
                           ('/oauth/mcp/approval', 'approval'),
                           ('/oauth/mcp/decision', 'decision'),
                           ('/oauth/mcp/token', 'token')):
            with self.subTest(step=step):
                self.setUp()
                self.http.on(path, Reply(500, {}, {'error': CODE, 'message': TOKEN}))
                self.assertIn('T3 answered 500', self.assert_fails(step))

    def test_an_unreachable_t3_fails_the_step(self) -> None:
        for error in (TimeoutError(TOKEN), ConnectionRefusedError(TOKEN)):
            with self.subTest(error=type(error).__name__):
                self.setUp()
                self.http.on('/oauth/mcp/register', error)
                self.assert_fails('register')

    def test_a_t3_that_moved_during_pairing_fails(self) -> None:
        def moved(call) -> Reply:
            make_t3_home(self.base, origin='http://127.0.0.1:1')
            return from_recording('oauth-approval')

        self.http.on('/oauth/mcp/approval', moved)
        err = self.assert_fails('decision')
        self.assertIn('not running at', err)

    def test_a_missing_failing_or_garbled_t3_fails(self) -> None:
        missing = self.assert_fails_with(t3_bin=str(self.base / 'no-such-t3'))
        self.assertTrue(missing.startswith('loom: t3: pairing failed: session-list: '))
        self.setUp()
        self.t3.exit('pairing-create', 3)
        self.assertIn('exited with code 3', self.assert_fails('pairing-create'))
        self.setUp()
        self.t3.output('pairing-create', f'code {CODE}\n'.encode())
        self.assertIn('printed no JSON', self.assert_fails('pairing-create'))
        self.setUp()
        self.t3.output('session-list', b'oops', call=1)
        self.assertIn('printed no JSON', self.assert_fails('session-list'))

    def assert_fails_with(self, t3_bin: str) -> str:
        code, out, err = self.pair(t3_bin)
        self.assertEqual((code, out), (1, ''))
        self.assertFalse(self.file.exists())
        return err

    def test_the_t3_bin_setting_is_used(self) -> None:
        other = FakeT3(self.base / 'bin')
        other.install()
        other.output('pairing-create', recorded('pairing-create')['stdout'])
        other.output('session-list', [], call=0)
        other.output('session-list', [session_entry()], call=1)
        self.assertEqual(self.pair(str(other.path))[0], 0)
        self.assertEqual(len(other.calls()), 3)
        self.assertEqual(self.t3.calls(), [])

    def test_no_new_session_or_two_new_sessions_fail(self) -> None:
        for sessions in ([session_entry(OTHER_SESSION, 'Thread Voice')],
                         [session_entry(OTHER_SESSION, 'Loom')],
                         [session_entry(), session_entry(NEW_SESSION[:-1] + '3')]):
            with self.subTest(sessions=len(sessions)):
                self.setUp()
                self.t3.output('session-list', [session_entry(OTHER_SESSION, 'Loom')], call=0)
                self.t3.output('session-list', sessions, call=1)
                self.assertIn('T3 Settings → Connections', self.assert_fails('session-list'))

    def test_pairing_again_stores_first_then_revokes_the_old_session(self) -> None:
        self.data_dir.mkdir(mode=0o700)
        self.file.write_text(credential_text(access_token='old-token', session_id=OLD_SESSION))
        self.file.chmod(0o600)
        seen = []

        def at_revoke(argv: list[str]) -> None:
            if argv[3] == 'revoke':
                seen.append(parse_credential(self.file.read_text()).access_token)

        self.on_run = at_revoke
        code, out, err = self.pair()
        self.assertEqual((code, err), (0, ''))
        self.assertEqual(seen, [TOKEN])
        self.assertEqual(self.order, [*ORDER, 't3 session-revoke'])
        revoked = self.t3.calls()[-1]
        self.assertEqual(revoked['argv'], [str(self.t3.path), 'auth', 'session', 'revoke',
                                           '--base-dir', str(self.home), OLD_SESSION])

    def test_a_failed_revoke_of_the_old_session_only_warns(self) -> None:
        self.data_dir.mkdir(mode=0o700)
        self.file.write_text(credential_text(session_id=OLD_SESSION))
        self.file.chmod(0o600)
        self.t3.exit('session-revoke', 1)
        code, out, err = self.pair()
        self.assertEqual(code, 0)
        self.assertTrue(out.startswith("Paired as 'Loom'"))
        self.assertEqual(err.count('\n'), 1)
        self.assertTrue(err.startswith('loom: warning: could not revoke the old session'), err)
        self.assertEqual(parse_credential(self.file.read_text()).session_id, NEW_SESSION)

    def test_an_old_file_with_no_session_id_only_warns(self) -> None:
        self.data_dir.mkdir(mode=0o700)
        self.file.write_text('garbage')
        self.file.chmod(0o600)
        code, _, err = self.pair()
        self.assertEqual(code, 0)
        self.assertTrue(err.startswith('loom: warning: '))
        self.assertNotIn('t3 session-revoke', self.order)


class RunT3Test(unittest.TestCase):
    def test_a_missing_binary_raises_os_error(self) -> None:
        with self.assertRaises(OSError):
            run_t3(['/nonexistent/t3', 'auth'], {})

    def test_stdout_is_captured(self) -> None:
        code, out = run_t3([sys.executable, '-c', 'print("hi")'], {})
        self.assertEqual((code, out), (0, b'hi\n'))


if __name__ == '__main__':
    unittest.main()
