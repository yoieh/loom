import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

from loom_scheduler import CheckResult
from loom_service.checks import CHECK_TIMEOUT, MAX_CHECK_OUTPUT, run_check
from loom_service.config import Config
from loom_service.host import ENV_KEYS, Host

from .support import TIMEOUT, ServerCase, clean_env, wait_until

GONE_WITHIN = 5.0
GRANDCHILD = '''
import os, subprocess, sys, time
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
with open(sys.argv[1] + '.tmp', 'w') as file:
    file.write(f'{os.getpid()} {child.pid}')
os.rename(sys.argv[1] + '.tmp', sys.argv[1])
{rest}
'''
SCRIPT = '''
import json, os
names = ' '.join(sorted(os.environ))
print(json.dumps({'text': f'cwd {os.getcwd()} env {names}'}))
'''


def is_gone(pid: int) -> bool:
    try:
        text = Path(f'/proc/{pid}/stat').read_text()
    except (FileNotFoundError, ProcessLookupError):
        return True
    return text[text.rindex(')') + 2] == 'Z'


class RunCheckTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name).resolve()
        self.pids = self.dir / 'pids'

    async def python(self, code: str, **options: object) -> CheckResult:
        return await run_check([sys.executable, '-c', code, str(self.pids)], str(self.dir),
                               clean_env(), **options)

    def grandchild(self, rest: str) -> str:
        return GRANDCHILD.replace('{rest}', rest)

    async def assert_group_gone(self) -> None:
        pids = [int(pid) for pid in self.pids.read_text().split()]
        await wait_until(lambda: all(is_gone(pid) for pid in pids), GONE_WITHIN)

    def test_the_limits(self) -> None:
        self.assertEqual((CHECK_TIMEOUT, MAX_CHECK_OUTPUT), (60, 65_536))

    async def test_it_runs_in_the_cwd_with_only_the_environment_given(self) -> None:
        code = 'import json, os; print(json.dumps([os.getcwd(), sorted(os.environ)]))'
        result = await run_check([sys.executable, '-c', code], str(self.dir),
                                 clean_env(MARK='1'))
        self.assertEqual((result.code, result.failure), (0, None))
        cwd, names = json.loads(result.output)
        self.assertEqual(cwd, str(self.dir))
        self.assertEqual(set(names) - {'LC_CTYPE'}, {'PATH', 'LANG', 'MARK'})

    async def test_the_exit_code_and_stdout_come_back(self) -> None:
        for code in (0, 3):
            with self.subTest(code=code):
                result = await self.python(f'import sys; print("out é"); sys.exit({code})')
                self.assertEqual(result, CheckResult(code, 'out é\n', None))

    async def test_stdout_is_decoded_with_replacement(self) -> None:
        result = await self.python('import sys; sys.stdout.buffer.write(b"a\\xffb")')
        self.assertEqual(result, CheckResult(0, 'a�b', None))

    async def test_stdout_up_to_the_limit_is_taken_whole(self) -> None:
        result = await self.python('import sys; sys.stdout.write("x" * 65536)')
        self.assertEqual(result, CheckResult(0, 'x' * 65_536, None))

    async def test_stdout_over_the_limit_fails_and_kills_the_group(self) -> None:
        result = await self.python('import sys; sys.stdout.write("x" * 65537)')
        self.assertEqual(result, CheckResult(None, '', 'check printed over 65536 bytes'))
        code = self.grandchild('import sys; sys.stdout.write("x" * 70000); sys.stdout.flush(); '
                               'time.sleep(30)')
        result = await self.python(code, timeout=20)
        self.assertEqual(result.failure, 'check printed over 65536 bytes')
        await self.assert_group_gone()

    async def test_a_missing_program_cannot_run(self) -> None:
        result = await run_check([str(self.dir / 'nope')], str(self.dir), clean_env())
        self.assertEqual((result.code, result.output), (None, ''))
        self.assertTrue(result.failure.startswith('cannot run '), result.failure)
        self.assertEqual(result.failure, f'cannot run {self.dir / "nope"}: '
                                         'No such file or directory')

    async def test_a_check_past_its_timeout_is_killed_with_its_group(self) -> None:
        result = await self.python(self.grandchild('time.sleep(30)'), timeout=0.5)
        self.assertEqual(result, CheckResult(None, '', 'check ran over 0.5 s'))
        await self.assert_group_gone()

    async def test_a_grandchild_holding_stdout_open_runs_into_the_timeout(self) -> None:
        result = await self.python(self.grandchild('print("{}")'), timeout=0.5)
        self.assertEqual(result.failure, 'check ran over 0.5 s')
        await self.assert_group_gone()

    async def test_a_check_killed_by_a_signal_fails(self) -> None:
        result = await self.python('import os, signal; os.kill(os.getpid(), signal.SIGTERM)')
        self.assertEqual(result, CheckResult(None, '', 'check was killed by signal 15'))

    async def test_a_cancelled_check_is_killed_with_its_group(self) -> None:
        task = asyncio.create_task(self.python(self.grandchild('time.sleep(30)'), timeout=20))
        await wait_until(self.pids.exists, TIMEOUT)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await self.assert_group_gone()


class CheckEnvironmentTest(unittest.TestCase):
    def host(self, environ: dict[str, str]) -> Host:
        return Host(Path('/data'), Path('/run/user/1000/loom/api.sock'), Config(), environ)

    def test_it_holds_the_allowlist_and_the_socket_only(self) -> None:
        environ = {key: f'v-{key}' for key in ENV_KEYS}
        environ.update(LOOM_THREAD='inherited', OUTSIDE='leak', LOOM_SOCKET='/elsewhere.sock')
        self.assertEqual(self.host(environ).check_environment(), {
            **{key: f'v-{key}' for key in ENV_KEYS},
            'LOOM_SOCKET': '/run/user/1000/loom/api.sock',
        })

    def test_missing_keys_stay_out(self) -> None:
        self.assertEqual(self.host({'PATH': '/bin', 'LOOM_THREAD': 't1'}).check_environment(),
                         {'PATH': '/bin', 'LOOM_SOCKET': '/run/user/1000/loom/api.sock'})


class ServedCheckTest(ServerCase):
    async def runs(self, schedule_id: str) -> list[tuple]:
        connection = self.serving.service.store.connection
        return connection.execute('SELECT outcome, reason FROM schedule_runs '
                                  'WHERE schedule_id = ?', (schedule_id,)).fetchall()

    async def add(self, conn: object, check: list[str]) -> dict:
        trigger = {'type': 'at', 'at': '2000-01-01T00:00:00.000+00:00'}
        return await conn.ok('schedules.add', id=conn.new_id(), trigger=trigger, text='unused',
                             action='launch', project_id='repo', check=check,
                             workspace={'strategy': 'root'})

    async def test_a_persons_check_launches_on_its_text_or_skips(self) -> None:
        script = self.base / 'check.py'
        script.write_text(SCRIPT)
        conn = await self.connect()
        await self.add_project(conn)
        added = await self.add(conn, [sys.executable, str(script)])
        self.assertEqual(added['check'], [sys.executable, str(script)])
        fired = await self.wait_fired(conn, added['id'])
        [result] = await self.wait_events(conn, 'turn.result', fired['thread_id'])
        text = result['data']['text']
        late, _, printed = text.partition('\n\n')
        self.assertTrue(late.startswith('This run is late: it was due at 2000-01-01T'), late)
        head = f'cwd {self.repo} env '
        self.assertTrue(printed.startswith(head), printed)
        names = set(printed[len(head):].split())
        self.assertIn('LOOM_SOCKET', names)
        self.assertLessEqual(names, {*ENV_KEYS, 'LOOM_SOCKET'})
        self.assertNotIn('LOOM_THREAD', names)
        self.assertNotIn('OUTSIDE', names)
        self.assertEqual(fired['data']['text'], printed)

        skipped = await self.add(conn, ['false'])
        await wait_until(lambda: self.serving.service.store.connection.execute(
            'SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = ?',
            (skipped['id'],)).fetchone()[0])
        self.assertEqual(await self.runs(skipped['id']),
                         [('skipped_check', 'check exited with 1')])
        threads = await conn.ok('threads.list')
        self.assertEqual([thread['origin'] for thread in threads],
                         [f'schedule:{added["id"]}'])

    async def wait_fired(self, conn: object, schedule_id: str) -> dict:
        found: list[dict] = []

        async def look() -> None:
            events = await self.all_events(conn)
            found[:] = [event for event in events if event['type'] == 'schedule.fired'
                        and event['data']['schedule_id'] == schedule_id]

        deadline = asyncio.get_running_loop().time() + TIMEOUT
        while not found:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f'schedule {schedule_id} did not fire within {TIMEOUT} s')
            await look()
            await asyncio.sleep(0.02)
        return found[0]


if __name__ == '__main__':
    unittest.main()
