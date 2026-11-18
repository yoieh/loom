import asyncio
import json
import os
import shutil
import stat
import tempfile
import unittest
from pathlib import Path

from loom_service.config import Config

from .support import (
    TIMEOUT, Connection, ServerCase, make_codex_bin, path_without_agents, wait_until,
)

SHELL_KEYS = {'PWD', 'OLDPWD', 'SHLVL', '_', 'LC_CTYPE'}
AGENT_ENV_KEYS = {'HOME', 'LANG', 'PATH', 'LOOM_SOCKET', 'LOOM_THREAD', 'CODEX_HOME',
                  'CLIPROXY_API_KEY'}
TOKEN = 'sk-codex-token'


def mode_of(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


class CodexCase(ServerCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.environ.update({'ANTHROPIC_AUTH_TOKEN': 'outer', 'ANTHROPIC_API_KEY': 'outer'})
        token = self.data_dir / 'agent-token'
        token.write_text(f'{TOKEN}\n')
        token.chmod(0o600)
        await self.restart(self.config())

    def config(self, **values: object) -> Config:
        values.setdefault('codex_bin', str(make_codex_bin(self.base)))
        values.setdefault('codex_home', str(self.base / 'codex-home'))
        return Config(claude_bin=str(self.claude_bin), **values)

    async def launch_codex(self, conn: Connection, message: str = 'hello',
                           **params: object) -> dict:
        return await self.launch(conn, message, runner='codex', **params)

    async def codex_turn(self, number: int) -> dict:
        path = self.repo / f'codex-{number}.json'
        await wait_until(path.exists)
        return json.loads(path.read_text())

    async def state(self, conn: Connection, thread_id: str, wanted: str) -> None:
        deadline = asyncio.get_running_loop().time() + TIMEOUT
        while (await conn.ok('threads.get', thread_id=thread_id))['state'] != wanted:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f'thread {thread_id} not {wanted} within {TIMEOUT} s')
            await asyncio.sleep(0.02)

    def codex_id(self, thread_id: str) -> str:
        return (self.data_dir / 'codex' / thread_id).read_text().rstrip('\n')

    async def send(self, conn: Connection, thread_id: str, message: str) -> dict:
        return await conn.ok('threads.send', id=conn.new_id(), thread_id=thread_id,
                             message=message)


class LaunchTest(CodexCase):
    async def test_a_codex_thread_runs_two_turns_in_one_codex_thread(self) -> None:
        conn = await self.connect()
        await self.add_project(conn, permission_mode='auto')
        thread = await self.launch_codex(conn, 'hello')
        self.assertEqual(thread['runner'], 'codex')
        results = await self.wait_events(conn, 'turn.result', thread['id'])
        self.assertEqual(results[0]['data'], {'state': 'completed', 'text': 'hello'})
        await self.state(conn, thread['id'], 'idle')
        first = await self.codex_turn(0)
        self.assertEqual(first['argv'][1:], ['exec', '--json', '-C', str(self.repo),
                                             '-s', 'workspace-write', '-'])
        self.assertEqual(first['cwd'], str(self.repo))
        self.assertEqual(first['stdin'], 'hello')
        codex_id = self.codex_id(thread['id'])

        await self.send(conn, thread['id'], 'again')
        results = await self.wait_events(conn, 'turn.result', thread['id'], 2)
        self.assertEqual(results[1]['data'], {'state': 'completed', 'text': 'again'})
        second = await self.codex_turn(1)
        self.assertEqual(second['argv'][1:], ['-C', str(self.repo), '-s', 'workspace-write',
                                              'exec', 'resume', codex_id, '--json', '-'])
        self.assertEqual(second['stdin'], 'again')
        got = await conn.ok('threads.get', thread_id=thread['id'])
        self.assertEqual((got['runner'], got['state']), ('codex', 'idle'))
        types = {event['type'] for event in await self.all_events(conn, thread_id=thread['id'])}
        self.assertNotIn('request.opened', types)

    async def test_a_delegated_codex_thread_answers_its_task(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        parent = await self.launch(conn, 'hello')
        await self.wait_events(conn, 'turn.result', parent['id'])
        child = await conn.ok('threads.delegate', id=conn.new_id(), parent_id=parent['id'],
                              task='check it', runner='codex')
        self.assertEqual(child['runner'], 'codex')
        results = await self.wait_events(conn, 'turn.result', child['id'])
        self.assertEqual(results[0]['data'], {'state': 'completed', 'text': 'check it'})


class ResumeTest(CodexCase):
    async def test_a_thread_put_to_sleep_resumes_its_codex_thread(self) -> None:
        await self.restart(self.config(agents_max=1))
        conn = await self.connect()
        await self.add_project(conn)
        thread = await self.launch_codex(conn, 'hello')
        await self.wait_events(conn, 'turn.result', thread['id'])
        await self.state(conn, thread['id'], 'idle')
        codex_id = self.codex_id(thread['id'])

        other = await self.launch(conn, 'other', title='Other')
        await self.wait_events(conn, 'turn.result', other['id'])
        await self.state(conn, thread['id'], 'sleeping')

        await self.send(conn, thread['id'], 'again')
        results = await self.wait_events(conn, 'turn.result', thread['id'], 2)
        self.assertEqual(results[1]['data'], {'state': 'completed', 'text': 'again'})
        resumed = await self.codex_turn(1)
        self.assertEqual(resumed['argv'][1:], ['-C', str(self.repo), '-s', 'workspace-write',
                                               'exec', 'resume', codex_id, '--json', '-'])

    async def test_a_restart_resumes_the_codex_thread_from_its_record(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        thread = await self.launch_codex(conn, 'hello')
        await self.wait_events(conn, 'turn.result', thread['id'])
        await self.state(conn, thread['id'], 'idle')
        record = self.data_dir / 'codex' / thread['id']
        codex_id = record.read_text().rstrip('\n')
        self.assertEqual(record.read_text(), f'{codex_id}\n')
        self.assertEqual(mode_of(record), 0o600)
        self.assertEqual(mode_of(record.parent), 0o700)
        await conn.close()

        await self.restart(self.config())
        conn = await self.connect()
        await self.state(conn, thread['id'], 'sleeping')
        await self.send(conn, thread['id'], 'again')
        results = await self.wait_events(conn, 'turn.result', thread['id'], 2)
        self.assertEqual(results[1]['data'], {'state': 'completed', 'text': 'again'})
        resumed = await self.codex_turn(1)
        self.assertEqual(resumed['argv'][1:], ['-C', str(self.repo), '-s', 'workspace-write',
                                               'exec', 'resume', codex_id, '--json', '-'])
        self.assertEqual(self.codex_id(thread['id']), codex_id)


class CodexEnvironmentTest(CodexCase):
    async def test_codex_gets_its_keys_and_the_token_and_nothing_else(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        thread = await self.launch_codex(conn)
        env = (await self.codex_turn(0))['env']
        self.assertEqual(set(env) - SHELL_KEYS, AGENT_ENV_KEYS)
        self.assertEqual(env['CLIPROXY_API_KEY'], TOKEN)
        self.assertEqual(env['CODEX_HOME'], str(self.base / 'codex-home'))
        self.assertEqual(env['LOOM_THREAD'], thread['id'])
        self.assertEqual(env['LOOM_SOCKET'], str(self.socket))
        self.assertEqual(env['PATH'], self.environ['PATH'])
        self.assertNotIn('OUTSIDE', env)
        self.assertNotIn('ANTHROPIC_AUTH_TOKEN', env)
        self.assertNotIn('ANTHROPIC_API_KEY', env)
        self.assertNotIn(TOKEN, json.dumps((await self.codex_turn(0))['argv']))

    async def test_the_directory_holding_node_goes_last_on_path(self) -> None:
        directory = self.base / 'node-bin'
        directory.mkdir()
        node = directory / 'node'
        node.write_text('#!/bin/sh\n')
        node.chmod(0o700)
        await self.restart(self.config(codex_bin=str(make_codex_bin(directory))))
        conn = await self.connect()
        await self.add_project(conn)
        await self.launch_codex(conn)
        env = (await self.codex_turn(0))['env']
        self.assertEqual(env['PATH'], f'{self.environ["PATH"]}:{directory}')

    async def test_approval_required_gives_a_read_only_sandbox(self) -> None:
        conn = await self.connect()
        await self.add_project(conn, permission_mode='approval-required')
        thread = await self.launch_codex(conn, 'hello')
        self.assertEqual(thread['permission_mode'], 'approval-required')
        await self.wait_events(conn, 'turn.result', thread['id'])
        await self.state(conn, thread['id'], 'idle')
        first = (await self.codex_turn(0))['argv']
        self.assertEqual(first[first.index('-s') + 1], 'read-only')
        await self.send(conn, thread['id'], 'again')
        await self.wait_events(conn, 'turn.result', thread['id'], 2)
        second = (await self.codex_turn(1))['argv']
        self.assertEqual(second[second.index('-s') + 1], 'read-only')
        self.assertLess(second.index('-s'), second.index('exec'))



class PathWithoutAgentsTest(unittest.TestCase):
    def test_it_drops_every_entry_holding_claude_or_codex(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            with_claude, with_codex, tools = (base / 'with-claude', base / 'with-codex',
                                              base / 'tools')
            for directory in (with_claude, with_codex, tools):
                directory.mkdir()
            (with_claude / 'claude').write_text('')
            os.symlink(base / 'missing', with_codex / 'codex')
            (tools / 'git').write_text('')
            path = os.pathsep.join([str(with_claude), str(with_codex), '', str(tools)])
            self.assertEqual(path_without_agents(path), str(tools))


class NoAgentBinTest(CodexCase):
    async def test_a_config_with_no_agent_bins_makes_a_launch_fail_to_spawn(self) -> None:
        for name in ('claude', 'codex'):
            self.assertIsNone(shutil.which(name, path=self.environ['PATH']), name)
        await self.restart(Config(codex_home=str(self.base / 'codex-home')))
        conn = await self.connect()
        await self.add_project(conn)
        claude = await self.launch(conn, 'hello')
        [failed] = await self.wait_events(conn, 'thread.failed', claude['id'])
        self.assertEqual(failed['data'],
                         {'reason': 'could not start claude: No such file or directory'})
        codex = await self.launch_codex(conn, 'hello', title='Codex')
        [failed] = await self.wait_events(conn, 'thread.failed', codex['id'])
        self.assertEqual(failed['data'],
                         {'reason': 'could not start codex: No such file or directory'})


if __name__ == '__main__':
    unittest.main()
