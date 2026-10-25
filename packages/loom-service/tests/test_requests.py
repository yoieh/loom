import asyncio
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from loom_core.clock import iso

from .fake_claude import ASK, RESPONSES, asked_request, first_turn
from .support import TIMEOUT, Connection, ServerCase, clean_env, run_client, wait_until

REQUEST = asked_request(first_turn())
REQUEST_ID = REQUEST['request_id']
TOOL_INPUT = REQUEST['request']['input']
CONFIG_ERROR = 'request_timeout must be a whole number of 1 or more'


class ManualClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class RequestCase(ServerCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.clock = self.use_clock()

    def use_clock(self) -> ManualClock:
        clock = ManualClock(datetime(2026, 10, 25, 9, 0, tzinfo=UTC))
        self.serving.service.store.clock = clock
        return clock

    async def serve_with_timeout(self, seconds: int) -> None:
        await self.restart(self.config(request_timeout=seconds))
        self.clock = self.use_clock()

    @property
    def orchestrator(self):
        return self.serving.service.orchestrator

    async def asked(self, conn: Connection, **params: object) -> tuple[dict, dict]:
        await self.add_project(conn)
        thread = await self.launch(conn, ASK, **params)
        opened = await self.wait_events(conn, 'request.opened', thread['id'])
        return thread, opened[0]

    def responses(self) -> list[dict]:
        path = self.repo / RESPONSES
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    async def recorded(self, count: int = 1) -> list[dict]:
        await wait_until(lambda: len(self.responses()) >= count)
        return self.responses()

    async def respond(self, conn: Connection, request_id: str, decision: str,
                      **params: object) -> dict:
        return await conn.call('requests.respond', id=conn.new_id(), request_id=request_id,
                               decision=decision, **params)

    async def settled(self, conn: Connection, thread_id: str) -> dict:
        deadline = asyncio.get_running_loop().time() + TIMEOUT
        while True:
            info = await conn.ok('threads.get', thread_id=thread_id)
            if info['state'] == 'idle':
                return info
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f'{thread_id} not idle within {TIMEOUT} s')
            await asyncio.sleep(0.02)

    async def closed(self, conn: Connection, thread_id: str) -> list[dict]:
        events = await self.wait_events(conn, 'request.closed', thread_id)
        return [event['data'] for event in events]


class OpenTest(RequestCase):
    async def test_an_ask_opens_the_streams_request_and_the_thread_waits(self) -> None:
        conn = await self.connect()
        opened_at = self.clock.now
        thread, opened = await self.asked(conn)
        self.assertEqual(opened['data'], {
            'request_id': REQUEST_ID, 'tool_name': 'Bash', 'input': TOOL_INPUT,
            'description': REQUEST['request']['description'],
            'expires_at': iso(opened_at + timedelta(seconds=600)),
        })
        info = await conn.ok('threads.get', thread_id=thread['id'])
        self.assertEqual((info['state'], info['waiting']), ('running', True))
        self.assertEqual(self.responses(), [])


class AnswerTest(RequestCase):
    async def test_allow_sends_the_streams_input_and_the_turn_completes(self) -> None:
        conn = await self.connect()
        thread, _ = await self.asked(conn)
        answer = await self.respond(conn, REQUEST_ID, 'allow')
        self.assertEqual(answer['ok'], True, answer)
        self.assertEqual(answer['result']['id'], thread['id'])
        self.assertEqual(answer['result']['waiting'], False)
        [response] = await self.recorded()
        self.assertEqual(response['response']['request_id'], REQUEST_ID)
        self.assertEqual(response['response']['response'],
                         {'behavior': 'allow', 'updatedInput': TOOL_INPUT})
        self.assertEqual(await self.closed(conn, thread['id']),
                         [{'request_id': REQUEST_ID, 'decision': 'allow'}])
        result = await self.wait_events(conn, 'turn.result', thread['id'])
        self.assertEqual(result[0]['data']['state'], 'completed')
        info = await self.settled(conn, thread['id'])
        self.assertEqual((info['state'], info['waiting']), ('idle', False))
        mode = stat.S_IMODE(os.stat(self.repo / RESPONSES).st_mode)
        self.assertEqual(mode, 0o600)

    async def test_deny_sends_the_message_and_the_turn_completes(self) -> None:
        conn = await self.connect()
        thread, _ = await self.asked(conn)
        answer = await self.respond(conn, REQUEST_ID, 'deny', message='Not this one.')
        self.assertEqual(answer['ok'], True, answer)
        [response] = await self.recorded()
        self.assertEqual(response['response']['response'],
                         {'behavior': 'deny', 'message': 'Not this one.'})
        self.assertEqual(await self.closed(conn, thread['id']),
                         [{'request_id': REQUEST_ID, 'decision': 'deny'}])
        result = await self.wait_events(conn, 'turn.result', thread['id'])
        self.assertEqual(result[0]['data']['state'], 'completed')
        info = await self.settled(conn, thread['id'])
        self.assertEqual(info['waiting'], False)

    async def test_a_second_answer_from_another_connection_is_a_conflict(self) -> None:
        conn = await self.connect()
        thread, _ = await self.asked(conn)
        self.assertEqual((await self.respond(conn, REQUEST_ID, 'deny'))['ok'], True)
        other = await self.connect()
        self.assertEqual(await self.respond(other, REQUEST_ID, 'allow'), {
            'ok': False,
            'error': {'code': 'conflict', 'message': f'request {REQUEST_ID} is closed'},
        })
        await self.wait_events(conn, 'turn.result', thread['id'])
        await self.settled(conn, thread['id'])
        self.assertEqual([response['response']['response']['behavior']
                          for response in self.responses()], ['deny'])
        self.assertEqual(len(await self.closed(conn, thread['id'])), 1)


class TimeoutTest(RequestCase):
    async def test_the_configured_timeout_denies_at_exactly_its_seconds(self) -> None:
        await self.serve_with_timeout(30)
        self.assertEqual(self.orchestrator.request_timeout, 30)
        conn = await self.connect()
        opened_at = self.clock.now
        thread, opened = await self.asked(conn)
        self.assertEqual(opened['data']['expires_at'], iso(opened_at + timedelta(seconds=30)))

        self.clock.advance(29.999)
        await self.orchestrator.expire_requests()
        info = await conn.ok('threads.get', thread_id=thread['id'])
        self.assertEqual((info['state'], info['waiting']), ('running', True))
        self.assertEqual([event for event in await self.all_events(conn, thread_id=thread['id'])
                          if event['type'] == 'request.closed'], [])
        self.assertEqual(self.responses(), [])

        self.clock.advance(0.001)
        await self.orchestrator.expire_requests()
        self.assertEqual(await self.closed(conn, thread['id']), [
            {'request_id': REQUEST_ID, 'decision': 'deny', 'reason': 'timeout'},
        ])
        [response] = await self.recorded()
        self.assertEqual(response['response']['response'], {
            'behavior': 'deny',
            'message': 'No answer came within 30 s, so Loom denied this request.',
        })
        await self.wait_events(conn, 'turn.result', thread['id'])
        info = await self.settled(conn, thread['id'])
        self.assertEqual(info['waiting'], False)
        self.assertNotIn('allow', [response['response']['response']['behavior']
                                   for response in self.responses()])


class WhoAnswersTest(RequestCase):
    async def test_a_caller_thread_refuses_an_agent_and_takes_the_persons_answer(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        other = await self.launch(conn, 'hello', title='Other')
        await self.wait_events(conn, 'turn.result', other['id'])
        thread = await self.launch(conn, ASK, requests='caller')
        await self.wait_events(conn, 'request.opened', thread['id'])

        request = {'v': 'v1', 'call': 'requests.respond', 'params': {
            'id': 'agent-1', 'request_id': REQUEST_ID, 'decision': 'allow'}}
        answers = await run_client(self.socket, [request], clean_env(LOOM_THREAD=other['id']))
        self.assertEqual(answers, [{'ok': False, 'error': {
            'code': 'forbidden',
            'message': 'only the caller that launched this thread may answer its requests',
        }}])
        info = await conn.ok('threads.get', thread_id=thread['id'])
        self.assertEqual(info['waiting'], True)
        self.assertEqual(self.responses(), [])

        answer = await self.respond(conn, REQUEST_ID, 'allow')
        self.assertEqual(answer['ok'], True, answer)
        [response] = await self.recorded()
        self.assertEqual(response['response']['response']['behavior'], 'allow')
        await self.wait_events(conn, 'turn.result', thread['id'])


class ConfigTest(unittest.TestCase):
    def test_a_bad_request_timeout_stops_the_server_with_exit_2(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp).resolve()
            config = base / 'config.toml'
            for value in ('0', '-1', '"60"', '1.5', 'true'):
                with self.subTest(value=value):
                    config.write_text(f'request_timeout = {value}\n')
                    done = subprocess.run(
                        [sys.executable, '-m', 'loom_service', 'serve', '--socket',
                         str(base / 'api.sock'), '--data-dir', str(base / 'data'), '--config',
                         str(config)],
                        env=clean_env(HOME=str(base)), cwd=base, stdin=subprocess.DEVNULL,
                        capture_output=True, text=True, timeout=TIMEOUT)
                    self.assertEqual((done.returncode, done.stderr),
                                     (2, f'loom: {config}: {CONFIG_ERROR}\n'))
                    self.assertFalse((base / 'api.sock').exists())


if __name__ == '__main__':
    unittest.main()
