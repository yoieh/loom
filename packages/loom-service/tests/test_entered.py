import unittest

from .fake_claude import STEER
from .support import Connection, ServerCase
from .test_codex import CodexCase

TURN = ('turn.started', 'message.entered', 'turn.tool', 'turn.text', 'turn.result')
SECOND = 'Reply with the word second'


class EnteredCase(ServerCase):
    async def launch_as(self, conn: Connection, message: str, **params: object) -> tuple:
        launch_id = conn.new_id()
        thread = await conn.ok('threads.launch', id=launch_id, title='First', project_id='repo',
                               workspace={'strategy': 'root'}, message=message, **params)
        return launch_id, thread['id']

    async def turn(self, conn: Connection, thread_id: str) -> list[tuple[str, dict]]:
        return [(event['type'], event['data'])
                for event in await self.all_events(conn, thread_id=thread_id)
                if event['type'] in TURN]


class ClaudeEnteredTest(EnteredCase):
    async def test_a_steered_message_enters_before_the_shared_turn_result(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        launch_id, thread_id = await self.launch_as(conn, STEER)
        await self.wait_events(conn, 'turn.tool', thread_id)
        await conn.ok('threads.send', id='s1', thread_id=thread_id, message=SECOND)
        await self.wait_events(conn, 'turn.result', thread_id)
        turn = await self.turn(conn, thread_id)
        self.assertEqual([kind for kind, _ in turn], [
            'turn.started', 'message.entered', 'turn.tool', 'message.entered', 'turn.text',
            'turn.result',
        ])
        self.assertEqual(turn[1][1], {'command_id': f'{launch_id}:message', 'text': STEER})
        self.assertEqual(turn[3][1], {'command_id': 's1', 'text': SECOND})
        self.assertEqual(turn[5][1]['text'], 'first\n\nsecond')

    async def test_a_plain_turn_logs_no_message_entered(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        _, thread_id = await self.launch_as(conn, 'hello')
        await self.wait_events(conn, 'turn.result', thread_id)
        kinds = [kind for kind, _ in await self.turn(conn, thread_id)]
        self.assertEqual(kinds, ['turn.started', 'turn.tool', 'turn.text', 'turn.result'])


class CodexEnteredTest(CodexCase, EnteredCase):
    async def test_a_codex_message_enters_after_turn_started(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        launch_id, thread_id = await self.launch_as(conn, 'hello', runner='codex')
        await self.wait_events(conn, 'turn.result', thread_id)
        turn = [entry for entry in await self.turn(conn, thread_id) if entry[0] != 'turn.text']
        self.assertEqual(turn, [
            ('turn.started', {}),
            ('message.entered', {'command_id': f'{launch_id}:message', 'text': 'hello'}),
            ('turn.result', {'state': 'completed', 'text': 'hello'}),
        ])


if __name__ == '__main__':
    unittest.main()
