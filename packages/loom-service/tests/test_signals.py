import unittest

from .support import Connection, ServerCase, clean_env, run_client


class SignalCase(ServerCase):
    async def emit(self, conn: Connection, topic: str = 'demo.done', **params: object) -> dict:
        return await conn.ok('signals.emit', id=conn.new_id(), topic=topic,
                             payload={'to': 'done'}, **params)

    async def emit_as(self, thread_id: str, command_id: str, **params: object) -> dict:
        request = {'v': 'v1', 'call': 'signals.emit', 'params': {
            'id': command_id, 'topic': 'demo.done', 'payload': {'to': 'done'}, **params}}
        [answer] = await run_client(self.socket, [request], clean_env(LOOM_THREAD=thread_id))
        self.assertEqual(answer['ok'], True, answer)
        return answer['result']


class EmitTest(SignalCase):
    async def test_a_persons_signal_has_no_by_no_cause_and_no_hops(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        event = await self.emit(conn, project_id='repo', key='run-1')
        self.assertEqual((event['type'], event['thread_id']), ('signal', None))
        self.assertEqual(event['data'], {'topic': 'demo.done', 'payload': {'to': 'done'},
                                         'hops': 0, 'project_id': 'repo', 'key': 'run-1'})
        stored = [item for item in await self.all_events(conn) if item['type'] == 'signal']
        self.assertEqual(stored, [event])

    async def test_a_retry_over_the_socket_answers_the_first_event(self) -> None:
        conn = await self.connect()
        params = {'id': 'sig-1', 'topic': 'demo.done', 'payload': {}}
        first = await conn.ok('signals.emit', **params)
        again = await conn.ok('signals.emit', **params)
        self.assertEqual(again, first)
        signals = [item for item in await self.all_events(conn) if item['type'] == 'signal']
        self.assertEqual(signals, [first])

    async def test_a_bad_topic_is_refused_over_the_socket(self) -> None:
        conn = await self.connect()
        answer = await conn.call('signals.emit', id=conn.new_id(), topic='thread.settled',
                                 payload={})
        self.assertEqual(answer, {'ok': False, 'error': {
            'code': 'invalid', 'message': 'topic must not be a Loom event type'}})

    async def test_an_agent_threads_signal_has_its_by_cause_and_hops(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        first = await self.emit(conn)
        thread = await self.launch(conn, cause=first['id'])
        self.assertEqual((thread['cause'], thread['hops']), (first['id'], 1))
        info = await conn.ok('threads.get', thread_id=thread['id'])
        self.assertEqual((info['cause'], info['hops']), (first['id'], 1))

        own = await self.emit_as(thread['id'], 'agent-1')
        self.assertEqual(own['data'], {'topic': 'demo.done', 'payload': {'to': 'done'},
                                       'hops': 1, 'cause': first['id'], 'by': thread['id']})
        low = await self.emit_as(thread['id'], 'agent-2', cause=first['id'])
        self.assertEqual((low['data']['cause'], low['data']['hops']), (first['id'], 1))
        high = await self.emit_as(thread['id'], 'agent-3', cause=own['id'])
        self.assertEqual((high['data']['cause'], high['data']['hops'], high['data']['by']),
                         (own['id'], 2, thread['id']))
        person = await self.emit(conn, cause=high['id'])
        self.assertEqual((person['data']['cause'], person['data']['hops']), (high['id'], 3))
        self.assertNotIn('by', person['data'])

    async def test_an_agents_send_raises_its_targets_hops(self) -> None:
        conn = await self.connect()
        await self.add_project(conn)
        first = await self.emit(conn)
        sender = await self.launch(conn, title='Sender', cause=first['id'])
        target = await self.launch(conn, title='Target')
        request = {'v': 'v1', 'call': 'threads.send', 'params': {
            'id': 'send-1', 'thread_id': target['id'], 'message': 'more'}}
        [answer] = await run_client(self.socket, [request],
                                    clean_env(LOOM_THREAD=sender['id']))
        self.assertEqual(answer['ok'], True, answer)
        info = await conn.ok('threads.get', thread_id=target['id'])
        self.assertEqual((info['cause'], info['hops']), (first['id'], 1))


if __name__ == '__main__':
    unittest.main()
