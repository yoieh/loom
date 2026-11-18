import unittest

from .support import ServerCase, clean_env, run_client

ROOT = {'strategy': 'root'}


class TaskBoundaryTest(ServerCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.conn = await self.connect()
        await self.add_project(self.conn)
        self.own = (await self.launch(self.conn))['id']
        self.other = (await self.launch(self.conn))['id']

    def thread_ids(self) -> set[str]:
        return {info['id'] for info in self.serving.service.orchestrator.list()}

    def delegate(self, command_id: str, parent_id: str) -> dict:
        return {'call': 'threads.delegate',
                'params': {'id': command_id, 'parent_id': parent_id, 'task': 'Review'}}

    def launch_request(self, command_id: str, origin: str) -> dict:
        return {'call': 'threads.launch',
                'params': {'id': command_id, 'title': 'Task', 'message': 'hi',
                           'project_id': 'repo', 'workspace': ROOT, 'origin': origin}}

    async def test_an_agent_stays_inside_its_own_thread(self) -> None:
        answers = await run_client(self.socket, [
            self.delegate('d1', self.other),
            self.launch_request('l1', f'thread:{self.other}'),
            self.launch_request('l2', 'outside'),
        ], clean_env(LOOM_THREAD=self.own))
        self.assertEqual([answer['ok'] for answer in answers], [False, False, False])
        self.assertEqual(answers[0]['error'], {
            'code': 'invalid', 'message': f'parent_id must be the calling thread {self.own}'})
        for answer in answers[1:]:
            self.assertEqual(answer['error'], {
                'code': 'invalid',
                'message': f'origin must be thread:{self.own}, the calling thread'})
        self.assertEqual(self.thread_ids(), {self.own, self.other})

    async def test_an_agent_may_delegate_and_launch_as_its_own_thread(self) -> None:
        answers = await run_client(self.socket, [
            self.delegate('d1', self.own),
            self.launch_request('l1', f'thread:{self.own}'),
        ], clean_env(LOOM_THREAD=self.own))
        self.assertEqual([answer['ok'] for answer in answers], [True, True])
        self.assertEqual(answers[0]['result']['parent_id'], self.own)
        self.assertEqual(answers[1]['result']['origin'], f'thread:{self.own}')

    async def test_a_client_without_loom_thread_may_use_any_thread(self) -> None:
        answers = await run_client(self.socket, [
            self.delegate('d1', self.other),
            self.launch_request('l1', f'thread:{self.other}'),
            self.launch_request('l2', 'outside'),
        ], clean_env())
        self.assertEqual([answer['ok'] for answer in answers], [True, True, True])
        self.assertEqual(answers[0]['result']['parent_id'], self.other)
        self.assertEqual([answer['result']['origin'] for answer in answers[1:]],
                         [f'thread:{self.other}', 'outside'])


if __name__ == '__main__':
    unittest.main()
