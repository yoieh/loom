import unittest
from unittest import mock

from loom_contracts.events import EventType
from loom_contracts.runners import DispatchMode, TurnResult
from loom_contracts.states import TurnState

from loom_core import threads
from loom_core.delegation import child_title, result_text

from .fakes import FakeRunner, wait_for
from .test_orchestrator import BARE, OrchestratorTest, done, existing, unrecorded_worktree
from .test_workspaces import git

QUEUE = DispatchMode.QUEUE
WORKTREE = {'strategy': 'worktree'}


class DelegateTest(OrchestratorTest):
    async def delegate(self, parent_id: str, task: str = 'Review it', **extra: object) -> dict:
        return await self.orchestrator.delegate(self.delegate_params(parent_id, task, **extra))

    def delegate_params(self, parent_id: str, task: str = 'Review it', **extra: object) -> dict:
        return {'id': self.next_id(), 'parent_id': parent_id, 'task': task, **extra}

    async def child(self, parent_id: str, task: str = 'Review it', **extra: object) -> tuple:
        info = await self.delegate(parent_id, task, **extra)
        await self.orchestrator.drain()
        self.assertEqual(self.state(info['id']), 'running')
        return info, self.runner.sessions[info['id']]

    def row(self, thread_id: str):
        return threads.fetch(self.store.connection, thread_id)

    def sent_to(self, thread_id: str) -> list[str]:
        return [text for text, _ in self.runner.sessions[thread_id].sent]


class ChildTest(DelegateTest):
    async def test_child_shares_the_parents_worktree(self) -> None:
        parent_id, _ = await self.running(workspace=WORKTREE, model='opus')
        parent = self.orchestrator.get(parent_id)
        info, session = await self.child(parent_id, 'Review the diff')
        self.assertEqual((info['parent_id'], info['project_id'], info['origin']),
                         (parent_id, 'loom', None))
        self.assertEqual((info['workspace_path'], info['branch']),
                         (parent['workspace_path'], parent['branch']))
        self.assertEqual(parent['branch'], f'loom/{parent_id}')
        self.assertEqual((info['runner'], info['model']), ('claude', 'opus'))
        self.assertEqual(self.row(info['id'])['workspace_strategy'], 'existing_worktree')
        spec = self.runner.starts[-1]
        self.assertEqual((spec.id, spec.workspace_path, spec.parent_id, spec.model),
                         (info['id'], parent['workspace_path'], parent_id, 'opus'))
        self.assertEqual(session.sent, [('Review the diff', QUEUE)])
        self.assertEqual(self.states(info['id']), ['queued', 'starting', 'running'])

    async def test_an_explicit_parent_worktree_is_accepted(self) -> None:
        parent_id, _ = await self.running(workspace=WORKTREE)
        parent = self.orchestrator.get(parent_id)
        info, _ = await self.child(
            parent_id, workspace=existing(parent['workspace_path'], parent['branch']))
        self.assertEqual((info['workspace_path'], info['branch']),
                         (parent['workspace_path'], parent['branch']))
        self.assertEqual(self.row(info['id'])['workspace_strategy'], 'existing_worktree')
        self.assertEqual(self.runner.starts[-1].workspace_path, parent['workspace_path'])

    async def test_runner_and_model_default_to_the_parents(self) -> None:
        codex = FakeRunner('codex')
        await self.orchestrator.close()
        self.orchestrator = self.make(self.runner, codex)
        parent = await self.launch(runner='codex', model='gpt-5')
        await self.orchestrator.drain()
        info = await self.delegate(parent['id'])
        await self.orchestrator.drain()
        self.assertEqual((info['runner'], info['model']), ('codex', 'gpt-5'))
        self.assertEqual([spec.id for spec in codex.starts], [parent['id'], info['id']])
        given = await self.delegate(parent['id'], runner='claude', model='opus')
        await self.orchestrator.drain()
        self.assertEqual((given['runner'], given['model']), ('claude', 'opus'))
        self.assertEqual([spec.id for spec in self.runner.starts], [given['id']])

    async def test_child_of_a_root_parent_is_in_root(self) -> None:
        parent_id, _ = await self.running()
        info, _ = await self.child(parent_id)
        self.assertEqual(self.row(info['id'])['workspace_strategy'], 'root')
        self.assertEqual(info['workspace_path'], str(self.root))
        self.assertEqual(self.runner.starts[-1].workspace_path, str(self.root))

    async def test_a_worktree_workspace_gives_the_child_its_own(self) -> None:
        parent_id, _ = await self.running()
        info, _ = await self.child(parent_id, workspace=WORKTREE)
        path = self.data_dir.resolve() / 'worktrees' / 'loom' / info['id']
        self.assertEqual((info['workspace_path'], info['branch']),
                         (str(path), f'loom/{info["id"]}'))
        self.assertEqual(git(path, 'symbolic-ref', '--short', 'HEAD'), f'loom/{info["id"]}')
        self.assertEqual(self.runner.starts[-1].workspace_path, str(path))

    async def test_title_is_the_tasks_first_line_cut_to_80(self) -> None:
        parent_id, _ = await self.running()
        info = await self.delegate(parent_id, 'x' * 100 + '\nThe rest of the task')
        self.assertEqual(info['title'], 'x' * 80)
        info = await self.delegate(parent_id, 'Check the tests\nThen the docs')
        self.assertEqual(info['title'], 'Check the tests')
        self.assertEqual(child_title('One line'), 'One line')

    async def test_mode_is_the_projects_effective_mode(self) -> None:
        self.projects.set_mode({'id': self.next_id(), 'project_id': 'loom',
                                'permission_mode': 'full-access'})
        parent_id, _ = await self.running()
        self.assertEqual(self.orchestrator.get(parent_id)['permission_mode'], 'full-access')
        self.projects.set_mode({'id': self.next_id(), 'project_id': 'loom',
                                'permission_mode': 'approval-required'})
        info, _ = await self.child(parent_id)
        self.assertEqual(info['permission_mode'], 'approval-required')
        self.assertEqual(self.runner.starts[-1].permission_mode, 'approval-required')
        self.assertEqual(self.orchestrator.get(parent_id)['permission_mode'], 'full-access')

    async def test_info_rows_and_events_use_parent_id(self) -> None:
        parent_id, _ = await self.running()
        info, session = await self.child(parent_id)
        await self.finish(info['id'], session, done(text='done'))
        self.assertEqual(BARE & set(info), set())
        self.assertEqual(self.orchestrator.get(info['id'])['parent_id'], parent_id)
        self.assertEqual(BARE & set(self.row(info['id']).keys()), set())
        for event in self.events():
            self.assertEqual(BARE & set(event['data']), set())

    async def test_requests_follow_the_parent(self) -> None:
        parent_id, _ = await self.running(requests='caller')
        info = await self.delegate(parent_id)
        self.assertEqual(self.row(info['id'])['requests'], 'caller')


class ResultTest(DelegateTest):
    async def test_completed_and_failed_results_reach_an_idle_parent(self) -> None:
        parent_id, parent = await self.idle()
        info, session = await self.child(parent_id, 'Review it')
        await self.finish(info['id'], session, done(text='done'))
        self.assertEqual(parent.sent[-1],
                         (f'Task {info["id"]} (Review it) completed.\n\ndone', QUEUE))
        self.assertEqual(self.state(parent_id), 'running')
        await self.finish(parent_id, parent)
        other, session = await self.child(parent_id, 'Fix it')
        await self.finish(other['id'], session, TurnResult(TurnState.FAILED, None, 'boom'))
        self.assertEqual(parent.sent[-1], (f'Task {other["id"]} (Fix it) failed: boom.', QUEUE))

    async def test_result_texts(self) -> None:
        parent_id, _ = await self.running()
        info = await self.delegate(parent_id, 'Fix it')
        row = self.row(info['id'])
        head = f'Task {info["id"]} (Fix it)'
        self.assertEqual(result_text(row, done(text=None)), f'{head} completed.')
        self.assertEqual(result_text(row, TurnResult(TurnState.FAILED, 'log', None)),
                         f'{head} failed: no reason.\n\nlog')

    async def test_result_goes_before_a_running_parents_queue(self) -> None:
        parent_id, parent = await self.running()
        await self.send(parent_id, 'First', mode='queue')
        await self.send(parent_id, 'Second', mode='queue')
        info, session = await self.child(parent_id)
        await self.finish(info['id'], session, done(text='done'))
        self.assertEqual(self.state(parent_id), 'running')
        self.assertEqual(self.sent_to(parent_id), ['Hello'])
        result = f'Task {info["id"]} (Review it) completed.\n\ndone'
        self.assertEqual(self.store.connection.execute(
            'SELECT text, mode, priority FROM messages WHERE thread_id = ? '
            'ORDER BY priority DESC, seq', (parent_id,)).fetchall(),
            [(result, 'queue', 1), ('First', 'queue', 0), ('Second', 'queue', 0)])
        await self.finish(parent_id, parent)
        self.assertEqual(self.sent_to(parent_id), ['Hello', result])
        await self.finish(parent_id, parent)
        await self.finish(parent_id, parent)
        self.assertEqual(self.sent_to(parent_id), ['Hello', result, 'First', 'Second'])

    async def test_result_and_turn_result_share_one_transaction(self) -> None:
        parent_id, _ = await self.running()
        info, session = await self.child(parent_id)
        statements: list[str] = []
        self.store.connection.set_trace_callback(statements.append)
        try:
            await self.finish(info['id'], session, done(text='done'))
        finally:
            self.store.connection.set_trace_callback(None)
        groups: list[list[str]] = []
        group: list[str] | None = None
        for statement in statements:
            word = statement.strip().upper()
            if word.startswith('BEGIN'):
                group = []
            elif word == 'COMMIT' and group is not None:
                groups.append(group)
                group = None
            elif group is not None:
                group.append(statement)
        posted = [group for group in groups
                  if any(s.startswith('INSERT INTO messages') for s in group)]
        self.assertEqual(len(posted), 1)
        logged = [s for s in posted[0] if s.startswith('INSERT INTO events')
                  and "'turn.result'" in s and f"'{info['id']}'" in s]
        self.assertEqual(len(logged), 1)

    async def test_each_result_is_posted_once(self) -> None:
        parent_id, parent = await self.idle()
        info, session = await self.child(parent_id)
        await self.finish(info['id'], session, done(text='one'))
        await self.finish(parent_id, parent)
        session.emit(done(text='stray'))
        await wait_for(lambda: len(self.events(info['id'], EventType.TURN_RESULT)) == 2)
        await self.orchestrator.drain()
        results = [text for text in self.sent_to(parent_id) if text.startswith('Task ')]
        self.assertEqual(results, [f'Task {info["id"]} (Review it) completed.\n\none'])
        self.assertEqual(self.pending(parent_id), [])

    async def test_no_result_for_a_stopped_turn_or_a_settled_parent(self) -> None:
        parent_id, _ = await self.idle()
        info, session = await self.child(parent_id)
        await self.finish(info['id'], session, TurnResult(TurnState.STOPPED, 'cut', 'interrupted'))
        self.assertEqual(self.sent_to(parent_id), ['Hello'])
        self.assertEqual(self.pending(parent_id), [])
        self.assertEqual(self.state(parent_id), 'idle')
        other, session = await self.child(parent_id)
        await self.orchestrator.settle({'id': self.next_id(), 'thread_id': parent_id})
        await self.orchestrator.drain()
        await self.finish(other['id'], session, done(text='done'))
        self.assertEqual(self.state(other['id']), 'idle')
        self.assertEqual(self.pending(parent_id), [])
        self.assertEqual(self.state(parent_id), 'settled')


class DelegateErrorsTest(DelegateTest):
    async def test_unknown_parent_is_not_found(self) -> None:
        await self.assert_error('not_found', self.orchestrator.delegate,
                                self.delegate_params('nope'))

    async def test_settled_or_failed_parent_is_conflict(self) -> None:
        settled, _ = await self.running()
        await self.orchestrator.settle({'id': self.next_id(), 'thread_id': settled})
        await self.orchestrator.drain()
        await self.assert_error('conflict', self.orchestrator.delegate,
                                self.delegate_params(settled))
        await self.use(FakeRunner(fail_start=RuntimeError('no binary')))
        failed = (await self.launch())['id']
        await self.orchestrator.drain()
        await self.assert_error('conflict', self.orchestrator.delegate,
                                self.delegate_params(failed))

    async def test_an_unrecorded_worktree_is_invalid(self) -> None:
        parent_id, _ = await self.running(workspace=WORKTREE)
        path, branch = unrecorded_worktree(self.root, self.data_dir)
        await self.assert_error('invalid', self.orchestrator.delegate,
                                self.delegate_params(parent_id, workspace=existing(path, branch)),
                                'path')
        self.assertEqual([spec.id for spec in self.runner.starts], [parent_id])

    async def test_retry_returns_the_first_child_and_starts_once(self) -> None:
        parent_id, _ = await self.running()
        params = self.delegate_params(parent_id)
        first = await self.orchestrator.delegate(dict(params))
        await self.orchestrator.drain()
        with mock.patch.object(self.orchestrator.lanes, 'submit') as submit:
            again = await self.orchestrator.delegate(dict(params))
        submit.assert_not_called()
        await self.orchestrator.drain()
        self.assertEqual(again, first)
        self.assertEqual(len(self.orchestrator.list()), 2)
        self.assertEqual([spec.id for spec in self.runner.starts], [parent_id, first['id']])


class DelegateParamsTest(DelegateTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.parent_id, _ = await self.running()

    def params(self, **extra: object) -> dict:
        return {**self.delegate_params(self.parent_id), **extra}

    async def test_null_fields_are_invalid(self) -> None:
        for field in ('parent_id', 'task', 'workspace', 'runner', 'model'):
            with self.subTest(field=field):
                await self.assert_error('invalid', self.orchestrator.delegate,
                                        self.params(**{field: None}), field)

    async def test_missing_fields_are_invalid(self) -> None:
        for field in ('parent_id', 'task'):
            params = self.params()
            del params[field]
            with self.subTest(field=field):
                await self.assert_error('invalid', self.orchestrator.delegate, params, field)
        params = self.params(parent=self.parent_id)
        del params['parent_id']
        await self.assert_error('invalid', self.orchestrator.delegate, params, 'parent_id')
        await self.assert_error('invalid', self.orchestrator.delegate, ['task'], 'params')

    async def test_wrong_types_are_invalid(self) -> None:
        cases = {'parent_id': 5, 'task': '', 'workspace': 'root', 'runner': 'gpt', 'model': 5}
        for field, value in cases.items():
            with self.subTest(field=field):
                await self.assert_error('invalid', self.orchestrator.delegate,
                                        self.params(**{field: value}), field)
        await self.assert_error('invalid', self.orchestrator.delegate,
                                self.params(runner='codex'), 'runner')
        await self.assert_error('invalid', self.orchestrator.delegate,
                                self.params(workspace={'strategy': 'clone'}), 'strategy')
        self.assertEqual(len(self.runner.starts), 1)


if __name__ == '__main__':
    unittest.main()
