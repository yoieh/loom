import unittest
from datetime import timedelta
from unittest import mock

from loom_contracts.events import EventType
from loom_contracts.runners import Decision, PermissionRequest, RequestAnswer

from loom_core import threads
from loom_core.clock import iso
from loom_core.orchestrator import Orchestrator
from loom_core.service import SERVED, Caller, Service

from .fakes import FakeHost, FakeRunner, FakeSession, ManualClock, wait_for
from .test_orchestrator import START, done
from .test_service import AGENT, PERSON, SCHEDULED, UNKNOWN, ServiceTest

ALLOW = Decision.ALLOW
DENY = Decision.DENY
COULD_NOT_OPEN = RequestAnswer(DENY, 'Loom could not open this request.')
LAUNCHER_ONLY = 'only the caller that launched this thread may answer its requests'
OWN = 'a thread may not answer its own requests'
ENDED = {'request_id': 'r1', 'decision': 'deny', 'reason': 'ended'}


def timed_out(seconds: int) -> RequestAnswer:
    return RequestAnswer(DENY, f'No answer came within {seconds} s, so Loom denied this request.')


class RequestsTest(ServiceTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.clock = ManualClock(START)
        self.store.clock = self.clock

    async def use_orchestrator(self, **options: object) -> None:
        await self.orchestrator.close()
        self.runner = FakeRunner()
        self.orchestrator = Orchestrator(self.store, self.projects, {'claude': self.runner},
                                         FakeHost(str(self.data_dir)), **options)
        self.service = Service(self.store, self.projects, self.orchestrator)

    async def start(self, caller: Caller = PERSON, **extra: object) -> tuple[str, FakeSession]:
        info = await self.call('threads.launch', self.launch_params(**extra), caller)
        await self.orchestrator.drain()
        self.assertEqual(self.state(info['id']), 'running')
        return info['id'], self.runner.sessions[info['id']]

    async def delegated(self, parent_id: str) -> tuple[str, FakeSession]:
        info = await self.call('threads.delegate', {'id': self.next_id(), 'parent_id': parent_id,
                                                    'task': 'Review it'},
                               Caller(thread_id=parent_id))
        await self.orchestrator.drain()
        self.assertEqual(self.state(info['id']), 'running')
        return info['id'], self.runner.sessions[info['id']]

    async def ask(self, thread_id: str, session: FakeSession, request_id: str = 'r1',
                  description: str | None = 'Remove x') -> None:
        count = len(self.opened(thread_id))
        session.emit(PermissionRequest(request_id, 'Bash', {'command': 'rm x'}, description))
        await wait_for(lambda: len(self.opened(thread_id)) > count)
        await self.orchestrator.drain()

    async def ask_refused(self, session: FakeSession, request_id: str = 'r1') -> None:
        count = len(session.responses)
        session.emit(PermissionRequest(request_id, 'Bash', {'command': 'rm x'}, None))
        await wait_for(lambda: len(session.responses) > count)
        await self.orchestrator.drain()

    def respond_params(self, request_id: str = 'r1', decision: str = 'allow',
                       **extra: object) -> dict:
        return {'id': self.next_id(), 'request_id': request_id, 'decision': decision, **extra}

    async def respond(self, request_id: str = 'r1', decision: str = 'allow',
                      caller: Caller = PERSON, **extra: object) -> dict:
        answer = await self.call('requests.respond',
                                 self.respond_params(request_id, decision, **extra), caller)
        await self.orchestrator.drain()
        return answer

    async def respond_refused(self, code: str, request_id: str = 'r1',
                              caller: Caller = PERSON) -> str:
        message = await self.refused('requests.respond', self.respond_params(request_id),
                                     code, caller)
        await self.orchestrator.drain()
        return message

    def opened(self, thread_id: str) -> list[dict]:
        return [event['data'] for event in self.events(thread_id, EventType.REQUEST_OPENED)]

    def closed(self, thread_id: str) -> list[dict]:
        return [event['data'] for event in self.events(thread_id, EventType.REQUEST_CLOSED)]

    def waiting(self, thread_id: str) -> bool:
        return self.orchestrator.get(thread_id)['waiting']

    def last_state(self, thread_id: str) -> dict:
        return self.events(thread_id, EventType.THREAD_STATE)[-1]['data']

    def assert_no_allow(self) -> None:
        for session in self.runner.sessions.values():
            self.assertNotIn(ALLOW, [answer.decision for _, answer in session.responses])


class OpenTest(RequestsTest):
    async def test_a_permission_request_opens_with_its_expiry_and_the_thread_waits(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        self.assertEqual(self.opened(thread_id), [{
            'request_id': 'r1', 'tool_name': 'Bash', 'input': {'command': 'rm x'},
            'description': 'Remove x', 'expires_at': iso(START + timedelta(seconds=600)),
        }])
        info = await self.call('threads.get', {'thread_id': thread_id})
        self.assertEqual((info['state'], info['waiting']), ('running', True))
        self.assertEqual(self.last_state(thread_id), {'state': 'running', 'waiting': True})
        self.assertEqual(session.responses, [])

    async def test_a_request_id_loom_already_has_is_denied_at_once(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        await self.ask_refused(session)
        other_id, other = await self.start()
        await self.ask_refused(other)
        self.assertEqual(session.responses, [('r1', COULD_NOT_OPEN)])
        self.assertEqual(other.responses, [('r1', COULD_NOT_OPEN)])
        self.assertEqual(len(self.opened(thread_id)), 1)
        self.assertEqual(self.opened(other_id), [])
        self.assertIs(self.waiting(other_id), False)
        self.assert_no_allow()

    async def test_a_request_while_the_thread_is_stopping_is_denied_at_once(self) -> None:
        thread_id, session = await self.start()
        with mock.patch.object(self.orchestrator.lanes, 'submit'):
            await self.call('threads.stop', {'id': self.next_id(), 'thread_id': thread_id})
        await self.ask_refused(session)
        self.assertEqual(session.responses, [('r1', COULD_NOT_OPEN)])
        self.assertEqual(self.opened(thread_id), [])
        self.assertIs(self.waiting(thread_id), False)
        self.assert_no_allow()


class AnswerTest(RequestsTest):
    async def test_allow_answers_the_session_once_and_ends_waiting(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        answer = await self.respond(message='fine')
        self.assertEqual((answer['id'], answer['state'], answer['waiting']),
                         (thread_id, 'running', False))
        self.assertEqual(answer, self.orchestrator.get(thread_id))
        self.assertEqual(session.responses, [('r1', RequestAnswer(ALLOW, None))])
        self.assertEqual(self.closed(thread_id), [{'request_id': 'r1', 'decision': 'allow'}])
        self.assertEqual(self.last_state(thread_id), {'state': 'running', 'waiting': False})

    async def test_deny_sends_the_message_or_leaves_the_runners_default(self) -> None:
        thread_id, session = await self.start()
        for number, extra in enumerate(({'message': 'Not now'}, {'message': ''}, {})):
            await self.ask(thread_id, session, f'r{number}')
            await self.respond(f'r{number}', 'deny', **extra)
        self.assertEqual(session.responses, [('r0', RequestAnswer(DENY, 'Not now')),
                                             ('r1', RequestAnswer(DENY, None)),
                                             ('r2', RequestAnswer(DENY, None))])
        self.assertEqual([data['decision'] for data in self.closed(thread_id)],
                         ['deny', 'deny', 'deny'])

    async def test_the_first_answer_wins_and_a_retry_replays_it(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        params = self.respond_params()
        first = await self.call('requests.respond', params)
        await self.orchestrator.drain()
        message = await self.refused('requests.respond', self.respond_params(decision='deny'),
                                     'conflict')
        self.assertEqual(message, 'request r1 is closed')
        await self.finish(thread_id, session)
        again = await self.call('requests.respond', params)
        await self.orchestrator.drain()
        self.assertEqual(again, first)
        self.assertEqual(again['state'], 'running')
        self.assertEqual(session.responses, [('r1', RequestAnswer(ALLOW, None))])
        self.assertEqual(self.closed(thread_id), [{'request_id': 'r1', 'decision': 'allow'}])

    async def test_waiting_stays_until_the_last_open_request_is_answered(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session, 'r1')
        await self.ask(thread_id, session, 'r2')
        self.assertIs((await self.respond('r1'))['waiting'], True)
        self.assertIs(self.waiting(thread_id), True)
        self.assertIs((await self.respond('r2', 'deny'))['waiting'], False)
        self.assertIs(self.waiting(thread_id), False)
        self.assertEqual(self.last_state(thread_id), {'state': 'running', 'waiting': False})

    async def test_an_answer_while_the_thread_is_stopping_is_a_conflict(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        with mock.patch.object(self.orchestrator.lanes, 'submit'):
            await self.call('threads.stop', {'id': self.next_id(), 'thread_id': thread_id})
        message = await self.respond_refused('conflict')
        self.assertEqual(message, f'thread {thread_id} is stopping')
        self.assertEqual(session.responses, [])
        self.assertEqual(self.closed(thread_id), [])

    async def test_a_failed_send_keeps_the_callers_decision(self) -> None:
        for error in (KeyError('r1'), RuntimeError('session closed')):
            with self.subTest(error=type(error).__name__):
                request_id = type(error).__name__
                thread_id, session = await self.start()
                await self.ask(thread_id, session, request_id)
                session.fail_respond = error
                answer = await self.respond(request_id)
                self.assertEqual((answer['id'], answer['state']), (thread_id, 'running'))
                self.assertIsNone(session.fail_respond)
                self.assertEqual(self.closed(thread_id),
                                 [{'request_id': request_id, 'decision': 'allow'}])
                self.assertEqual(self.state(thread_id), 'running')
                self.assertIs(self.waiting(thread_id), False)
                self.assertEqual(self.store.connection.execute(
                    'SELECT decision, reason FROM permission_requests WHERE id = ?',
                    (request_id,)).fetchone(), ('allow', None))
                message = await self.respond_refused('conflict', request_id)
                self.assertEqual(message, f'request {request_id} is closed')
                self.assertEqual(session.responses, [])


class WhoAnswersTest(RequestsTest):
    async def test_a_caller_thread_launched_by_a_person_takes_only_a_persons_answer(self) -> None:
        thread_id, session = await self.start(requests='caller')
        await self.ask(thread_id, session)
        for caller in (AGENT, UNKNOWN, *SCHEDULED):
            with self.subTest(caller=caller):
                self.assertEqual(await self.respond_refused('forbidden', caller=caller),
                                 LAUNCHER_ONLY)
        self.assertEqual((session.responses, self.closed(thread_id)), ([], []))
        await self.respond(caller=PERSON)
        self.assertEqual(session.responses, [('r1', RequestAnswer(ALLOW, None))])

    async def test_a_caller_thread_launched_by_an_agent_takes_only_its_answer(self) -> None:
        thread_id, session = await self.start(Caller(thread_id='t1'), requests='caller')
        await self.ask(thread_id, session)
        others = (PERSON, UNKNOWN, Caller(thread_id='t2'),
                  Caller(thread_id='t1', schedule_id='s1'))
        for caller in others:
            with self.subTest(caller=caller):
                self.assertEqual(await self.respond_refused('forbidden', caller=caller),
                                 LAUNCHER_ONLY)
        await self.respond(caller=Caller(thread_id='t1'))
        self.assertEqual(session.responses, [('r1', RequestAnswer(ALLOW, None))])

    async def test_a_scheduled_launch_has_no_launcher_so_nobody_answers(self) -> None:
        thread_id, session = await self.start(SCHEDULED[0], requests='caller')
        self.assertIsNone(threads.fetch(self.store.connection, thread_id)['launched_by'])
        await self.ask(thread_id, session)
        for caller in (PERSON, AGENT, UNKNOWN, *SCHEDULED):
            with self.subTest(caller=caller):
                self.assertEqual(await self.respond_refused('forbidden', caller=caller),
                                 LAUNCHER_ONLY)
        self.assertEqual(session.responses, [])

    async def test_delegated_threads_keep_the_top_level_launcher(self) -> None:
        for launcher, identity in ((PERSON, 'person'), (Caller(thread_id='t1'), 'thread:t1')):
            with self.subTest(launcher=identity):
                parent_id, _ = await self.start(launcher, requests='caller')
                child_id, child = await self.delegated(parent_id)
                grandchild_id, grandchild = await self.delegated(child_id)
                for thread_id in (parent_id, child_id, grandchild_id):
                    row = threads.fetch(self.store.connection, thread_id)
                    self.assertEqual((row['requests'], row['launched_by']), ('caller', identity))
                await self.ask(child_id, child, f'{identity}-child')
                await self.ask(grandchild_id, grandchild, f'{identity}-grandchild')
                for request_id, delegators in ((f'{identity}-child', (parent_id,)),
                                               (f'{identity}-grandchild', (parent_id, child_id))):
                    for delegator in delegators:
                        message = await self.respond_refused(
                            'forbidden', request_id, Caller(thread_id=delegator))
                        self.assertEqual(message, LAUNCHER_ONLY)
                    await self.respond(request_id, caller=launcher)
                self.assertEqual(child.responses,
                                 [(f'{identity}-child', RequestAnswer(ALLOW, None))])
                self.assertEqual(grandchild.responses,
                                 [(f'{identity}-grandchild', RequestAnswer(ALLOW, None))])

    async def test_any_takes_a_person_or_another_agent_but_not_the_asking_thread(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session, 'r1')
        await self.ask(thread_id, session, 'r2')
        self.assertEqual(await self.respond_refused('forbidden', 'r1',
                                                    Caller(thread_id=thread_id)), OWN)
        await self.respond('r1', caller=Caller(thread_id='t2'))
        await self.respond('r2', 'deny', caller=PERSON)
        self.assertEqual(session.responses, [('r1', RequestAnswer(ALLOW, None)),
                                             ('r2', RequestAnswer(DENY, None))])

    async def test_a_caller_thread_may_not_answer_its_own_requests(self) -> None:
        thread_id, session = await self.start(requests='caller')
        await self.ask(thread_id, session)
        for caller in (Caller(thread_id=thread_id),
                       Caller(thread_id=thread_id, schedule_id='s1')):
            with self.subTest(caller=caller):
                self.assertEqual(await self.respond_refused('forbidden', caller=caller), OWN)
        self.assertEqual(session.responses, [])


class TimeoutTest(RequestsTest):
    async def test_the_request_is_denied_at_exactly_the_timeout(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        self.clock.advance(599.999)
        await self.orchestrator.expire_requests()
        await self.orchestrator.drain()
        self.assertEqual((session.responses, self.closed(thread_id)), ([], []))
        self.assertIs(self.waiting(thread_id), True)
        self.clock.advance(0.001)
        await self.orchestrator.expire_requests()
        await self.orchestrator.drain()
        self.assertEqual(session.responses, [('r1', timed_out(600))])
        self.assertEqual(self.closed(thread_id),
                         [{'request_id': 'r1', 'decision': 'deny', 'reason': 'timeout'}])
        self.assertIs(self.waiting(thread_id), False)
        self.assertEqual(self.last_state(thread_id), {'state': 'running', 'waiting': False})
        self.assertEqual(await self.respond_refused('conflict'), 'request r1 is closed')
        await self.orchestrator.expire_requests()
        await self.orchestrator.drain()
        self.assertEqual(len(session.responses), 1)
        self.assert_no_allow()

    async def test_the_timeout_can_be_set(self) -> None:
        await self.use_orchestrator(request_timeout=5)
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        self.assertEqual(self.opened(thread_id)[0]['expires_at'],
                         iso(START + timedelta(seconds=5)))
        self.clock.advance(4.999)
        await self.orchestrator.expire_requests()
        self.assertEqual(session.responses, [])
        self.clock.advance(0.001)
        await self.orchestrator.expire_requests()
        await self.orchestrator.drain()
        self.assertEqual(session.responses, [('r1', timed_out(5))])
        self.assert_no_allow()

    async def test_the_loop_from_recover_denies_and_close_stops_it(self) -> None:
        await self.use_orchestrator(request_check=0.01)
        await self.orchestrator.recover()
        loop = self.orchestrator._request_task
        self.assertIsNotNone(loop)
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        self.clock.advance(600)
        await wait_for(lambda: self.closed(thread_id))
        await self.orchestrator.drain()
        self.assertEqual(self.closed(thread_id),
                         [{'request_id': 'r1', 'decision': 'deny', 'reason': 'timeout'}])
        self.assertEqual(session.responses, [('r1', timed_out(600))])
        await self.orchestrator.close()
        self.assertTrue(loop.done())
        self.assertIsNone(self.orchestrator._request_task)


class EndedTest(RequestsTest):
    def assert_ended(self, thread_id: str) -> None:
        self.assertEqual(self.closed(thread_id), [ENDED])
        for session in self.runner.sessions.values():
            self.assertEqual(session.responses, [])

    async def test_the_turns_result_closes_an_open_request(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        await self.finish(thread_id, session, done())
        self.assert_ended(thread_id)
        self.assertEqual((self.state(thread_id), self.waiting(thread_id)), ('idle', False))
        self.assertEqual(await self.respond_refused('conflict'), 'request r1 is closed')
        self.assertEqual(session.responses, [])

    async def test_a_stop_closes_an_open_request(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        await self.call('threads.stop', {'id': self.next_id(), 'thread_id': thread_id})
        await self.orchestrator.drain()
        self.assertEqual(self.state(thread_id), 'idle')
        self.assert_ended(thread_id)

    async def test_a_crash_closes_an_open_request(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        session.exit(1)
        await wait_for(lambda: self.closed(thread_id))
        await self.orchestrator.drain()
        self.assertIn('sleeping', self.states(thread_id))
        self.assert_ended(thread_id)

    async def test_a_settle_closes_an_open_request(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        await self.call('threads.settle', {'id': self.next_id(), 'thread_id': thread_id})
        await self.orchestrator.drain()
        self.assertEqual(self.state(thread_id), 'settled')
        self.assert_ended(thread_id)

    async def test_recovery_after_a_restart_closes_a_stored_open_request(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        await self.use_orchestrator()
        self.assertEqual(self.closed(thread_id), [])
        await self.orchestrator.recover()
        await self.orchestrator.drain()
        self.assert_ended(thread_id)
        self.assertEqual(session.responses, [])
        self.assertEqual(self.events(thread_id, EventType.TURN_RESULT)[-1]['data'],
                         {'state': 'failed', 'reason': 'restart'})

    async def test_an_ended_request_does_not_time_out_later(self) -> None:
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        await self.finish(thread_id, session, done())
        self.clock.advance(600)
        await self.orchestrator.expire_requests()
        await self.orchestrator.drain()
        self.assert_ended(thread_id)


class ParamsTest(RequestsTest):
    async def test_the_call_is_served(self) -> None:
        self.assertIn('requests.respond', SERVED)
        self.assertTrue(callable(self.service.method('requests.respond')))

    async def test_params_that_are_not_an_object(self) -> None:
        for params in (None, [], 'r1', 5):
            with self.subTest(params=params):
                self.assertEqual(await self.refused('requests.respond', params),
                                 'params must be an object')

    async def test_request_id(self) -> None:
        cases = {'missing': ({}, 'request_id is required'),
                 'null': ({'request_id': None}, 'request_id is required'),
                 'number': ({'request_id': 5}, 'request_id must be a string')}
        for name, (fields, message) in cases.items():
            with self.subTest(case=name):
                params = {'id': self.next_id(), 'decision': 'allow', **fields}
                self.assertEqual(await self.refused('requests.respond', params), message)

    async def test_decision(self) -> None:
        cases = {'missing': ({}, 'decision is required'),
                 'null': ({'decision': None}, 'decision is required'),
                 'maybe': ({'decision': 'maybe'}, 'decision must be one of allow, deny'),
                 'number': ({'decision': 5}, 'decision must be one of allow, deny')}
        for name, (fields, message) in cases.items():
            with self.subTest(case=name):
                params = {'id': self.next_id(), 'request_id': 'r1', **fields}
                self.assertEqual(await self.refused('requests.respond', params), message)

    async def test_message(self) -> None:
        too_long = 'message must be a string of at most 2000 characters'
        cases = {'null': (None, 'message must not be null'), 'number': (5, too_long),
                 'too long': ('x' * 2001, too_long)}
        for name, (value, message) in cases.items():
            with self.subTest(case=name):
                params = self.respond_params(message=value)
                self.assertEqual(await self.refused('requests.respond', params), message)
        thread_id, session = await self.start()
        await self.ask(thread_id, session)
        await self.respond(decision='deny', message='x' * 2000)
        self.assertEqual(session.responses, [('r1', RequestAnswer(DENY, 'x' * 2000))])

    async def test_command_id(self) -> None:
        for name, fields in {'missing': {}, 'null': {'id': None}, 'number': {'id': 5}}.items():
            with self.subTest(case=name):
                params = {'request_id': 'r1', 'decision': 'allow', **fields}
                self.assertEqual(await self.refused('requests.respond', params),
                                 'id must be a string of 1 to 200 characters')

    async def test_an_unknown_request_is_not_found(self) -> None:
        self.assertEqual(await self.respond_refused('not_found', 'nope'),
                         "no request with id 'nope'")

    async def test_errors_come_in_the_contracts_order(self) -> None:
        cases = (
            ({'id': 5, 'request_id': 5, 'decision': 5, 'message': None},
             'invalid', 'request_id must be a string'),
            ({'id': 5, 'request_id': 'r1', 'decision': 5, 'message': None},
             'invalid', 'decision must be one of allow, deny'),
            ({'id': 5, 'request_id': 'r1', 'decision': 'allow', 'message': None},
             'invalid', 'message must not be null'),
            ({'id': 5, 'request_id': 'nope', 'decision': 'allow'},
             'invalid', 'id must be a string of 1 to 200 characters'),
        )
        for params, code, message in cases:
            with self.subTest(message=message):
                self.assertEqual(await self.refused('requests.respond', params, code), message)
        thread_id, session = await self.start(requests='caller')
        await self.ask(thread_id, session)
        with mock.patch.object(self.orchestrator.lanes, 'submit'):
            await self.call('threads.stop', {'id': self.next_id(), 'thread_id': thread_id})
        self.assertEqual(await self.respond_refused('forbidden', caller=Caller(
            thread_id=thread_id)), OWN)
        self.assertEqual(await self.respond_refused('forbidden', caller=AGENT), LAUNCHER_ONLY)
        self.assertEqual(await self.respond_refused('conflict'),
                         f'thread {thread_id} is stopping')


if __name__ == '__main__':
    unittest.main()
