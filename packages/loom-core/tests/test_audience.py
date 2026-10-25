import asyncio
import unittest

from loom_contracts.calls import Audience
from loom_contracts.events import EventType

from loom_core.errors import LoomError
from loom_core.service import Caller, Service
from loom_core.store import MAX_EVENTS

from .fakes import FakeRunner, wait_for
from .test_service import AGENT, PERSON, ServiceTest

SCHEDULE = Caller(schedule_id='s1')
NOT_NULL = 'audience must not be null'
NOT_ONE_OF = 'audience must be one of person, agent'
BAD_VALUES = ((None, NOT_NULL), ('people', NOT_ONE_OF), (1, NOT_ONE_OF))


class AudienceTest(ServiceTest):
    async def launch_as(self, audience: str | None = None, caller: Caller = PERSON,
                        **extra: object) -> dict:
        params = self.launch_params(**extra)
        if audience is not None:
            params['audience'] = audience
        info = await self.call('threads.launch', params, caller)
        await self.orchestrator.drain()
        return info

    async def update(self, thread_id: str, audience: str, command_id: str | None = None,
                     caller: Caller = PERSON) -> dict:
        params = {'id': command_id or self.next_id(), 'thread_id': thread_id,
                  'audience': audience}
        return await self.call('threads.update', params, caller)

    async def events(self, **params: object) -> list[dict]:
        return await self.call('threads.events', params)

    def updates(self, thread_id: str | None = None) -> list[dict]:
        return [event for event in self.store.events(thread_id=thread_id)
                if event['type'] == EventType.THREAD_UPDATED]

    def thread_events(self, found: list[dict], thread_id: str) -> list[int]:
        return [event['id'] for event in found if event['thread_id'] == thread_id]

    def ids(self, found: list[dict]) -> list[int]:
        return [event['id'] for event in found]

    def thread_count(self) -> int:
        return self.store.connection.execute('SELECT count(*) FROM threads').fetchone()[0]

    async def threadless(self) -> list[int]:
        with self.store.transaction() as tx:
            fired = self.store.append_event(tx, EventType.SCHEDULE_FIRED, None, {
                'schedule_id': 's1', 'action': 'notify', 'text': 'Stand up'})
            skipped = self.store.append_event(tx, EventType.TRIGGER_SKIPPED, None, {
                'schedule_id': 's1', 'event_id': fired, 'reason': 'hops'})
        signal = await self.call('signals.emit', {
            'id': self.next_id(), 'topic': 'demo.done', 'payload': {'to': 'done'}})
        return [fired, skipped, signal['id']]

    def threadless_ids(self, found: list[dict]) -> list[int]:
        return [event['id'] for event in found if event['thread_id'] is None]


class LaunchTest(AudienceTest):
    async def test_a_launch_without_audience_is_agent_whoever_calls(self) -> None:
        for caller in (PERSON, AGENT, SCHEDULE):
            with self.subTest(caller=caller):
                info = await self.launch_as(caller=caller)
                self.assertEqual(info['audience'], 'agent')
                self.assertEqual(self.orchestrator.get(info['id'])['audience'], 'agent')

    async def test_a_launch_stores_the_audience_it_names_from_any_caller(self) -> None:
        for audience, caller in (('person', PERSON), ('person', AGENT), ('agent', PERSON),
                                 ('agent', AGENT)):
            with self.subTest(audience=audience, caller=caller):
                info = await self.launch_as(audience, caller)
                self.assertEqual(info['audience'], audience)
                self.assertEqual(self.orchestrator.get(info['id'])['audience'], audience)

    async def test_a_bad_audience_is_refused_and_makes_no_thread(self) -> None:
        for value, expected in (*BAD_VALUES, (True, NOT_ONE_OF)):
            with self.subTest(audience=value):
                params = self.launch_params(audience=value)
                message = await self.refused('threads.launch', params)
                self.assertEqual(message, expected)
                self.assertEqual(self.receipts(params['id']), 0)
        self.assertEqual(self.thread_count(), 0)
        self.assertEqual(self.runner.starts, [])

    async def test_every_thread_result_carries_audience(self) -> None:
        parent = await self.launch_as('person')
        results = {'threads.launch': parent}
        results['threads.delegate'] = await self.call(
            'threads.delegate', {'id': 'd1', 'parent_id': parent['id'], 'task': 'Review'})
        results['threads.send'] = await self.call(
            'threads.send', {'id': 's1', 'thread_id': parent['id'], 'message': 'More'})
        results['threads.get'] = await self.call('threads.get', {'thread_id': parent['id']})
        results['threads.update'] = await self.update(parent['id'], 'agent')
        await self.orchestrator.drain()
        results['threads.stop'] = await self.call(
            'threads.stop', {'id': 'x1', 'thread_id': parent['id']})
        await self.orchestrator.drain()
        results['threads.settle'] = await self.call(
            'threads.settle', {'id': 'z1', 'thread_id': parent['id']})
        await self.orchestrator.drain()
        listed = await self.call('threads.list', {})
        self.assertEqual(len(listed), 2)
        for name, info in [*results.items(), *(('threads.list', info) for info in listed)]:
            with self.subTest(call=name):
                self.assertIn(info['audience'], ('person', 'agent'))
                keys = list(info)
                self.assertEqual(keys[keys.index('origin') + 1], 'audience')
        self.assertEqual(results['threads.send']['audience'], 'person')
        self.assertEqual(results['threads.update']['audience'], 'agent')
        self.assertEqual(results['threads.settle']['audience'], 'agent')


class DelegateTest(AudienceTest):
    async def test_a_delegated_child_is_agent_also_under_a_person_parent(self) -> None:
        for audience in ('person', 'agent'):
            with self.subTest(parent=audience):
                parent = await self.launch_as(audience)
                child = await self.call('threads.delegate', {
                    'id': self.next_id(), 'parent_id': parent['id'], 'task': 'Review'})
                self.assertEqual(child['audience'], 'agent')
                self.assertEqual(self.orchestrator.get(child['id'])['audience'], 'agent')


class ListTest(AudienceTest):
    async def test_list_filters_by_audience_and_by_project_too(self) -> None:
        added = await self.call('projects.add', self.project_params('a1'))
        person = await self.launch_as('person')
        agent = await self.launch_as('agent')
        other = await self.launch_as('person', project_id=added['id'])

        async def listed(**params: object) -> set[str]:
            return {info['id'] for info in await self.call('threads.list', params)}

        self.assertEqual(await listed(), {person['id'], agent['id'], other['id']})
        self.assertEqual(await listed(audience='person'), {person['id'], other['id']})
        self.assertEqual(await listed(audience='agent'), {agent['id']})
        self.assertEqual(await listed(audience='person', project_id='loom'), {person['id']})
        self.assertEqual(await listed(audience='agent', project_id=added['id']), set())

    async def test_a_bad_list_audience_is_refused(self) -> None:
        for value, expected in BAD_VALUES:
            with self.subTest(audience=value):
                self.assertEqual(await self.refused('threads.list', {'audience': value}),
                                 expected)


class EventsTest(AudienceTest):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.person = (await self.launch_as('person'))['id']
        self.agent = (await self.launch_as('agent'))['id']
        self.add_events(2, self.person)
        self.add_events(2, self.agent)

    async def test_without_audience_every_event_is_returned(self) -> None:
        self.assertEqual(await self.events(), self.store.events())
        self.assertEqual(await self.events(after=3), self.store.events(3))

    async def test_each_audience_returns_only_its_threads_events(self) -> None:
        everything = self.store.events()
        for audience, mine, theirs in (('person', self.person, self.agent),
                                       ('agent', self.agent, self.person)):
            with self.subTest(audience=audience):
                found = await self.events(audience=audience)
                self.assertEqual(self.ids(found), self.thread_events(everything, mine))
                self.assertEqual(self.thread_events(found, theirs), [])
                self.assertEqual(found, [event for event in everything
                                         if event['thread_id'] == mine])

    async def test_audience_combines_with_thread_id_and_after(self) -> None:
        mine = self.thread_events(self.store.events(), self.person)
        found = await self.events(audience='person', thread_id=self.person, after=mine[0])
        self.assertEqual(self.ids(found), mine[1:])
        self.assertEqual(await self.events(audience='agent', thread_id=self.person), [])

    async def test_an_event_with_no_thread_passes_both_audiences(self) -> None:
        with self.store.transaction() as tx:
            fired = self.store.append_event(tx, EventType.SCHEDULE_FIRED, None, {
                'schedule_id': 's1', 'action': 'notify', 'text': 'Stand up'})
        for audience, theirs in (('person', self.agent), ('agent', self.person)):
            with self.subTest(audience=audience):
                found = await self.events(audience=audience)
                self.assertIn(fired, self.ids(found))
                self.assertEqual(found[-1]['thread_id'], None)
                self.assertEqual(self.thread_events(found, theirs), [])

    async def test_a_person_gets_no_thread_less_signal_or_trigger_skipped(self) -> None:
        fired, skipped, signal = await self.threadless()
        found = await self.events(audience='person')
        self.assertEqual(self.threadless_ids(found), [fired])
        self.assertEqual(self.thread_events(found, self.agent), [])
        self.assertEqual(self.thread_events(found, self.person),
                         self.thread_events(self.store.events(), self.person))

    async def test_an_agent_and_an_unfiltered_reader_get_every_thread_less_event(self) -> None:
        made = await self.threadless()
        for params in ({'audience': 'agent'}, {}):
            with self.subTest(params=params):
                found = await self.events(**params)
                self.assertEqual(self.threadless_ids(found), made)
        self.assertEqual([event['type'] for event in self.store.events(made[0] - 1)],
                         ['schedule.fired', 'trigger.skipped', 'signal'])

    async def test_a_thread_that_leaves_person_shows_its_update_and_nothing_later(self) -> None:
        await self.update(self.person, 'agent')
        self.add_events(2, self.person)
        update = self.updates(self.person)
        self.assertEqual(len(update), 1)
        found = await self.events(audience='person')
        self.assertEqual(self.thread_events(found, self.person), [update[0]['id']])
        self.assertEqual(found[-1]['data'], {'audience': 'agent', 'previous_audience': 'person'})
        agent = await self.events(audience='agent')
        self.assertEqual(self.thread_events(agent, self.person),
                         self.thread_events(self.store.events(), self.person))

    async def test_a_thread_that_becomes_person_shows_its_earlier_and_later_events(self) -> None:
        earlier = self.thread_events(self.store.events(), self.agent)
        await self.update(self.agent, 'person')
        self.add_events(2, self.agent)
        everything = self.thread_events(self.store.events(), self.agent)
        self.assertEqual(len(everything), len(earlier) + 3)
        found = await self.events(audience='person')
        self.assertEqual(self.thread_events(found, self.agent), everything)
        update = self.updates(self.agent)
        self.assertEqual(self.thread_events(await self.events(audience='agent'), self.agent),
                         [update[0]['id']])

    async def test_a_bad_events_audience_is_refused_for_a_page_and_a_follow(self) -> None:
        for value, expected in BAD_VALUES:
            with self.subTest(audience=value):
                self.assertEqual(await self.refused('threads.events', {'audience': value}),
                                 expected)
                self.assertEqual(await self.refused(
                    'threads.events', {'audience': value, 'follow': False}), expected)
                self.assertEqual(self.follow_refused({'audience': value, 'follow': True}),
                                 expected)
                self.assertEqual(self.follow_refused({'audience': value}), expected)
        self.assertEqual(self.store.listeners, set())

    async def test_store_events_checks_its_own_audience(self) -> None:
        for value in ('people', 1, True, ['person']):
            with self.subTest(audience=value), self.assertRaises(LoomError) as caught:
                self.store.events(audience=value)
            self.assertEqual((caught.exception.code, caught.exception.message),
                             ('invalid', NOT_ONE_OF))
        self.assertEqual(self.store.events(audience=Audience.PERSON),
                         self.store.events(audience='person'))

    async def test_a_page_holds_at_most_a_thousand_matching_events_and_paging_reaches_all(
            self) -> None:
        with self.store.transaction() as tx:
            for number in range(MAX_EVENTS + 200):
                for thread_id in (self.agent, self.person):
                    self.store.append_event(tx, EventType.TURN_TEXT, thread_id,
                                            {'text': f'line {number}'})
        expected = [row[0] for row in self.store.connection.execute(
            'SELECT id FROM events WHERE thread_id = ? ORDER BY id', (self.person,))]
        self.assertGreater(len(expected), MAX_EVENTS)
        first = await self.events(audience='person')
        self.assertEqual(len(first), MAX_EVENTS)
        self.assertEqual({event['thread_id'] for event in first}, {self.person})
        got = self.ids(first)
        while True:
            page = await self.events(audience='person', after=got[-1])
            if not page:
                break
            self.assertLessEqual(len(page), MAX_EVENTS)
            self.assertEqual({event['thread_id'] for event in page}, {self.person})
            got += self.ids(page)
        self.assertEqual(got, expected)


class FollowTest(AudienceTest):
    async def asyncTearDown(self) -> None:
        self.assertEqual(self.store.listeners, set())
        await super().asyncTearDown()

    async def test_a_person_follower_sees_matching_events_and_a_thread_that_joins(self) -> None:
        person = (await self.launch_as('person'))['id']
        agent = (await self.launch_as('agent'))['id']
        self.add_events(1, person)
        self.add_events(1, agent)
        stored = self.thread_events(self.store.events(), person)
        got, task = self.consume(self.service.follow({'audience': 'person'}, PERSON))
        await wait_for(lambda: len(got) == len(stored), timeout=1.0)
        self.assertEqual(self.ids(got), stored)
        self.add_events(1, agent)
        self.add_events(1, person)
        await wait_for(lambda: len(got) == len(stored) + 1, timeout=1.0)
        await asyncio.sleep(0.05)
        self.assertEqual({event['thread_id'] for event in got}, {person})
        await self.update(agent, 'person')
        self.add_events(1, agent)
        await wait_for(lambda: len(got) == len(stored) + 3, timeout=1.0)
        joined = got[len(stored) + 1:]
        self.assertEqual([(event['thread_id'], event['type']) for event in joined],
                         [(agent, 'thread.updated'), (agent, 'turn.text')])
        self.assertEqual(joined[0]['data'], {'audience': 'person', 'previous_audience': 'agent'})
        self.assertEqual(self.ids(got), sorted(self.ids(got)))
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


    async def test_each_follower_gets_the_thread_less_events_its_audience_passes(
            self) -> None:
        streams = {name: self.consume(self.service.follow(params, PERSON))
                   for name, params in (('person', {'audience': 'person'}),
                                        ('agent', {'audience': 'agent'}), ('all', {}))}
        made = await self.threadless()
        for name, expected in (('person', made[:1]), ('agent', made), ('all', made)):
            got, _ = streams[name]
            await wait_for(lambda got=got, expected=expected: len(got) >= len(expected),
                           timeout=1.0)
        await asyncio.sleep(0.05)
        for name, expected in (('person', made[:1]), ('agent', made), ('all', made)):
            with self.subTest(follower=name):
                self.assertEqual(self.ids(streams[name][0]), expected)
        for _, task in streams.values():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class UpdateTest(AudienceTest):
    async def test_update_changes_the_audience_and_writes_one_event(self) -> None:
        info = await self.launch_as()
        current = self.orchestrator.get(info['id'])
        before = self.store.events()
        updated = await self.update(info['id'], 'person')
        self.assertEqual(updated, self.orchestrator.get(info['id']))
        self.assertEqual(updated['audience'], 'person')
        self.assertEqual({key: value for key, value in updated.items() if key != 'audience'},
                         {key: value for key, value in current.items() if key != 'audience'})
        added = self.store.events(before[-1]['id'])
        self.assertEqual(len(added), 1)
        self.assertEqual((added[0]['type'], added[0]['thread_id'], added[0]['data']),
                         ('thread.updated', info['id'],
                          {'audience': 'person', 'previous_audience': 'agent'}))
        back = await self.update(info['id'], 'agent')
        self.assertEqual(back['audience'], 'agent')
        self.assertEqual([event['data'] for event in self.updates(info['id'])], [
            {'audience': 'person', 'previous_audience': 'agent'},
            {'audience': 'agent', 'previous_audience': 'person'},
        ])

    async def test_update_to_the_same_audience_changes_nothing_and_writes_no_event(self) -> None:
        info = await self.launch_as('person')
        before = self.store.events()
        same = await self.update(info['id'], 'person', 'u1')
        self.assertEqual(same, self.orchestrator.get(info['id']))
        self.assertEqual(same['audience'], 'person')
        self.assertEqual(self.store.events(), before)
        self.assertEqual(self.receipts('u1'), 1)

    async def test_update_works_on_settled_and_failed_threads_and_keeps_their_state(
            self) -> None:
        settled = await self.launch_as()
        await self.call('threads.settle', {'id': 'z1', 'thread_id': settled['id']})
        await self.orchestrator.drain()
        await self.use(FakeRunner(fail_start=RuntimeError('no binary')))
        self.service = Service(self.store, self.projects, self.orchestrator)
        failed = await self.launch_as()
        for info, state, reason in ((settled, 'settled', None), (failed, 'failed', 'no binary')):
            with self.subTest(state=state):
                self.assertEqual(self.orchestrator.get(info['id'])['state'], state)
                updated = await self.update(info['id'], 'person')
                self.assertEqual((updated['audience'], updated['state'], updated['reason']),
                                 ('person', state, reason))
                got = self.orchestrator.get(info['id'])
                self.assertEqual((got['state'], got['reason']), (state, reason))
                self.assertEqual(len(self.updates(info['id'])), 1)

    async def test_update_leaves_a_live_thread_running_with_its_session(self) -> None:
        info = await self.launch_as()
        session = self.runner.sessions[info['id']]
        await self.update(info['id'], 'person')
        await self.orchestrator.drain()
        self.assertEqual(self.orchestrator.get(info['id'])['state'], 'running')
        self.assertFalse(session.closed)
        self.assertEqual(len(self.runner.starts), 1)

    async def test_a_retry_returns_the_first_answer_and_writes_no_second_event(self) -> None:
        info = await self.launch_as()
        first = await self.update(info['id'], 'person', 'u1')
        await self.update(info['id'], 'agent', 'u2')
        again = await self.update(info['id'], 'person', 'u1')
        self.assertEqual(again, first)
        self.assertEqual(again['audience'], 'person')
        self.assertEqual(self.orchestrator.get(info['id'])['audience'], 'agent')
        self.assertEqual(len(self.updates(info['id'])), 2)
        self.assertEqual(self.receipts('u1'), 1)
        await self.call('threads.settle', {'id': 'z1', 'thread_id': info['id']})
        await self.orchestrator.drain()
        message = await self.refused('threads.update', {
            'id': 'z1', 'thread_id': info['id'], 'audience': 'person'}, 'conflict')
        self.assertEqual(message, 'id was already used for threads.settle')
        self.assertEqual(len(self.updates(info['id'])), 2)

    async def test_update_refuses_a_bad_thread_id(self) -> None:
        info = await self.launch_as()
        cases = [
            ({'audience': 'person'}, 'invalid', 'thread_id is required'),
            ({'thread_id': None, 'audience': 'person'}, 'invalid', 'thread_id is required'),
            ({'thread_id': 1, 'audience': 'person'}, 'invalid', 'thread_id must be a string'),
            ({'thread_id': 'nope', 'audience': 'person'}, 'not_found',
             "no thread with id 'nope'"),
        ]
        await self.assert_refused(info['id'], cases)

    async def test_update_refuses_a_bad_audience_or_id(self) -> None:
        info = await self.launch_as()
        thread_id = info['id']
        cases = [
            ({'thread_id': thread_id}, 'invalid', 'audience is required'),
            ({'thread_id': thread_id, 'audience': None}, 'invalid', 'audience is required'),
            *(({'thread_id': thread_id, 'audience': value}, 'invalid', NOT_ONE_OF)
              for value in ('people', 1, True)),
        ]
        await self.assert_refused(thread_id, cases)
        message = await self.refused('threads.update',
                                     {'thread_id': thread_id, 'audience': 'person'})
        self.assertEqual(message, 'id must be a string of 1 to 200 characters')
        self.assertEqual(self.updates(), [])

    async def assert_refused(self, thread_id: str,
                             cases: list[tuple[dict, str, str]]) -> None:
        before = self.counts()
        for number, (params, code, expected) in enumerate(cases):
            command_id = f'bad-{number}'
            with self.subTest(params=params):
                message = await self.refused('threads.update', {'id': command_id, **params},
                                             code)
                self.assertEqual(message, expected)
                self.assertEqual(self.receipts(command_id), 0)
        self.assertEqual(self.counts(), before)
        self.assertEqual(self.orchestrator.get(thread_id)['audience'], 'agent')


if __name__ == '__main__':
    unittest.main()
