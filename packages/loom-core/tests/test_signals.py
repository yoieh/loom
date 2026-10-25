import unittest
from unittest import mock

from loom_contracts.events import EventType

from loom_core.service import SERVED, Caller

from .test_causes import BAD_CAUSES, CauseTest, scheduled
from .test_service import AGENT, PERSON, UNKNOWN

TOPIC_LENGTH = 'topic must be a string of 1 to 200 characters'
TOPIC_SHAPE = ('topic must start with a letter or digit and hold only letters, digits, ".", '
               '"_", ":" and "-"')
LOOM_TYPE = 'topic must not be a Loom event type'
PAYLOAD_OBJECT = 'payload must be a JSON object'
PAYLOAD_SIZE = 'payload must be at most 16384 bytes as JSON'
KEY_LENGTH = 'key must be a string of 1 to 200 characters'
ID_SHAPE = 'id must be a string of 1 to 200 characters'


def sized(size: int, fill: str = 'x') -> dict:
    return {'a': fill * (size - len('{"a":""}'))}


class SignalTest(CauseTest):
    def emit_params(self, **extra: object) -> dict:
        return {'id': self.next_id(), 'topic': 'demo.done', 'payload': {'to': 'done'}, **extra}

    async def emit(self, caller: Caller = PERSON, **extra: object) -> dict:
        return await self.call('signals.emit', self.emit_params(**extra), caller)

    def signals(self) -> list[dict]:
        return [event for event in self.store.events() if event['type'] == EventType.SIGNAL]

    async def assert_refused(self, rows: list[tuple[dict, str, str]],
                             drop: tuple[str, ...] = ()) -> None:
        for extra, code, expected in rows:
            with self.subTest(params=extra):
                params = self.emit_params(**extra)
                for field in drop:
                    params.pop(field, None)
                before = self.counts()
                self.assertEqual(await self.refused('signals.emit', params, code), expected)
                self.assertEqual(self.counts(), before)
                if code != 'conflict' and isinstance(params.get('id'), str):
                    self.assertEqual(self.receipts(params['id']), 0)


class EmitTest(SignalTest):
    async def test_signals_emit_is_served(self) -> None:
        self.assertIn('signals.emit', SERVED)
        self.assertTrue(callable(self.service.method('signals.emit')))

    async def test_emit_writes_one_thread_less_signal_and_answers_with_it(self) -> None:
        await self.thread()
        before = self.store.events()
        event = await self.emit(project_id='loom', key='run-7')
        added = self.store.events(before[-1]['id'])
        self.assertEqual(added, [event])
        self.assertGreater(event['id'], max(item['id'] for item in before))
        self.assertEqual(set(event), {'id', 'type', 'at', 'thread_id', 'data'})
        self.assertEqual((event['type'], event['thread_id']), ('signal', None))
        self.assertEqual(event['data'], {'topic': 'demo.done', 'payload': {'to': 'done'},
                                         'hops': 0, 'project_id': 'loom', 'key': 'run-7'})
        self.assertEqual(list(event['data']), ['topic', 'payload', 'hops', 'project_id', 'key'])

    async def test_each_new_id_writes_a_signal_with_a_greater_id(self) -> None:
        first = await self.emit()
        second = await self.emit()
        self.assertGreater(second['id'], first['id'])
        self.assertEqual(self.signals(), [first, second])

    async def test_a_retry_answers_the_first_event_and_writes_none(self) -> None:
        params = self.emit_params()
        first = await self.call('signals.emit', params)
        before = self.counts()
        again = await self.call('signals.emit', params)
        changed = await self.call('signals.emit', {**params, 'topic': 'other'})
        self.assertEqual(again, first)
        self.assertEqual(changed, first)
        self.assertEqual(self.counts(), before)
        self.assertEqual(self.signals(), [first])
        self.assertEqual(self.receipts(params['id']), 1)

    async def test_an_id_used_for_another_call_is_a_conflict(self) -> None:
        launch = self.launch_params()
        await self.call('threads.launch', launch)
        await self.assert_refused([
            ({'id': launch['id']}, 'conflict', 'id was already used for threads.launch')])
        await self.emit(id='sig-1')
        message = await self.refused('threads.launch', self.launch_params(id='sig-1'),
                                     'conflict')
        self.assertEqual(message, 'id was already used for signals.emit')

    async def test_missing_project_id_key_and_cause_are_left_out_of_data(self) -> None:
        event = await self.emit()
        self.assertEqual(event['data'], {'topic': 'demo.done', 'payload': {'to': 'done'},
                                         'hops': 0})
        empty = await self.emit(payload={})
        self.assertEqual(empty['data']['payload'], {})


class RefusalTest(SignalTest):
    async def test_a_bad_id_is_refused(self) -> None:
        await self.assert_refused([({}, 'invalid', ID_SHAPE)], drop=('id',))
        await self.assert_refused([
            ({'id': None}, 'invalid', ID_SHAPE),
            ({'id': 5}, 'invalid', ID_SHAPE),
            ({'id': ''}, 'invalid', ID_SHAPE),
            ({'id': 'c' * 201}, 'invalid', ID_SHAPE),
        ])
        await self.emit(id='c' * 200)

    async def test_a_bad_topic_is_refused(self) -> None:
        await self.assert_refused([({}, 'invalid', 'topic is required')], drop=('topic',))
        rows = [
            ({'topic': None}, 'invalid', 'topic is required'),
            ({'topic': 5}, 'invalid', TOPIC_LENGTH),
            ({'topic': ['a']}, 'invalid', TOPIC_LENGTH),
            ({'topic': ''}, 'invalid', TOPIC_LENGTH),
            ({'topic': 't' * 201}, 'invalid', TOPIC_LENGTH),
            ({'topic': 'a b'}, 'invalid', TOPIC_SHAPE),
            ({'topic': '-x'}, 'invalid', TOPIC_SHAPE),
            ({'topic': '.x'}, 'invalid', TOPIC_SHAPE),
            ({'topic': 'x\n'}, 'invalid', TOPIC_SHAPE),
            ({'topic': 'é'}, 'invalid', TOPIC_SHAPE),
            ({'topic': 'a/b'}, 'invalid', TOPIC_SHAPE),
        ]
        rows += [({'topic': kind.value}, 'invalid', LOOM_TYPE) for kind in EventType]
        await self.assert_refused(rows)

    async def test_good_topics_are_accepted(self) -> None:
        for topic in ('t' * 200, 'factory.run.status', 'A9:b_c-d.e', '0'):
            with self.subTest(topic=topic):
                event = await self.emit(topic=topic)
                self.assertEqual(event['data']['topic'], topic)

    async def test_a_bad_payload_is_refused(self) -> None:
        await self.assert_refused([({}, 'invalid', 'payload is required')], drop=('payload',))
        await self.assert_refused([
            ({'payload': None}, 'invalid', 'payload is required'),
            ({'payload': [1]}, 'invalid', PAYLOAD_OBJECT),
            ({'payload': 'done'}, 'invalid', PAYLOAD_OBJECT),
            ({'payload': 5}, 'invalid', PAYLOAD_OBJECT),
            ({'payload': sized(16_385)}, 'invalid', PAYLOAD_SIZE),
            ({'payload': {'a': 'é' * 8188 + 'x'}}, 'invalid', PAYLOAD_SIZE),
        ])

    async def test_a_payload_of_exactly_16384_bytes_is_accepted(self) -> None:
        for payload in (sized(16_384), {'a': 'é' * 8188}):
            with self.subTest(size=len(payload['a'])):
                event = await self.emit(payload=payload)
                self.assertEqual(event['data']['payload'], payload)

    async def test_a_bad_project_id_or_key_is_refused(self) -> None:
        await self.assert_refused([
            ({'project_id': None}, 'invalid', 'project_id must not be null'),
            ({'project_id': 5}, 'invalid', 'project_id must be a string'),
            ({'project_id': 'nope'}, 'not_found', "no project with id 'nope'"),
            ({'key': None}, 'invalid', 'key must not be null'),
            ({'key': 5}, 'invalid', KEY_LENGTH),
            ({'key': ''}, 'invalid', KEY_LENGTH),
            ({'key': 'k' * 201}, 'invalid', KEY_LENGTH),
        ])
        event = await self.emit(key='k' * 200)
        self.assertEqual(event['data']['key'], 'k' * 200)

    async def test_a_bad_cause_is_refused(self) -> None:
        self.fired(0)
        await self.assert_refused([({'cause': value}, 'invalid', expected)
                                   for value, expected in BAD_CAUSES])

    async def test_params_must_be_an_object(self) -> None:
        for params in (None, [], 'x'):
            with self.subTest(params=params):
                self.assertEqual(await self.refused('signals.emit', params),
                                 'params must be an object')


class ChainTest(SignalTest):
    async def test_a_persons_signal_has_no_by_no_cause_and_no_hops(self) -> None:
        for caller in (PERSON, UNKNOWN, AGENT):
            with self.subTest(caller=caller):
                data = (await self.emit(caller))['data']
                self.assertEqual(data['hops'], 0)
                self.assertNotIn('by', data)
                self.assertNotIn('cause', data)

    async def test_an_agents_signal_has_its_threads_id_cause_and_hops(self) -> None:
        cause = self.fired(2)
        own = await self.thread(cause, 3)
        data = (await self.emit(Caller(thread_id=own)))['data']
        self.assertEqual((data['by'], data['cause'], data['hops']), (own, cause, 3))
        bare = await self.thread()
        data = (await self.emit(Caller(thread_id=bare)))['data']
        self.assertEqual((data['by'], data['hops']), (bare, 0))
        self.assertNotIn('cause', data)

    async def test_a_cause_counts_one_more_hop_than_it(self) -> None:
        event_id = self.fired(4)
        data = (await self.emit(cause=event_id))['data']
        self.assertEqual((data['cause'], data['hops']), (event_id, 5))
        self.assertNotIn('by', data)

    async def test_an_agents_cause_never_lowers_its_threads_hops(self) -> None:
        own = await self.thread(self.fired(5), 6)
        low = self.fired(1)
        data = (await self.emit(Caller(thread_id=own), cause=low))['data']
        self.assertEqual((data['by'], data['cause'], data['hops']), (own, low, 6))
        high = self.fired(8)
        data = (await self.emit(Caller(thread_id=own), cause=high))['data']
        self.assertEqual((data['cause'], data['hops']), (high, 9))

    async def test_a_schedules_signal_ignores_its_maker_thread(self) -> None:
        own = await self.thread(self.fired(5), 6)
        event_id = self.fired(1)
        for caller in scheduled(own):
            with self.subTest(caller=caller):
                data = (await self.emit(caller))['data']
                self.assertEqual(data, {'topic': 'demo.done', 'payload': {'to': 'done'},
                                        'hops': 0})
                data = (await self.emit(caller, cause=event_id))['data']
                self.assertEqual((data['cause'], data['hops']), (event_id, 2))
                self.assertNotIn('by', data)

    async def test_a_signals_hops_carry_on_down_a_chain(self) -> None:
        first = await self.emit()
        second = await self.emit(cause=first['id'])
        self.assertEqual(second['data']['hops'], 1)
        info = await self.launched(cause=second['id'])
        self.assertEqual((info['cause'], info['hops']), (second['id'], 2))
        third = await self.emit(Caller(thread_id=info['id']))
        self.assertEqual((third['data']['cause'], third['data']['hops']), (second['id'], 2))

    async def test_emit_wakes_followers(self) -> None:
        woken = mock.Mock()
        remove = self.store.listen(woken)
        self.addCleanup(remove)
        await self.emit()
        woken.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
