import json
import unittest

from loom_contracts.calls import CallName
from loom_contracts.events import EventType

from loom_scheduler import CheckResult

from .fakes import AGENT, PERSON, UNKNOWN, Event
from .test_events import EventCase

CHECK = ['python3', '-c', 'print(1)']
DUE = '2026-10-23T07:01:00.000+00:00'
NO_TEXT = 'check printed no JSON object with a non-empty text'
SHAPE = 'check must be a list of 1 to 64 strings, the first not empty, with no NUL'


def printed(text: object) -> CheckResult:
    return CheckResult(0, json.dumps({'text': text}) + '\n', None)


class CheckCase(EventCase):
    async def launcher(self, command_id: str = 'c1', caller: object = PERSON,
                       **fields: object) -> dict:
        fields.setdefault('check', CHECK)
        return await self.add(command_id, caller, action='launch', thread_id=...,
                              project_id='p1', **fields)

    async def watcher(self, command_id: str = 'c1', trigger: dict | None = None,
                      **fields: object) -> dict:
        fields.setdefault('check', CHECK)
        return await self.on('demo.done', command_id, trigger=trigger, action='launch',
                             thread_id=..., project_id='p1', **fields)

    def launches(self) -> list[dict]:
        return [call.params for call in self.host.calls_of(CallName.THREADS_LAUNCH)]

    async def pause(self, info: dict) -> None:
        await self.call('schedules.pause', {'id': f'pause-{info["id"]}',
                                            'schedule_id': info['id']})

    def stored_check(self, schedule_id: str) -> tuple:
        return self.host.rows('SELECT check_argv, check_root FROM schedules WHERE id = ?',
                              schedule_id)[0]


class AddTest(CheckCase):
    async def test_a_persons_launch_shows_its_check_and_keeps_the_root(self) -> None:
        info = await self.launcher()
        self.assertEqual(info['check'], CHECK)
        self.assertEqual(list(info)[list(info).index('workspace') + 1:][:2], ['check', 'by'])
        self.assertEqual(await self.call('schedules.list', {}), [info])
        self.assertEqual(self.stored_check(info['id']),
                         ('["python3","-c","print(1)"]', '/projects/p1'))
        plain = await self.add('c2', action='launch', thread_id=..., project_id='p1')
        self.assertIsNone(plain['check'])
        self.assertEqual(self.stored_check(plain['id']), (None, None))

    async def test_an_agent_or_an_unknown_caller_may_not_add_a_check(self) -> None:
        for name, caller in (('agent', AGENT), ('unknown', UNKNOWN)):
            with self.subTest(caller=name):
                rows, receipts = self.schedule_rows(), self.receipts()
                self.host.calls.clear()
                message = await self.refused('schedules.add', {
                    'id': f'{name}-1', 'trigger': {'type': 'interval', 'everyMs': 60_000},
                    'text': 'sweep', 'action': 'launch', 'project_id': 'p1', 'check': CHECK,
                }, 'forbidden', caller)
                self.assertEqual(message, 'agent threads may not add a schedule with a check')
                self.assertEqual((self.schedule_rows(), self.receipts()), (rows, receipts))
                self.assertEqual(self.host.calls, [])
                info = await self.launcher(f'{name}-2', caller, check=...)
                self.assertIsNone(info['check'])
                self.assertEqual(self.schedule_rows(), rows + 1)

    async def test_each_bad_check_is_refused_with_its_message(self) -> None:
        cases = [
            ({'check': None}, 'check must not be null'),
            ({'check': 'python3'}, SHAPE),
            ({'check': {'0': 'true'}}, SHAPE),
            ({'check': []}, SHAPE),
            ({'check': ['a'] * 65}, SHAPE),
            ({'check': ['true', 3]}, SHAPE),
            ({'check': ['', 'x']}, SHAPE),
            ({'check': ['tr\x00ue']}, SHAPE),
            ({'check': ['true', 'a\x00']}, SHAPE),
            ({'check': ['x' * 4093]}, 'check must be at most 4096 bytes as JSON'),
            ({'check': ['a', 'é' * 2045]}, 'check must be at most 4096 bytes as JSON'),
        ]
        rows = self.schedule_rows()
        for fields, message in cases:
            with self.subTest(fields=str(fields)[:60]):
                self.assertEqual(await self.refused('schedules.add', {
                    'id': 'bad', 'trigger': {'type': 'interval', 'everyMs': 60_000},
                    'text': 'sweep', 'action': 'launch', 'project_id': 'p1', **fields,
                }), message)
        for action in ({'action': 'post', 'thread_id': 't1'}, {'thread_id': 't1'},
                       {'action': 'notify'}):
            with self.subTest(action=action):
                self.assertEqual(await self.refused('schedules.add', {
                    'id': 'bad', 'trigger': {'type': 'interval', 'everyMs': 60_000},
                    'text': 'sweep', 'check': CHECK, **action,
                }), 'check goes only with launch')
        self.assertEqual(self.schedule_rows(), rows)

    async def test_the_largest_checks_are_taken(self) -> None:
        longest = await self.launcher('c1', check=['a'] * 64)
        self.assertEqual(longest['check'], ['a'] * 64)
        exact = ['x' * 4092]
        self.assertEqual(len(json.dumps(exact, separators=(',', ':'))), 4096)
        self.assertEqual((await self.launcher('c2', check=exact))['check'], exact)
        wide = ['a', 'é' * 2044]
        self.assertEqual(len(json.dumps(wide, separators=(',', ':'),
                                        ensure_ascii=False).encode()), 4096)
        self.assertEqual((await self.launcher('c3', check=wide))['check'], wide)


class ClockTest(CheckCase):
    async def test_a_positive_exit_skips_the_run(self) -> None:
        info = await self.launcher()
        self.host.checks.append(CheckResult(1, 'busy\n', None))
        await self.run_pass(minutes=1)
        self.assertEqual(self.launches(), [])
        self.assertEqual(self.host.events, [])
        self.assertEqual(self.runs(info['id']),
                         [(DUE, DUE, 'skipped_check', 0, None, 'check exited with 1')])
        self.assertEqual(self.next_run(info['id']), '2026-10-23T07:02:00.000+00:00')
        self.assertEqual(self.host.checked, [(CHECK, '/projects/p1')])

    async def test_exit_zero_with_a_text_launches_with_it(self) -> None:
        info = await self.launcher(runner='t3', model='sonnet', workspace={'strategy': 'root'})
        text = 'Sweep: LM-12 is ready.\nmore'
        self.host.checks.append(printed(text))
        await self.run_pass(minutes=1)
        origin = f'schedule:{info["id"]}'
        launched = self.runs(info['id'])[0][4]
        self.assertEqual(self.launches(), [{
            'id': f'{origin}:{DUE}', 'title': 'Sweep: LM-12 is ready.', 'message': text,
            'project_id': 'p1', 'origin': origin, 'runner': 't3', 'model': 'sonnet',
            'workspace': {'strategy': 'root'},
        }])
        self.assertEqual(self.runs(info['id']), [(DUE, DUE, 'fired', 0, launched, None)])
        self.assertEqual(self.host.events, [Event(EventType.SCHEDULE_FIRED, launched, {
            'schedule_id': info['id'], 'action': 'launch', 'text': text, 'late': False,
        })])

    async def test_a_title_stays_and_the_text_is_not_filled(self) -> None:
        await self.launcher(title='Sweep')
        self.host.checks.append(printed('Go {{event.id}}'))
        await self.run_pass(minutes=1)
        [launch] = self.launches()
        self.assertEqual((launch['title'], launch['message']), ('Sweep', 'Go {{event.id}}'))

    async def test_a_late_run_puts_its_late_line_before_the_text(self) -> None:
        info = await self.launcher()
        self.host.checks.append(printed('Sweep: LM-12 is ready.'))
        await self.run_pass(minutes=2, seconds=1)
        [launch] = self.launches()
        self.assertEqual(launch['message'], f'This run is late: it was due at {DUE}.\n\n'
                                            'Sweep: LM-12 is ready.')
        self.assertEqual(launch['title'], 'Sweep: LM-12 is ready.')
        self.assertEqual(self.host.events[-1].data['text'], 'Sweep: LM-12 is ready.')
        self.assertEqual(self.runs(info['id'])[0][2:4], ('fired', 1))

    async def test_exit_zero_with_no_output_launches_with_the_schedules_text(self) -> None:
        for number, output in enumerate(('', ' \n\t\n')):
            with self.subTest(output=output):
                info = await self.launcher(f'c{number}', text='check the build')
                self.host.checks.append(CheckResult(0, output, None))
                await self.run_pass(minutes=1)
                [launch] = [params for params in self.launches()
                            if params['origin'] == f'schedule:{info["id"]}']
                self.assertEqual((launch['title'], launch['message']),
                                 ('check the build', 'check the build'))
                self.assertEqual(self.runs(info['id'])[0][2], 'fired')
                await self.pause(info)

    async def test_output_without_a_text_fails_the_run(self) -> None:
        cases = {
            'not json': 'Sweep: LM-12 is ready.\n',
            'a list': '["Sweep"]\n',
            'no text': '{"title": "Sweep"}\n',
            'an empty text': '{"text": ""}\n',
            'a number': '{"text": 12}\n',
            'two values': '{"text": "a"}\n{"text": "b"}\n',
        }
        for number, (name, output) in enumerate(cases.items()):
            with self.subTest(case=name):
                info = await self.launcher(f'c{number}')
                self.host.checks.append(CheckResult(0, output, None))
                await self.run_pass(minutes=1)
                self.assertEqual(self.runs(info['id'])[0][2:],
                                 ('failed', 0, None, NO_TEXT))
                await self.pause(info)
        self.assertEqual(self.launches(), [])
        self.assertEqual(self.host.events, [])

    async def test_a_check_that_did_not_finish_fails_the_run(self) -> None:
        info = await self.launcher()
        self.host.checks.append(CheckResult(None, '', 'check ran over 60 s'))
        await self.run_pass(minutes=1)
        self.assertEqual(self.runs(info['id']),
                         [(DUE, DUE, 'failed', 0, None, 'check ran over 60 s')])
        self.assertEqual(self.launches(), [])
        self.assertEqual(self.next_run(info['id']), '2026-10-23T07:02:00.000+00:00')

    async def test_a_long_reason_is_cut(self) -> None:
        info = await self.launcher()
        self.host.checks.append(CheckResult(None, '', 'cannot run ' + 'x' * 600))
        await self.run_pass(minutes=1)
        self.assertEqual(self.runs(info['id'])[0][5], 'cannot run ' + 'x' * 489)

    async def test_a_busy_run_runs_no_check(self) -> None:
        info = await self.launcher()
        await self.run_pass(minutes=1)
        self.host.threads[self.runs(info['id'])[0][4]]['state'] = 'running'
        await self.run_pass(minutes=1)
        self.assertEqual([run[2] for run in self.runs(info['id'])], ['fired', 'skipped_busy'])
        self.assertEqual(len(self.host.checked), 1)
        self.assertEqual(len(self.launches()), 1)

    async def test_the_check_runs_in_the_root_it_was_added_with(self) -> None:
        info = await self.launcher()
        await self.run_pass(minutes=1)
        self.assertEqual(self.host.checked, [(CHECK, '/projects/p1')])
        self.host.threads[self.runs(info['id'])[0][4]]['state'] = 'idle'
        self.host.projects[0]['workspace_root'] = '/elsewhere'
        await self.run_pass(minutes=1)
        self.host.projects.pop(0)
        await self.run_pass(minutes=1)
        self.assertEqual([run[2:] for run in self.runs(info['id'])[1:]], [
            ('failed', 0, None, "the project's root changed since the schedule was added"),
            ('failed', 0, None, "no project with id 'p1'"),
        ])
        self.assertEqual(len(self.host.checked), 1)
        self.assertEqual(len(self.launches()), 1)


class EventTest(CheckCase):
    async def test_a_positive_exit_skips_the_event_and_a_zero_launches(self) -> None:
        info = await self.watcher(text='Done: {{payload.to}}')
        self.host.checks.append(CheckResult(2, '', None))
        skipped = self.host.signal('demo.done', {'to': 'a'})
        await self.read_events()
        self.assertEqual(self.event_runs(info['id']),
                         [('skipped_check', skipped, self.at(skipped), None,
                           'check exited with 2')])
        self.assertEqual(self.cursor(info['id']), skipped)
        self.assertEqual(self.fired(), [])
        self.assertEqual(self.skips(), [])
        self.assertEqual(self.launches(), [])
        self.host.checks.append(printed('Go: LM-7\nnow'))
        fired = self.host.signal('demo.done', {'to': 'b'})
        await self.read_events()
        [launch] = self.launches()
        self.assertEqual((launch['message'], launch['title'], launch['cause']),
                         ('Go: LM-7\nnow', 'Go: LM-7', fired))
        self.assertEqual(self.fired()[0].data['text'], 'Go: LM-7\nnow')
        self.assertEqual(self.event_runs(info['id'])[1][:2], ('fired', fired))
        self.assertEqual(self.host.checked, [(CHECK, '/projects/p1')] * 2)

    async def test_no_output_launches_with_the_filled_text(self) -> None:
        await self.watcher(text='Done: {{payload.to}}')
        self.host.signal('demo.done', {'to': 'a'})
        await self.read_events()
        [launch] = self.launches()
        self.assertEqual((launch['message'], launch['title']), ('Done: a', 'Done: a'))

    async def test_a_skipped_event_runs_no_check(self) -> None:
        await self.watcher(trigger={'rate_max': 1})
        self.host.signal('demo.done')
        await self.read_events()
        self.host.threads['launched-1']['hops'] = 9
        self.host.signal('demo.done', by='launched-1', hops=9)
        self.host.signal('demo.done', hops=9)
        self.host.signal('demo.done')
        await self.read_events()
        self.assertEqual([skip['reason'] for skip in self.skips()], ['own', 'hops', 'rate'])
        self.assertEqual(len(self.host.checked), 1)

    async def test_a_skipped_check_does_not_count_toward_rate_max(self) -> None:
        info = await self.watcher(trigger={'rate_max': 1})
        self.host.checks.append(CheckResult(1, '', None))
        self.host.signal('demo.done')
        self.host.signal('demo.done')
        await self.read_events()
        self.assertEqual([run[0] for run in self.event_runs(info['id'])],
                         ['skipped_check', 'fired'])


if __name__ == '__main__':
    unittest.main()
