import unittest

from loom_scheduler.fill import MAX_FILL, MAX_FILL_TOTAL, fill


class FillTest(unittest.TestCase):
    def test_event_id(self) -> None:
        self.assertEqual(fill('Event {{event.id}}, again {{event.id}}.', 42, {}),
                         'Event 42, again 42.')

    def test_each_kind_of_field(self) -> None:
        payload = {'name': 'build', 'n': 3, 'f': 1.5, 'ok': True, 'no': False,
                   'list': [1, 'a', None], 'obj': {'b': 1, 'a': 'é'}}
        cases = [
            ('{{payload.name}}', 'build'),
            ('{{payload.n}}', '3'),
            ('{{payload.f}}', '1.5'),
            ('{{payload.ok}}', 'true'),
            ('{{payload.no}}', 'false'),
            ('{{payload.list}}', '[1,"a",null]'),
            ('{{payload.obj}}', '{"b":1,"a":"é"}'),
        ]
        for text, filled in cases:
            with self.subTest(text=text):
                self.assertEqual(fill(text, 1, payload), filled)

    def test_missing_and_null_fields_are_empty(self) -> None:
        self.assertEqual(fill('[{{payload.gone}}][{{payload.none}}]', 1, {'none': None}), '[][]')

    def test_field_names(self) -> None:
        payload = {'a_b-C9': 'x', 'a.b': 'y'}
        self.assertEqual(fill('{{payload.a_b-C9}} {{payload.a.b}}', 1, payload),
                         'x {{payload.a.b}}')

    def test_each_value_is_cut_at_two_thousand_characters(self) -> None:
        self.assertEqual(MAX_FILL, 2_000)
        whole, long = 'w' * 2_000, 'l' * 2_001
        filled = fill('{{payload.whole}}|{{payload.long}}', 1, {'whole': whole, 'long': long})
        self.assertEqual(filled, whole + '|' + 'l' * 2_000)
        listed = fill('{{payload.list}}', 1, {'list': ['x' * 3_000]})
        self.assertEqual(listed, '["' + 'x' * 1_998)

    def test_a_filled_value_is_not_filled_again(self) -> None:
        payload = {'a': '{{event.id}}', 'b': '{{payload.c}}', 'c': 'no'}
        self.assertEqual(fill('{{payload.a}} {{payload.b}}', 7, payload),
                         '{{event.id}} {{payload.c}}')

    def test_other_braces_stay_as_they_are(self) -> None:
        text = '{{ event.id }} {{payload.}} {{payload.x y}} {event.id} {{thread.id}}'
        self.assertEqual(fill(text, 7, {'x y': 1}), text)

    def test_values_together_are_cut_at_eight_thousand_characters(self) -> None:
        self.assertEqual(MAX_FILL_TOTAL, 8_000)
        text = ('<{{payload.a}}|{{payload.b}}|{{payload.c}}|{{payload.d}}|{{payload.e}}'
                '|{{payload.f}}|{{event.id}}>')
        payload = {'a': 'a' * 2_000, 'b': 'b' * 3_000, 'c': 'c' * 2_000, 'd': 'd' * 1_500,
                   'e': 'e' * 1_000, 'f': 'f' * 10}
        self.assertEqual(fill(text, 7, payload),
                         '<' + 'a' * 2_000 + '|' + 'b' * 2_000 + '|' + 'c' * 2_000 + '|'
                         + 'd' * 1_500 + '|' + 'e' * 500 + '||>')

    def test_values_up_to_the_total_fill_whole(self) -> None:
        payload = {'a': 'a' * 2_000, 'b': 'b' * 2_000, 'c': 'c' * 2_000, 'd': 'd' * 2_000}
        text = '{{payload.a}}|{{payload.b}}|{{payload.c}}|{{payload.d}}'
        whole = 'a' * 2_000 + '|' + 'b' * 2_000 + '|' + 'c' * 2_000 + '|' + 'd' * 2_000
        with self.subTest(case='at the total'):
            self.assertEqual(fill(text, 7, payload), whole)
        with self.subTest(case='at the total, then event.id'):
            self.assertEqual(fill(text + '|{{event.id}}', 7, payload), whole + '|')
        with self.subTest(case='under the total'):
            self.assertEqual(fill('{{payload.long}}|{{payload.whole}}', 7,
                                  {'long': 'l' * 3_000, 'whole': 'w' * 2_000}),
                             'l' * 2_000 + '|' + 'w' * 2_000)

    def test_the_schedule_text_is_never_cut(self) -> None:
        text = 'x' * 10_000 + '{{payload.a}}' + 'y' * 10_000
        self.assertEqual(fill(text, 7, {'a': 'a' * 2_000}),
                         'x' * 10_000 + 'a' * 2_000 + 'y' * 10_000)


if __name__ == '__main__':
    unittest.main()
