import sys
import unicodedata
import unittest

from loom_client.output import escape

INVISIBLE = {
    '­': '\\u00ad',
    '؜': '\\u061c',
    '᠎': '\\u180e',
    '​': '\\u200b',
    '‍': '\\u200d',
    '‎': '\\u200e',
    '‮': '\\u202e',
    '⁦': '\\u2066',
    '﻿': '\\ufeff',
    ' ': '\\u2028',
    ' ': '\\u2029',
    '\U000110bd': '\\U000110bd',
    '\U000e0041': '\\U000e0041',
}


class EscapeTest(unittest.TestCase):
    def test_each_format_and_separator_character_has_its_escape(self) -> None:
        for char, escaped in INVISIBLE.items():
            with self.subTest(code=f'U+{ord(char):04X}'):
                self.assertEqual(escape(char), escaped)

    def test_every_cf_zl_and_zp_code_point_prints_as_an_ascii_escape(self) -> None:
        chars = [chr(code) for code in range(sys.maxunicode + 1)
                 if unicodedata.category(chr(code)) in ('Cf', 'Zl', 'Zp')]
        self.assertGreater(len(chars), 150)
        for char in chars:
            escaped = escape(char)
            with self.subTest(code=f'U+{ord(char):04X}'):
                self.assertTrue(escaped.isascii())
                self.assertNotEqual(escaped, char)
                self.assertTrue(escaped.startswith(('\\u', '\\U')))

    def test_other_characters_stay_as_they_are(self) -> None:
        for char in ('…', ' ', 'é', '\U0001f600', '͏'):
            with self.subTest(code=f'U+{ord(char):04X}'):
                self.assertEqual(escape(char), char)

    def test_a_mixed_text_escapes_each_kind_in_place(self) -> None:
        self.assertEqual(escape('é\x1b‮…\n\ud800\U000e0041x'),
                         'é\\x1b\\u202e…\\n\\ud800\\U000e0041x')


if __name__ == '__main__':
    unittest.main()
