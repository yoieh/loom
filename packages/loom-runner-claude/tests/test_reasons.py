import unittest

from loom_runner_claude.reasons import MAX_REASON, REDACTED, safe_reason

TOKEN = 'loom-test-token-3f9c2a7e'


class SafeReasonTest(unittest.TestCase):
    def test_constants(self) -> None:
        self.assertEqual(MAX_REASON, 500)
        self.assertEqual(REDACTED, '[redacted]')

    def test_text_of_500_is_kept_and_501_is_cut(self) -> None:
        self.assertEqual(safe_reason('a' * 500, []), 'a' * 500)
        self.assertEqual(safe_reason('a' * 499 + 'bc', []), 'a' * 499 + 'b')
        self.assertEqual(len(safe_reason('a' * 501, [TOKEN])), 500)

    def test_redacts_before_the_cut(self) -> None:
        text = 'a' * 495 + TOKEN
        text += 'b' * (1_000 - len(text))
        self.assertEqual(len(text), 1_000)
        result = safe_reason(text, [TOKEN])
        self.assertEqual(result, ('a' * 495 + REDACTED + 'b' * 1_000)[:500])
        self.assertEqual(len(result), 500)
        for size in range(4, len(TOKEN) + 1):
            with self.subTest(size=size):
                self.assertNotIn(TOKEN[:size], result)

    def test_redacts_every_occurrence(self) -> None:
        text = f'{TOKEN} and {TOKEN}{TOKEN} end'
        result = safe_reason(text, [TOKEN])
        self.assertEqual(result, f'{REDACTED} and {REDACTED}{REDACTED} end')

    def test_longer_secret_is_redacted_whole(self) -> None:
        result = safe_reason(f'x {TOKEN} y', ['loom', TOKEN])
        self.assertEqual(result, f'x {REDACTED} y')

    def test_empty_and_whitespace_secrets_are_ignored(self) -> None:
        text = 'could not start claude: No such file or directory'
        for secrets in ([''], [' '], ['\n\t '], ['', ' ', '  ']):
            with self.subTest(secrets=secrets):
                self.assertEqual(safe_reason(text, secrets), text)

    def test_short_text_without_a_secret_is_unchanged(self) -> None:
        for text in ['', 'error_max_turns', 'x' * 500]:
            with self.subTest(size=len(text)):
                self.assertEqual(safe_reason(text, [TOKEN]), text)
        self.assertEqual(safe_reason('error_max_turns', iter([])), 'error_max_turns')


if __name__ == '__main__':
    unittest.main()
