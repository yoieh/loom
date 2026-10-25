import json
import os
import tempfile
import unittest
from pathlib import Path

from loom_runner_t3.credential import (
    FIELDS,
    MAX_FILE,
    Credential,
    CredentialError,
    delete_credential_file,
    is_loopback_origin,
    parse_credential,
    read_credential_file,
    stored_session_id,
    usable_credential,
    write_credential_file,
)

from .replay import NOW, TOKEN, credential, credential_text, mode_of

WRONG_TYPES = {
    'origin': 41773,
    'access_token': ['a'],
    'expires_at': '1761400000',
    'session_id': 7,
    'client_id': {'id': 'x'},
    'scope': False,
}


class ParseTest(unittest.TestCase):
    def test_a_full_credential_parses(self) -> None:
        parsed = parse_credential(credential_text())
        self.assertEqual(parsed, Credential(**credential()))
        self.assertNotIn(TOKEN, repr(parsed))

    def test_none_and_non_json_are_unusable(self) -> None:
        for text in (None, '', 'not json', '[1, 2]', '"text"', '{"origin": '):
            with self.subTest(text=text):
                self.assertIsNone(parse_credential(text))

    def test_each_null_field_is_unusable(self) -> None:
        for name in FIELDS:
            with self.subTest(field=name):
                self.assertIsNone(parse_credential(credential_text(**{name: None})))

    def test_each_missing_field_is_unusable(self) -> None:
        for name in FIELDS:
            with self.subTest(field=name):
                data = credential()
                del data[name]
                self.assertIsNone(parse_credential(json.dumps(data)))

    def test_each_wrong_typed_field_is_unusable(self) -> None:
        for name, value in WRONG_TYPES.items():
            with self.subTest(field=name):
                self.assertIsNone(parse_credential(credential_text(**{name: value})))

    def test_expires_at_must_be_an_int_and_not_a_bool(self) -> None:
        for value in (True, False, 1.5e9, float(NOW)):
            with self.subTest(value=value):
                self.assertIsNone(parse_credential(credential_text(expires_at=value)))

    def test_the_origin_must_be_loopback(self) -> None:
        for origin in ('http://example.com:41773', 'https://127.0.0.1:41773',
                       'http://127.0.0.1', 'http://127.0.0.1:41773/', 'http://127.0.0.2:1',
                       'http://127.0.0.1:0', 'http://127.0.0.1:70000',
                       'http://127.0.0.1:41773@evil:80', 'http://localhost.evil:80'):
            with self.subTest(origin=origin):
                self.assertFalse(is_loopback_origin(origin))
                self.assertIsNone(parse_credential(credential_text(origin=origin)))
        for origin in ('http://127.0.0.1:41773', 'http://localhost:1', 'http://[::1]:65535'):
            with self.subTest(origin=origin):
                self.assertTrue(is_loopback_origin(origin))
                self.assertIsNotNone(parse_credential(credential_text(origin=origin)))

    def test_a_token_with_spaces_or_control_characters_is_unusable(self) -> None:
        for token in ('', 'a b', 'a\r\nX-Evil: 1', 'tab\there', 'é'):
            with self.subTest(token=token):
                self.assertIsNone(parse_credential(credential_text(access_token=token)))

    def test_a_session_id_that_looks_like_an_option_is_unusable(self) -> None:
        for session_id in ('', '--base-dir', '-x', 'a b'):
            with self.subTest(session_id=session_id):
                self.assertIsNone(parse_credential(credential_text(session_id=session_id)))

    def test_expiry_is_at_expires_at(self) -> None:
        text = credential_text(expires_at=NOW)
        self.assertIsNone(usable_credential(text, NOW))
        self.assertIsNone(usable_credential(text, NOW + 1))
        self.assertIsNotNone(usable_credential(text, NOW - 1))

    def test_stored_session_id_reads_a_partial_file(self) -> None:
        self.assertEqual(stored_session_id('{"session_id": "abc-1"}'), 'abc-1')
        for text in (None, '', 'nope', '{"session_id": null}', '{"session_id": "-x"}', '[]'):
            with self.subTest(text=text):
                self.assertIsNone(stored_session_id(text))


class FileTest(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name).resolve()
        self.data_dir = self.base / 'data'
        self.path = self.data_dir / 't3-client.json'

    def write(self, data: bytes, mode: int = 0o600) -> None:
        self.data_dir.mkdir(mode=0o700, exist_ok=True)
        self.path.write_bytes(data)
        self.path.chmod(mode)

    def test_write_makes_a_private_dir_and_file(self) -> None:
        write_credential_file(str(self.data_dir), Credential(**credential()))
        self.assertEqual(mode_of(self.data_dir), 0o700)
        self.assertEqual(mode_of(self.path), 0o600)
        self.assertEqual(json.loads(self.path.read_text()), credential())
        self.assertEqual(os.listdir(self.data_dir), ['t3-client.json'])

    def test_write_replaces_and_narrows_an_old_file(self) -> None:
        self.write(b'old', 0o644)
        write_credential_file(str(self.data_dir), Credential(**credential()))
        self.assertEqual(mode_of(self.path), 0o600)
        self.assertEqual(json.loads(self.path.read_text()), credential())

    def test_write_does_not_follow_a_symlinked_data_dir(self) -> None:
        target = self.base / 'elsewhere'
        target.mkdir()
        self.data_dir.symlink_to(target)
        with self.assertRaises(OSError):
            write_credential_file(str(self.data_dir), Credential(**credential()))
        self.assertEqual(os.listdir(target), [])

    def test_read_round_trips(self) -> None:
        write_credential_file(str(self.data_dir), Credential(**credential()))
        text = read_credential_file(str(self.data_dir))
        self.assertEqual(parse_credential(text), Credential(**credential()))

    def test_a_missing_file_reads_as_none(self) -> None:
        self.assertIsNone(read_credential_file(str(self.data_dir)))

    def test_group_or_other_bits_are_refused(self) -> None:
        for mode in (0o640, 0o604, 0o660, 0o644):
            with self.subTest(mode=oct(mode)):
                self.write(credential_text().encode(), mode)
                with self.assertRaisesRegex(CredentialError, 'make it 600'):
                    read_credential_file(str(self.data_dir))

    def test_a_symlink_is_refused(self) -> None:
        real = self.base / 'real.json'
        real.write_text(credential_text())
        real.chmod(0o600)
        self.data_dir.mkdir()
        self.path.symlink_to(real)
        with self.assertRaises(CredentialError):
            read_credential_file(str(self.data_dir))

    def test_a_directory_is_refused(self) -> None:
        self.path.mkdir(parents=True)
        self.path.chmod(0o700)
        with self.assertRaisesRegex(CredentialError, 'not a regular file'):
            read_credential_file(str(self.data_dir))

    def test_a_file_over_64_kib_is_refused(self) -> None:
        self.write(b' ' * MAX_FILE)
        self.assertEqual(read_credential_file(str(self.data_dir)), '')
        self.write(b' ' * (MAX_FILE + 1))
        with self.assertRaisesRegex(CredentialError, 'over 65536 bytes'):
            read_credential_file(str(self.data_dir))

    def test_non_utf8_is_refused(self) -> None:
        self.write(b'\xff\xfe')
        with self.assertRaisesRegex(CredentialError, 'not UTF-8'):
            read_credential_file(str(self.data_dir))

    def test_delete(self) -> None:
        self.assertFalse(delete_credential_file(str(self.data_dir)))
        self.write(b'{}')
        self.assertTrue(delete_credential_file(str(self.data_dir)))
        self.assertFalse(self.path.exists())


if __name__ == '__main__':
    unittest.main()
