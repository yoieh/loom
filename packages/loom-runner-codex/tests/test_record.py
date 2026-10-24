import os
import tempfile
import unittest
from pathlib import Path

from loom_runner_codex.record import MAX_RECORD, RecordError, read_record, write_record

from .replay import CODEX_ID, THREAD_ID

OTHER_ID = '01a163a2-40b1-7412-ab12-1043a0b6795a'


def mode(path: Path) -> int:
    return path.lstat().st_mode & 0o7777


class RecordTest(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.data_dir = Path(temp.name).resolve()
        self.directory = self.data_dir / 'codex'
        self.record = self.directory / THREAD_ID

    def write(self, data: bytes) -> None:
        self.directory.mkdir(mode=0o700, exist_ok=True)
        self.record.write_bytes(data)

    def read(self) -> str | None:
        return read_record(str(self.data_dir), THREAD_ID)

    def test_write_makes_a_private_dir_and_file(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, CODEX_ID)
        self.assertEqual(self.record.read_bytes(), f'{CODEX_ID}\n'.encode())
        self.assertEqual(mode(self.directory), 0o700)
        self.assertEqual(mode(self.record), 0o600)
        self.assertEqual(os.listdir(self.directory), [THREAD_ID])

    def test_a_wide_dir_is_narrowed(self) -> None:
        for wide in (0o755, 0o777, 0o750, 0o1700):
            with self.subTest(mode=oct(wide)):
                self.directory.mkdir(exist_ok=True)
                self.directory.chmod(wide)
                write_record(str(self.data_dir), THREAD_ID, CODEX_ID)
                self.assertEqual(mode(self.directory), 0o700)

    def test_write_replaces_the_old_id(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, CODEX_ID)
        write_record(str(self.data_dir), THREAD_ID, OTHER_ID)
        self.assertEqual(self.record.read_bytes(), f'{OTHER_ID}\n'.encode())
        self.assertEqual(mode(self.record), 0o600)
        self.assertEqual(os.listdir(self.directory), [THREAD_ID])

    def test_write_narrows_a_wide_old_file(self) -> None:
        self.write(b'old\n')
        self.record.chmod(0o644)
        write_record(str(self.data_dir), THREAD_ID, CODEX_ID)
        self.assertEqual(mode(self.record), 0o600)

    def test_write_does_not_follow_a_symlink(self) -> None:
        target = self.data_dir / 'target'
        target.write_bytes(b'keep\n')
        self.directory.mkdir(mode=0o700)
        self.record.symlink_to(target)
        write_record(str(self.data_dir), THREAD_ID, CODEX_ID)
        self.assertEqual(target.read_bytes(), b'keep\n')
        self.assertFalse(self.record.is_symlink())
        self.assertEqual(self.record.read_bytes(), f'{CODEX_ID}\n'.encode())

    def test_write_into_a_symlinked_dir_fails(self) -> None:
        elsewhere = self.data_dir / 'elsewhere'
        elsewhere.mkdir(mode=0o700)
        self.directory.symlink_to(elsewhere)
        with self.assertRaises(OSError):
            write_record(str(self.data_dir), THREAD_ID, CODEX_ID)
        self.assertEqual(os.listdir(elsewhere), [])

    def test_write_where_the_dir_is_a_file_fails(self) -> None:
        self.directory.write_bytes(b'')
        with self.assertRaises(OSError):
            write_record(str(self.data_dir), THREAD_ID, CODEX_ID)

    def test_write_refuses_ids_that_are_not_uuids(self) -> None:
        for thread_id, codex_id in (('../x', CODEX_ID), (THREAD_ID, '--help'),
                                    (THREAD_ID, CODEX_ID.upper())):
            with self.subTest(thread_id=thread_id, codex_id=codex_id):
                with self.assertRaises(ValueError):
                    write_record(str(self.data_dir), thread_id, codex_id)
        self.assertFalse(self.directory.exists())

    def test_read_what_was_written(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, CODEX_ID)
        self.assertEqual(self.read(), CODEX_ID)

    def test_read_strips_whitespace(self) -> None:
        self.write(f'  {CODEX_ID}\r\n\n'.encode())
        self.assertEqual(self.read(), CODEX_ID)

    def test_a_missing_record_or_dir_is_none(self) -> None:
        self.assertIsNone(self.read())
        self.directory.mkdir(mode=0o700)
        self.assertIsNone(self.read())

    def test_a_record_of_the_limit_is_read(self) -> None:
        self.write(CODEX_ID.encode() + b' ' * (MAX_RECORD - len(CODEX_ID)))
        self.assertEqual(self.read(), CODEX_ID)

    def test_unreadable_records_raise(self) -> None:
        target = self.data_dir / 'target'
        target.write_bytes(f'{CODEX_ID}\n'.encode())
        cases = {
            'symlink': lambda: self.record.symlink_to(target),
            'dangling symlink': lambda: self.record.symlink_to(self.data_dir / 'gone'),
            'directory': lambda: self.record.mkdir(),
            'fifo': lambda: os.mkfifo(self.record),
            'too long': lambda: self.record.write_bytes(
                CODEX_ID.encode() + b' ' * (MAX_RECORD + 1 - len(CODEX_ID))),
            'not utf-8': lambda: self.record.write_bytes(b'\xff\xfe' + CODEX_ID.encode()),
            'not a uuid': lambda: self.record.write_bytes(b'--help\n'),
            'uppercase uuid': lambda: self.record.write_bytes(CODEX_ID.upper().encode()),
            'two ids': lambda: self.record.write_bytes(f'{CODEX_ID}\n{OTHER_ID}\n'.encode()),
            'empty': lambda: self.record.write_bytes(b''),
        }
        for name, make in cases.items():
            with self.subTest(case=name):
                self.directory.mkdir(mode=0o700, exist_ok=True)
                make()
                try:
                    with self.assertRaises(RecordError):
                        self.read()
                finally:
                    if self.record.is_dir() and not self.record.is_symlink():
                        self.record.rmdir()
                    else:
                        self.record.unlink()

    def test_a_dir_that_is_a_symlink_or_a_file_raises(self) -> None:
        elsewhere = self.data_dir / 'elsewhere'
        elsewhere.mkdir(mode=0o700)
        (elsewhere / THREAD_ID).write_bytes(f'{CODEX_ID}\n'.encode())
        self.directory.symlink_to(elsewhere)
        with self.assertRaises(RecordError):
            self.read()
        self.directory.unlink()
        self.directory.write_bytes(b'')
        with self.assertRaises(RecordError):
            self.read()


if __name__ == '__main__':
    unittest.main()
