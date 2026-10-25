import os
import tempfile
import unittest
from pathlib import Path

from loom_runner_t3.record import MAX_RECORD, RecordError, read_record, write_record

from .replay import T3_THREAD, T3_THREAD_2, THREAD_ID, mode_of


class RecordTest(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.data_dir = Path(temp.name).resolve()
        self.directory = self.data_dir / 't3'
        self.record = self.directory / THREAD_ID

    def write(self, data: bytes) -> None:
        self.directory.mkdir(mode=0o700, exist_ok=True)
        self.record.write_bytes(data)

    def read(self) -> str | None:
        return read_record(str(self.data_dir), THREAD_ID)

    def test_write_makes_a_private_dir_and_file(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        self.assertEqual(self.record.read_bytes(), f'{T3_THREAD}\n'.encode())
        self.assertEqual(mode_of(self.directory), 0o700)
        self.assertEqual(mode_of(self.record), 0o600)
        self.assertEqual(os.listdir(self.directory), [THREAD_ID])

    def test_a_wide_dir_is_narrowed(self) -> None:
        self.directory.mkdir()
        self.directory.chmod(0o755)
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        self.assertEqual(mode_of(self.directory), 0o700)

    def test_write_replaces_the_old_id(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD_2)
        self.assertEqual(self.read(), T3_THREAD_2)
        self.assertEqual(mode_of(self.record), 0o600)

    def test_write_refuses_ids_of_another_shape(self) -> None:
        for loom_id, t3_id in ((THREAD_ID, '00000000-0000-4000-8000-000000000001'),
                               (THREAD_ID, 'mcp:../../etc'),
                               (THREAD_ID, None),
                               ('../escape', T3_THREAD)):
            with self.subTest(loom_id=loom_id, t3_id=t3_id):
                with self.assertRaises(ValueError):
                    write_record(str(self.data_dir), loom_id, t3_id)

    def test_write_does_not_follow_a_symlinked_dir(self) -> None:
        target = self.data_dir / 'elsewhere'
        target.mkdir()
        self.directory.symlink_to(target)
        with self.assertRaises(OSError):
            write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        self.assertEqual(os.listdir(target), [])

    def test_a_missing_record_reads_as_none(self) -> None:
        self.assertIsNone(self.read())
        self.directory.mkdir()
        self.assertIsNone(self.read())

    def test_read_round_trips(self) -> None:
        write_record(str(self.data_dir), THREAD_ID, T3_THREAD)
        self.assertEqual(self.read(), T3_THREAD)

    def test_unreadable_records_raise(self) -> None:
        cases = {
            'not the id shape': b'00000000-0000-4000-8000-000000000001\n',
            'upper case': T3_THREAD.upper().encode(),
            'empty': b'',
            'too long': b'x' * (MAX_RECORD + 1),
            'not utf-8': b'\xff\xfe',
        }
        for name, data in cases.items():
            with self.subTest(name):
                self.write(data)
                with self.assertRaises(RecordError):
                    self.read()

    def test_a_symlinked_record_is_refused(self) -> None:
        real = self.data_dir / 'real'
        real.write_bytes(f'{T3_THREAD}\n'.encode())
        self.directory.mkdir()
        self.record.symlink_to(real)
        with self.assertRaises(RecordError):
            self.read()

    def test_a_directory_record_is_refused(self) -> None:
        self.record.mkdir(parents=True)
        with self.assertRaisesRegex(RecordError, 'not a regular file'):
            self.read()

    def test_a_symlinked_record_dir_is_refused(self) -> None:
        target = self.data_dir / 'elsewhere'
        target.mkdir()
        (target / THREAD_ID).write_bytes(f'{T3_THREAD}\n'.encode())
        self.directory.symlink_to(target)
        with self.assertRaises(RecordError):
            self.read()


if __name__ == '__main__':
    unittest.main()
