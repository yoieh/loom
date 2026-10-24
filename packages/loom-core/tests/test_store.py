import os
import sqlite3
import stat
import tempfile
import unittest
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from loom_contracts.calls import CallName
from loom_contracts.events import EventType

from loom_core.clock import iso, utc_now
from loom_core.errors import LoomError
from loom_core.store import DB_NAME, MAX_COMMAND_ID, SCHEMA, Receipt, Store

OLD_MESSAGES = '''CREATE TABLE IF NOT EXISTS messages (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_id TEXT NOT NULL,
        command_id TEXT NOT NULL,
        text TEXT NOT NULL,
        mode TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )'''


def mode_of(path: Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


class TempDirTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.data_dir = self.root / 'data'

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def open_store(self) -> Store:
        store = Store.open(self.data_dir)
        self.addCleanup(store.close)
        return store


class ClockTest(unittest.TestCase):
    def test_iso_is_utc_with_milliseconds(self) -> None:
        at = datetime(2026, 10, 24, 15, 8, 27, 123456, tzinfo=timezone(timedelta(hours=2)))
        self.assertEqual(iso(at), '2026-10-24T13:08:27.123+00:00')
        self.assertEqual(iso(datetime(2026, 10, 24, 13, 8, 27, tzinfo=UTC)),
                         '2026-10-24T13:08:27.000+00:00')

    def test_utc_now_is_aware(self) -> None:
        self.assertEqual(utc_now().utcoffset(), timedelta(0))


class ErrorTest(unittest.TestCase):
    def test_to_api(self) -> None:
        error = LoomError('not_found', 'no such thing')
        self.assertEqual(error.code, 'not_found')
        self.assertEqual(error.message, 'no such thing')
        self.assertEqual(error.to_api(), {'code': 'not_found', 'message': 'no such thing'})

    def test_unknown_code_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            LoomError('oops', 'x')


class OpenTest(TempDirTest):
    def test_missing_data_dir_is_made_0700_and_db_0600(self) -> None:
        self.data_dir = self.root / 'share' / 'loom'
        store = self.open_store()
        self.assertEqual(store.data_dir, self.data_dir)
        self.assertEqual(mode_of(self.data_dir), 0o700)
        self.assertEqual(mode_of(self.data_dir / DB_NAME), 0o600)

    def test_existing_data_dir_keeps_its_mode(self) -> None:
        self.data_dir.mkdir()
        os.chmod(self.data_dir, 0o750)
        self.open_store()
        self.assertEqual(mode_of(self.data_dir), 0o750)

    def test_existing_wide_db_is_set_back_to_0600(self) -> None:
        self.data_dir.mkdir(mode=0o700)
        db = self.data_dir / DB_NAME
        db.touch()
        os.chmod(db, 0o644)
        self.open_store()
        self.assertEqual(mode_of(db), 0o600)

    def test_every_db_file_is_0600_during_and_after_a_write(self) -> None:
        store = self.open_store()
        with store.transaction() as tx:
            store.append_event(tx, EventType.TURN_STARTED, 't1', {})
            files = sorted(self.data_dir.glob(f'{DB_NAME}*'))
            self.assertIn(self.data_dir / f'{DB_NAME}-journal', files)
            for path in files:
                self.assertEqual(mode_of(path), 0o600, path.name)
        for path in self.data_dir.glob(f'{DB_NAME}*'):
            self.assertEqual(mode_of(path), 0o600, path.name)

    def test_schema_and_foreign_keys(self) -> None:
        store = self.open_store()
        tables = {row[0] for row in store.connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'")}
        self.assertLessEqual({'commands', 'events', 'projects'}, tables)
        self.assertEqual(store.connection.execute('PRAGMA foreign_keys').fetchone(), (1,))
        self.assertEqual(store.connection.execute('PRAGMA journal_mode').fetchone(), ('delete',))
        self.assertTrue(all(s.startswith('CREATE ') for s in SCHEMA))

    def test_reopen_keeps_data(self) -> None:
        store = Store.open(self.data_dir)
        store.command('c1', CallName.PROJECTS_LIST, lambda tx: 7)
        store.close()
        self.assertEqual(self.open_store().command('c1', CallName.PROJECTS_LIST, lambda tx: 8),
                         Receipt(7, True))


class TransactionTest(TempDirTest):
    def test_commit(self) -> None:
        store = self.open_store()
        seen: list[str] = []
        store.connection.set_trace_callback(seen.append)
        with store.transaction() as tx:
            tx.execute("INSERT INTO commands VALUES ('a', 'x', '1', 'now')")
        self.assertEqual(seen[0], 'BEGIN IMMEDIATE')
        self.assertEqual(seen[-1], 'COMMIT')
        self.assertEqual(store.connection.execute('SELECT count(*) FROM commands').fetchone(),
                         (1,))

    def test_rollback_and_reraise(self) -> None:
        store = self.open_store()
        with self.assertRaisesRegex(RuntimeError, 'boom'):
            with store.transaction() as tx:
                tx.execute("INSERT INTO commands VALUES ('a', 'x', '1', 'now')")
                raise RuntimeError('boom')
        self.assertFalse(store.connection.in_transaction)
        self.assertEqual(store.connection.execute('SELECT count(*) FROM commands').fetchone(),
                         (0,))


class CommandTest(TempDirTest):
    def setUp(self) -> None:
        super().setUp()
        self.store = self.open_store()
        self.runs = 0

    def change(self, tx: sqlite3.Connection) -> dict:
        self.runs += 1
        self.store.append_event(tx, EventType.TURN_STARTED, 't1', {})
        return {'n': self.runs}

    def count(self, table: str) -> int:
        return self.store.connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]

    def test_first_call_runs_and_stores_the_receipt(self) -> None:
        receipt = self.store.command('c1', CallName.PROJECTS_ADD, self.change)
        self.assertEqual(receipt, Receipt({'n': 1}, False))
        row = self.store.connection.execute('SELECT id, call, answer FROM commands').fetchone()
        self.assertEqual(row, ('c1', 'projects.add', '{"n": 1}'))
        self.assertEqual(self.count('events'), 1)

    def test_retry_returns_the_first_answer_and_does_not_run(self) -> None:
        self.store.command('c1', CallName.PROJECTS_ADD, self.change)
        receipt = self.store.command('c1', CallName.PROJECTS_ADD, self.change)
        self.assertEqual(receipt, Receipt({'n': 1}, True))
        self.assertEqual(self.runs, 1)
        self.assertEqual(self.count('events'), 1)
        self.assertEqual(self.count('commands'), 1)

    def test_same_id_for_another_call_is_conflict(self) -> None:
        self.store.command('c1', CallName.PROJECTS_ADD, self.change)
        with self.assertRaises(LoomError) as caught:
            self.store.command('c1', CallName.PROJECTS_REMOVE, self.change)
        self.assertEqual(caught.exception.code, 'conflict')
        self.assertEqual(self.runs, 1)

    def test_failed_change_keeps_nothing(self) -> None:
        def change(tx: sqlite3.Connection) -> object:
            tx.execute("INSERT INTO projects VALUES ('p', 'P', '/p', NULL, 'now')")
            self.store.append_event(tx, EventType.TURN_STARTED, 't1', {})
            raise RuntimeError('boom')

        with self.assertRaisesRegex(RuntimeError, 'boom'):
            self.store.command('c1', CallName.PROJECTS_ADD, change)
        self.assertEqual(self.count('projects'), 0)
        self.assertEqual(self.count('events'), 0)
        self.assertEqual(self.count('commands'), 0)
        self.assertEqual(self.store.command('c1', CallName.PROJECTS_ADD, self.change),
                         Receipt({'n': 1}, False))

    def test_answer_that_is_not_json_keeps_nothing(self) -> None:
        def change(tx: sqlite3.Connection) -> object:
            self.store.append_event(tx, EventType.TURN_STARTED, 't1', {})
            return object()

        with self.assertRaises(TypeError):
            self.store.command('c1', CallName.PROJECTS_ADD, change)
        self.assertEqual(self.count('events'), 0)
        self.assertEqual(self.count('commands'), 0)

    def test_command_id_bounds(self) -> None:
        longest = 'a' * MAX_COMMAND_ID
        self.assertFalse(self.store.command(longest, CallName.PROJECTS_ADD, self.change).replayed)
        for bad in (None, 5, '', 'a' * (MAX_COMMAND_ID + 1), b'abc', ['x']):
            with self.subTest(command_id=bad):
                with self.assertRaises(LoomError) as caught:
                    self.store.command(bad, CallName.PROJECTS_ADD, self.change)
                self.assertEqual(caught.exception.code, 'invalid')
                self.assertIn('id', caught.exception.message)
        self.assertEqual(self.runs, 1)
        self.assertEqual(self.count('commands'), 1)


class MessageSourceTest(TempDirTest):
    def open_traced(self) -> tuple[Store, list[str]]:
        seen: list[str] = []
        connect = sqlite3.connect

        def traced(*args: object, **kwargs: object) -> sqlite3.Connection:
            connection = connect(*args, **kwargs)
            connection.set_trace_callback(seen.append)
            return connection

        with mock.patch.object(sqlite3, 'connect', traced):
            store = Store.open(self.data_dir)
        self.addCleanup(store.close)
        return store, seen

    def columns(self, store: Store) -> list[str]:
        return [row[1] for row in store.connection.execute('PRAGMA table_info(messages)')]

    def alters(self, seen: list[str]) -> list[str]:
        return [statement for statement in seen if statement.startswith('ALTER')]

    def test_a_new_store_has_the_column_from_its_schema(self) -> None:
        store, seen = self.open_traced()
        self.assertEqual(self.columns(store).count('source'), 1)
        self.assertEqual(self.alters(seen), [])

    def test_an_old_store_gains_the_column_once_and_its_rows_read_as_an_agents(self) -> None:
        self.data_dir.mkdir(mode=0o700)
        db = sqlite3.connect(self.data_dir / DB_NAME)
        db.execute(OLD_MESSAGES)
        db.execute("INSERT INTO messages (thread_id, command_id, text, mode, created_at) "
                   "VALUES ('t1', 'c1', 'held', 'steer', 'now')")
        db.commit()
        db.close()
        store, seen = self.open_traced()
        self.assertEqual(self.columns(store).count('source'), 1)
        self.assertEqual(self.alters(seen),
                         ['ALTER TABLE messages ADD COLUMN source INTEGER NOT NULL DEFAULT 1'])
        alter = seen.index(self.alters(seen)[0])
        begin = max(at for at, statement in enumerate(seen[:alter])
                    if statement == 'BEGIN IMMEDIATE')
        self.assertNotIn('COMMIT', seen[begin:alter])
        self.assertEqual(seen[begin + 1], SCHEMA[0])
        self.assertEqual(seen[alter + 1], 'COMMIT')
        self.assertEqual(store.connection.execute(
            'SELECT thread_id, text, source FROM messages').fetchall(), [('t1', 'held', 1)])
        store.close()
        again, seen = self.open_traced()
        self.assertEqual(self.columns(again).count('source'), 1)
        self.assertEqual(self.alters(seen), [])
        self.assertEqual(again.connection.execute(
            'SELECT thread_id, text, source FROM messages').fetchall(), [('t1', 'held', 1)])


if __name__ == '__main__':
    unittest.main()
