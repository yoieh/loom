import sqlite3
from datetime import timedelta

MAX_REASON = 500
MAX_TITLE = 80
RETRY_AFTER = timedelta(seconds=5)

FIRED = 'fired'
SKIPPED_LATE = 'skipped_late'
SKIPPED_BUSY = 'skipped_busy'
FAILED = 'failed'

SCHEMA: list[str] = [
    '''CREATE TABLE IF NOT EXISTS schedules (
        id TEXT PRIMARY KEY,
        trigger TEXT NOT NULL,
        action TEXT NOT NULL,
        text TEXT NOT NULL,
        thread_id TEXT,
        project_id TEXT,
        title TEXT,
        concurrency_key TEXT,
        on_busy TEXT,
        settle TEXT,
        by TEXT,
        verified INTEGER NOT NULL,
        paused INTEGER NOT NULL DEFAULT 0,
        next_run TEXT,
        created_at TEXT NOT NULL,
        event_after INTEGER
    )''',
    '''CREATE TABLE IF NOT EXISTS schedule_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        schedule_id TEXT NOT NULL,
        due TEXT NOT NULL,
        at TEXT NOT NULL,
        outcome TEXT NOT NULL,
        late INTEGER NOT NULL,
        thread_id TEXT,
        reason TEXT,
        event_id INTEGER
    )''',
    'CREATE INDEX IF NOT EXISTS schedule_runs_schedule ON schedule_runs (schedule_id, id)',
]
ADDED_COLUMNS = (
    ('schedules', 'concurrency_key TEXT'),
    ('schedules', 'on_busy TEXT'),
    ('schedules', 'settle TEXT'),
    ('schedules', 'event_after INTEGER'),
    ('schedule_runs', 'event_id INTEGER'),
)

COLUMNS = ('id', 'trigger', 'action', 'text', 'thread_id', 'project_id', 'title',
           'concurrency_key', 'on_busy', 'settle', 'by', 'verified', 'paused', 'next_run',
           'created_at', 'event_after')
SELECT = f'SELECT {", ".join(COLUMNS)} FROM schedules'


def add_column(tx: sqlite3.Connection, table: str, column: str) -> None:
    columns = {row[1] for row in tx.execute(f'PRAGMA table_info({table})')}
    if column.split()[0] not in columns:
        tx.execute(f'ALTER TABLE {table} ADD COLUMN {column}')


def create(tx: sqlite3.Connection) -> None:
    for statement in SCHEMA:
        tx.execute(statement)
    for table, column in ADDED_COLUMNS:
        add_column(tx, table, column)


def to_row(found: tuple | None) -> dict | None:
    return None if found is None else dict(zip(COLUMNS, found))


def title(given: str | None, text: str) -> str:
    if given is not None:
        return given
    line = next((line.strip() for line in text.splitlines() if line.strip()), text)
    return line[:MAX_TITLE]
