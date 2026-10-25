import json
import sqlite3
from dataclasses import dataclass

from .errors import LoomError
from .store import MAX_EVENT_ID

CAUSE_KEY = 'cause'
NOT_AN_EVENT_ID = 'cause must be an event id'
NO_STORED_EVENT = 'cause names no stored event'


@dataclass(frozen=True)
class Chain:
    cause: int | None = None
    hops: int = 0


NONE = Chain()


def _whole(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def cause_param(params: dict) -> int | None:
    if CAUSE_KEY not in params:
        return None
    value = params[CAUSE_KEY]
    if value is None:
        raise LoomError('invalid', f'{CAUSE_KEY} must not be null')
    if not _whole(value) or value < 1:
        raise LoomError('invalid', NOT_AN_EVENT_ID)
    return value


def thread_chain(tx: sqlite3.Connection, thread_id: str | None) -> Chain | None:
    if thread_id is None:
        return None
    found = tx.execute('SELECT cause, hops FROM threads WHERE id = ?', (thread_id,)).fetchone()
    return None if found is None else Chain(found[0], found[1])


def event_hops(tx: sqlite3.Connection, event_id: int) -> int:
    found = None
    if event_id <= MAX_EVENT_ID:
        found = tx.execute('SELECT thread_id, data FROM events WHERE id = ?',
                           (event_id,)).fetchone()
    if found is None:
        raise LoomError('invalid', NO_STORED_EVENT)
    thread_id, data = found
    hops = json.loads(data).get('hops')
    if _whole(hops):
        return hops
    own = thread_chain(tx, thread_id)
    return 0 if own is None else own.hops


def of_call(tx: sqlite3.Connection, cause: int | None, thread_id: str | None) -> Chain:
    own = thread_chain(tx, thread_id)
    if cause is not None:
        hops = event_hops(tx, cause) + 1
        return Chain(cause, hops if own is None else max(hops, own.hops))
    return NONE if own is None else own


def delegated(tx: sqlite3.Connection, parent: sqlite3.Row, thread_id: str | None) -> Chain:
    chain = Chain(parent['cause'], parent['hops'])
    own = thread_chain(tx, thread_id)
    return own if own is not None and own.hops > chain.hops else chain


def raise_thread(tx: sqlite3.Connection, thread_id: str, chain: Chain) -> None:
    tx.execute('UPDATE threads SET cause = ?, hops = ? WHERE id = ? AND hops < ?',
               (chain.cause, chain.hops, thread_id, chain.hops))
