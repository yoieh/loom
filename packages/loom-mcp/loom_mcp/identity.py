from collections.abc import Mapping
from dataclasses import dataclass

from loom_client import Client, LoomApiError

OUTSIDE = 'outside'


@dataclass(frozen=True)
class Caller:
    thread_id: str | None
    origin: str


def loom_thread(thread_id: str) -> Caller:
    return Caller(thread_id, f'thread:{thread_id}')


class Identity:
    def __init__(self, environ: Mapping[str, str]) -> None:
        self._thread = environ.get('LOOM_THREAD') or None
        self._session = environ.get('CLAUDE_CODE_SESSION_ID') or None
        self._known: Caller | None = None

    def caller(self, client: Client) -> Caller:
        if self._thread is not None:
            return loom_thread(self._thread)
        if self._session is None:
            return Caller(None, OUTSIDE)
        if self._known is None:
            self._known = self._look_up(client, self._session)
        return self._known

    @staticmethod
    def _look_up(client: Client, session: str) -> Caller:
        try:
            client.threads_get(session)
        except LoomApiError as error:
            if error.code != 'not_found':
                raise
            return Caller(None, f'{OUTSIDE}:{session}')
        return loom_thread(session)
