import json

from loom_contracts.calls import CallName

from .host import Maker, SchedulerHost
from .table import FAILED, MAX_REASON, SKIPPED_CHECK, check

NO_TEXT = 'check printed no JSON object with a non-empty text'
ROOT_CHANGED = "the project's root changed since the schedule was added"


class Stopped(Exception):
    def __init__(self, outcome: str, reason: str) -> None:
        super().__init__(reason)
        self.outcome = outcome
        self.reason = reason[:MAX_REASON]


def _printed(output: str) -> str:
    try:
        value = json.loads(output)
    except (ValueError, RecursionError):
        raise Stopped(FAILED, NO_TEXT) from None
    text = value.get('text') if isinstance(value, dict) else None
    if not isinstance(text, str) or not text:
        raise Stopped(FAILED, NO_TEXT)
    return text


async def checked_text(host: SchedulerHost, row: dict, maker: Maker, text: str) -> str:
    projects = await host.call(CallName.PROJECTS_LIST, {}, maker)
    project = next((project for project in projects if project['id'] == row['project_id']),
                   None)
    if project is None:
        raise Stopped(FAILED, f"no project with id '{row['project_id']}'")
    if project['workspace_root'] != row['check_root']:
        raise Stopped(FAILED, ROOT_CHANGED)
    result = await host.run_check(check(row), row['check_root'])
    if result.failure is not None:
        raise Stopped(FAILED, result.failure)
    if result.code > 0:
        raise Stopped(SKIPPED_CHECK, f'check exited with {result.code}')
    output = result.output.strip()
    return text if not output else _printed(output)
