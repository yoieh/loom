import sqlite3

from loom_contracts.runners import TurnResult
from loom_contracts.states import TurnState
from loom_contracts.workspaces import WorkspaceStrategy

TITLE_LENGTH = 80
POSTED = frozenset({TurnState.COMPLETED, TurnState.FAILED})


def child_title(task: str) -> str:
    for line in task.splitlines():
        if line.strip():
            return line.strip()[:TITLE_LENGTH]
    return task[:TITLE_LENGTH]


def parent_workspace(parent: sqlite3.Row) -> dict[str, str]:
    if parent['workspace_strategy'] == WorkspaceStrategy.ROOT:
        return {'strategy': WorkspaceStrategy.ROOT.value}
    return {'strategy': WorkspaceStrategy.EXISTING_WORKTREE.value,
            'path': parent['workspace_path'], 'branch': parent['branch']}


def result_text(child: sqlite3.Row, result: TurnResult) -> str:
    head = f'Task {child["id"]} ({child["title"]})'
    if result.state == TurnState.COMPLETED:
        text = f'{head} completed.'
    else:
        text = f'{head} failed: {result.reason or "no reason"}.'
    if result.text:
        text += f'\n\n{result.text}'
    return text
