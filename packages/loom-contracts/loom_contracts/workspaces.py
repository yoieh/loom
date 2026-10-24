from enum import StrEnum
from typing import Literal, NotRequired, TypedDict


class WorkspaceStrategy(StrEnum):
    ROOT = 'root'
    WORKTREE = 'worktree'
    EXISTING_WORKTREE = 'existing_worktree'


class RootWorkspace(TypedDict):
    strategy: Literal['root']


class NewWorktree(TypedDict):
    strategy: Literal['worktree']
    branch: NotRequired[str]
    base: NotRequired[str]


class ExistingWorktree(TypedDict):
    strategy: Literal['existing_worktree']
    path: str
    branch: str


Workspace = RootWorkspace | NewWorktree | ExistingWorktree
