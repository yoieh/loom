import dataclasses
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from loom_contracts.calls import ProjectInfo
from loom_contracts.workspaces import WorkspaceStrategy

from .errors import LoomError
from .git import run_git
from .projects import PROJECT_ID

THREAD_ID = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')


@dataclass(frozen=True)
class WorkspacePlan:
    strategy: WorkspaceStrategy
    path: str
    branch: str | None
    base: str | None


def worktree_path(data_dir: Path, project_id: str, thread_id: str) -> Path:
    return Path(data_dir) / 'worktrees' / project_id / thread_id


def _project_id(project: ProjectInfo) -> str:
    project_id = project.get('id') if isinstance(project, dict) else None
    if not isinstance(project_id, str) or not PROJECT_ID.fullmatch(project_id):
        raise LoomError('invalid', 'project id is not a valid project id')
    return project_id


def _resolve(path: str | Path, field: str) -> Path:
    try:
        return Path(path).resolve()
    except (OSError, RuntimeError, ValueError) as error:
        raise LoomError('invalid', f'{field} cannot be resolved: {error}') from error


def _strategy(value: object) -> WorkspaceStrategy:
    if isinstance(value, str):
        try:
            return WorkspaceStrategy(value)
        except ValueError:
            pass
    strategies = ', '.join(strategy.value for strategy in WorkspaceStrategy)
    raise LoomError('invalid', f'strategy must be one of {strategies}')


def _ref_name(workspace: dict, field: str) -> str:
    value = workspace[field]
    if not isinstance(value, str):
        raise LoomError('invalid', f'{field} must be a string')
    if not value:
        raise LoomError('invalid', f'{field} must not be empty')
    if value.startswith('-'):
        raise LoomError('invalid', f'{field} must not start with -')
    if '\x00' in value:
        raise LoomError('invalid', f'{field} must not hold a NUL character')
    return value


def _required(workspace: dict, field: str) -> str:
    if field not in workspace:
        raise LoomError('invalid', f'{field} is required')
    return _ref_name(workspace, field)


def _optional(workspace: dict, field: str) -> str | None:
    if field not in workspace:
        return None
    return _ref_name(workspace, field)


def _existing_path(workspace: dict, project: ProjectInfo, project_dir: Path) -> str:
    if 'path' not in workspace:
        raise LoomError('invalid', 'path is required')
    value = workspace['path']
    if not isinstance(value, str):
        raise LoomError('invalid', 'path must be a string')
    if '\x00' in value or not os.path.isabs(value):
        raise LoomError('invalid', 'path must be absolute')
    path = _resolve(value, 'path')
    root = _resolve(project['workspace_root'], 'workspace_root')
    if path != root and (path == project_dir or not path.is_relative_to(project_dir)):
        raise LoomError(
            'invalid', "path must be the project's root or a worktree Loom made for it",
        )
    return str(path)


def plan_workspace(workspace: object, project: ProjectInfo, thread_id: str,
                   data_dir: Path) -> WorkspacePlan:
    project_id = _project_id(project)
    if not isinstance(thread_id, str) or not THREAD_ID.fullmatch(thread_id):
        raise LoomError('invalid', 'thread id is not a valid thread id')
    if not isinstance(workspace, dict):
        raise LoomError('invalid', 'workspace must be an object')
    strategy = _strategy(workspace.get('strategy'))
    data_dir = _resolve(data_dir, 'data_dir')
    if strategy is WorkspaceStrategy.ROOT:
        root = _resolve(project['workspace_root'], 'workspace_root')
        return WorkspacePlan(strategy, str(root), None, None)
    if strategy is WorkspaceStrategy.WORKTREE:
        branch = _optional(workspace, 'branch') or f'loom/{thread_id}'
        base = _optional(workspace, 'base')
        path = worktree_path(data_dir, project_id, thread_id)
        return WorkspacePlan(strategy, str(path), branch, base)
    branch = _required(workspace, 'branch')
    path = _existing_path(workspace, project, data_dir / 'worktrees' / project_id)
    return WorkspacePlan(strategy, path, branch, None)


async def _git_or_none(args: list[str], cwd: Path) -> str | None:
    try:
        return await run_git(args, cwd)
    except LoomError:
        return None


async def _current_branch(cwd: Path) -> str | None:
    return await _git_or_none(['symbolic-ref', '--quiet', '--short', 'HEAD'], cwd)


async def _common_dir(cwd: Path) -> Path | None:
    found = await _git_or_none(['rev-parse', '--path-format=absolute', '--git-common-dir'], cwd)
    return None if found is None else _resolve(found, 'git common dir')


async def _is_checkout(path: Path, common_dir: Path, branch: str) -> bool:
    if path.is_symlink() or not path.is_dir():
        return False
    top = await _git_or_none(['rev-parse', '--show-toplevel'], path)
    if top is None or _resolve(top, 'path') != path:
        return False
    if await _common_dir(path) != common_dir:
        return False
    return await _current_branch(path) == branch


async def _repo_root(root: Path) -> Path:
    top = await _git_or_none(['rev-parse', '--show-toplevel'], root)
    if top is None or _resolve(top, 'workspace_root') != root:
        raise LoomError('invalid', "the project's root is not the top of a git repo")
    return root


def _private_dir(path: Path) -> None:
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        if not stat.S_ISDIR(os.lstat(path).st_mode):
            raise LoomError('conflict', f'{path} exists and is not a directory') from None
        return
    except OSError as error:
        raise LoomError('failed', f'cannot make {path}: {error.strerror}') from error
    os.chmod(path, 0o700)


async def _make_worktree(plan: WorkspacePlan, project: ProjectInfo,
                         data_dir: Path) -> WorkspacePlan:
    project_dir = _resolve(data_dir, 'data_dir') / 'worktrees' / _project_id(project)
    path = Path(plan.path)
    if path.parent != project_dir:
        raise LoomError('invalid', 'path is not a worktree path of this project')
    root = await _repo_root(_resolve(project['workspace_root'], 'workspace_root'))
    branch = plan.branch
    if await _git_or_none(['check-ref-format', '--branch', branch], root) != branch:
        raise LoomError('invalid', f'branch {branch!r} is not a valid branch name')
    base = plan.base
    if base is None:
        base = await _current_branch(root) or await _git_or_none(['rev-parse', 'HEAD'], root)
    if base is None or await _git_or_none(
            ['rev-parse', '--verify', '--quiet', f'{base}^{{commit}}'], root) is None:
        raise LoomError('invalid', f'base {base!r} is not a commit')
    made = dataclasses.replace(plan, base=base)
    _private_dir(project_dir.parent)
    _private_dir(project_dir)
    if os.path.lexists(path):
        if await _is_checkout(path, await _common_dir(root), branch):
            return made
        raise LoomError('conflict', f'{path} exists already')
    if await _git_or_none(['show-ref', '--verify', '--quiet', f'refs/heads/{branch}'],
                          root) is not None:
        raise LoomError('conflict', f'branch {branch!r} exists already')
    await run_git(['worktree', 'add', '-b', branch, str(path), base], root)
    os.chmod(path, 0o700)
    return made


async def _check_existing(plan: WorkspacePlan, project: ProjectInfo) -> WorkspacePlan:
    root = _resolve(project['workspace_root'], 'workspace_root')
    common_dir = await _common_dir(root)
    if common_dir is None or not await _is_checkout(Path(plan.path), common_dir, plan.branch):
        raise LoomError(
            'invalid', f'{plan.path} is not a checkout of the project on branch {plan.branch!r}',
        )
    return plan


async def make_workspace(plan: WorkspacePlan, project: ProjectInfo,
                         data_dir: Path) -> WorkspacePlan:
    if plan.strategy is WorkspaceStrategy.ROOT:
        return dataclasses.replace(plan, branch=await _current_branch(Path(plan.path)))
    if plan.strategy is WorkspaceStrategy.WORKTREE:
        return await _make_worktree(plan, project, data_dir)
    return await _check_existing(plan, project)
