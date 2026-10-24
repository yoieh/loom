import re
import sqlite3
import unicodedata
from pathlib import Path

from loom_contracts.calls import CallName, ProjectInfo
from loom_contracts.runners import PermissionMode

from . import keys, threads
from .clock import iso
from .errors import LoomError
from .store import Store

PROJECT_ID = re.compile(r'^[a-z0-9][a-z0-9-]{0,39}$')
MAX_PROJECT_ID = 40

_COLUMNS = 'id, title, workspace_root, permission_mode, created_at'


def slug(title: str) -> str:
    text = unicodedata.normalize('NFKD', title).encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')
    return text[:MAX_PROJECT_ID].rstrip('-')


def _params(params: object) -> dict:
    if not isinstance(params, dict):
        raise LoomError('invalid', 'params must be an object')
    return params


def _required(params: dict, field: str) -> object:
    value = params.get(field)
    if value is None:
        raise LoomError('invalid', f'{field} is required')
    return value


def _string(params: dict, field: str) -> str:
    value = _required(params, field)
    if not isinstance(value, str):
        raise LoomError('invalid', f'{field} must be a string')
    return value


def _mode(value: object, field: str) -> PermissionMode:
    if isinstance(value, str):
        try:
            return PermissionMode(value)
        except ValueError:
            pass
    modes = ', '.join(mode.value for mode in PermissionMode)
    raise LoomError('invalid', f'{field} must be one of {modes}')


def _info(row: tuple) -> ProjectInfo:
    return {
        'id': row[0],
        'title': row[1],
        'workspace_root': row[2],
        'permission_mode': None if row[3] is None else PermissionMode(row[3]),
        'created_at': row[4],
    }


def _row(tx: sqlite3.Connection, project_id: str) -> ProjectInfo:
    row = tx.execute(f'SELECT {_COLUMNS} FROM projects WHERE id = ?', (project_id,)).fetchone()
    if row is None:
        raise LoomError('not_found', f'no project with id {project_id!r}')
    return _info(row)


def _workspace_root(path: str) -> str:
    candidate = Path(path)
    if not candidate.is_dir():
        raise LoomError('invalid', 'path must name an existing directory')
    return str(candidate.resolve())


def _free_id(tx: sqlite3.Connection, base: str) -> str:
    candidate = base
    number = 1
    while tx.execute('SELECT 1 FROM projects WHERE id = ?', (candidate,)).fetchone():
        number += 1
        suffix = f'-{number}'
        candidate = base[:MAX_PROJECT_ID - len(suffix)].rstrip('-') + suffix
    return candidate


class Projects:
    def __init__(self, store: Store, default_mode: object = PermissionMode.AUTO) -> None:
        self.store = store
        self.default_mode = _mode(default_mode, 'default_mode')

    def add(self, params: object) -> ProjectInfo:
        params = _params(params)
        title = _string(params, 'title')
        base = slug(title)
        if not base:
            raise LoomError('invalid', 'title must have a letter or digit')
        path = _string(params, 'path')
        if not Path(path).is_absolute():
            raise LoomError('invalid', 'path must be absolute')
        mode = None
        if 'permission_mode' in params:
            mode = _mode(params['permission_mode'], 'permission_mode').value

        def change(tx: sqlite3.Connection) -> ProjectInfo:
            root = _workspace_root(path)
            if tx.execute('SELECT 1 FROM projects WHERE workspace_root = ?', (root,)).fetchone():
                raise LoomError('conflict', 'a project already has this path as its root')
            project_id = _free_id(tx, base)
            tx.execute(
                f'INSERT INTO projects ({_COLUMNS}) VALUES (?, ?, ?, ?, ?)',
                (project_id, title, root, mode, iso(self.store.clock())),
            )
            return _row(tx, project_id)

        return self.store.command(params.get('id'), CallName.PROJECTS_ADD, change).answer

    def set_mode(self, params: object) -> ProjectInfo:
        params = _params(params)
        project_id = _string(params, keys.PROJECT_KEY)
        mode = _mode(_required(params, 'permission_mode'), 'permission_mode').value

        def change(tx: sqlite3.Connection) -> ProjectInfo:
            _row(tx, project_id)
            tx.execute(
                'UPDATE projects SET permission_mode = ? WHERE id = ?', (mode, project_id),
            )
            return _row(tx, project_id)

        return self.store.command(params.get('id'), CallName.PROJECTS_SET_MODE, change).answer

    def remove(self, params: object) -> ProjectInfo:
        params = _params(params)
        project_id = _string(params, keys.PROJECT_KEY)

        def change(tx: sqlite3.Connection) -> ProjectInfo:
            info = _row(tx, project_id)
            if threads.has_live(tx, project_id):
                raise LoomError('conflict', 'the project still has a live thread')
            tx.execute('DELETE FROM projects WHERE id = ?', (project_id,))
            return info

        return self.store.command(params.get('id'), CallName.PROJECTS_REMOVE, change).answer

    def list(self) -> list[ProjectInfo]:
        rows = self.store.connection.execute(
            f'SELECT {_COLUMNS} FROM projects ORDER BY created_at, id',
        )
        return [_info(row) for row in rows]

    def get(self, project_id: str) -> ProjectInfo:
        if not isinstance(project_id, str):
            raise LoomError('invalid', f'{keys.PROJECT_KEY} must be a string')
        return _row(self.store.connection, project_id)

    def effective_mode(self, project_id: str) -> PermissionMode:
        mode = self.get(project_id)['permission_mode']
        if mode is not None:
            return mode
        if self.default_mode is PermissionMode.FULL_ACCESS:
            return PermissionMode.AUTO
        return self.default_mode
