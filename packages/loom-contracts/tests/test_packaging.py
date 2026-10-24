import ast
import re
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

RUNNERS = ('loom-runner-claude', 'loom-runner-codex', 'loom-runner-t3')

TABLE: dict[str, frozenset[str]] = {
    'loom-contracts': frozenset(),
    'loom-core': frozenset({'loom-contracts'}),
    'loom-scheduler': frozenset({'loom-contracts'}),
    **{runner: frozenset({'loom-contracts'}) for runner in RUNNERS},
    'loom-client': frozenset({'loom-contracts'}),
    'loom-mcp': frozenset({'loom-client'}),
    'loom-service': frozenset({'loom-core', 'loom-scheduler', *RUNNERS}),
}

CONTRACTS_MODULES = frozenset({'__future__', 'collections', 'dataclasses', 'enum', 'typing'})

MAX_LINES = 1500

REPO_ROOT = Path(__file__).resolve().parents[3]

_REQUIREMENT_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]*')


def import_name(package: str) -> str:
    return package.replace('-', '_')


def normalize(name: str) -> str:
    return re.sub(r'[-_.]+', '-', name).lower()


def package_dirs(root: Path) -> list[Path]:
    packages = root / 'packages'
    if not packages.is_dir():
        return []
    return sorted(p for p in packages.iterdir() if (p / 'pyproject.toml').is_file())


def _rel(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def _imported_modules(tree: ast.AST) -> list[tuple[int, str]]:
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.append((node.lineno, alias.name.split('.')[0]))
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.append((node.lineno, node.module.split('.')[0]))
    return sorted(found)


def check_table(root: Path) -> list[str]:
    return [
        f'{_rel(root, d / "pyproject.toml")}: package {d.name} is not in the packaging table'
        for d in package_dirs(root)
        if d.name not in TABLE
    ]


def check_init_files(root: Path) -> list[str]:
    problems = []
    for d in package_dirs(root):
        for init in (d / '__init__.py', d / 'tests' / '__init__.py'):
            if not init.is_file():
                problems.append(f'{_rel(root, init)}: missing, test discovery needs it')
    return problems


def check_imports(root: Path) -> list[str]:
    problems = []
    workspace = {import_name(name): name for name in TABLE}
    for d in package_dirs(root):
        package = d.name
        own = import_name(package)
        listed = TABLE.get(package, frozenset())
        allowed = {own} | {import_name(name) for name in listed}
        contracts_dir = d / own if package == 'loom-contracts' else None
        for path in sorted(d.rglob('*.py')):
            rel = _rel(root, path)
            try:
                tree = ast.parse(path.read_text(encoding='utf-8'), filename=rel)
            except (SyntaxError, ValueError) as error:
                problems.append(f'{rel}: does not parse: {error}')
                continue
            in_contracts = contracts_dir is not None and contracts_dir in path.parents
            for line, module in _imported_modules(tree):
                where = f'{rel}:{line}'
                if module in allowed or module in sys.stdlib_module_names:
                    reason = None
                elif module in workspace:
                    reason = f'but {package} may not import {workspace[module]}'
                elif module.startswith('loom_'):
                    reason = 'which is no workspace package'
                else:
                    reason = 'which is not in the stdlib'
                if reason:
                    problems.append(f'{where}: imports {module}, {reason}')
                if in_contracts and module not in CONTRACTS_MODULES:
                    allowed_list = ', '.join(sorted(CONTRACTS_MODULES))
                    problems.append(
                        f'{where}: imports {module}, but {own} may only import {allowed_list}'
                    )
    return problems


def check_dependencies(root: Path) -> list[str]:
    problems = []
    for d in package_dirs(root):
        pyproject = d / 'pyproject.toml'
        rel = _rel(root, pyproject)
        try:
            data = tomllib.loads(pyproject.read_text(encoding='utf-8'))
        except (tomllib.TOMLDecodeError, UnicodeDecodeError) as error:
            problems.append(f'{rel}: does not parse: {error}')
            continue
        listed = {normalize(name) for name in TABLE.get(d.name, frozenset())}
        for requirement in data.get('project', {}).get('dependencies', []):
            match = _REQUIREMENT_NAME.match(requirement.strip())
            name = match.group(0) if match else requirement
            if normalize(name) not in listed:
                problems.append(
                    f'{rel}: depends on {name}, which {d.name} may not depend on'
                )
    return problems


def check_line_counts(root: Path) -> list[str]:
    problems = []
    packages = root / 'packages'
    if not packages.is_dir():
        return problems
    for path in sorted(packages.rglob('*.py')):
        lines = len(path.read_text(encoding='utf-8', errors='replace').splitlines())
        if lines > MAX_LINES:
            problems.append(f'{_rel(root, path)}: {lines} lines, over the {MAX_LINES}-line limit')
    return problems


def find_problems(root: Path) -> list[str]:
    return [
        *check_table(root),
        *check_init_files(root),
        *check_imports(root),
        *check_dependencies(root),
        *check_line_counts(root),
    ]


def make_package(
    root: Path,
    name: str,
    modules: dict[str, str] | None = None,
    dependencies: tuple[str, ...] = (),
    init: bool = True,
    tests_init: bool = True,
) -> Path:
    (root / 'packages').mkdir(exist_ok=True)
    (root / 'packages' / '__init__.py').touch()
    package = root / 'packages' / name
    (package / import_name(name)).mkdir(parents=True)
    (package / 'tests').mkdir()
    deps = ', '.join(f"'{dep}'" for dep in dependencies)
    (package / 'pyproject.toml').write_text(
        f"[project]\nname = '{name}'\nversion = '0.1.0'\ndependencies = [{deps}]\n",
        encoding='utf-8',
    )
    if init:
        (package / '__init__.py').touch()
    if tests_init:
        (package / 'tests' / '__init__.py').touch()
    (package / import_name(name) / '__init__.py').touch()
    for rel, text in (modules or {}).items():
        path = package / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8')
    return package


class RealTreeTest(unittest.TestCase):
    def test_tree_as_built_has_no_problems(self) -> None:
        self.assertEqual(find_problems(REPO_ROOT), [])

    def test_root_is_a_workspace_over_packages(self) -> None:
        data = tomllib.loads((REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
        self.assertNotIn('project', data)
        workspace = data['tool']['uv']['workspace']
        self.assertEqual(workspace['members'], ['packages/*'])
        self.assertIn('packages/__pycache__', workspace['exclude'])
        self.assertEqual((REPO_ROOT / '.python-version').read_text().strip(), '3.12')

    def test_lock_lists_the_workspace_packages(self) -> None:
        lock = tomllib.loads((REPO_ROOT / 'uv.lock').read_text(encoding='utf-8'))
        expected = sorted(d.name for d in package_dirs(REPO_ROOT))
        self.assertEqual(sorted(lock['manifest']['members']), expected)


class ImportRuleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_unlisted_workspace_import_fails(self) -> None:
        make_package(self.root, 'loom-runner-claude', {
            'loom_runner_claude/run.py': 'from loom_core.store import x\n',
        })
        make_package(self.root, 'loom-mcp', {
            'loom_mcp/server.py': 'import loom_contracts\n',
        })
        self.assertEqual(check_imports(self.root), [
            'packages/loom-mcp/loom_mcp/server.py:1: imports loom_contracts, '
            'but loom-mcp may not import loom-contracts',
            'packages/loom-runner-claude/loom_runner_claude/run.py:1: imports loom_core, '
            'but loom-runner-claude may not import loom-core',
        ])

    def test_listed_workspace_imports_pass(self) -> None:
        make_package(self.root, 'loom-core', {
            'loom_core/store.py': 'from loom_contracts.states import ThreadState\n',
        })
        make_package(self.root, 'loom-mcp', {
            'loom_mcp/server.py': 'import loom_client.api\n',
        })
        make_package(self.root, 'loom-service', {
            'loom_service/serve.py': (
                'import loom_core\nimport loom_scheduler\nimport loom_runner_claude\n'
                'import loom_runner_codex\nfrom loom_runner_t3 import runner\n'
            ),
        })
        self.assertEqual(check_imports(self.root), [])

    def test_import_inside_function_is_checked(self) -> None:
        make_package(self.root, 'loom-runner-codex', {
            'loom_runner_codex/run.py': (
                'import json\n\n\ndef start():\n    from loom_core import store\n'
                '    import requests\n    return store, requests, json\n'
            ),
        })
        self.assertEqual(check_imports(self.root), [
            'packages/loom-runner-codex/loom_runner_codex/run.py:5: imports loom_core, '
            'but loom-runner-codex may not import loom-core',
            'packages/loom-runner-codex/loom_runner_codex/run.py:6: imports requests, '
            'which is not in the stdlib',
        ])

    def test_third_party_import_fails(self) -> None:
        make_package(self.root, 'loom-client', {
            'loom_client/api.py': 'import requests\n',
        })
        self.assertEqual(check_imports(self.root), [
            'packages/loom-client/loom_client/api.py:1: imports requests, '
            'which is not in the stdlib',
        ])

    def test_unknown_loom_module_fails(self) -> None:
        make_package(self.root, 'loom-core', {
            'loom_core/store.py': 'from loom_widgets import knob\n',
        })
        self.assertEqual(check_imports(self.root), [
            'packages/loom-core/loom_core/store.py:1: imports loom_widgets, '
            'which is no workspace package',
        ])

    def test_stdlib_own_and_relative_imports_pass(self) -> None:
        make_package(self.root, 'loom-scheduler', {
            'loom_scheduler/tick.py': (
                'import asyncio\nimport os.path\nfrom datetime import datetime\n'
                'import loom_scheduler.math\nfrom loom_scheduler import math\n'
                'from . import math\nfrom .math import next_run\nfrom .. import x\n'
            ),
        })
        self.assertEqual(check_imports(self.root), [])

    def test_test_file_is_checked(self) -> None:
        make_package(self.root, 'loom-core', {
            'tests/test_store.py': 'import unittest\nimport loom_client\n',
        })
        self.assertEqual(check_imports(self.root), [
            'packages/loom-core/tests/test_store.py:2: imports loom_client, '
            'but loom-core may not import loom-client',
        ])

    def test_contracts_importing_outside_its_list_fails(self) -> None:
        make_package(self.root, 'loom-contracts', {
            'loom_contracts/calls.py': 'import os\nfrom json import dumps\n',
        })
        allowed = '__future__, collections, dataclasses, enum, typing'
        self.assertEqual(check_imports(self.root), [
            f'packages/loom-contracts/loom_contracts/calls.py:1: imports os, '
            f'but loom_contracts may only import {allowed}',
            f'packages/loom-contracts/loom_contracts/calls.py:2: imports json, '
            f'but loom_contracts may only import {allowed}',
        ])

    def test_contracts_allowed_modules_pass(self) -> None:
        make_package(self.root, 'loom-contracts', {
            'loom_contracts/events.py': (
                'from __future__ import annotations\nimport collections.abc\n'
                'from dataclasses import dataclass\nfrom enum import StrEnum\n'
                'from typing import Protocol\nfrom . import states\nfrom .calls import Call\n'
            ),
        })
        self.assertEqual(check_imports(self.root), [])

    def test_contracts_tests_may_import_more_stdlib(self) -> None:
        make_package(self.root, 'loom-contracts', {
            'tests/test_events.py': (
                'import ast\nimport tomllib\nimport unittest\nfrom loom_contracts import events\n'
            ),
        })
        self.assertEqual(check_imports(self.root), [])

    def test_syntax_error_is_a_problem_and_checking_goes_on(self) -> None:
        make_package(self.root, 'loom-core', {
            'loom_core/broken.py': 'def oops(:\n',
            'loom_core/store.py': 'import requests\n',
        })
        problems = check_imports(self.root)
        self.assertEqual(len(problems), 2)
        self.assertTrue(
            problems[0].startswith('packages/loom-core/loom_core/broken.py: does not parse:'),
        )
        self.assertEqual(
            problems[1],
            'packages/loom-core/loom_core/store.py:1: imports requests, which is not in the stdlib',
        )
        self.assertEqual(len(find_problems(self.root)), 2)


class LayoutRuleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_package_not_in_table_fails(self) -> None:
        make_package(self.root, 'loom-contracts')
        make_package(self.root, 'loom-extras')
        self.assertEqual(check_table(self.root), [
            'packages/loom-extras/pyproject.toml: package loom-extras is not in the packaging table',
        ])

    def test_directory_without_pyproject_is_no_package(self) -> None:
        make_package(self.root, 'loom-contracts')
        (self.root / 'packages' / '__pycache__').mkdir()
        self.assertEqual(find_problems(self.root), [])

    def test_missing_package_init_fails(self) -> None:
        make_package(self.root, 'loom-core', init=False)
        self.assertEqual(check_init_files(self.root), [
            'packages/loom-core/__init__.py: missing, test discovery needs it',
        ])

    def test_missing_tests_init_fails(self) -> None:
        make_package(self.root, 'loom-core', tests_init=False)
        self.assertEqual(check_init_files(self.root), [
            'packages/loom-core/tests/__init__.py: missing, test discovery needs it',
        ])


class DependencyRuleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_unlisted_dependency_fails(self) -> None:
        make_package(self.root, 'loom-core', dependencies=('httpx>=0.27', 'loom-client'))
        self.assertEqual(check_dependencies(self.root), [
            'packages/loom-core/pyproject.toml: depends on httpx, which loom-core may not depend on',
            'packages/loom-core/pyproject.toml: depends on loom-client, '
            'which loom-core may not depend on',
        ])

    def test_listed_dependency_passes_with_a_version(self) -> None:
        make_package(self.root, 'loom-core', dependencies=('loom-contracts>=0.1',))
        make_package(self.root, 'loom-mcp', dependencies=('Loom_Client == 0.1.0',))
        self.assertEqual(check_dependencies(self.root), [])


class LineCountRuleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_module_of_1500_lines_passes(self) -> None:
        make_package(self.root, 'loom-core', {'loom_core/big.py': 'x = 1\n' * 1500})
        self.assertEqual(check_line_counts(self.root), [])

    def test_module_of_1501_lines_fails(self) -> None:
        make_package(self.root, 'loom-core', {'loom_core/big.py': 'x = 1\n' * 1501})
        self.assertEqual(check_line_counts(self.root), [
            'packages/loom-core/loom_core/big.py: 1501 lines, over the 1500-line limit',
        ])


if __name__ == '__main__':
    unittest.main()
