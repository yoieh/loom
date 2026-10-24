import ast
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import loom_core
from loom_core import keys
from loom_core.projects import Projects
from loom_core.store import Store

SOURCES = Path(loom_core.__file__).parent
BARE = {'project', 'parent'}


class KeysTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertEqual(keys.PROJECT_KEY, 'project_id')
        self.assertEqual(keys.PARENT_KEY, 'parent_id')

    def test_no_source_holds_a_bare_key(self) -> None:
        sources = sorted(SOURCES.glob('*.py'))
        self.assertIn(SOURCES / 'projects.py', sources)
        found = []
        for path in sources:
            tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
            found += [
                f'{path.name}:{node.lineno}: {node.value!r}'
                for node in ast.walk(tree)
                if isinstance(node, ast.Constant) and node.value in BARE
            ]
        self.assertEqual(found, [])


class ProjectKeyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.store = Store.open(root / 'data')
        self.addCleanup(self.store.close)
        self.projects = Projects(self.store)
        (root / 'repo').mkdir()
        self.projects.add({'id': 'a1', 'title': 'Loom', 'path': str(root / 'repo')})

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_set_mode_and_remove_read_the_project_key(self) -> None:
        with mock.patch.object(keys, 'PROJECT_KEY', 'ref'):
            info = self.projects.set_mode({'id': 'm1', 'ref': 'loom', 'permission_mode': 'auto'})
            self.assertEqual(info['permission_mode'], 'auto')
            self.assertEqual(self.projects.remove({'id': 'r1', 'ref': 'loom'})['id'], 'loom')
        self.assertEqual(self.projects.list(), [])


if __name__ == '__main__':
    unittest.main()
