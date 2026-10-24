import unittest
from enum import StrEnum
from typing import get_args, get_type_hints

from loom_contracts.workspaces import (
    ExistingWorktree,
    NewWorktree,
    RootWorkspace,
    Workspace,
    WorkspaceStrategy,
)


class WorkspaceStrategyTest(unittest.TestCase):
    def test_values(self) -> None:
        self.assertTrue(issubclass(WorkspaceStrategy, StrEnum))
        self.assertEqual(
            {strategy.value for strategy in WorkspaceStrategy},
            {'root', 'worktree', 'existing_worktree'},
        )


class WorkspaceShapeTest(unittest.TestCase):
    def assert_keys(self, shape: type, required: set[str], optional: set[str]) -> None:
        self.assertEqual(shape.__required_keys__, frozenset(required))
        self.assertEqual(shape.__optional_keys__, frozenset(optional))

    def test_root(self) -> None:
        self.assert_keys(RootWorkspace, {'strategy'}, set())

    def test_new_worktree(self) -> None:
        self.assert_keys(NewWorktree, {'strategy'}, {'branch', 'base'})

    def test_existing_worktree(self) -> None:
        self.assert_keys(ExistingWorktree, {'strategy', 'path', 'branch'}, set())

    def test_each_shape_names_its_strategy(self) -> None:
        shapes = {
            WorkspaceStrategy.ROOT: RootWorkspace,
            WorkspaceStrategy.WORKTREE: NewWorktree,
            WorkspaceStrategy.EXISTING_WORKTREE: ExistingWorktree,
        }
        for strategy, shape in shapes.items():
            with self.subTest(strategy=strategy):
                self.assertEqual(get_args(get_type_hints(shape)['strategy']), (strategy.value,))

    def test_workspace_is_the_union_of_the_shapes(self) -> None:
        self.assertEqual(
            set(get_args(Workspace)), {RootWorkspace, NewWorktree, ExistingWorktree},
        )


if __name__ == '__main__':
    unittest.main()
