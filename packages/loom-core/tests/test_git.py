import asyncio
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from loom_core.errors import LoomError
from loom_core.git import GIT_ENV_KEYS, MAX_STDERR, run_git

SETUP_ENV = {**os.environ, 'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_CONFIG_NOSYSTEM': '1'}


def make_repo(path: Path) -> None:
    path.mkdir()
    subprocess.run(['git', 'init', '-q', '-b', 'main', str(path)], env=SETUP_ENV, check=True)
    subprocess.run(['git', '-c', 'user.name=Test', '-c', 'user.email=test@example.com',
                    'commit', '-q', '--allow-empty', '-m', 'First'],
                   cwd=path, env=SETUP_ENV, check=True)


def parse_env(text: str) -> dict[str, str]:
    return dict(line.split('=', 1) for line in text.splitlines() if '=' in line)


class RunGitTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        self.repo = self.root / 'repo'
        make_repo(self.repo)
        patcher = mock.patch.dict(os.environ, {
            'HOME': str(self.home), 'TMPDIR': str(self.root), 'LOOM_SECRET': 'hunter2',
            'GIT_DIR': str(self.root / 'elsewhere'), 'GIT_CONFIG_GLOBAL': '/dev/null',
        })
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    async def test_git_sees_only_the_allowlisted_environment(self) -> None:
        env = parse_env(await run_git(['-c', 'alias.e=!env', 'e'], self.repo))
        self.assertNotIn('LOOM_SECRET', env)
        self.assertNotIn('GIT_CONFIG_GLOBAL', env)
        self.assertNotEqual(env.get('GIT_DIR'), str(self.root / 'elsewhere'))
        self.assertEqual(env['GIT_TERMINAL_PROMPT'], '0')
        self.assertEqual(env['HOME'], str(self.home))
        self.assertEqual(env['TMPDIR'], str(self.root))
        self.assertTrue(env['PATH'].endswith(os.environ['PATH']))

    async def test_child_gets_an_argument_list_and_exactly_the_set_keys(self) -> None:
        os.environ.pop('LANG', None)
        os.environ['LC_ALL'] = 'C'
        real = asyncio.create_subprocess_exec
        seen: list[tuple] = []

        async def spy(*args: object, **kwargs: object) -> object:
            seen.append((args, kwargs))
            return await real(*args, **kwargs)

        with mock.patch('asyncio.create_subprocess_exec', spy):
            await run_git(['status', '--short'], self.repo)
        (args, kwargs), = seen
        self.assertEqual(args, ('git', 'status', '--short'))
        self.assertEqual(kwargs['cwd'], self.repo)
        self.assertEqual(kwargs['stdin'], asyncio.subprocess.DEVNULL)
        self.assertNotIn('shell', kwargs)
        expected = {key: os.environ[key] for key in GIT_ENV_KEYS if key in os.environ}
        self.assertNotIn('LANG', expected)
        self.assertEqual(kwargs['env'], {**expected, 'GIT_TERMINAL_PROMPT': '0'})

    async def test_arguments_reach_git_unchanged(self) -> None:
        text = "a b; $(touch x) `id` 'q'"
        self.assertEqual(await run_git(['rev-parse', '--sq-quote', text], self.repo),
                         "'a b; $(touch x) `id` '\\''q'\\'''")
        self.assertFalse((self.repo / 'x').exists())

    async def test_output_is_stripped(self) -> None:
        self.assertEqual(await run_git(['symbolic-ref', '--short', 'HEAD'], self.repo), 'main')

    async def test_failure_raises_failed_with_stderr(self) -> None:
        with self.assertRaises(LoomError) as caught:
            await run_git(['rev-parse', '--verify', 'no-such-ref'], self.repo)
        self.assertEqual(caught.exception.code, 'failed')
        self.assertIn('Needed a single revision', caught.exception.message)

    async def test_long_stderr_is_cut(self) -> None:
        with self.assertRaises(LoomError) as caught:
            await run_git(['checkout', 'x' * 2000], self.repo)
        self.assertEqual(caught.exception.code, 'failed')
        self.assertIn('xxx', caught.exception.message)
        self.assertEqual(len(caught.exception.message), MAX_STDERR)

    async def test_failure_without_stderr_still_says_why(self) -> None:
        with self.assertRaises(LoomError) as caught:
            await run_git(['rev-parse', '--verify', '--quiet', 'no-such-ref'], self.repo)
        self.assertEqual(caught.exception.code, 'failed')
        self.assertIn('exited with 1', caught.exception.message)

    async def test_missing_git_is_failed(self) -> None:
        os.environ['PATH'] = str(self.home)
        with self.assertRaises(LoomError) as caught:
            await run_git(['status'], self.repo)
        self.assertEqual(caught.exception.code, 'failed')

    async def test_missing_cwd_is_failed(self) -> None:
        with self.assertRaises(LoomError) as caught:
            await run_git(['status'], self.root / 'gone')
        self.assertEqual(caught.exception.code, 'failed')


if __name__ == '__main__':
    unittest.main()
