import asyncio
import os
from collections.abc import Sequence
from pathlib import Path

from .errors import LoomError

GIT_ENV_KEYS = ('HOME', 'LANG', 'LC_ALL', 'PATH', 'TMPDIR')
MAX_STDERR = 500
KILL_GRACE = 2.0


def git_env() -> dict[str, str]:
    env = {key: os.environ[key] for key in GIT_ENV_KEYS if key in os.environ}
    env['GIT_TERMINAL_PROMPT'] = '0'
    return env


async def _stop(process: asyncio.subprocess.Process) -> None:
    try:
        process.terminate()
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(process.wait(), KILL_GRACE)
    except TimeoutError:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        await process.wait()


async def run_git(args: Sequence[str], cwd: Path) -> str:
    try:
        process = await asyncio.create_subprocess_exec(
            'git', *args,
            cwd=cwd,
            env=git_env(),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (OSError, ValueError) as error:
        raise LoomError('failed', f'git could not start in {cwd}: {error}') from error
    try:
        stdout, stderr = await process.communicate()
    except asyncio.CancelledError:
        await _stop(process)
        raise
    if process.returncode != 0:
        message = stderr.decode('utf-8', 'replace').strip()
        if not message:
            message = f'git {args[0] if args else ""} exited with {process.returncode}'
        raise LoomError('failed', message[:MAX_STDERR])
    return stdout.decode('utf-8', 'replace').strip()
