import asyncio
import os
import signal
from collections.abc import Mapping

from loom_scheduler import CheckResult

CHECK_TIMEOUT = 60
MAX_CHECK_OUTPUT = 65_536
CHUNK = 65_536


def _failed(failure: str) -> CheckResult:
    return CheckResult(None, '', failure)


def _kill(process: asyncio.subprocess.Process) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


async def _read(stream: asyncio.StreamReader, limit: int) -> bytes | None:
    data = bytearray()
    while chunk := await stream.read(CHUNK):
        data += chunk
        if len(data) > limit:
            return None
    return bytes(data)


async def run_check(argv: list[str], cwd: str, env: Mapping[str, str], *,
                    timeout: float = CHECK_TIMEOUT,
                    limit: int = MAX_CHECK_OUTPUT) -> CheckResult:
    try:
        process = await asyncio.create_subprocess_exec(
            *argv, cwd=cwd, env=dict(env), stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as error:
        return _failed(f'cannot run {argv[0]}: {error.strerror or type(error).__name__}')
    except ValueError:
        return _failed('cannot run the check: its arguments are not valid text')
    try:
        async with asyncio.timeout(timeout):
            output = await _read(process.stdout, limit)
            if output is not None:
                await process.wait()
    except TimeoutError:
        _kill(process)
        await process.wait()
        return _failed(f'check ran over {timeout:g} s')
    except BaseException:
        _kill(process)
        await process.wait()
        raise
    if output is None:
        _kill(process)
        await process.wait()
        return _failed(f'check printed over {limit} bytes')
    if process.returncode < 0:
        return _failed(f'check was killed by signal {-process.returncode}')
    return CheckResult(process.returncode, output.decode('utf-8', 'replace'), None)
