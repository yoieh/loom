import json
import os
import subprocess
import sys
import time


def write_private(path: str, data: bytes, append: bool = False) -> None:
    flags = os.O_WRONLY | os.O_CREAT | (os.O_APPEND if append else os.O_TRUNC)
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, 'ab' if append else 'wb') as file:
        file.write(data)


def replay_turns(path: str) -> list[list[bytes]]:
    turns: list[list[bytes]] = [[]]
    with open(path, 'rb') as file:
        for line in file:
            line = line.rstrip(b'\n')
            if not line:
                continue
            turns[-1].append(line)
            if json.loads(line).get('type') == 'result':
                turns.append([])
    return [turn for turn in turns if turn]


def is_user_line(line: bytes) -> bool:
    try:
        data = json.loads(line)
    except ValueError:
        return False
    return isinstance(data, dict) and data.get('type') == 'user'


def fail_code(cwd: str) -> int | None:
    try:
        with open(os.path.join(cwd, 'fail-exit')) as file:
            return int(file.read().strip())
    except (OSError, ValueError):
        return None


def fail(code: int) -> None:
    err = sys.stderr.buffer
    err.write(os.environ.get('ANTHROPIC_AUTH_TOKEN', '').encode())
    err.write(b'x' * 100_000)
    err.flush()
    sys.exit(code)


def start_grandchild(cwd: str) -> None:
    child = subprocess.Popen(['sleep', '60'], stdin=subprocess.DEVNULL)
    write_private(os.path.join(cwd, 'grandchild.pid'), str(child.pid).encode())


def main() -> None:
    cwd = os.getcwd()
    spawned = {
        'argv': sys.argv,
        'cwd': cwd,
        'env': dict(os.environ),
        'pid': os.getpid(),
        'pgid': os.getpgrp(),
    }
    write_private(os.path.join(cwd, 'spawned.json'), json.dumps(spawned).encode())
    if os.path.exists(os.path.join(cwd, 'grandchild')):
        start_grandchild(cwd)
    exit_after_turn = os.path.exists(os.path.join(cwd, 'exit-after-turn'))
    code = fail_code(cwd)
    turns = [] if code is not None else replay_turns(os.path.join(cwd, 'replay.jsonl'))
    out = sys.stdout.buffer
    for line in sys.stdin.buffer:
        write_private(os.path.join(cwd, 'stdin.jsonl'), line, append=True)
        if code is not None:
            fail(code)
        if is_user_line(line) and turns:
            for reply in turns.pop(0):
                out.write(reply + b'\n')
            out.flush()
            if exit_after_turn:
                return
    if os.path.exists(os.path.join(cwd, 'ignore-eof')):
        while True:
            time.sleep(60)


if __name__ == '__main__':
    main()
