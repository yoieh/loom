import json
import os
import subprocess
import sys
import time


def write_private(path: str, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'wb') as file:
        file.write(data)


def turn_number(cwd: str) -> int:
    number = 0
    while os.path.exists(os.path.join(cwd, f'spawned-{number}.json')):
        number += 1
    return number


def main() -> None:
    cwd = os.getcwd()
    number = turn_number(cwd)
    spawned = {
        'argv': sys.argv,
        'cwd': cwd,
        'env': dict(os.environ),
        'pid': os.getpid(),
        'pgid': os.getpgrp(),
    }
    write_private(os.path.join(cwd, f'spawned-{number}.json'), json.dumps(spawned).encode())
    write_private(os.path.join(cwd, f'stdin-{number}.txt'), sys.stdin.buffer.read())
    if os.path.exists(os.path.join(cwd, 'grandchild')):
        child = subprocess.Popen(['sleep', '60'], stdin=subprocess.DEVNULL)
        write_private(os.path.join(cwd, 'grandchild.pid'), str(child.pid).encode())
    out = sys.stdout.buffer
    replay = os.path.join(cwd, f'turn-{number}.jsonl')
    if os.path.exists(replay):
        with open(replay, 'rb') as file:
            for line in file:
                out.write(line)
                out.flush()
    if os.path.exists(os.path.join(cwd, f'hang-{number}')):
        time.sleep(60)


if __name__ == '__main__':
    main()
