import json
import os
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


def main() -> None:
    cwd = os.getcwd()
    spawned = {'argv': sys.argv, 'cwd': cwd, 'env': dict(os.environ)}
    write_private(os.path.join(cwd, 'spawned.json'), json.dumps(spawned).encode())
    turns = replay_turns(os.path.join(cwd, 'replay.jsonl'))
    out = sys.stdout.buffer
    for line in sys.stdin.buffer:
        write_private(os.path.join(cwd, 'stdin.jsonl'), line, append=True)
        if is_user_line(line) and turns:
            for reply in turns.pop(0):
                out.write(reply + b'\n')
            out.flush()
    if os.path.exists(os.path.join(cwd, 'ignore-eof')):
        while True:
            time.sleep(60)


if __name__ == '__main__':
    main()
