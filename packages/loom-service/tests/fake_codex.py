import json
import os
import sys
import uuid


def write_private(path: str, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as file:
        file.write(data)


def turn_number(cwd: str) -> int:
    number = 0
    while os.path.exists(os.path.join(cwd, f'codex-{number}.json')):
        number += 1
    return number


def thread_id(argv: list[str]) -> str:
    if 'resume' in argv[:-1]:
        return argv[argv.index('resume') + 1]
    return str(uuid.uuid4())


def emit(out, value: dict) -> None:
    out.write(json.dumps(value).encode() + b'\n')
    out.flush()


def main() -> None:
    cwd = os.getcwd()
    message = sys.stdin.buffer.read().decode('utf-8')
    spawned = {'argv': sys.argv, 'cwd': cwd, 'env': dict(os.environ), 'pid': os.getpid(),
               'stdin': message}
    write_private(os.path.join(cwd, f'codex-{turn_number(cwd)}.json'),
                  json.dumps(spawned).encode())
    out = sys.stdout.buffer
    emit(out, {'type': 'thread.started', 'thread_id': thread_id(sys.argv)})
    emit(out, {'type': 'turn.started'})
    emit(out, {'type': 'item.completed',
               'item': {'id': 'item_0', 'type': 'agent_message', 'text': message}})
    emit(out, {'type': 'turn.completed',
               'usage': {'input_tokens': 1, 'cached_input_tokens': 0, 'output_tokens': 1}})


if __name__ == '__main__':
    main()
