import json
import os
import sys

ENDLESS = 'Never end this turn.'


def write_private(path: str, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'wb') as file:
        file.write(data)


def session_id(argv: list[str]) -> str | None:
    for flag in ('--session-id', '--resume'):
        if flag in argv[:-1]:
            return argv[argv.index(flag) + 1]
    return None


def message_text(data: dict) -> str:
    message = data.get('message')
    content = message.get('content') if isinstance(message, dict) else None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return ''.join(block.get('text', '') for block in content if isinstance(block, dict))
    return ''


def emit(out, value: dict) -> None:
    out.write(json.dumps(value).encode() + b'\n')


def turn(out, session: str | None, number: int, text: str) -> None:
    tool_use_id = f'toolu_fake_{number}'
    emit(out, {'type': 'system', 'subtype': 'init', 'cwd': os.getcwd(), 'session_id': session,
               'model': 'fake', 'permissionMode': 'auto'})
    emit(out, {'type': 'assistant', 'session_id': session, 'message': {
        'role': 'assistant', 'type': 'message', 'content': [{
            'type': 'tool_use', 'id': tool_use_id, 'name': 'Bash',
            'input': {'command': 'true', 'description': f'Fake tool call {number}'},
        }],
    }})
    emit(out, {'type': 'user', 'session_id': session, 'message': {
        'role': 'user', 'content': [{
            'tool_use_id': tool_use_id, 'type': 'tool_result', 'content': '', 'is_error': False,
        }],
    }})
    emit(out, {'type': 'assistant', 'session_id': session, 'message': {
        'role': 'assistant', 'type': 'message', 'content': [{'type': 'text', 'text': text}],
    }})
    emit(out, {'type': 'result', 'subtype': 'success', 'is_error': False, 'result': text,
               'session_id': session, 'num_turns': 2, 'terminal_reason': 'completed'})
    out.flush()


def main() -> None:
    cwd = os.getcwd()
    spawned = {'argv': sys.argv, 'cwd': cwd, 'env': dict(os.environ), 'pid': os.getpid()}
    write_private(os.path.join(cwd, 'spawned.json'), json.dumps(spawned).encode())
    session = session_id(sys.argv)
    out = sys.stdout.buffer
    number = 0
    for line in sys.stdin.buffer:
        try:
            data = json.loads(line)
        except ValueError:
            continue
        if isinstance(data, dict) and data.get('type') == 'user':
            if message_text(data) == ENDLESS:
                continue
            number += 1
            turn(out, session, number, message_text(data))


if __name__ == '__main__':
    main()
