import json
import os
import sys
import uuid
from pathlib import Path

ENDLESS = 'Never end this turn.'
ASK = 'Ask for permission.'
STREAM = (Path(__file__).resolve().parents[2] / 'loom-runner-claude' / 'tests' / 'streams'
          / 'perm-tool.jsonl')
RESPONSES = 'responses.jsonl'


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


def first_turn() -> list[dict]:
    lines = [json.loads(line) for line in STREAM.read_text().splitlines() if line.strip()]
    end = next(index for index, line in enumerate(lines) if line.get('type') == 'result')
    return lines[:end + 1]


def asked_request(turn_lines: list[dict]) -> dict:
    return next(line for line in turn_lines if line.get('type') == 'control_request')


def with_session(line: dict, session: str | None) -> dict:
    return {**line, 'session_id': session} if 'session_id' in line else line


def append_private(path: str, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, 'ab') as file:
        file.write(data)


def control_response(line: bytes) -> dict | None:
    try:
        data = json.loads(line)
    except ValueError:
        return None
    if not isinstance(data, dict) or data.get('type') != 'control_response':
        return None
    response = data.get('response')
    if not isinstance(response, dict):
        return None
    append_private(os.path.join(os.getcwd(), RESPONSES), line)
    return response


def answer_to(stdin, request_id: str) -> dict | None:
    for line in stdin:
        response = control_response(line)
        if response is not None and response.get('request_id') == request_id:
            answer = response.get('response')
            return answer if isinstance(answer, dict) else {}
    return None


def answered(request_id: str) -> bool:
    try:
        lines = Path(os.getcwd(), RESPONSES).read_bytes().splitlines()
    except FileNotFoundError:
        return False
    for line in lines:
        response = json.loads(line).get('response')
        if isinstance(response, dict) and response.get('request_id') == request_id:
            return True
    return False


def ask(stdin, out, session: str | None) -> None:
    turn_lines = first_turn()
    request = asked_request(turn_lines)
    cut = turn_lines.index(request) + 1
    request_id = request['request_id']
    if answered(request_id):
        request_id = str(uuid.uuid4())
    for line in turn_lines[:cut]:
        line = {**line, 'request_id': request_id} if line is request else line
        emit(out, with_session(line, session))
    out.flush()
    answer = answer_to(stdin, request_id)
    if answer is None:
        return
    if answer.get('behavior') == 'allow':
        for line in turn_lines[cut:]:
            emit(out, with_session(line, session))
        out.flush()
        return
    message = answer.get('message', '')
    text = 'The tool call was denied.'
    emit(out, {'type': 'user', 'session_id': session, 'message': {
        'role': 'user', 'content': [{
            'type': 'tool_result', 'content': message, 'is_error': True,
            'tool_use_id': request['request']['tool_use_id'],
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
    stdin = sys.stdin.buffer
    number = 0
    for line in stdin:
        if control_response(line) is not None:
            continue
        try:
            data = json.loads(line)
        except ValueError:
            continue
        if isinstance(data, dict) and data.get('type') == 'user':
            if message_text(data) == ENDLESS:
                continue
            if message_text(data) == ASK:
                ask(stdin, out, session)
                continue
            number += 1
            turn(out, session, number, message_text(data))


if __name__ == '__main__':
    main()
