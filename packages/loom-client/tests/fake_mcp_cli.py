import json
import os
import sys
import time

GUARD_EXIT = 97


def under(root: str, path: str | None) -> bool:
    if not path:
        return False
    return os.path.commonpath([root, os.path.realpath(path)]) == root


def guarded(root: str) -> bool:
    if not under(root, os.environ.get('HOME')):
        return True
    return any(os.environ.get(key) and not under(root, os.environ[key])
               for key in ('CLAUDE_CONFIG_DIR', 'CODEX_HOME'))


def failure(state: str, agent: str, action: str) -> dict:
    try:
        with open(os.path.join(state, f'{agent}-fail.json')) as file:
            return json.load(file).get(action, {})
    except FileNotFoundError:
        return {}


def claude_path() -> str:
    base = os.environ.get('CLAUDE_CONFIG_DIR') or os.environ['HOME']
    return os.path.join(base, '.claude.json')


def codex_path() -> str:
    base = os.environ.get('CODEX_HOME') or os.path.join(os.environ['HOME'], '.codex')
    return os.path.join(base, 'config.toml')


def claude(args: list[str]) -> int:
    path = claude_path()
    try:
        with open(path) as file:
            data = json.load(file)
    except (OSError, ValueError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    if not isinstance(data.get('mcpServers'), dict):
        data['mcpServers'] = {}
    servers = data['mcpServers']
    if args[:3] == ['mcp', 'add', '--scope'] and args[3] == 'user' and args[5] == '--':
        name = args[4]
        if name in servers:
            sys.stderr.write(f'MCP server {name} already exists in user config\n')
            return 1
        servers[name] = {'type': 'stdio', 'command': args[6], 'args': args[7:], 'env': {}}
        print(f'Added stdio MCP server {name}')
    elif args[:4] == ['mcp', 'remove', '--scope', 'user']:
        name = args[4]
        if name not in servers:
            sys.stderr.write(f'No user-scoped MCP server found with name: {name}\n')
            return 1
        del servers[name]
        print(f'Removed MCP server {name}')
    else:
        sys.stderr.write(f'unknown command: {args}\n')
        return 2
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as file:
        json.dump(data, file, indent=2)
    return 0


def without_table(text: str, name: str) -> tuple[str, bool]:
    header = f'[mcp_servers.{name}]'
    kept, skipping, found = [], False, False
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith('['):
            skipping = stripped == header or stripped.startswith(f'[mcp_servers.{name}.')
            found = found or skipping
        if not skipping:
            kept.append(line)
    return ''.join(kept), found


def codex(args: list[str]) -> int:
    path = codex_path()
    try:
        with open(path) as file:
            text = file.read()
    except FileNotFoundError:
        text = ''
    if args[:2] == ['mcp', 'add'] and args[3] == '--':
        name = args[2]
        text, _ = without_table(text, name)
        if text and not text.endswith('\n'):
            text += '\n'
        gap = '\n' if text else ''
        text += (f'{gap}[mcp_servers.{name}]\n'
                 f'command = {json.dumps(args[4])}\nargs = {json.dumps(args[5:])}\n')
        print(f"Added global MCP server '{name}'.")
    elif args[:2] == ['mcp', 'remove']:
        name = args[2]
        text, found = without_table(text, name)
        if not found:
            print(f"No MCP server named '{name}' found.")
            return 0
        print(f"Removed global MCP server '{name}'.")
    else:
        sys.stderr.write(f'unknown command: {args}\n')
        return 2
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as file:
        file.write(text)
    return 0


def main() -> int:
    root, state, agent, *args = sys.argv[1:]
    if guarded(root):
        sys.stderr.write('fake: refusing a home outside the temp dir\n')
        return GUARD_EXIT
    with open(os.path.join(state, 'calls.jsonl'), 'a') as file:
        file.write(json.dumps({'agent': agent, 'argv': args, 'env': dict(os.environ)}) + '\n')
    mode = failure(state, agent, args[1] if len(args) > 1 else '')
    if mode.get('sleep'):
        time.sleep(mode['sleep'])
    if mode.get('exit'):
        sys.stdout.write(mode.get('stdout', ''))
        sys.stderr.write(mode.get('stderr', ''))
        return mode['exit']
    if mode.get('noop'):
        return 0
    return claude(args) if agent == 'claude' else codex(args)


if __name__ == '__main__':
    sys.exit(main())
