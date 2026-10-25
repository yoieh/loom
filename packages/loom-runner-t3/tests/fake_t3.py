import json
import os
import sys
from pathlib import Path


def command_key(argv: list[str]) -> str:
    words = [word for word in argv[1:4] if not word.startswith('-')]
    return '-'.join(words[1:3]) if words[:1] == ['auth'] else 'unknown'


def main() -> None:
    here = Path(sys.argv[0]).resolve().parent
    key = command_key(sys.argv)
    log = here / 'calls.jsonl'
    previous = 0
    if log.exists():
        previous = sum(1 for line in log.read_text().splitlines()
                       if json.loads(line)['key'] == key)
    with open(log, 'a', encoding='utf-8') as file:
        file.write(json.dumps({'key': key, 'argv': sys.argv, 'env': dict(os.environ)}) + '\n')
    out = here / f'{key}-{previous}.out'
    if not out.exists():
        out = here / f'{key}.out'
    if out.exists():
        sys.stdout.buffer.write(out.read_bytes())
    sys.stderr.write('fake t3 stderr\n')
    code = here / f'{key}.exit'
    raise SystemExit(int(code.read_text()) if code.exists() else 0)


class FakeT3:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.path = directory / 't3'

    def install(self) -> Path:
        self.directory.mkdir(parents=True, exist_ok=True)
        source = Path(__file__).read_text(encoding='utf-8')
        self.path.write_text(f'#!{sys.executable} -I\n{source}', encoding='utf-8')
        self.path.chmod(0o700)
        return self.path

    def output(self, key: str, data: object, call: int | None = None) -> None:
        name = f'{key}.out' if call is None else f'{key}-{call}.out'
        raw = data if isinstance(data, bytes) else json.dumps(data).encode('utf-8')
        (self.directory / name).write_bytes(raw)

    def exit(self, key: str, code: int) -> None:
        (self.directory / f'{key}.exit').write_text(str(code))

    def calls(self) -> list[dict]:
        log = self.directory / 'calls.jsonl'
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]


if __name__ == '__main__':
    main()
