import json
import re
from collections.abc import Mapping

MAX_FILL = 2_000

_PLACE = re.compile(r'\{\{(?:event\.id|payload\.([A-Za-z0-9_-]+))\}\}')


def compact(value: object) -> str:
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False)


def _text(value: object) -> str:
    if value is None:
        return ''
    if isinstance(value, str):
        return value
    return compact(value)


def fill(text: str, event_id: int, payload: Mapping[str, object]) -> str:
    def value(found: re.Match[str]) -> str:
        field = found.group(1)
        filled = str(event_id) if field is None else _text(payload.get(field))
        return filled[:MAX_FILL]

    return _PLACE.sub(value, text)
