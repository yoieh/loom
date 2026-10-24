import os
from enum import IntEnum
from pathlib import Path

MEMINFO = '/proc/meminfo'
GIB = 2**30
MIB = 2**20
RESERVED = 2 * GIB
PER_AGENT = 350 * MIB


class StartSource(IntEnum):
    PERSON = 0
    AGENT = 1
    SCHEDULE = 2


def _meminfo_available(meminfo: str | Path) -> int | None:
    try:
        text = Path(meminfo).read_text()
    except (OSError, UnicodeDecodeError):
        return None
    for line in text.splitlines():
        name, _, value = line.partition(':')
        if name != 'MemAvailable':
            continue
        fields = value.split()
        if len(fields) == 2 and fields[1] == 'kB' and fields[0].isascii() and fields[0].isdigit():
            return int(fields[0]) * 1024
        return None
    return None


def _sysconf_available() -> int | None:
    try:
        pages = os.sysconf('SC_AVPHYS_PAGES')
        size = os.sysconf('SC_PAGE_SIZE')
    except (OSError, ValueError):
        return None
    if pages < 0 or size < 0:
        return None
    return pages * size


def available_memory(meminfo: str | Path = MEMINFO) -> int | None:
    found = _meminfo_available(meminfo)
    return found if found is not None else _sysconf_available()


def default_agents_max(available: int) -> int:
    return max(1, (available - RESERVED) // PER_AGENT)
