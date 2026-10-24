from collections.abc import Callable
from datetime import UTC, datetime

Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(at: datetime) -> str:
    return at.astimezone(UTC).isoformat(timespec='milliseconds')
