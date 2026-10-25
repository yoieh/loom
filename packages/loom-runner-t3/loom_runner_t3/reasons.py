from collections.abc import Iterable

MAX_REASON = 200
REDACTED = '[redacted]'

NOT_PAIRED = 't3: not paired (run loom t3 pair)'
NOT_RUNNING = 't3: T3 is not running at the paired address'
ANSWERED_IN_T3 = 't3: permission requests are answered in T3'


class T3Error(Exception):
    pass


class NotPaired(T3Error):
    def __init__(self) -> None:
        super().__init__(NOT_PAIRED)


class NotRunning(T3Error):
    def __init__(self) -> None:
        super().__init__(NOT_RUNNING)


def one_line(text: str) -> str:
    return ' '.join(text.split())


def safe_reason(text: str, secrets: Iterable[str]) -> str:
    kept = [secret for secret in secrets if isinstance(secret, str) and secret.strip()]
    for secret in sorted(kept, key=len, reverse=True):
        text = text.replace(secret, REDACTED)
    return one_line(text)[:MAX_REASON]


def reason(code: str, message: str, secrets: Iterable[str] = ()) -> str:
    return safe_reason(f't3: {code}: {message}', secrets)


def failure(code: str, message: str, secrets: Iterable[str] = ()) -> T3Error:
    return T3Error(reason(code, message, secrets))
