from collections.abc import Iterable

MAX_REASON = 500
REDACTED = '[redacted]'


def safe_reason(text: str, secrets: Iterable[str]) -> str:
    kept = [secret for secret in secrets if isinstance(secret, str) and secret.strip()]
    for secret in sorted(kept, key=len, reverse=True):
        text = text.replace(secret, REDACTED)
    return text[:MAX_REASON]
