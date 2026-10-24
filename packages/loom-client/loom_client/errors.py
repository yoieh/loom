class LoomApiError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f'{code}: {message}')
        self.code = code
        self.message = message


class LoomConnectionError(Exception):
    def __init__(self, message: str, last_event_id: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.last_event_id = last_event_id


class LoomProtocolError(LoomConnectionError):
    pass
