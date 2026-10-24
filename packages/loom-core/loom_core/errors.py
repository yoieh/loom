from loom_contracts.calls import ApiError

CODES = frozenset({'invalid', 'not_found', 'conflict', 'failed', 'forbidden'})


class LoomError(Exception):
    def __init__(self, code: str, message: str) -> None:
        if code not in CODES:
            raise ValueError(f'unknown error code: {code!r}')
        super().__init__(f'{code}: {message}')
        self.code = code
        self.message = message

    def to_api(self) -> ApiError:
        return {'code': self.code, 'message': self.message}
