from enum import StrEnum


class ThreadState(StrEnum):
    QUEUED = 'queued'
    STARTING = 'starting'
    RUNNING = 'running'
    IDLE = 'idle'
    SLEEPING = 'sleeping'
    SETTLED = 'settled'
    FAILED = 'failed'


class TurnState(StrEnum):
    QUEUED = 'queued'
    RUNNING = 'running'
    WAITING = 'waiting'
    COMPLETED = 'completed'
    FAILED = 'failed'
    STOPPED = 'stopped'
