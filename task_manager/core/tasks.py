from dataclasses import dataclass


class TaskStatus:
    NEW = 0
    IN_PROGRESS = 1
    FINISHED = 2


@dataclass
class ConsumedTask:
    idn: str
    topic: str
    payload: dict


@dataclass
class Task(ConsumedTask):
    status: int
    error: None | str
    description: str = ''
