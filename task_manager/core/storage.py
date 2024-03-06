import asyncio
import json
from dataclasses import dataclass
from typing import Callable, Awaitable, Any, TypeVar, Generic

from task_manager.core.utils import UUIDEncoder

NEW = 0
IN_PROGRESS = 1
FINISHED = 2


@dataclass
class ConsumedTask:
    idn: str
    topic: str
    payload: dict


OnTaskCallback = Callable[[str], Awaitable[Any]]
T = TypeVar('T')


class TransactionalResult(Generic[T]):
    async def get_data(self):
        raise NotImplemented()

    async def commit(self):
        raise NotImplemented()

    async def rollback(self):
        raise NotImplemented()


class StorageInterface:
    async def create_task(self, queue, payload) -> str:
        raise NotImplemented()

    async def finish_task(self, idn: str, error: None | str = None, message: str = ''):
        raise NotImplemented()

    async def take_pending(self, idn) -> TransactionalResult[ConsumedTask] | None:
        raise NotImplemented()

    async def take_first_pending(self, topics: list[str]) -> TransactionalResult[ConsumedTask] | None:
        raise NotImplemented()

    def add_on_task_callback(self, callback: OnTaskCallback):
        raise NotImplemented()


@dataclass
class Task:
    idn: str
    topic: str
    payload: dict
    status: int
    error: None | str
    description: str = ''

    @staticmethod
    def from_dict(data: dict):
        if isinstance(data.get('payload'), str):
            data['payload'] = json.loads(data['payload'])
        return Task(**json.loads(json.dumps(data, cls=UUIDEncoder)))


class ConsumedTaskResult(TransactionalResult[ConsumedTask]):
    def __init__(self, task: Task, lock: asyncio.Lock):
        self.task = task
        self.lock = lock

    async def get_data(self) -> ConsumedTask:
        return ConsumedTask(self.task.idn, self.task.topic, self.task.payload)

    async def commit(self):
        self.lock.release()
        ...

    async def rollback(self):
        self.task.status = NEW
        self.lock.release()
        ...
