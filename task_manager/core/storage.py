from dataclasses import dataclass
from typing import Callable, Awaitable, Any, TypeVar, Generic


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

    async def finish_task(self, idn: str, error: None|str = None, message: str = ''):
        raise NotImplemented()

    async def take_pending(self, idn) -> TransactionalResult[ConsumedTask] | None:
        raise NotImplemented()

    async def take_first_pending(self, topics: list[str]) -> TransactionalResult[ConsumedTask] | None:
        raise NotImplemented()

    def add_on_task_callback(self, callback: OnTaskCallback):
        raise NotImplemented()
