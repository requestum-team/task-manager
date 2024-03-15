import asyncio
import uuid

from task_manager.core.storage import (
    StorageInterface, TransactionalResult,
    ConsumedTask, OnTaskCallback
)
from task_manager.storage.tasks import Task, TaskStatus


class ConsumedTaskResult(TransactionalResult[ConsumedTask]):
    def __init__(self, task: Task, lock: asyncio.Lock):
        self.task = task
        self.lock = lock

    async def get_data(self) -> ConsumedTask:
        return ConsumedTask(self.task.idn, self.task.topic, self.task.payload)

    async def commit(self):
        self.lock.release()

    async def rollback(self):
        self.task.status = TaskStatus.NEW
        self.lock.release()


# todo: refactor this horror
class InMemoryStorage(StorageInterface):
    def __init__(self):
        self.lock = asyncio.Lock()
        self.tasks = []
        self.on_task_callbacks = []

    async def create_task(self, queue, payload) -> str:
        task = Task(str(uuid.uuid4()), queue, payload, TaskStatus.NEW, None)
        self.tasks.append(task)

        for callback in self.on_task_callbacks:
            asyncio.create_task(callback(task.idn))

        return task.idn

    async def take_pending(self, idn) -> TransactionalResult[ConsumedTask] | None:
        await self.lock.acquire()

        for task in self.tasks:
            if task.idn == idn and task.status == TaskStatus.NEW:
                task.status = TaskStatus.IN_PROGRESS
                return ConsumedTaskResult(
                    task,
                    self.lock
                )

        self.lock.release()
        return None

    async def take_first_pending(self, topics: list[str]) -> TransactionalResult[ConsumedTask] | None:
        await self.lock.acquire()

        for task in self.tasks:
            if task.topic in topics and task.status == TaskStatus.NEW:
                task.status = TaskStatus.IN_PROGRESS
                return ConsumedTaskResult(
                    task,
                    self.lock
                )

        self.lock.release()
        return None

    async def finish_task(self, idn: str, error: None | str = None, message: str = ''):
        for task in self.tasks:
            if task.idn == idn:
                task.status = TaskStatus.FINISHED
                task.error = error
                task.message = message
                return

        # todo: add exceptions inheritance level to interface and raise appropriate one
        raise Exception(f"Can't finish task. Task '{idn}' not found in storage")

    def add_on_task_callback(self, callback: OnTaskCallback):
        self.on_task_callbacks.append(callback)
