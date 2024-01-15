from __future__ import annotations
from task_manager.core.storage import StorageInterface, ConsumedTask
from task_manager.core.exceptions import SessionClosedException
from task_manager.core.utils import TaskWaiter


class TaskManager:
    def __init__(self, storage: StorageInterface):
        self.sessions: list[Session] = []
        self.storage = storage
        self.storage.add_on_task_callback(self._on_new_task)
        self.delivering = []

    def new_session(self):
        self.sessions.append(Session(self))
        return self.sessions[-1]

    def close_session(self, session: Session):
        self.sessions.remove(session)
        session.is_closed = True

    async def create_task(self, queue, payload):
        return await self.storage.create_task(queue, payload)

    async def finish_task(self, idn: str, error: None|str, message: str = ''):
        await self.storage.finish_task(idn, error, message)

    async def take_pending_task(self, topics: list[str]):
        if transaction := await self.storage.take_first_pending(topics):
            await transaction.commit()
            return await transaction.get_data()
        return None

    async def _on_new_task(self, idn: str):
        if not (transaction := await self.storage.take_pending(idn)):
            return

        for session in self.sessions:
            if session.deliver_task(await transaction.get_data()):
                await transaction.commit()
                return

        await transaction.rollback()


class Session:
    def __init__(self, engine: TaskManager):
        self._engine = engine
        self._task_waiter: TaskWaiter | None = None
        self.is_closed = False

    def _check_closed(self):
        if self.is_closed:
            raise SessionClosedException()

    async def publish_task(self, topic, payload):
        self._check_closed()
        return await self._engine.create_task(topic, payload)

    async def consume_task(self, topics: list[str]) -> ConsumedTask:
        self._check_closed()
        if task := await self._engine.take_pending_task(topics):
            return task

        self._task_waiter = TaskWaiter(topics)
        return await self._task_waiter.wait()

    def close(self):
        self._engine.close_session(self)
        if self._task_waiter:
            self._task_waiter.result.set_exception(SessionClosedException())

    def deliver_task(self, task: ConsumedTask):
        if self._task_waiter and task.topic in self._task_waiter.topics:
            self._task_waiter.result.set_result(task)
            self._task_waiter = None
            return True

        return False
