import asyncio
import json
import os
import uuid
from typing import Optional

import asyncpg
from asyncpg import Connection
from asyncpg.transaction import Transaction

from task_manager.core.storage import StorageInterface, OnTaskCallback, TransactionalResult, ConsumedTask, NEW, \
    IN_PROGRESS, ConsumedTaskResult, FINISHED, Task


class AsyncPGStorage(StorageInterface):
    def __init__(self):
        self.conn: Optional[Connection] = None
        self.lock = asyncio.Lock()
        self.tasks = []  # todo postgres impl

        # todo postgres impl
        # self.conn.add_listener() ?
        self.on_task_callbacks = []

    async def create_connection(self):
        if not self.conn or self.conn.is_closed():
            self.conn = await asyncpg.connect(
                user=os.environ.get("POSTGRES_USER", "postgres"),
                password=os.environ.get("POSTGRES_PASS", "task_manager_password"),
                database=os.environ.get("POSTGRES_NAME", "task_manager"),
                host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
                port=os.environ.get("POSTGRES_PORT", "5432")
            )

    async def close_connection(self):
        await self.conn.close()

    async def create_task(self, queue, payload) -> str:
        """
        idn UUID PRIMARY KEY,
        topic VARCHAR (50) NOT NULL,
        payload JSON NOT NULL,
        status int NOT NULL,
        error VARCHAR (255) NULL,
        description VARCHAR (255) NULL
        """
        task = Task(str(uuid.uuid4()), queue, payload, NEW, None)

        q = "INSERT INTO tasks (" \
            "idn, topic, payload, status" \
            ") VALUES (" \
            f"'{task.idn}', '{queue}', '{json.dumps(payload)}', {NEW}" \
            ")"

        await self.conn.fetch(q)

        self.tasks.append(task)

        # todo postgres impl
        for callback in self.on_task_callbacks:
            asyncio.create_task(callback(task.idn))

        return task.idn

    async def take_pending(self, idn) -> TransactionalResult[ConsumedTask] | None:
        """
        idn UUID PRIMARY KEY,
        topic VARCHAR (50) NOT NULL,
        payload JSON NOT NULL,
        status int NOT NULL,
        error VARCHAR (255) NULL,
        description VARCHAR (255) NULL
        """

        # FIXME pg impl conflicts
        # await self.lock.acquire()

        q = ("UPDATE tasks "
             f"SET status = {IN_PROGRESS} "
             f"WHERE (idn='{idn}' and status={NEW}) RETURNING * ;")

        res: list | asyncpg.Record = await self.conn.fetch(q)

        if res:
            res: asyncpg.Record = res[0]
            task = Task.from_dict(dict(res))
            return ConsumedTaskResult(
                task,
                self.lock
            )

        # FIXME pg impl conflicts
        # self.lock.release()
        return None

    # todo postgres impl
    async def take_first_pending(self, topics: list[str]) -> TransactionalResult[ConsumedTask] | None:
        # FIXME pg impl conflicts
        # await self.lock.acquire()

        for task in self.tasks:
            if task.topic in topics and task.status == NEW:
                task.status = IN_PROGRESS
                return ConsumedTaskResult(
                    task,
                    self.lock
                )

        # FIXME pg impl conflicts
        # self.lock.release()
        return None

    # todo postgres impl
    async def finish_task(self, idn: str, error: None | str = None, message: str = ''):
        for task in self.tasks:
            if task.idn == idn:
                task.status = FINISHED
                task.error = error
                task.message = message
                return

        # todo: add exceptions inheritance level to interface and raise appropriate one
        raise Exception(f"Can't finish task. Task '{idn}' not found in storage")

    def add_on_task_callback(self, callback: OnTaskCallback):
        # todo postgres impl
        self.on_task_callbacks.append(callback)
