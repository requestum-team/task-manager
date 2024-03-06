import asyncio
import json
import os
import uuid
from typing import Optional

import asyncpg
from asyncpg import Connection, Pool
from asyncpg.transaction import Transaction

from task_manager.core.storage import StorageInterface, OnTaskCallback, TransactionalResult, ConsumedTask, NEW, \
    IN_PROGRESS, ConsumedTaskResult, FINISHED, Task


class AsyncPGStorage(StorageInterface):
    def __init__(self):
        self.pool: Optional[Pool] = None
        self.lock = asyncio.Lock()
        self.tasks = []  # todo postgres impl

        # todo postgres impl
        # self.conn.add_listener() ?
        self.on_task_callbacks = []

    async def create_connection(self):
        if not self.pool or self.pool._closed:
            self.pool = await asyncpg.create_pool(
                user=os.environ.get("POSTGRES_USER", "postgres"),
                password=os.environ.get("POSTGRES_PASS", "task_manager_password"),
                database=os.environ.get("POSTGRES_NAME", "task_manager"),
                host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
                port=os.environ.get("POSTGRES_PORT", "5432"),
                max_size=10
            )

    async def close_connection(self):
        await self.pool.close()

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

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.fetch(q)

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

        await self.lock.acquire()

        q = ("UPDATE tasks "
             f"SET status = {IN_PROGRESS} "
             f"WHERE (idn='{idn}' and status={NEW}) RETURNING * ;")

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                res: list | asyncpg.Record = await connection.fetch(q)

        if res:
            res: asyncpg.Record = res[0]
            task = Task.from_dict(dict(res))
            return ConsumedTaskResult(
                task,
                self.lock
            )

        self.lock.release()
        return None

    async def take_first_pending(self, topics: list[str]) -> TransactionalResult[ConsumedTask] | None:
        await self.lock.acquire()
        """
        idn UUID PRIMARY KEY,
        topic VARCHAR (50) NOT NULL,
        payload JSON NOT NULL,
        status int NOT NULL,
        error VARCHAR (255) NULL,
        description VARCHAR (255) NULL
        """

        q = (
            "UPDATE tasks "
            f"SET status = {IN_PROGRESS} "
            f"WHERE ("
            f"status = {NEW} and "
            f"topic in ({str([f'{i}' for i in topics])[1:-1]})) "
            f"RETURNING * ;"
        )

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                res: list | asyncpg.Record = await connection.fetch(q)

        if res:
            res: asyncpg.Record = res[0]
            task = Task.from_dict(dict(res))
            return ConsumedTaskResult(
                task,
                self.lock
            )

        self.lock.release()
        return None

    async def finish_task(self, idn: str, error: None | str = None, message: str = ''):

        q = (
            "UPDATE tasks "
            f"SET status = {FINISHED}, "
            f"error = {error}, "
            f"description = {message} "
            f"WHERE idn = '{idn}'"
            f"RETURNING *;"
        )
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                res: list | asyncpg.Record = await connection.fetch(q)

        if not res:
            # todo: add exceptions inheritance level to interface and raise appropriate one
            raise Exception(f"Can't finish task. Task '{idn}' not found in storage")

    def add_on_task_callback(self, callback: OnTaskCallback):
        # todo postgres impl
        self.on_task_callbacks.append(callback)
