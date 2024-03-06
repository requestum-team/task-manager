import asyncio
import json
import os
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple

import asyncpg
from asyncpg import Connection, Pool
from asyncpg.prepared_stmt import PreparedStatement
from asyncpg.transaction import Transaction

from task_manager.core.tasks import ConsumedTask, TaskStatus
from task_manager.core.storage import (
    StorageInterface, OnTaskCallback, TransactionalResult
)
from task_manager.core.utils import UUIDEncoder
from task_manager.storage.in_memory import Task


@dataclass
class AsyncPGTask(Task):
    @staticmethod
    def from_dict(data: dict):
        if isinstance(data.get('payload'), str):
            data['payload'] = json.loads(data['payload'])
        return AsyncPGTask(**json.loads(json.dumps(data, cls=UUIDEncoder)))


class AsyncPGConsumedTaskResult(TransactionalResult[ConsumedTask]):
    def __init__(
            self,
            task: AsyncPGTask,
            lock: Transaction
    ):
        self.task = task
        self.lock = lock

    async def get_data(self) -> ConsumedTask:
        # TODO pg impl
        return ConsumedTask(self.task.idn, self.task.topic, self.task.payload)
        # ...

    async def commit(self):
        await self.lock.commit()

    async def rollback(self):
        await self.lock.rollback()


class AsyncPGStorage(StorageInterface):
    def __init__(self):
        self.pool: Optional[Pool] = None
        self.subscribe_conn: Optional[Connection] = None
        self.connection_data = {
            "user": os.environ.get("POSTGRES_USER", "postgres"),
            "password": os.environ.get("POSTGRES_PASS", "task_manager_password"),
            "database": os.environ.get("POSTGRES_NAME", "task_manager"),
            "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            "port": os.environ.get("POSTGRES_PORT", "5432"),
        }
        asyncio.get_event_loop().run_until_complete(
            self.create_connection()
        )

        self.tasks = []  # todo postgres impl

        # todo postgres impl
        # self.conn.add_listener() ?
        self.on_task_callbacks = []

    async def create_connection(self):
        if not self.pool or self.pool.is_closing():
            self.pool = await asyncpg.create_pool(
                **self.connection_data,
                max_size=10
            )
        if not self.subscribe_conn or self.subscribe_conn.is_closed():
            self.subscribe_conn = self.pool.acquire()

    async def close_connection(self):
        if self.subscribe_conn:
            await self.pool.release(self.subscribe_conn)
            await self.subscribe_conn.close()

        await self.pool.close() if self.pool else None

    async def __create_transaction(self) -> Tuple[Transaction, Connection]:
        conn: Connection = await self.pool.acquire()
        transaction: Transaction = conn.transaction()
        await transaction.start()

        return transaction, conn

    async def create_task(self, queue, payload) -> str:
        """
        idn UUID PRIMARY KEY,
        topic VARCHAR (50) NOT NULL,
        payload JSON NOT NULL,
        status int NOT NULL,
        error VARCHAR (255) NULL,
        description VARCHAR (255) NULL
        """
        idn = str(uuid.uuid4())
        q = "INSERT INTO tasks (" \
            "idn, topic, payload, status" \
            ") VALUES (" \
            f"'{idn}', '{queue}', '{json.dumps(payload)}', {TaskStatus.NEW}" \
            ")"

        transaction, conn = await self.__create_transaction()
        await conn.fetch(q)
        await transaction.commit()
        await self.pool.release(conn)

        # FIXME what is this ?
        # self.tasks.append(task)

        # todo postgres impl
        for callback in self.on_task_callbacks:
            asyncio.create_task(callback(idn))

        return idn

    async def take_pending(self, idn) -> TransactionalResult[ConsumedTask] | None:
        """
        idn UUID PRIMARY KEY,
        topic VARCHAR (50) NOT NULL,
        payload JSON NOT NULL,
        status int NOT NULL,
        error VARCHAR (255) NULL,
        description VARCHAR (255) NULL
        """

        transaction, conn = await self.__create_transaction()
        q = "SELECT * FROM tasks " \
            f"WHERE (idn='{idn}' and status={TaskStatus.NEW}) " \
            "FOR UPDATE;"
        await conn.fetchrow(q)

        q = ("UPDATE tasks "
             f"SET status = {TaskStatus.IN_PROGRESS} "
             f"WHERE (idn='{idn}' and status={TaskStatus.NEW}) RETURNING * ;")
        rec: asyncpg.Record = await conn.fetchrow(q)

        if rec:
            task = AsyncPGTask.from_dict(dict(rec))
            return AsyncPGConsumedTaskResult(
                task,
                transaction
            )
        await transaction.rollback()
        await self.pool.release(conn)

        return None

    async def take_first_pending(self, topics: list[str]) -> TransactionalResult[ConsumedTask] | None:
        """
        idn UUID PRIMARY KEY,
        topic VARCHAR (50) NOT NULL,
        payload JSON NOT NULL,
        status int NOT NULL,
        error VARCHAR (255) NULL,
        description VARCHAR (255) NULL
        """

        transaction, conn = await self.__create_transaction()
        q = "SELECT * FROM tasks " \
            f"WHERE (" \
            f"status = {TaskStatus.NEW} and " \
            f"topic in ({str([f'{i}' for i in topics])[1:-1]})) " \
            "FOR UPDATE;"
        await conn.fetchrow(q)

        q = (
            "UPDATE tasks "
            f"SET status = {TaskStatus.IN_PROGRESS} "
            f"WHERE ("
            f"status = {TaskStatus.NEW} and "
            f"topic in ({str([f'{i}' for i in topics])[1:-1]})) "
            f"RETURNING * ;"
        )

        rec: asyncpg.Record = await conn.fetchrow(q)

        if rec:
            task = AsyncPGTask.from_dict(dict(rec))
            return AsyncPGConsumedTaskResult(
                task,
                transaction
            )

        await transaction.rollback()
        await self.pool.release(conn)

        return None

    async def finish_task(self, idn: str, error: None | str = None, message: str = ''):

        transaction, conn = await self.__create_transaction()
        q = "SELECT * FROM tasks " \
            f"WHERE idn = '{idn}'" \
            "FOR UPDATE;"
        await conn.fetchrow(q)

        q = (
            "UPDATE tasks "
            f"SET status = {TaskStatus.FINISHED}, "
            f"error = {error}, "
            f"description = {message} "
            f"WHERE idn = '{idn}'"
            f"RETURNING *;"
        )

        rec: asyncpg.Record = await conn.fetchrow(q)

        if not rec:
            await transaction.rollback()
            await self.pool.release(conn)
            # todo: add exceptions inheritance level to interface and raise appropriate one
            raise Exception(f"Can't finish task. Task '{idn}' not found in storage")

    def add_on_task_callback(self, callback: OnTaskCallback, channel: str = 'tasks'):
        # todo postgres impl
        self.on_task_callbacks.append(callback)

        # self.subscribe_conn.add_listener(
        #     channel=channel,
        #     callback=callback
        # )
