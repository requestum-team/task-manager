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
        self.subscribe_conn: Optional[Connection] = None
        self.connection_data = {
            "user": os.environ.get("POSTGRES_USER", "postgres"),
            "password": os.environ.get("POSTGRES_PASS", "task_manager_password"),
            "database": os.environ.get("POSTGRES_NAME", "task_manager"),
            "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            "port": os.environ.get("POSTGRES_PORT", "5432"),
        }
        self.lock = asyncio.Lock()
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
            self.subscribe_conn = await asyncpg.connect(
                **self.connection_data
            )

    async def close_connection(self):
        await self.pool.close() if self.pool else None
        await self.subscribe_conn.close() if self.subscribe_conn else None

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
            f"'{idn}', '{queue}', '{json.dumps(payload)}', {NEW}" \
            ")"

        async with self.pool.acquire() as connection:
            connection: Connection
            async with connection.transaction():
                await connection.fetch(q)

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

        await self.lock.acquire()

        q = ("UPDATE tasks "
             f"SET status = {IN_PROGRESS} "
             f"WHERE (idn='{idn}' and status={NEW}) RETURNING * ;")

        async with self.pool.acquire() as connection:
            connection: Connection

            async with connection.transaction():
                rec: asyncpg.Record = await connection.fetchrow(q)

        if rec:
            task = Task.from_dict(dict(rec))
            return ConsumedTaskResult(
                task,
                self.lock
            )

        # TODO is this legal? (release only without rec flow)
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
            connection: Connection

            async with connection.transaction():
                rec: asyncpg.Record = await connection.fetchrow(q)

        if rec:
            task = Task.from_dict(dict(rec))
            return ConsumedTaskResult(
                task,
                self.lock
            )

        # TODO is this legal? (release only without rec flow)
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
            connection: Connection

            async with connection.transaction():
                rec: asyncpg.Record = await connection.fetchrow(q)

        if not rec:
            # todo: add exceptions inheritance level to interface and raise appropriate one
            raise Exception(f"Can't finish task. Task '{idn}' not found in storage")

    def add_on_task_callback(self, callback: OnTaskCallback, channel: str = 'tasks'):
        # todo postgres impl
        self.on_task_callbacks.append(callback)

        # self.subscribe_conn.add_listener(
        #     channel=channel,
        #     callback=callback
        # )
