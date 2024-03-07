import asyncio
import json
import os
from pathlib import Path
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple, List

import asyncpg
from asyncpg import Connection, Pool
from asyncpg.transaction import Transaction

from task_manager.core.tasks import ConsumedTask, TaskStatus
from task_manager.core.storage import (
    StorageInterface, OnTaskCallback, TransactionalResult
)
from task_manager.core.utils import UUIDEncoder
from task_manager.storage.in_memory import Task
from task_manager.storage.utils import generator


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

    async def get_data(self) -> AsyncPGTask:
        # TODO another returning dataclass impl ?
        # TODO I think current AsyncPGTask impl not bed
        return self.task

    async def commit(self):
        await self.lock.commit()

    async def rollback(self):
        await self.lock.rollback()


class AsyncPGStorage(StorageInterface):
    def __init__(self, subscription_channel: str = 'tasks'):
        self.pool: Optional[Pool] = None
        self.subscription_channel = subscription_channel
        self.subscribe_conn: Optional[Connection] = None
        self.connection_data = {
            "user": os.environ.get("POSTGRES_USER", "postgres"),
            "password": os.environ.get("POSTGRES_PASS", "task_manager_password"),
            "database": os.environ.get("POSTGRES_NAME", "task_manager"),
            "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            "port": os.environ.get("POSTGRES_PORT", "5432"),
        }
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            self.create_connection()
        )
        loop.run_until_complete(
            self.__create_schema()
        )
        loop.run_until_complete(
            self.create_subscriber()
        )

        self.tasks = []  # FIXME what is this ?
        self.on_task_callbacks: List[OnTaskCallback] = []

    async def __create_schema(self):
        project_dir = str(Path(
            os.path.dirname(os.path.realpath(__file__))
        ).resolve().parents[1])

        sql_migrations_fir_path = os.path.join(
            project_dir, 'sqlmigrations'
        )
        files = []

        # async loop is noway
        for migration_file in os.listdir(sql_migrations_fir_path):
            file = open(
                os.path.join(
                    sql_migrations_fir_path,
                    migration_file
                )
            )
            files.append(file.read())
            file.close()

        async with self.pool.acquire() as connection:
            connection: Connection

            await connection.execute(
                "\r".join(files)
            )

    async def __subscription_callback(
            self,
            connection: Connection,
            pid: int,
            channel: str,
            payload: str
    ):
        """
        :param callable callback:
            A callable or a coroutine function receiving the following
            arguments:
            **connection**: a Connection the callback is registered with;
            **pid**: PID of the Postgres server that sent the notification;
            **channel**: name of the channel the notification was sent to;
            **payload**: task idn (str[uuid]).
        :return:
        """

        async for callback in generator(self.on_task_callbacks):
            print(f"callback from postgres: {payload}")
            await callback(payload)

    async def create_subscriber(self):

        await self.subscribe_conn.add_listener(
            channel=self.subscription_channel,
            callback=self.__subscription_callback
        )

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
        if self.subscribe_conn:
            await self.pool.release(self.subscribe_conn)
            await self.subscribe_conn.close()

        await self.pool.close() if self.pool else None

    async def __create_transaction(self) -> Tuple[Transaction, Connection]:
        """
            for transfer transaction control flow
            to the Session -> AsyncPGConsumedTaskResult -> (commit/rollback)
        """
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

        async with self.pool.acquire() as connection:
            connection: Connection
            await connection.execute(q)

            if self.on_task_callbacks:
                notify_q = f"NOTIFY {self.subscription_channel}, '{idn}';"
                await connection.execute(notify_q)

        # FIXME what is this ?
        # self.tasks.append(task)

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

        q = (
            "WITH selected_for_update as ("
            "SELECT * FROM tasks "
            f"WHERE (idn='{idn}' and status={TaskStatus.NEW}) "
            "FOR UPDATE"
            ") "
            "UPDATE tasks "
            f"SET status = {TaskStatus.IN_PROGRESS} "
            f"FROM selected_for_update "
            f"WHERE (tasks.idn=selected_for_update.idn) "
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

        q = (
            "WITH selected_for_update as ("
            "SELECT * FROM tasks "
            "WHERE ("
            f"status = {TaskStatus.NEW} and "
            f"topic in ({str([f'{i}' for i in topics])[1:-1]})) "
            "FOR UPDATE"
            ") "
            "UPDATE tasks "
            f"SET status = {TaskStatus.IN_PROGRESS} "
            f"FROM selected_for_update "
            f"WHERE tasks.idn = selected_for_update.idn "
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

        bool_map = {
            True: 'true',
            False: 'false',
            None: 'null'
        }

        q = (
            "WITH selected_for_update as ("
            "SELECT * FROM tasks "
            f"WHERE idn = '{idn}' "
            f"FOR UPDATE "
            ") "
            "UPDATE tasks "
            f"SET status = {TaskStatus.FINISHED}, "
            f"error = {bool_map[error]}, "
            f"description = {message if message else bool_map[None]} "
            f"FROM selected_for_update "
            f"WHERE tasks.idn = selected_for_update.idn "
            f"RETURNING *;"
        )

        async with self.pool.acquire() as connection:
            connection: Connection

            async with connection.transaction() as transaction:
                transaction: Transaction

                rec: asyncpg.Record = await connection.fetchrow(q)

                if not rec:
                    await transaction.rollback()
                    await self.pool.release(connection)
                    # todo: add exceptions inheritance level to interface and raise appropriate one
                    raise Exception(f"Can't finish task. Task '{idn}' not found in storage")

    def add_on_task_callback(self, callback: OnTaskCallback):
        self.on_task_callbacks.append(callback)
