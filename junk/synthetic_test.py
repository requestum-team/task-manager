import asyncio
import os
import sys
from pathlib import Path
import argparse

# resolving import problem
parent_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(str(Path(parent_dir).resolve().parents[0]))

from task_manager.core import TaskManager
from task_manager.core import SessionClosedException
from task_manager.core.storage import StorageInterface


class StorageType:
    IN_MEMORY_STORAGE = 'in_memory_storage'
    ASYNCPG_STORAGE = 'asyncpg_storage'

    @staticmethod
    def choices():
        return [
            StorageType.IN_MEMORY_STORAGE,
            StorageType.ASYNCPG_STORAGE
        ]


parser = argparse.ArgumentParser()

parser.add_argument(
    "storage_type",
    default=StorageType.IN_MEMORY_STORAGE,
    choices=StorageType.choices(),
    help="Storage type for synthetic test",
)

args = parser.parse_args()

if args.storage_type == StorageType.IN_MEMORY_STORAGE:
    from task_manager.storage import InMemoryStorage as storage
elif args.storage_type == StorageType.ASYNCPG_STORAGE:
    from task_manager.storage import AsyncPGStorage as storage
else:
    exit('Invalid storage type')

storage: StorageInterface = storage()
engine = TaskManager(storage)

loop = asyncio.get_event_loop()


def make_consumer(name, max_messages):
    async def consumer():
        session = engine.new_session()

        for i in range(max_messages):
            task = await session.consume_task(['test'])
            print(f"consumer '{name}' got task '{task.idn}' with payload {task.payload} in '{task.topic}'")
            await asyncio.sleep(3)
            await engine.finish_task(task.idn, None)
            print(f"consumer '{name}' finished task '{task.idn}' with payload {task.payload} in '{task.topic}'")

    return consumer()


async def main():
    loop.create_task(make_consumer("permanent", 100))
    loop.create_task(make_consumer("1 message", 1))

    await asyncio.sleep(0.1)

    publisher = engine.new_session()

    # publish when consumers are running
    for i in range(3):
        print(f'publish {i}')
        await publisher.publish_task('test', {"id": i})

    print(f'publish nonce')
    await publisher.publish_task('another', {"nonce": "nonce"})

    # publish when permanent consumer still in progress
    await asyncio.sleep(3)
    print(f'publish 3')
    await publisher.publish_task('test', {"id": 3})

    # publish when permanent consumer pending
    await asyncio.sleep(7)

    print(f'publish nonce')
    await publisher.publish_task('another', {"nonce": "nonce"})

    print(f'publish 4')
    await publisher.publish_task('test', {"id": 4})

    # publish when permanent consumer in progress and run new consumer after 1 sec passed
    print(f'publish 5')
    await publisher.publish_task('test', {"id": 5})

    await asyncio.sleep(1)
    loop.create_task(make_consumer('new', 100))

    await asyncio.sleep(3.5)

    # test session closing
    session = engine.new_session()

    async def close_session():
        await asyncio.sleep(1)
        print('clossing session')
        session.close()

    loop.create_task(close_session())
    try:
        print('waiting for new task')
        task = await session.consume_task(['test'])
    except SessionClosedException as e:
        print('successfully caught exception')

    await asyncio.sleep(2)


loop.run_until_complete(main())
