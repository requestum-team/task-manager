import asyncio

from task_manager.core import TaskManager
from task_manager.core import SessionClosedException
from task_manager.storage import InMemoryStorage

storage = InMemoryStorage()
engine = TaskManager(storage)


def make_consumer(name, max_messages):
    async def consumer():
        session = engine.new_session()

        for i in range(max_messages):
            task = await session.consume_task(['test'])
            print(f"consumer '{name}' got task '{task.idn}' with payload {task.payload} in '{task.topic}'")
            await asyncio.sleep(3)
            print(f"consumer '{name}' finished task '{task.idn}' with payload {task.payload} in '{task.topic}'")

    return consumer()


async def main():
    asyncio.create_task(make_consumer("1 message", 1))
    asyncio.create_task(make_consumer("permanent", 100))

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
    asyncio.create_task(make_consumer('new', 100))

    await asyncio.sleep(3.5)

    # test session closing
    session = engine.new_session()
    async def close_session():
        await asyncio.sleep(1)
        print('clossing session')
        session.close()

    asyncio.create_task(close_session())
    try:
        print('waiting for new task')
        task = await session.consume_task(['test'])
    except SessionClosedException as e:
        print('successfully caught exception')

    await asyncio.sleep(2)

asyncio.run(main())
