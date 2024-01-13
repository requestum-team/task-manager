import asyncio


class TaskWaiter:
    def __init__(self, topics: list[str]):
        self.topics = topics
        self.result = asyncio.Future()

    async def wait(self):
        return await asyncio.wait_for(self.result, None)
