import asyncio
import json
from uuid import UUID


class TaskWaiter:
    def __init__(self, topics: list[str]):
        self.topics = topics
        self.result = asyncio.Future()

    async def wait(self):
        return await asyncio.wait_for(self.result, None)


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return str(obj)
        return json.JSONEncoder.default(self, obj)
