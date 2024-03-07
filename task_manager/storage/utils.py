from typing import Iterable


async def generator(iterable: Iterable):
    for i in iterable:
        yield i
