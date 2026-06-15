import asyncio
from asyncio import Future, iscoroutine
from collections.abc import Awaitable


def normalize[T](awaitable: Awaitable[T]) -> Future[T]:
    if isinstance(awaitable, Future):
        return awaitable

    if iscoroutine(awaitable):
        return asyncio.create_task(awaitable)
    else:

        async def wrapper():
            return await awaitable

        return asyncio.create_task(wrapper())


__all__ = ["normalize"]
