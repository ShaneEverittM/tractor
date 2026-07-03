import asyncio
from asyncio import Future

import pytest

from tractor import Sel0, select
from tractor import oneshot


async def test_basic_send_and_receive():
    tx, rx = oneshot.channel(str)
    tx.send("hello")
    assert await rx == "hello"


async def test_send_error_raises_on_receive():
    tx, rx = oneshot.channel(str)
    tx.send(ValueError("boom"))
    with pytest.raises(ValueError, match="boom"):
        await rx


async def test_receiver_works_with_select():
    tx, rx = oneshot.channel(int)
    tx.send(42)

    never: Future[int] = asyncio.get_running_loop().create_future()

    match await select(rx, never):
        case Sel0(value):
            assert value == 42
        case _:
            raise AssertionError("expected oneshot to win")

    assert never.cancelled()


async def test_sender_from_another_task():
    tx, rx = oneshot.channel(str)

    async def producer(sender: oneshot.Sender[str]) -> None:
        await asyncio.sleep(0)
        sender.send("from task")

    asyncio.create_task(producer(tx))
    assert await rx == "from task"
