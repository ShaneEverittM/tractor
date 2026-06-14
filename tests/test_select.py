import asyncio
from asyncio import Future

from tractor.select import first, select, Sel0, Sel1


async def test_select_returns_the_completed_branch():
    f0: Future[str] = asyncio.get_running_loop().create_future()
    f1: Future[str] = asyncio.get_running_loop().create_future()
    f1.set_result("one")  # only the second is ready

    match await select(f0, f1):
        case Sel1(value):
            assert value == "one"
        case _:
            raise AssertionError("expected the position-1 branch to win")

    # The losing branch was cancelled, not left dangling.
    assert f0.cancelled()


async def test_select_is_biased_to_the_earliest():
    f0: Future[str] = asyncio.get_running_loop().create_future()
    f1: Future[str] = asyncio.get_running_loop().create_future()
    f0.set_result("zero")
    f1.set_result("one")  # both ready on the same turn

    match await select(f0, f1):
        case Sel0(value):
            assert value == "zero"  # lowest index wins the tie
        case _:
            raise AssertionError("expected the position-0 branch to win")


async def test_select_preserves_each_position_type():
    async def num() -> int:
        return 1

    async def text() -> str:
        await asyncio.sleep(0.05)
        return "x"

    # num() completes first; the Sel1 arm is unreached but still type-checked.
    match await select(num(), text()):
        case Sel0(n):
            assert n + 1 == 2  # n is typed int
        case Sel1(s):
            assert s.upper() == "X"  # s is typed str


async def test_select_keeps_future_done_across_calls():
    f0: Future[str] = asyncio.get_running_loop().create_future()
    f1: Future[int] = asyncio.get_running_loop().create_future()
    f0.set_result("zero")
    f1.set_result(1)

    match await first(f0, f1):
        case str(value):
            assert value == "zero"
        case _:
            raise AssertionError("expected the position-0 branch to win")

    # f0 is a new pending future, but f1 is still done from before.
    f0 = asyncio.get_running_loop().create_future()

    match await first(f0, f1):
        case int(value):
            assert value == 1
        case _:
            raise AssertionError("expected the position-1 branch to win")
