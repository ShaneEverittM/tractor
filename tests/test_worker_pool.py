import asyncio
from asyncio import Event, timeout
from tractor import Actor, Message, ActorRef
from tractor.message import Context
from tractor.actors.pool import WorkerPool, Submit, TrySubmit
from typing import override, final


async def worker() -> None:
    await asyncio.sleep(0.5)


async def quick() -> None:
    return


@final
class ActorOne(Actor):
    def __init__(self, done: Event):
        self.done = done

    def mark_done(self) -> None:
        self.done.set()


class WorkerDone(Message[ActorOne, None]):
    @override
    async def dispatch(self, actor: ActorOne, ctx: Context[ActorOne]) -> None:
        actor.mark_done()


def _notifier() -> ActorRef[ActorOne]:
    """An actor whose completion notifications we don't care to observe."""
    return ActorRef(ActorOne(Event()))


async def test_create_worker():
    done = Event()
    a1 = ActorRef(ActorOne(done))
    pool = ActorRef(WorkerPool())
    reservation = await pool.ask(Submit(worker, WorkerDone(), WorkerDone.sender(a1)))
    await reservation
    async with timeout(1):
        _ = await done.wait()


async def test_unlimited_pool_grants_immediately():
    a1 = _notifier()
    pool = ActorRef(WorkerPool())
    res = await pool.ask(Submit(quick, WorkerDone(), WorkerDone.sender(a1)))
    assert res.pending is False
    await res


async def test_try_submit_rejects_when_full():
    a1 = _notifier()
    pool = ActorRef(WorkerPool(limit=1))

    started = Event()
    release = Event()

    async def slow() -> None:
        started.set()
        _ = await release.wait()

    # The single slot is taken by the first task...
    accepted = await pool.ask(TrySubmit(slow, WorkerDone(), WorkerDone.sender(a1)))
    assert accepted is True
    async with timeout(1):
        _ = await started.wait()  # ensure it is running and holding the slot

    # ...so a second early-reject submission bounces immediately.
    rejected = await pool.ask(TrySubmit(quick, WorkerDone(), WorkerDone.sender(a1)))
    assert rejected is False

    # Free the slot; an early-reject submission is then accepted again.
    release.set()
    async with timeout(1):
        while not await pool.ask(
            TrySubmit(quick, WorkerDone(), WorkerDone.sender(a1))
        ):
            await asyncio.sleep(0.01)


async def test_submit_queues_until_a_slot_frees():
    a1 = _notifier()
    pool = ActorRef(WorkerPool(limit=1))

    release = Event()

    async def slow() -> None:
        _ = await release.wait()

    # First submission gets the only slot immediately.
    res1 = await pool.ask(Submit(slow, WorkerDone(), WorkerDone.sender(a1)))
    assert res1.pending is False
    await res1

    # Second submission is accepted but queued; its reservation stays pending.
    second_ran = Event()

    async def second() -> None:
        second_ran.set()

    res2 = await pool.ask(Submit(second, WorkerDone(), WorkerDone.sender(a1)))
    assert res2.pending is True
    await asyncio.sleep(0.05)
    assert res2.pending is True  # still queued while the first task holds the slot

    # Releasing the first frees the slot, which is handed to the queued waiter.
    release.set()
    async with timeout(1):
        await res2
    assert res2.pending is False
    async with timeout(1):
        _ = await second_ran.wait()


async def test_try_submit_does_not_jump_the_queue():
    a1 = _notifier()
    pool = ActorRef(WorkerPool(limit=1))

    release = Event()

    async def slow() -> None:
        _ = await release.wait()

    # Fill the slot, then queue a back-pressure waiter behind it.
    res1 = await pool.ask(Submit(slow, WorkerDone(), WorkerDone.sender(a1)))
    await res1
    res2 = await pool.ask(Submit(quick, WorkerDone(), WorkerDone.sender(a1)))
    assert res2.pending is True

    # A try-submission must be rejected: a waiter is already ahead of it.
    accepted = await pool.ask(TrySubmit(quick, WorkerDone(), WorkerDone.sender(a1)))
    assert accepted is False

    # Drain.
    release.set()
    async with timeout(1):
        await res2
