"""A typed, ``match``-friendly ``select`` over several awaitables.

Inspired by ``tokio::select!``: await a handful of awaitables at once and branch
on whichever finishes first. Each argument position is reported as its own
``Sel`` wrapper, so the result destructures with ``match`` and every arm keeps
that position's own type::

    match await select(inbox.get(), slot_freed):
        case Sel0(message):
            ...   # message has inbox.get()'s element type
        case Sel1(_):
            ...   # the slot-freed branch won

``select`` is *biased to the earliest argument*: if several complete on the same
turn, the lowest-indexed one wins. The awaitables that did not win are cancelled,
so pass sources you can cheaply recreate next call — a fresh ``queue.get()``, a
re-polled stream — exactly as you would reconstruct branches on each iteration of
a ``tokio::select!`` loop. (Put your highest-priority, must-not-drop source, such
as an actor's inbox, in position 0 so a tie never discards it.)

The winning awaitable's result is returned; if it raised, ``select`` re-raises.
"""

import asyncio
from asyncio import FIRST_COMPLETED, ensure_future
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import final, overload


class _Sel:
    """
    Common base for the position wrappers.

    It exists only to give ``select``'s implementation a single concrete return
    type; callers always receive the precise ``Sel0``..``Sel5`` subtypes via the
    overloads.
    """


@final
@dataclass(frozen=True)
class Sel0[T](_Sel):
    """The awaitable in position 0 completed first."""

    value: T


@final
@dataclass(frozen=True)
class Sel1[T](_Sel):
    """The awaitable in position 1 completed first."""

    value: T


@final
@dataclass(frozen=True)
class Sel2[T](_Sel):
    """The awaitable in position 2 completed first."""

    value: T


@final
@dataclass(frozen=True)
class Sel3[T](_Sel):
    """The awaitable in position 3 completed first."""

    value: T


@final
@dataclass(frozen=True)
class Sel4[T](_Sel):
    """The awaitable in position 4 completed first."""

    value: T


@final
@dataclass(frozen=True)
class Sel5[T](_Sel):
    """The awaitable in position 5 completed first."""

    value: T


_WRAPPERS: tuple[Callable[[object], _Sel], ...] = (
    Sel0,
    Sel1,
    Sel2,
    Sel3,
    Sel4,
    Sel5,
)


@overload
async def select[T0](a0: Coroutine[object, object, T0], /) -> Sel0[T0]: ...
@overload
async def select[T0, T1](
    a0: Coroutine[object, object, T0], a1: Coroutine[object, object, T1], /
) -> Sel0[T0] | Sel1[T1]: ...
@overload
async def select[T0, T1, T2](
    a0: Coroutine[object, object, T0],
    a1: Coroutine[object, object, T1],
    a2: Coroutine[object, object, T2],
    /,
) -> Sel0[T0] | Sel1[T1] | Sel2[T2]: ...
@overload
async def select[T0, T1, T2, T3](
    a0: Coroutine[object, object, T0],
    a1: Coroutine[object, object, T1],
    a2: Coroutine[object, object, T2],
    a3: Coroutine[object, object, T3],
    /,
) -> Sel0[T0] | Sel1[T1] | Sel2[T2] | Sel3[T3]: ...
@overload
async def select[T0, T1, T2, T3, T4](
    a0: Coroutine[object, object, T0],
    a1: Coroutine[object, object, T1],
    a2: Coroutine[object, object, T2],
    a3: Coroutine[object, object, T3],
    a4: Coroutine[object, object, T4],
    /,
) -> Sel0[T0] | Sel1[T1] | Sel2[T2] | Sel3[T3] | Sel4[T4]: ...
@overload
async def select[T0, T1, T2, T3, T4, T5](
    a0: Coroutine[object, object, T0],
    a1: Coroutine[object, object, T1],
    a2: Coroutine[object, object, T2],
    a3: Coroutine[object, object, T3],
    a4: Coroutine[object, object, T4],
    a5: Coroutine[object, object, T5],
    /,
) -> Sel0[T0] | Sel1[T1] | Sel2[T2] | Sel3[T3] | Sel4[T4] | Sel5[T5]: ...


async def select(*awaitables: Coroutine[object, object, object]) -> _Sel:
    """Await ``awaitables`` and return the first to complete (biased to the earliest)."""
    tasks = [ensure_future(a) for a in awaitables]
    try:
        done, pending = await asyncio.wait(tasks, return_when=FIRST_COMPLETED)
    except BaseException:
        for task in tasks:
            task.cancel()
        raise
    for task in pending:
        _ = task.cancel()
    for index, task in enumerate(tasks):
        if task in done:
            return _WRAPPERS[index](task.result())
    raise AssertionError("select(): asyncio.wait returned with nothing done")


@overload
async def first[T0](a0: Coroutine[object, object, T0], /) -> T0: ...
@overload
async def first[T0, T1](
    a0: Coroutine[object, object, T0], a1: Coroutine[object, object, T1], /
) -> T0 | T1: ...
@overload
async def first[T0, T1, T2](
    a0: Coroutine[object, object, T0],
    a1: Coroutine[object, object, T1],
    a2: Coroutine[object, object, T2],
    /,
) -> T0 | T1 | T2: ...
@overload
async def first[T0, T1, T2, T3](
    a0: Coroutine[object, object, T0],
    a1: Coroutine[object, object, T1],
    a2: Coroutine[object, object, T2],
    a3: Coroutine[object, object, T3],
    /,
) -> T0 | T1 | T2 | T3: ...
@overload
async def first[T0, T1, T2, T3, T4](
    a0: Coroutine[object, object, T0],
    a1: Coroutine[object, object, T1],
    a2: Coroutine[object, object, T2],
    a3: Coroutine[object, object, T3],
    a4: Coroutine[object, object, T4],
    /,
) -> T0 | T1 | T2 | T3 | T4: ...
@overload
async def first[T0, T1, T2, T3, T4, T5](
    a0: Coroutine[object, object, T0],
    a1: Coroutine[object, object, T1],
    a2: Coroutine[object, object, T2],
    a3: Coroutine[object, object, T3],
    a4: Coroutine[object, object, T4],
    a5: Coroutine[object, object, T5],
    /,
) -> T0 | T1 | T2 | T3 | T4 | T5: ...


async def first(*awaitables: Coroutine[object, object, object]) -> object:
    """Await ``awaitables`` and return the first result; use when types are distinct."""
    tasks = [ensure_future(a) for a in awaitables]
    try:
        done, pending = await asyncio.wait(tasks, return_when=FIRST_COMPLETED)
    except BaseException:
        for task in tasks:
            task.cancel()
        raise
    for task in pending:
        _ = task.cancel()
    for task in tasks:
        if task in done:
            return task.result()
    raise AssertionError("first(): asyncio.wait returned with nothing done")


__all__ = ["select", "first", "Sel0", "Sel1", "Sel2", "Sel3", "Sel4", "Sel5"]
