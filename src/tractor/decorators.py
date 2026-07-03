"""
Decorators for message ergonomics and application lifecycle.

This module includes ``@handler`` and ``@main``.

>>> from tractor import Actor, Context, Message, Runtime, handler, main
...
>>> class PingPong(Actor):
...    # This allows ``PingPong.ping`` to create a generated message that
...    # accepts the same arguments as this method, and when sent to this
...    # actor, calls this method.
...    @handler
...    async def ping(self) -> None:
...        print("pong")
...
... # This causes ``app`` to be executed after the module is done being evaluated.
>>> @main
... async def app(rt: Runtime) -> None:
...     ping_pong = rt.spawn(PingPong())
...     await rt.tell(ping_pong, PingPong.ping())
pong
"""

import asyncio
import sys
from collections.abc import Awaitable, Callable
from functools import wraps
from types import CodeType, FrameType
from typing import Concatenate, final, overload, override

from tractor.actor import Actor
from tractor.control_flow import CrashPolicy
from tractor.message import Context, Message
from tractor.runtime import Runtime


@final
class HandlerMessage[A: Actor, **P, R](Message[A, R]):
    """
    A message synthesized from a handler method plus captured arguments.

    All Actors can receive this message automatically, and the binding
    of message arguments to method arguments is done via ``@handler``.
    """

    def __init__(
        self,
        fn: Callable[Concatenate[A, P], Awaitable[R]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    @override
    async def dispatch(self, actor: A, ctx: Context[A]) -> R:
        # This gets invoked as part of the regular message dispatch
        # loop on the actor, so we can just delegate to the actor's
        # method as it would in the "boring" case of method wiring.
        return await self._fn(actor, *self._args, **self._kwargs)


@final
class HandlerFactory[A: Actor, **P, R]:
    """
    The synthetic type returned via class attribute access to methods annotated with ``@handler``.
    """

    def __init__(self, fn: Callable[Concatenate[A, P], Awaitable[R]]) -> None:
        self.raw = fn

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> HandlerMessage[A, P, R]:
        return HandlerMessage(self.raw, *args, **kwargs)


# This is a decorator, so...
# noinspection PyPep8Naming
@final
class handler[A: Actor, **P, R]:
    """
    A decorator for methods that should be dispatchable via ``HandlerMessage``.

    Simply place it above a method, and you can then do:

    ```python
    await runtime.tell(ref, SomeActor.method()))
    ```
    """

    def __init__(self, fn: Callable[Concatenate[A, P], Awaitable[R]]) -> None:
        self._fn = fn
        self._factory = HandlerFactory(fn)

    @overload
    def __get__(
        self, obj: A, objtype: type[A] | None = None
    ) -> Callable[P, Awaitable[R]]:
        """When looked up as a method, return a proxy to the original callable."""
        ...

    @overload
    def __get__(self, obj: None, objtype: type[A]) -> HandlerFactory[A, P, R]:
        """When looked up as an attribute on the class, return the factory."""
        ...

    def __get__(
        self, obj: A | None, objtype: type[A] | None = None
    ) -> Callable[P, Awaitable[R]] | HandlerFactory[A, P, R]:
        # If we aren't being retrieved as a method...
        if obj is None:
            # ...just return the factory.
            return self._factory

        # Otherwise, bind the function to the object, and return the callable.
        def bound(*args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
            return self._fn(obj, *args, **kwargs)

        return bound


type _Entry = Callable[[Runtime], Awaitable[None]]

_TOOL_NAME = "tractor.main"

# True while a deferred entry point is waiting for its module to finish
# executing; guards against a second `@main` in the same script.
_deferred_main_pending = False


def _run_on_module_return(entry: Callable[[], None]) -> None:
    """
    Arrange for `entry` to run the moment the ``__main__`` module's
    top-level code finishes executing.

    This is what lets ``@main`` appear anywhere in the module rather than
    only as the last definition: `sys.monitoring` (PEP 669) instruments the
    module frame's code object for its ``PY_RETURN`` event — the point a
    bottom-of-file ``if __name__ == "__main__":`` guard would have run —
    and fires `entry` there. If the module frame can't be seen (embedded
    interpreters, exotic ``exec`` setups) or no monitoring tool id is free,
    `entry` runs immediately instead, as if the guard sat at the
    decoration site.
    """
    global _deferred_main_pending
    if _deferred_main_pending:
        raise RuntimeError("only one @main entry point may be active per script")

    frame: FrameType | None = sys._getframe(1)  # pyright: ignore[reportPrivateUsage]
    while frame is not None:
        if (
            frame.f_code.co_name == "<module>"
            and frame.f_globals.get("__name__") == "__main__"
        ):
            break
        frame = frame.f_back

    if frame is None:
        entry()
        return

    code = frame.f_code
    mon = sys.monitoring
    # Tool ids 0-2 and 5 are conventionally debugger/coverage/profiler/
    # optimizer; 3 and 4 are unassigned, so claim whichever is free.
    for tool_id in (3, 4):
        try:
            mon.use_tool_id(tool_id, _TOOL_NAME)
        except ValueError:
            continue
        break
    else:
        entry()
        return

    def on_module_return(returned: CodeType, _offset: int, _retval: object) -> None:
        global _deferred_main_pending
        if returned is not code:
            return
        mon.set_local_events(tool_id, code, 0)
        mon.free_tool_id(tool_id)
        _deferred_main_pending = False
        entry()

    _ = mon.register_callback(tool_id, mon.events.PY_RETURN, on_module_return)
    mon.set_local_events(tool_id, code, mon.events.PY_RETURN)
    _deferred_main_pending = True


@overload
def main(fn: _Entry, /) -> Callable[[], None]: ...


@overload
def main(*, crash_policy: CrashPolicy) -> Callable[[_Entry], Callable[[], None]]: ...


def main(
    fn: _Entry | None = None,
    /,
    *,
    crash_policy: CrashPolicy | None = None,
) -> Callable[[], None] | Callable[[_Entry], Callable[[], None]]:
    """
    Mark an async function as the application entry point.

    The decorated function is rewritten into a *synchronous* zero-argument
    entry point that constructs the `Runtime`, starts the event loop, and
    runs the original body — mirroring ``#[tokio::main]``. When the defining
    module is the script being executed (its ``__module__`` is
    ``"__main__"``), the entry point runs automatically once the module's
    top-level code completes, so no ``if __name__ == "__main__":`` guard is
    needed and ``@main`` may appear anywhere in the file:

    ```python
    @main
    async def app(runtime: Runtime) -> None:
        ref = runtime.spawn(MyActor())  # even if MyActor is defined below
        ...
    ```

    When the module is imported instead, nothing runs; the decorated name
    is the sync entry point, callable as ``app()`` by whoever owns the
    process (do not add your own guard on top — the app would run twice).
    As with a real guard, the app does not run if module execution raises
    first.

    Runtime configuration goes on the decorator, not the call site:

    ```python
    @main(crash_policy=MyPolicy())
    async def app(runtime: Runtime) -> None: ...
    ```

    :param fn: the async entry point (when used as a bare ``@main``)
    :param crash_policy: forwarded to the `Runtime` constructor
    """

    def decorate(f: _Entry) -> Callable[[], None]:
        @wraps(f)
        def entry() -> None:
            # `asyncio.run` insists on a Coroutine; wrapping keeps `_Entry`
            # accepting any Awaitable-returning function.
            async def run() -> None:
                await f(Runtime(crash_policy))

            asyncio.run(run())

        # Scripts see "__main__" here, imports see the real module name.
        if f.__module__ == "__main__":
            _run_on_module_return(entry)

        return entry

    # Bare `@main` hands us the function directly; `@main(...)` calls us
    # with config only and applies `decorate` to the function afterward.
    return decorate if fn is None else decorate(fn)


if __name__ == "__main__":
    import doctest

    _ = doctest.testmod()

__all__ = [
    "HandlerFactory",
    "HandlerMessage",
    "handler",
    "main",
]
