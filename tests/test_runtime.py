import asyncio
from dataclasses import dataclass
from typing import final, override

import pytest

from tractor import Actor, ActorRef, ActorStoppedError, ControlFlow, Message, Runtime
from tractor.message import Context


# ---------------------------------------------------------------------------
# Shared actors and messages
# ---------------------------------------------------------------------------


@final
class Echo(Actor):
    @staticmethod
    async def echo(value: int) -> int:
        return value


@final
@dataclass
class EchoMsg(Message[Echo, int]):
    value: int

    @override
    async def dispatch(self, actor: Echo, ctx: Context[Echo]) -> int:
        return await actor.echo(self.value)


@final
class Crasher(Actor):
    """Actor whose dispatch always raises."""

    _flow: ControlFlow = ControlFlow.Stop

    def __init__(self, flow: ControlFlow = ControlFlow.Stop):
        self._flow = flow
        self.panics: int = 0
        self.stopped: bool = False

    @override
    async def on_panic(self, exc: BaseException) -> ControlFlow:
        self.panics += 1
        return self._flow

    @override
    async def on_stop(self):
        self.stopped = True


@final
@dataclass
class Boom(Message[Crasher, None]):
    @override
    async def dispatch(self, actor: Crasher, ctx: Context[Crasher]) -> None:
        raise ValueError("boom")


@final
@dataclass
class Ping(Message[Crasher, str]):
    @override
    async def dispatch(self, actor: Crasher, ctx: Context[Crasher]) -> str:
        return "pong"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_runtime_spawn():
    runtime = Runtime()
    ref = runtime.spawn(Echo())
    result = await runtime.ask(ref, EchoMsg(42))
    assert result == 42
    await ref.stop()


async def test_runtime_ask_and_tell():
    runtime = Runtime()
    ref = runtime.spawn(Echo())
    result = await runtime.ask(ref, EchoMsg(7))
    assert result == 7
    await runtime.tell(ref, EchoMsg(0))  # just confirm it doesn't raise
    await ref.stop()


async def test_on_start_called():
    started = asyncio.Event()

    @final
    class StartWatcher(Actor):
        @override
        async def on_start(self):
            started.set()

    ref = ActorRef(StartWatcher())
    async with asyncio.timeout(1):
        _ = await started.wait()
    await ref.stop()


async def test_on_stop_called_after_stop():
    actor = Crasher()
    ref = ActorRef(actor)
    await ref.stop()
    assert actor.stopped


async def test_on_stop_called_after_crash():
    actor = Crasher(flow=ControlFlow.Stop)
    runtime = Runtime()
    ref = runtime.spawn(actor)
    runtime.try_tell(ref, Boom())
    # Wait for the driver task to finish after the panic
    async with asyncio.timeout(1):
        await ref._task  # pyright: ignore[reportPrivateUsage]
    assert actor.stopped


async def test_dispatch_exception_resolves_future():
    actor = Crasher(flow=ControlFlow.Stop)
    runtime = Runtime()
    ref = runtime.spawn(actor)
    with pytest.raises(ValueError, match="boom"):
        await runtime.ask(ref, Boom())
    await ref.stop()


async def test_stop_resolves_pending_ask_with_stopped_error():
    @final
    class Blocker(Actor):
        """Never processes messages — stays blocked in on_start."""

        _gate: asyncio.Event

        def __init__(self):
            self._gate = asyncio.Event()

        @override
        async def on_start(self):
            _ = await self._gate.wait()  # blocks forever

    runtime = Runtime()
    ref = runtime.spawn(Blocker())

    # Enqueue an ask before the actor ever starts processing messages
    pending = asyncio.ensure_future(
        runtime.ask(ref, EchoMsg(1))  # pyright: ignore[reportArgumentType]
    )

    # Give the event loop a turn so the enqueue attempt can proceed
    await asyncio.sleep(0)

    # Stop the actor — should drain the inbox and resolve the future with ActorStoppedError
    await ref.stop()

    with pytest.raises(ActorStoppedError):
        await pending


async def test_on_panic_continue_keeps_running():
    actor = Crasher(flow=ControlFlow.Continue)
    runtime = Runtime()
    ref = runtime.spawn(actor)

    # First: send a crash — actor should survive
    with pytest.raises(ValueError, match="boom"):
        await runtime.ask(ref, Boom())

    assert actor.panics == 1

    # Second: actor still responds
    result = await runtime.ask(ref, Ping())
    assert result == "pong"

    await ref.stop()


@final
class RecordPolicy:
    def __init__(self):
        self.calls: list[tuple[object, BaseException, ControlFlow]] = []

    def on_crash(self, actor: object, exc: BaseException, flow: ControlFlow) -> None:
        self.calls.append((actor, exc, flow))


async def test_crash_policy_called():
    policy = RecordPolicy()
    actor = Crasher(flow=ControlFlow.Stop)
    runtime = Runtime(crash_policy=policy)
    ref = runtime.spawn(actor)

    with pytest.raises(ValueError):
        await runtime.ask(ref, Boom())

    async with asyncio.timeout(1):
        await ref._task  # pyright: ignore[reportPrivateUsage]

    assert len(policy.calls) == 1
    _, exc, flow = policy.calls[0]
    assert isinstance(exc, ValueError)
    assert flow is ControlFlow.Stop


async def test_context_tell():
    """A handler can send a message to another actor via ctx.tell."""
    result: asyncio.Future[int] = asyncio.get_event_loop().create_future()

    @final
    class Receiver(Actor):
        async def receive(self, value: int) -> None:
            result.set_result(value)

    @final
    @dataclass
    class Receive(Message[Receiver, None]):
        value: int

        @override
        async def dispatch(self, actor: Receiver, ctx: Context[Receiver]) -> None:
            await actor.receive(self.value)

    @final
    class Sender(Actor):
        def __init__(self, target: ActorRef[Receiver]):
            self.target = target

    @final
    @dataclass
    class Forward(Message[Sender, None]):
        value: int

        @override
        async def dispatch(self, actor: Sender, ctx: Context[Sender]) -> None:
            await ctx.tell(actor.target, Receive(self.value))

    runtime = Runtime()
    receiver_ref = runtime.spawn(Receiver())
    sender_ref = runtime.spawn(Sender(receiver_ref))

    await runtime.tell(sender_ref, Forward(99))

    async with asyncio.timeout(1):
        assert await result == 99

    await sender_ref.stop()
    await receiver_ref.stop()


async def test_context_ask():
    """A handler can query another actor via ctx.ask."""

    @final
    class Adder(Actor):
        async def add(self, a: int, b: int) -> int:
            return a + b

    @final
    @dataclass
    class Add(Message[Adder, int]):
        a: int
        b: int

        @override
        async def dispatch(self, actor: Adder, ctx: Context[Adder]) -> int:
            return await actor.add(self.a, self.b)

    @final
    class Delegator(Actor):
        def __init__(self, adder: ActorRef[Adder]):
            self.adder = adder

    @final
    @dataclass
    class DelegateAdd(Message[Delegator, int]):
        a: int
        b: int

        @override
        async def dispatch(self, actor: Delegator, ctx: Context[Delegator]) -> int:
            return await ctx.ask(actor.adder, Add(self.a, self.b))

    runtime = Runtime()
    adder_ref = runtime.spawn(Adder())
    delegator_ref = runtime.spawn(Delegator(adder_ref))

    result = await runtime.ask(delegator_ref, DelegateAdd(3, 4))
    assert result == 7

    await delegator_ref.stop()
    await adder_ref.stop()


async def test_default_runtime_backward_compat():
    """ActorRef(actor) without a runtime argument still works."""
    from tractor.ref import _get_default_runtime  # pyright: ignore[reportPrivateUsage]

    ref = ActorRef(Echo())
    runtime = _get_default_runtime()
    result = await runtime.ask(ref, EchoMsg(5))
    assert result == 5
    await ref.stop()
