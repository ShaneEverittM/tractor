from typing import override

from tractor import Actor, ActorRef, Context, Message, Runtime


class ActorOne(Actor):
    pass


class ActorTwo(Actor):
    pass


class MessageForActorOne(Message[ActorOne, int]):
    @override
    async def dispatch(self, actor: ActorOne, ctx: Context[ActorOne]) -> int:
        return 42


class MessageForActorTwo(Message[ActorTwo, float]):
    @override
    async def dispatch(self, actor: ActorTwo, ctx: Context[ActorTwo]) -> float:
        return 3.14


# Because we throw an error if a type ignore is unnecessary,
# if we type ignore the expected errors, we will get an error
# if the _don't_ fire. This test doesn't actually need to do
# anything at runtime.
async def test_type_safety() -> None:
    runtime = Runtime()
    actor1 = runtime.spawn(ActorOne())
    actor2 = runtime.spawn(ActorTwo())

    # Using ask should propagate the message's return type.
    r1: int = await runtime.ask(actor1, MessageForActorOne())
    r2: float = await runtime.ask(actor2, MessageForActorTwo())

    # And obviously it should work.
    assert r1 == 42
    assert r2 == 3.14

    # You should not be able to send a message to the wrong actor.
    await runtime.tell(actor1, MessageForActorTwo())  # pyright: ignore[reportArgumentType]
    await runtime.tell(actor2, MessageForActorOne())  # pyright: ignore[reportArgumentType]

    # You should not be able to misuse Senders.
    sender1 = MessageForActorOne.sender(runtime, actor1)
    misuse = sender1.send(MessageForActorTwo())  # pyright: ignore[reportArgumentType]
    misuse.close()  # only constructed to assert the type error above; don't leak it

    await actor1.stop()
    await actor2.stop()


class ChildOfOne(ActorOne):
    pass


class MessageForChild(Message[ChildOfOne, str]):
    @override
    async def dispatch(self, actor: ChildOfOne, ctx: Context[ChildOfOne]) -> str:
        return "child"


# The variance of the public generics is part of the shipped API: it is
# *inferred* by the type checker from the class bodies, so an innocent new
# member could silently flip it and break downstream code without any error
# in this repo. These assignments pin the contract; the ignored lines pin
# the unsound directions (same unnecessary-ignore trick as above).
async def test_variance_contract() -> None:
    runtime = Runtime()
    child = runtime.spawn(ChildOfOne())

    # ActorRef is covariant: a ref to a subtype is a ref to the supertype...
    base_ref: ActorRef[ActorOne] = child
    # ...and messages targeting the supertype reach the actual subtype actor.
    r: int = await runtime.ask(base_ref, MessageForActorOne())
    assert r == 42

    # Message is contravariant: a supertype-targeted message serves a subtype.
    _narrowed_msg: Message[ChildOfOne, int] = MessageForActorOne()

    # But never the unsound directions.
    bad_ref: ActorRef[ChildOfOne] = runtime.spawn(ActorOne())  # pyright: ignore[reportAssignmentType]
    _bad_msg: Message[ActorOne, str] = MessageForChild()  # pyright: ignore[reportAssignmentType]

    await bad_ref.stop()
    await child.stop()


# Context is covariant.
async def test_context_covariance_contract() -> None:
    runtime = Runtime()
    child = runtime.spawn(ChildOfOne())

    _base_ctx: Context[ActorOne] = Context(child)

    await child.stop()
