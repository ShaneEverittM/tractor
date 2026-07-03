"""A simple, modern, type-safe actor framework for asynchronous, fault-tolerant systems.

Everything a typical application touches is re-exported here. Two pieces are
deliberately left namespaced rather than flattened into this module:

- `tractor.oneshot` — its `Sender`/`Receiver` are channel ends, which would
  collide with the message-sending `Sender`/`TellSender`; use it as
  `oneshot.channel(T)` (mirroring `tokio::sync::oneshot`).
- `tractor.actors` — the toolbox of ready-made actors (`WorkerPool`, ...),
  imported explicitly by the applications that want them.
"""

from tractor.actor import Actor
from tractor.combinators import Sel0, Sel1, Sel2, Sel3, Sel4, Sel5, first, select
from tractor.control_flow import ControlFlow, CrashPolicy, LogCrashPolicy
from tractor.decorators import (
    HandlerFactory,
    HandlerMessage,
    handler,
    main,
)
from tractor.errors import ActorStoppedError
from tractor.handles import InboxHandle, ResponderHandle
from tractor.message import Context, Message, Responder, Sender, TellSender
from tractor.protocols import MessagePort
from tractor.ref import ActorRef
from tractor.runtime import Runtime

__all__ = [
    "Actor",
    "ActorRef",
    "ActorStoppedError",
    "Context",
    "ControlFlow",
    "CrashPolicy",
    "HandlerFactory",
    "HandlerMessage",
    "InboxHandle",
    "LogCrashPolicy",
    "Message",
    "MessagePort",
    "Responder",
    "ResponderHandle",
    "Runtime",
    "Sel0",
    "Sel1",
    "Sel2",
    "Sel3",
    "Sel4",
    "Sel5",
    "Sender",
    "TellSender",
    "first",
    "handler",
    "main",
    "select",
]
