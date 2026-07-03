# tractor

> [!WARNING]
> **Pre-release.** This is an early prototype: APIs are unstable and will change
> without notice, and there are no published releases yet.

A simple, modern, type-safe actor framework for asynchronous, fault-tolerant systems.

tractor gives you isolated, single-threaded **actors** that communicate only by
message passing on top of `asyncio`. Messages are ordinary typed objects, so
your editor and type checker understand exactly which actor a message targets
and what it replies with — no `Any`, no string dispatch.

## Features

- **Type-safe messaging** — `Message[A, R]` ties a message to the actor type `A`
  it's sent to and the reply type `R` it produces. `ask` returns `R`; `tell`
  returns nothing. Mismatches are caught statically.
- **`ask` / `tell` semantics** — request/reply or fire-and-forget, plus
  non-blocking `try_ask` / `try_tell` and reply **forwarding** for delegation.
- **Lifecycle hooks** — `on_start`, `on_stop`, and `on_panic` with a per-actor
  `ControlFlow` decision (stop or keep running).
- **Fault tolerance** — panics are isolated to the actor and reported to a
  pluggable `CrashPolicy`.
- **Custom work sources** — override `step` to await timers, futures, or streams
  alongside the inbox using the biased `select`.
- **Fully typed** — ships `py.typed`; built around PEP 695 generics.

## Requirements

- **Python 3.14+** (the API uses recent typing features).

## Installation

Not yet published to PyPI (note that the `tractor` name on PyPI currently
belongs to an unrelated project — do not `pip install tractor` expecting this
library). For now, install from source:

```bash
uv add git+https://github.com/ShaneEverittM/tractor
# or
pip install git+https://github.com/ShaneEverittM/tractor
```

## Quick start

```python
import asyncio
from dataclasses import dataclass
from typing import override

from tractor import Actor, Message, Runtime
from tractor.message import Context


class Counter(Actor):
    def __init__(self) -> None:
        self.count = 0


@dataclass
class Increment(Message[Counter, None]):
    by: int = 1

    @override
    async def dispatch(self, actor: Counter, ctx: Context[Counter]) -> None:
        actor.count += self.by


@dataclass
class Get(Message[Counter, int]):
    @override
    async def dispatch(self, actor: Counter, ctx: Context[Counter]) -> int:
        return actor.count


async def main() -> None:
    runtime = Runtime()
    counter = runtime.spawn(Counter())

    await runtime.tell(counter, Increment(by=3))  # fire-and-forget
    await runtime.tell(counter, Increment())

    total = await runtime.ask(counter, Get())      # request/reply -> int
    print(total)  # 4

    await counter.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

## Concepts

- **`Actor`** — your state lives here. Subclass it and add plain methods;
  lifecycle hooks (`on_start`/`on_stop`/`on_panic`) and `step` are all optional
  with sensible defaults.
- **`Message[A, R]`** — a typed, dispatchable message. Implement `dispatch` to
  invoke the actor and produce the reply `R`.
- **`Runtime`** — created once at startup; `spawn`s actors and routes
  `ask`/`tell`. Inside a handler, use `ctx.ask` / `ctx.tell` so sender identity
  is carried through.
- **`ActorRef[A]`** — an opaque handle to a spawned actor; the only way to
  address it.
- **`CrashPolicy`** — observer invoked after every panic (defaults to logging).

## Examples

See [`examples/`](examples/) for a runnable pub/sub demo covering fanout,
lifecycle hooks, and a custom `step` heartbeat:

```bash
python examples/pub_sub.py
```

## Development

This repo uses [uv](https://docs.astral.sh/uv/) and provides a Nix flake.

```bash
# with uv
uv sync
uv run pytest

# with Nix (provides Python 3.14 + uv, tractor installed editable)
nix develop
pytest
```

Build the PyPI artifacts (sdist + wheel) reproducibly with Nix:

```bash
nix build        # -> ./result/{tractor-*.tar.gz, tractor-*.whl}
```

## License

[MIT](LICENSE) © Shane Murphy
