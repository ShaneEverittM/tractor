# tractor

A type-safe actor framework for asynchronous, fault-tolerant systems, built on
`asyncio`. Actors are isolated and single-threaded; they communicate only by
passing typed messages. See [README.md](README.md) for the user-facing pitch and
quick start.

## Commands

This repo uses a nix flake dev env (`.envrc` = `use flake`). Run tooling through
`uv run` — `uv` is on `PATH` and the flake points it at the Nix-built virtualenv
(`UV_PROJECT_ENVIRONMENT` / `VIRTUAL_ENV`), so this works regardless of whether
the venv's `bin/` made it onto `PATH`:

```bash
uv run pytest                                  # run the test suite (asyncio_mode = auto)
uv run pytest tests/test_runtime.py::test_name # single test
uv run ruff check                              # lint
uv run ruff format                             # format
uv run pyrefly check                           # type check (the authoritative checker — see below)
```

Always run `pyrefly check` before considering a change done — type correctness
is part of this library's contract.

In a fully-loaded interactive direnv shell the venv `bin/` is on `PATH`, so a
human can drop the `uv run` prefix and just call `pytest` / `ruff` / `pyrefly`.
That shortcut is **not** reliable for tooling/agents whose `PATH` is pinned and
doesn't include the venv — prefer `uv run` there.

## Hard constraints

- **Python 3.14+ only.** The API relies on PEP 695 generics (`class Message[A, R]`)
  and recent typing features. Do not add compatibility shims for older versions.
- **Zero runtime dependencies.** `dependencies = []` in
  [pyproject.toml](pyproject.toml) is intentional — keep it that way. Anything new
  goes in the `dev` dependency group only.
- **Typed public API.** The package ships `py.typed`
  ([src/tractor/py.typed](src/tractor/py.typed)); callers rely on precise types.
  No `Any`, no string dispatch.

## Type checking

The type checker of record is **pyrefly** (config under `[tool.pyrefly]` in
[pyproject.toml](pyproject.toml), `preset = "strict"`). It is *not* pyright or
mypy. The `# pyright: ignore[...]` comments in the source are deliberate and
coexist with pyrefly via `enabled-ignores`.

Note: [.vscode/settings.json](.vscode/settings.json) sets
`python.analysis.typeCheckingMode: "off"`. That only disables Pylance's
in-editor checking to avoid duplicate diagnostics — it does **not** mean types
are unchecked. pyrefly strict is the source of truth.

## Architecture

The public surface is re-exported from [src/tractor/__init__.py](src/tractor/__init__.py).

- **`Actor`** ([src/tractor/actor.py](src/tractor/actor.py)) — base class. Holds
  state; lifecycle hooks `on_start` / `on_stop` / `on_panic` and the driver
  `step` are all optional with sensible defaults. Override `step` to await
  external sources (timers, futures, streams) alongside the inbox.
- **`Message[A, R]`** ([src/tractor/message.py](src/tractor/message.py)) — a typed
  envelope tying a message to the actor type `A` it targets and the reply type
  `R` it produces. Subclass it and implement `async def dispatch(self, actor, ctx)`;
  keep `dispatch` thin — route to a method on the actor that owns the real logic.
- **`Runtime`** ([src/tractor/runtime.py](src/tractor/runtime.py)) — created once
  at app startup; the only way to spawn actors (`runtime.spawn(actor)`). Holds
  the crash policy.
- **`Context`** — passed into every handler; carries sender identity and exposes
  `ctx.tell` / `ctx.ask` / `ctx.forward`.

### The Runtime invariant (key design decision)

**Every message send goes through the `Runtime`** so no message is invisible to
it — this is what makes complete causal trace reconstruction possible.

- Outside actors: `runtime.tell(ref, msg)` / `runtime.ask(ref, msg)`.
- Inside handlers: `ctx.tell(other_ref, msg)` / `ctx.ask(other_ref, msg)`, which
  forward to the Runtime with sender identity implicit from `ctx`.

`ActorRef.tell` / `ActorRef.ask` are intentionally not the primary API. Route new
message-passing paths through the Runtime, not around it.

Crash handling is layered: the actor's own `on_panic` decides
`ControlFlow.Stop`/`Continue` first, and the Runtime's `CrashPolicy` is always
notified afterward regardless of that decision.

## Conventions

- New messages: `@dataclass` subclass of `Message[ActorType, ReplyType]` with an
  `@override async def dispatch(...)`.
- Match the existing style in [src/tractor/](src/tractor/) — module docstrings,
  rich method docstrings, `@final` on classes not meant to be subclassed.
- The clearest spec of intended usage lives in [examples/](examples/) and
  [tests/](tests/). Read those before changing public behavior.
