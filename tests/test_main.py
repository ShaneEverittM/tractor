"""Tests for the ``@main`` entry-point decorator.

``@main``'s defining behavior — auto-running when its module is the script
being executed — cannot be observed from inside this (imported) test module,
so each test writes a small script to disk and runs it in a fresh
interpreter.
"""

import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import tractor

# Guarantees the subprocess can import tractor no matter which venv or
# editable-install scheme resolved it for the test process itself.
_PACKAGE_PARENT = str(Path(tractor.__file__).resolve().parent.parent)

GUARDLESS_APP = """
    from tractor import Runtime, main


    @main
    async def app(runtime: Runtime) -> None:
        print("app ran")
    """


def run_python(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ | {"PYTHONPATH": _PACKAGE_PARENT}
    return subprocess.run(
        [sys.executable, *args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def write_script(tmp_path: Path, source: str) -> None:
    _ = (tmp_path / "app.py").write_text(dedent(source))


def test_runs_after_module_completes_without_guard(tmp_path: Path) -> None:
    """No guard needed, and definitions below ``@main`` are visible to it."""
    write_script(
        tmp_path,
        """
        from typing import final

        from tractor import Actor, Runtime, handler, main


        @main
        async def app(runtime: Runtime) -> None:
            ref = runtime.spawn(Greeter())
            print(await runtime.ask(ref, Greeter.greet(SUFFIX)))
            await ref.stop()


        @final
        class Greeter(Actor):
            @handler
            async def greet(self, name: str) -> str:
                return f"hello, {name}"


        SUFFIX = "from below"
        print("top-level done")
        """,
    )
    result = run_python(tmp_path, "app.py")
    assert result.returncode == 0, result.stderr
    # The app must run strictly after the module's top-level code.
    assert result.stdout.splitlines() == ["top-level done", "hello, from below"]


def test_import_does_not_run_but_entry_point_is_callable(tmp_path: Path) -> None:
    write_script(tmp_path, GUARDLESS_APP)
    result = run_python(tmp_path, "-c", "import app; print('imported'); app.app()")
    assert result.returncode == 0, result.stderr
    # "imported" first proves the import alone triggered nothing.
    assert result.stdout.splitlines() == ["imported", "app ran"]


def test_module_exception_prevents_run(tmp_path: Path) -> None:
    """Same semantics as a real guard: a failing module never starts the app."""
    write_script(tmp_path, GUARDLESS_APP + '\n    raise RuntimeError("boom")\n')
    result = run_python(tmp_path, "app.py")
    assert result.returncode != 0
    assert "app ran" not in result.stdout
    assert "boom" in result.stderr


def test_second_main_raises(tmp_path: Path) -> None:
    write_script(
        tmp_path,
        """
        from tractor import Runtime, main


        @main
        async def app(runtime: Runtime) -> None: ...


        @main
        async def app2(runtime: Runtime) -> None: ...
        """,
    )
    result = run_python(tmp_path, "app.py")
    assert result.returncode != 0
    assert "only one @main entry point" in result.stderr


def test_crash_policy_form_also_defers(tmp_path: Path) -> None:
    write_script(
        tmp_path,
        """
        from tractor import ControlFlow, CrashPolicy, Runtime, main


        class Quiet(CrashPolicy):
            def on_crash(
                self, actor: object, exc: BaseException, flow: ControlFlow
            ) -> None: ...


        @main(crash_policy=Quiet())
        async def app(runtime: Runtime) -> None:
            print(f"later is {LATER}")


        LATER = 42
        """,
    )
    result = run_python(tmp_path, "app.py")
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["later is 42"]
