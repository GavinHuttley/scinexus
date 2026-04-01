"""Tests that type-checker support works for both inheritance and decorator paths."""

import subprocess
import sys
import textwrap

from scinexus import AppBase, ComposableApp
from scinexus.composable import NON_COMPOSABLE, NotCompleted, define_app


class IntToStr(ComposableApp[int, str]):
    def main(self, val: int) -> str:
        return str(val)


class StrToInt(ComposableApp[str, int]):
    def main(self, val: str) -> int:
        return int(val)


class NonComp(AppBase[int, int], app_type=NON_COMPOSABLE):
    def main(self, val: int) -> int:
        return val * 2


def test_inheritance_basic_call():
    app = IntToStr()
    result = app(42)
    assert result == "42"


def test_inheritance_composition():
    app = IntToStr() + StrToInt()
    result = app(42)
    assert result == 42


def test_inheritance_not_completed_propagation():
    app = IntToStr()
    nc = NotCompleted("ERROR", "test", "msg")
    result = app(nc)
    assert isinstance(result, NotCompleted)


def test_inheritance_non_composable():
    app = NonComp()
    assert app(5) == 10
    assert not hasattr(app, "__add__") or app.app_type is NON_COMPOSABLE


def test_inheritance_repr():
    app = IntToStr()
    assert "IntToStr()" in repr(app)


def test_inheritance_disconnect():
    a = IntToStr()
    b = StrToInt()
    composed = a + b
    composed.disconnect()
    assert b.input is None


def test_inheritance_type_validation():
    app = IntToStr()
    result = app("wrong type")
    assert isinstance(result, NotCompleted)


def test_inheritance_init_vals():
    class WithInit(ComposableApp[int, int]):
        def __init__(self, factor: int = 1):
            self.factor = factor

        def main(self, val: int) -> int:
            return val * self.factor

    app = WithInit(factor=3)
    assert app._init_vals == {"factor": 3}  # noqa: SLF001
    assert app(5) == 15


def test_inheritance_citations():
    from citeable import Software

    cite = Software(
        author=["Doe, J"],
        title="test",
        year=2024,
        url="https://example.com",
        version="1.0",
        license="MIT",
        doi="10.0/test",
        publisher="test",
    )

    class Cited(ComposableApp[int, int], cite=cite):
        def main(self, val: int) -> int:
            return val

    app = Cited()
    assert app.citations == (cite,)
    assert cite.app == "Cited"


def test_inheritance_pickle():
    import pickle

    app = IntToStr()
    data = pickle.dumps(app)
    restored = pickle.loads(data)  # noqa: S301
    assert restored(42) == "42"


def test_decorator_still_works():
    """define_app decorator continues to produce working apps."""

    @define_app
    class dec_app:
        def main(self, val: int) -> int:
            return val + 1

    app = dec_app()
    assert app(1) == 2


def test_decorator_composable_with_inheritance():
    """Decorator-created and inheritance-created apps compose together."""

    @define_app
    class adder:
        def main(self, val: int) -> int:
            return val + 1

    composed = adder() + IntToStr()
    assert composed(1) == "2"


def _run_mypy(code: str, tmp_path) -> tuple[int, str]:
    src = tmp_path / "check.py"
    src.write_text(code)
    result = subprocess.run(
        [sys.executable, "-m", "mypy", "--no-error-summary", str(src)],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, result.stdout + result.stderr


def test_mypy_decorator_reveal_type(tmp_path):
    code = textwrap.dedent("""\
        from scinexus.composable import define_app

        @define_app
        class MyApp:
            def main(self, val: int) -> str:
                return str(val)

        app = MyApp()
        reveal_type(app(42))
    """)
    _, output = _run_mypy(code, tmp_path)
    assert "str" in output or "Union" in output


def test_mypy_inheritance_reveal_type(tmp_path):
    code = textwrap.dedent("""\
        from scinexus import ComposableApp

        class MyApp(ComposableApp[int, str]):
            def main(self, val: int) -> str:
                return str(val)

        app = MyApp()
        reveal_type(app(42))
    """)
    _, output = _run_mypy(code, tmp_path)
    # Should see str (or Union[str, NotCompleted]) — not Any
    assert "str" in output
