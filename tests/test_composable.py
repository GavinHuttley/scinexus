import inspect
import pickle
import shutil
from copy import copy
from pathlib import Path
from pickle import dumps, loads
from unittest.mock import Mock

import pytest
from citeable import Software
from numpy import array, ndarray
from scitrack import CachingLogger

try:
    import cogent3 as c3
    from cogent3.app import typing as c3types
    from cogent3.util.union_dict import UnionDict
except ImportError:
    c3 = None
    c3types = None
    UnionDict = None

from scinexus import open_data_store
from scinexus import typing as snx_types
from scinexus.composable import (
    GENERIC,
    LOADER,
    NON_COMPOSABLE,
    WRITER,
    ComposableApp,
    LoaderApp,
    NonComposableApp,
    NotCompleted,
    NotCompletedType,
    WriterApp,
    _get_raw_hints,
    _make_logfile_name,
    _proxy_input,
    define_app,
    is_app,
    is_app_composable,
    propagate_source,
    source_proxy,
)
from scinexus.data_store import (
    DataMember,
    DataStoreDirectory,
    Mode,
    get_unique_id,
    set_id_from_source,
)
from scinexus.deserialise import deserialise_object
from scinexus.sqlite_data_store import DataStoreSqlite


def test_composable():
    """correctly form string"""

    @define_app
    class app_dummyclass_1:
        def __init__(self, a):
            self.a = a

        def main(self, val: int) -> int:
            return val

    @define_app
    class app_dummyclass_2:
        def __init__(self, b):
            self.b = b

        def main(self, val: int) -> int:
            return val

    aseqfunc1 = app_dummyclass_1(1)
    aseqfunc2 = app_dummyclass_2(2)
    comb = aseqfunc1 + aseqfunc2
    expect = "app_dummyclass_1(a=1) + app_dummyclass_2(b=2)"
    got = str(comb)
    assert got == expect


def test_composables_reuse():
    """apps can be reused in multiple compositions"""

    @define_app
    class app_dummyclass_1:
        def __init__(self, a):
            self.a = a

        def main(self, val: int) -> int:
            return val

    @define_app
    class app_dummyclass_2:
        def __init__(self, b):
            self.b = b

        def main(self, val: int) -> int:
            return val

    @define_app
    class app_dummyclass_3:
        def __init__(self, c):
            self.c = c

        def main(self, val: int) -> int:
            return val

    one = app_dummyclass_1(1)
    two = app_dummyclass_2(2)
    three = app_dummyclass_3(3)
    one + three
    two + three  # reuse of three now works
    # originals are not mutated
    assert one.input is None
    assert two.input is None
    assert three.input is None


def test_composable_to_self():
    """this should raise a ValueError"""

    @define_app
    class app_dummyclass_1:
        def __init__(self, a):
            self.a = a

        def main(self, val: int) -> int:
            return val

    app1 = app_dummyclass_1(1)
    with pytest.raises(ValueError):
        _ = app1 + app1


def test_err_result():
    """excercise creation of NotCompletedResult"""
    result = NotCompleted(NotCompletedType.FAIL, "this", "some obj")
    assert not result
    assert result.origin == "this"
    assert result.message == "some obj"
    assert result.source is None

    # check source correctly deduced from provided object
    fake_source = Mock()
    fake_source.source = "blah"
    del fake_source.info
    result = NotCompleted(NotCompletedType.FAIL, "this", "err", source=fake_source)
    assert result.source == "blah"

    try:
        _ = 0
        msg = "error message"
        raise ValueError(msg)
    except ValueError as err:
        result = NotCompleted(NotCompletedType.FAIL, "this", err.args[0])

    assert result.message == "error message"


def test_not_completed_result():
    """should survive roundtripping pickle"""
    err = NotCompleted(NotCompletedType.FAIL, "mytest", "can we roundtrip")
    p = dumps(err)
    new = loads(p)
    assert err.type == new.type
    assert err.message == new.message
    assert err.source == new.source
    assert err.origin == new.origin


def test_composable_variable_positional_args():
    """correctly associate argument vals with their names when have variable
    positional args"""

    @define_app
    class pos_var_pos1:
        def __init__(self, a, b, *args):
            self.a = a
            self.b = b
            self.args = args

        def main(self, val: int) -> int:
            return val

    instance = pos_var_pos1(2, 3, 4, 5, 6)
    assert instance._init_vals == {"a": 2, "b": 3, "args": (4, 5, 6)}  # noqa: SLF001


def test_composable_minimum_parameters():
    """correctly associate argument vals with their names when have variable
    positional args and kwargs"""

    def test_func1(arg1) -> int:
        return 1

    with pytest.raises(ValueError):
        _, _ = _get_raw_hints(test_func1, 2)


def test_composable_return_type_hint():
    """correctly associate argument vals with their names when have variable
    positional args and kwargs"""

    def test_func1(arg1):
        return 1

    with pytest.raises(TypeError):
        _, _ = _get_raw_hints(test_func1, 1)


def test_composable_firstparam_type_hint():
    """correctly associate argument vals with their names when have variable
    positional args and kwargs"""

    def test_func1(arg1) -> int:
        return 1

    with pytest.raises(TypeError):
        _, _ = _get_raw_hints(test_func1, 1)


def test_composable_firstparam_type_is_None():
    """correctly associate argument vals with their names when have variable
    positional args and kwargs"""

    def test_func1(arg1: None) -> int:
        return 1

    with pytest.raises(TypeError):
        _, _ = _get_raw_hints(test_func1, 1)


def test_composable_return_type_is_None():
    """correctly associate argument vals with their names when have variable
    positional args and kwargs"""

    def test_func1(arg1: int) -> None:
        return

    with pytest.raises(TypeError):
        _, _ = _get_raw_hints(test_func1, 1)


def test_composable_variable_positional_args_and_kwargs():
    """correctly associate argument vals with their names when have variable
    positional args and kwargs"""

    @define_app
    class pos_var_pos_kw2:
        def __init__(self, a, *args, c=False):
            self.a = a
            self.c = c
            self.args = args

        def main(self, val: int) -> int:
            return val

    instance = pos_var_pos_kw2(2, 3, 4, 5, 6, c=True)
    assert instance._init_vals == {"a": 2, "args": (3, 4, 5, 6), "c": True}  # noqa: SLF001


def test_app_decoration_fails_with_slots():
    with pytest.raises(NotImplementedError):

        @define_app
        class app_not_supported_slots1:
            __slots__ = ("a",)

            def __init__(self, a):
                self.a = a

            def main(self, val: int) -> int:
                return val


def test_repeated_decoration():
    @define_app
    class app_decorated_repeated1:
        def __init__(self, a):
            self.a = a

        def main(self, val: int) -> int:
            return val

    with pytest.raises(TypeError):
        define_app(app_decorated_repeated1)


def test_recursive_decoration():
    @define_app
    class app_docorated_recursive1:
        def __init__(self, a):
            self.a = a

        def main(self, val: int) -> int:
            define_app(app_docorated_recursive1)
            return val

    with pytest.raises(TypeError):
        app_docorated_recursive1().main(1)


def test_inheritance_from_decorated_class():
    @define_app
    class app_decorated_first1:
        def __init__(self, a):
            self.a = a

        def main(self, val: int) -> int:
            return val

    with pytest.raises(TypeError):

        @define_app
        class app_inherits_decorated1(app_decorated_first1):
            def __init__(self, a):
                self.a = a

            def main(self, val: int) -> int:
                return val


# have to define this at module level for pickling to work
@define_app
def func2app(arg1: int, exponent: int) -> float:
    return arg1**exponent


@define_app
def float2int(val: float) -> int:
    return int(val)


def test_decorate_app_function():
    """works on functions now"""

    sqd = func2app(exponent=2)
    assert sqd(3) == 9
    assert inspect.isclass(func2app)


def test_roundtrip_decorated_function():
    """decorated function can be pickled/unpickled"""

    sqd = func2app(exponent=2)
    u = pickle.loads(pickle.dumps(sqd))
    assert u(4) == 16


def test_roundtrip_composed_app():
    """composed app can be pickled/unpickled"""
    composed = func2app(exponent=2) + float2int()
    u = pickle.loads(pickle.dumps(composed))
    assert u(3) == 9


def test_decorated_func_optional():
    @define_app(app_type=NON_COMPOSABLE)
    def power(val: int, pow: int = 1) -> int:
        return val**pow

    sqd = power(2)
    assert sqd(3) == 9


def test_decorated_func_repr():
    def kw(val: int = 1) -> int:
        return val**val

    def kw_kw(val: int = 1, pow: int = 1) -> int:  # noqa: A002
        return val**pow

    def pos(val: int) -> int:
        return val**val

    def pos_pos(val: int, pow: int) -> int:  # noqa: A002
        return val**pow

    def pos_kw(val: int, pow: int = 1) -> int:  # noqa: A002
        return val**pow

    fns = {fn: func for fn, func in locals().items() if callable(func)}
    args = {"pos": 4, "kw": {"pow": 3}}
    for name, func in fns.items():
        app = define_app(func)
        if len(name.split("_")) == 1:
            instance = app()
            expect = f"{name}()"
        elif name.endswith("kw"):
            instance = app(**args["kw"])
            expect = f"{name}(pow={args['kw']['pow']})"
        else:
            instance = app(args["pos"])
            expect = f"{name}(pow={args['pos']})"

        assert repr(instance) == expect, name


def test_decorated_func_just_args():
    @define_app(app_type=NON_COMPOSABLE)
    def power(val: int, pow: int) -> int:  # noqa: A002
        return val**pow

    sqd = power()
    assert sqd(3, 3) == 27


def test_decorated_app_is_app():
    """check is_app for define_app decorated apps"""

    @define_app
    class app_test_isapp1:
        def main(self, data: int) -> int:
            return data

    assert is_app(app_test_isapp1)


def test_undecorated_app_is_not_an_app():
    """check is_app for non-decorated apps"""

    class app_not_composable1:
        def main(self, data: int) -> int:
            return data

    assert not is_app(app_not_composable1)


def test_add_non_composable_apps():
    @define_app(app_type=NON_COMPOSABLE)
    class app_non_composable1:
        def __init__(self):
            pass

        def main(self, val: int) -> int:
            return val

    @define_app(app_type=NON_COMPOSABLE)
    class app_non_composable2:
        def __init__(self):
            pass

        def main(self, val: int) -> int:
            return val

    app_non_composable1.__add__ = ComposableApp.__add__
    app_non_composable2.__add__ = ComposableApp.__add__
    app1 = app_non_composable1()
    app2 = app_non_composable2()
    with pytest.raises(TypeError):
        app1 + app2


_types_null = (list, []), (tuple, ())


@pytest.mark.parametrize(("in_type", "input_"), _types_null)
def test_handles_null_series_input(in_type, input_):
    """apps correctly handle null output"""

    @define_app
    def null_in(val: in_type, pow: int) -> int:  # noqa: A002, ARG001
        return 2

    app = null_in(pow=2)
    got = app(input_)
    assert isinstance(got, NotCompleted)


@pytest.mark.parametrize("ret_type", [0, array([]), [], {}])
def test_handles_null_output(ret_type):
    """apps correctly handle null output"""

    @define_app
    def null_out(val: ndarray, pow: int) -> int:  # noqa: A002, ARG001
        return ret_type

    app = null_out(pow=2)
    d = array([3, 3])
    got = app(d)
    assert isinstance(got, type(ret_type))


def test_handles_None():
    """apps correctly handle null output"""

    @define_app
    def none_out(val: ndarray, pow: int) -> int:  # noqa: A002, ARG001
        return None

    @define_app
    def take_int(val: int) -> int:
        return val

    app = none_out(pow=2)
    d = array([3, 3])
    got = app(d)
    assert isinstance(got, NotCompleted)

    app = none_out(pow=2) + take_int()
    d = array([3, 3])
    got = app(d)
    assert isinstance(got, NotCompleted)


def test_validate_data_type_not_completed_pass_through():
    # returns the instance of a NotCompleted created by an input
    @define_app
    def take_int1(val: int) -> int:  # noqa: ARG001
        return NotCompleted(
            NotCompletedType.ERROR, "take_int1", "external to app", source="unknown"
        )

    @define_app
    def take_int2(val: int) -> int:
        return val

    app = take_int1() + take_int2()
    got = app(2)
    assert got.origin == "take_int1"


@pytest.mark.parametrize(
    ("first", "ret"),
    [(tuple[set[str]], int), (int, tuple[set[str]])],
)
def test_complex_type(first, ret):
    # deep nesting now allowed (typeguard handles arbitrary nesting)
    @define_app
    class x:
        def main(self, data: first) -> ret:
            return data


@pytest.mark.parametrize("hint", [tuple[set[str]], tuple[tuple[set[str]]]])
def test_complex_type_depths(hint):
    # deep nesting now allowed (typeguard handles arbitrary nesting)
    @define_app
    class x:
        def main(self, data: hint) -> bool:  # noqa: ARG002
            return True


@pytest.mark.parametrize("hint", [int, set[str]])
def test_complex_type_allowed_depths(hint):
    # allowed <=2-deep nesting of types
    @define_app
    class x:
        def main(self, data: hint) -> int:  # noqa: ARG002
            return int


@pytest.mark.parametrize(
    "meth",
    [
        "__call__",
        "__repr__",
        "__str__",
        "__new__",
        "__add__",
        "input",
        "apply_to",
        "_validate_data_type",
    ],
)
def test_forbidden_methods_composable_app(meth):
    class app_forbidden_methods1:
        def __init__(self, a):
            self.a = a

        def main(self, val: int) -> int:
            return val

    def function1():
        pass

    setattr(app_forbidden_methods1, meth, function1)
    with pytest.raises(TypeError):
        define_app(app_type=WRITER)(app_forbidden_methods1)


@pytest.mark.parametrize(
    "meth",
    ["__call__", "__repr__", "__str__", "__new__", "_validate_data_type"],
)
def test_forbidden_methods_non_composable_app(meth):
    class app_forbidden_methods2:
        def __init__(self, a):
            self.a = a

        def main(self, val: int) -> int:
            return val

    def function1():
        pass

    setattr(app_forbidden_methods2, meth, function1)
    with pytest.raises(TypeError):
        define_app(app_type=NON_COMPOSABLE)(app_forbidden_methods2)


def test_forbidden_input_attribute():
    """user-defined input attribute (non-function) is rejected on composable apps"""

    class app_with_input_attr:
        input = "some_value"

        def main(self, val: int) -> int:
            return val

    with pytest.raises(TypeError):
        define_app(app_with_input_attr)


def test_skip_not_completed():
    @define_app(skip_not_completed=False)
    def takes_not_completed(val: snx_types.SerialisableType) -> dict:
        return val.to_rich_dict()

    app = takes_not_completed()
    nc = NotCompleted(NotCompletedType.ERROR, "test", "for tracing", source="blah")
    got = app(nc)
    assert isinstance(got, dict)
    assert got == nc.to_rich_dict()


def test_copies_doc_from_func():
    @define_app
    def delme(val: snx_types.SerialisableType) -> dict:
        """my docstring"""
        return val.to_rich_dict()

    assert delme.__doc__ == "my docstring"

    @define_app
    def delme2(val: snx_types.SerialisableType) -> dict:
        """my docstring
        Notes
        -----
        body
        """
        return val.to_rich_dict()

    assert delme2.__doc__ == "my docstring"
    assert delme2.__init__.__doc__.split() == ["Notes", "-----", "body"]


def test_bad_wrap():
    def foo(a: "str") -> int:
        return int(a)

    with pytest.raises(NotImplementedError):
        define_app(foo)

    def bar(a: str) -> "int":
        return int(a)

    with pytest.raises(NotImplementedError):
        define_app(bar)


def _make_cite(**kwargs):
    defaults = {
        "author": ["Doe, J"],
        "title": "test",
        "year": 2024,
        "url": "https://example.com",
        "version": "1.0",
        "license": "MIT",
        "doi": "10.0/test",
        "publisher": "test",
    }
    defaults.update(kwargs)
    return Software(**defaults)


def test_single_app_with_citation():
    cite = _make_cite()

    @define_app(cite=cite)
    class cited_app:
        def main(self, val: int) -> int:
            return val

    app = cited_app()
    assert app._cite is cite  # noqa: SLF001
    assert app.citations == (cite,)


def test_single_app_without_citation():
    @define_app
    class uncited_app:
        def main(self, val: int) -> int:
            return val

    app = uncited_app()
    assert app._cite is None  # noqa: SLF001
    assert app.citations == ()


def test_composed_apps_all_with_citations():
    cite_a = _make_cite(title="A")
    cite_b = _make_cite(title="B")

    @define_app(cite=cite_a)
    class app_a:
        def main(self, val: int) -> int:
            return val

    @define_app(cite=cite_b)
    class app_b:
        def main(self, val: int) -> int:
            return val

    composed = app_a() + app_b()
    assert composed.citations == (cite_b, cite_a)


def test_composed_apps_some_with_citations():
    cite_a = _make_cite(title="A")

    @define_app(cite=cite_a)
    class app_c:
        def main(self, val: int) -> int:
            return val

    @define_app
    class app_d:
        def main(self, val: int) -> int:
            return val

    composed = app_c() + app_d()
    assert composed.citations == (cite_a,)


def test_composed_apps_none_with_citations():
    @define_app
    class app_e:
        def main(self, val: int) -> int:
            return val

    @define_app
    class app_f:
        def main(self, val: int) -> int:
            return val

    composed = app_e() + app_f()
    assert composed.citations == ()


def test_composed_three_app_chain():
    cite_l = _make_cite(title="L")
    cite_g = _make_cite(title="G")
    cite_w = _make_cite(title="W")

    @define_app(cite=cite_l)
    class chain_l:
        def main(self, val: int) -> int:
            return val

    @define_app(cite=cite_g)
    class chain_g:
        def main(self, val: int) -> int:
            return val

    @define_app(cite=cite_w)
    class chain_w:
        def main(self, val: int) -> int:
            return val

    composed = chain_l() + chain_g() + chain_w()
    assert composed.citations == (cite_w, cite_g, cite_l)


def test_duplicate_citation_deduplication():
    cite = _make_cite()

    @define_app(cite=cite)
    class dup_a:
        def main(self, val: int) -> int:
            return val

    @define_app(cite=cite)
    class dup_b:
        def main(self, val: int) -> int:
            return val

    composed = dup_a() + dup_b()
    assert composed.citations == (cite,)


def test_non_composable_app_with_citation():
    cite = _make_cite()

    @define_app(app_type=NON_COMPOSABLE, cite=cite)
    class nc_cited:
        def main(self, val: int) -> int:
            return val

    app = nc_cited()
    assert app.citations == (cite,)


def test_cite_sets_app_attribute():
    cite = _make_cite()

    @define_app(cite=cite)
    class my_special_app:
        def main(self, val: int) -> int:
            return val

    app = my_special_app()
    assert app.citations[0].app == "my_special_app"


def test_cite_shared_across_apps():
    cite = _make_cite()

    @define_app(cite=cite)
    class app_one:
        def main(self, val: int) -> int:
            return val

    @define_app(cite=cite)
    class app_two:
        def main(self, val: int) -> int:
            return val

    a = app_one()
    b = app_two()
    assert a.citations[0].app == "app_one"
    assert b.citations[0].app == "app_two"


def test_bib_single_app_with_citation():
    cite = _make_cite()

    @define_app(cite=cite)
    class bib_app:
        def main(self, val: int) -> int:
            return val

    app = bib_app()
    assert app.bib == str(cite)


def test_bib_app_without_citation():
    @define_app
    class no_bib_app:
        def main(self, val: int) -> int:
            return val

    app = no_bib_app()
    assert app.bib == ""


def test_bib_composed_apps():
    cite_a = _make_cite(title="A")
    cite_b = _make_cite(title="B")

    @define_app(cite=cite_a)
    class bib_a:
        def main(self, val: int) -> int:
            return val

    @define_app(cite=cite_b)
    class bib_b:
        def main(self, val: int) -> int:
            return val

    composed = bib_a() + bib_b()
    assert str(cite_b) in composed.bib
    assert str(cite_a) in composed.bib
    assert "\n\n" in composed.bib


def test_app_copy():
    """shallow copy creates new instance sharing attribute references"""

    @define_app
    class app_copy_test:
        def __init__(self, data):
            self.data = data

        def main(self, val: int) -> int:
            return val

    original = app_copy_test([1, 2, 3])
    copied = copy(original)
    assert copied is not original
    assert copied.data is original.data
    assert copied._init_vals is original._init_vals  # noqa: SLF001


def test_composition_does_not_mutate_originals():
    """composition uses copies so originals stay independent"""

    @define_app
    class app_mut_a:
        def main(self, val: int) -> int:
            return val

    @define_app
    class app_mut_b:
        def main(self, val: int) -> int:
            return val

    a = app_mut_a()
    b = app_mut_b()
    _ = a + b
    assert a.input is None
    assert b.input is None


def test_app_reuse_in_multiple_compositions():
    """same app instance can be used in multiple compositions"""

    @define_app
    class app_reuse_a:
        def main(self, val: int) -> int:
            return val

    @define_app
    class app_reuse_b:
        def main(self, val: int) -> int:
            return val

    @define_app
    class app_reuse_c:
        def main(self, val: int) -> int:
            return val

    a = app_reuse_a()
    b = app_reuse_b()
    c = app_reuse_c()
    comb1 = a + b
    comb2 = a + c
    comb3 = c + b
    assert comb1(1) == 1
    assert comb2(1) == 1
    assert comb3(1) == 1


def test_app_eq():
    """__eq__ checks attribute identity"""

    @define_app
    class app_eq_test:
        def __init__(self, x):
            self.x = x

        def main(self, val: int) -> int:
            return val

    a = app_eq_test(1)
    b = copy(a)
    assert a == b
    c = app_eq_test(1)
    assert a != c  # different instances with different attribute objects
    assert a != "not an app"


def test_check_data_type_default_true():
    @define_app
    class app_cdt:
        def main(self, val: int) -> int:
            return val

    assert app_cdt().check_data_type is True


def test_check_data_type_true_rejects_bad_type():
    @define_app
    class app_cdt:
        def main(self, val: int) -> int:
            return val

    app = app_cdt()
    app.check_data_type = True
    got = app("not_an_int")
    assert isinstance(got, NotCompleted)


def test_check_data_type_false_skips_validation():
    @define_app
    class app_cdt:
        def main(self, val: int) -> int:
            return val

    app = app_cdt()
    app.check_data_type = False
    got = app("42")
    assert got == "42"


def test_check_data_type_propagates_in_composition():
    @define_app
    class app_cdt_a:
        def main(self, val: int) -> int:
            return val

    @define_app
    class app_cdt_b:
        def main(self, val: int) -> int:
            return val

    @define_app
    class app_cdt_c:
        def main(self, val: int) -> int:
            return val

    composed = app_cdt_a() + app_cdt_b() + app_cdt_c()
    assert composed.check_data_type is True
    assert composed.input.check_data_type is True
    assert composed.input.input.check_data_type is True

    composed.check_data_type = False
    assert composed.check_data_type is False
    assert composed.input.check_data_type is False
    assert composed.input.input.check_data_type is False


def test_check_data_type_does_not_affect_originals():
    @define_app
    class app_cdt_a:
        def main(self, val: int) -> int:
            return val

    @define_app
    class app_cdt_b:
        def main(self, val: int) -> int:
            return val

    a = app_cdt_a()
    b = app_cdt_b()
    composed = a + b
    composed.check_data_type = False
    assert a.check_data_type is True
    assert b.check_data_type is True


def test_check_data_type_false_composed_end_to_end():
    @define_app
    class app_cdt_a:
        def main(self, val: int) -> int:
            return val

    @define_app
    class app_cdt_b:
        def main(self, val: int) -> int:
            return val

    composed = app_cdt_a() + app_cdt_b()
    composed.check_data_type = False
    got = composed("not_an_int")
    assert got == "not_an_int"


def test_check_data_type_re_enable():
    @define_app
    class app_cdt:
        def main(self, val: int) -> int:
            return val

    app = app_cdt()
    app.check_data_type = False
    assert app("not_int") == "not_int"

    app.check_data_type = True
    got = app("not_int")
    assert isinstance(got, NotCompleted)


def test_make_logfile_name():
    @define_app
    class logname_app:
        def main(self, val: int) -> int:
            return val

    app = logname_app()
    name = _make_logfile_name(app)
    assert name.startswith("logname_app")
    assert name.endswith(".log")


def test_not_completed_repr():
    nc = NotCompleted(NotCompletedType.ERROR, "origin", "msg", source="src")
    r = repr(nc)
    assert "ERROR" in r
    assert "origin" in r
    assert "msg" in r


def test_not_completed_source_exception():
    """source that raises in get_data_source results in None"""
    nc = NotCompleted(NotCompletedType.ERROR, "test", "msg", source=42)
    assert nc.source is None


def test_call_with_none():
    @define_app
    class none_app:
        def main(self, val: int) -> int:
            return val

    app = none_app()
    got = app(None)
    assert isinstance(got, NotCompleted)
    assert "None" in got.message


def test_validate_data_type_source_proxy():
    @define_app
    class proxy_app:
        def main(self, val: int) -> int:
            return val

    app = proxy_app()
    sp = source_proxy(42)
    got = app(sp)
    assert got == 42


def test_add_writer_lhs_raises():
    @define_app(app_type=WRITER)
    class writer_app:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: int, identifier: str = "") -> int:
            return data

    @define_app
    class generic_app:
        def main(self, val: int) -> int:
            return val

    ds = Mock()
    ds.source = "/tmp"
    w = writer_app(data_store=ds)
    g = generic_app()
    with pytest.raises(TypeError, match="writer"):
        w + g


def test_add_loader_rhs_raises():
    @define_app(app_type=LOADER)
    class loader_app:
        def main(self, val: str) -> int:
            return int(val)

    @define_app
    class generic_app:
        def main(self, val: int) -> int:
            return val

    g = generic_app()
    lo = loader_app()
    with pytest.raises(TypeError, match="loader"):
        g + lo


def test_add_incompatible_types():
    @define_app
    class str_app:
        def main(self, val: int) -> str:
            return str(val)

    @define_app
    class int_only_app:
        def main(self, val: int) -> int:
            return val

    with pytest.raises(TypeError, match="incompatible"):
        str_app() + int_only_app()


def test_is_app_composable_true():
    @define_app
    class comp_app:
        def main(self, val: int) -> int:
            return val

    assert is_app_composable(comp_app)


def test_is_app_composable_false_non_composable():
    @define_app(app_type=NON_COMPOSABLE)
    class nc_app:
        def main(self, val: int) -> int:
            return val

    assert not is_app_composable(nc_app)


def test_is_app_composable_false_not_app():
    assert not is_app_composable("not an app")


def test_define_app_on_non_class():
    with pytest.raises(ValueError, match="not a class"):
        define_app(42)


def test_source_proxy_basic():
    obj = [1, 2, 3]
    sp = source_proxy(obj)
    assert sp.obj is obj
    assert sp.source is obj
    assert isinstance(sp.uuid, str)


def test_source_proxy_set_obj():
    sp = source_proxy([1, 2])
    sp.set_obj([3, 4])
    assert sp.obj == [3, 4]
    assert sp.source == [1, 2]


def test_source_proxy_source_setter():
    sp = source_proxy("original")
    # property setter must be called directly because __setattr__ intercepts
    type(sp).source.fset(sp, "new_source")
    assert sp.source == "new_source"


def test_source_proxy_getattr():
    sp = source_proxy([1, 2, 3])
    assert sp.count(1) == 1


def test_source_proxy_setattr():
    obj = Mock()
    sp = source_proxy(obj)
    sp.value = 42
    assert obj.value == 42


def test_source_proxy_bool():
    assert bool(source_proxy([1]))
    assert not bool(source_proxy([]))


def test_source_proxy_repr_str():
    sp = source_proxy([1, 2])
    assert repr(sp) == repr([1, 2])
    assert str(sp) == str([1, 2])


def test_source_proxy_eq():
    sp = source_proxy(42)
    assert sp == 42
    assert sp != 43


def test_source_proxy_len():
    sp = source_proxy([1, 2, 3])
    assert len(sp) == 3


def test_source_proxy_pickle():
    sp = source_proxy("hello")
    restored = pickle.loads(pickle.dumps(sp))
    assert restored.obj == "hello"
    assert restored.source == "hello"


def test_proxy_input_with_source():
    item = Mock()
    item.source = "test"
    item.__bool__ = lambda self: True
    result = _proxy_input([item])
    assert len(result) == 1
    assert result[0] is item


def test_proxy_input_without_source():
    result = _proxy_input(["a", "b"])
    assert len(result) == 2
    assert all(isinstance(r, source_proxy) for r in result)


def test_proxy_input_skips_falsy():
    result = _proxy_input([0, "", "valid"])
    assert len(result) == 1


def test_propagate_source_non_proxy():
    @define_app
    class prop_app:
        def main(self, val: int) -> int:
            return val * 2

    app = prop_app()
    ps = propagate_source(app, get_unique_id)
    got = ps(4)
    assert got == 8


def test_propagate_source_proxy_with_source():
    @define_app
    class prop_app2:
        def main(self, val: str) -> str:
            return val.upper()

    app = prop_app2()

    class HasSource:
        def __init__(self, v, src):
            self.value = v
            self.source = src

        def upper(self):
            return HasSource(self.value.upper(), self.source)

    obj = HasSource("hello", "my_source")
    sp = source_proxy(obj)
    ps = propagate_source(app, get_unique_id)
    got = ps(sp)
    # result has source via get_unique_id, so returned directly
    assert not isinstance(got, source_proxy) or got.obj is not obj


def test_propagate_source_proxy_no_source():
    @define_app
    class prop_app3:
        def main(self, val: int) -> int:
            return val + 1

    app = prop_app3()
    sp = source_proxy(10)
    ps = propagate_source(app, get_unique_id)
    got = ps(sp)
    assert isinstance(got, source_proxy)
    assert got.obj == 11


def test_as_completed_serial(tmp_path):
    @define_app
    class ac_app:
        def main(self, val: str) -> str:
            return val.upper()

    app = ac_app()
    results = list(app.as_completed(["a", "b", "c"], show_progress=False))
    objs = [r.obj if isinstance(r, source_proxy) else r for r in results]
    assert "A" in objs
    assert len(objs) == 3


def test_as_completed_empty():
    @define_app
    class ac_empty:
        def main(self, val: int) -> int:
            return val

    app = ac_empty()
    results = list(app.as_completed([], show_progress=False))
    assert results == []


def test_as_completed_string_input():
    @define_app
    class ac_str:
        def main(self, val: str) -> str:
            return val

    app = ac_str()
    results = list(app.as_completed("hello", show_progress=False))
    assert len(results) == 1


def test_as_completed_with_progress_instance():
    from scinexus.progress import NoProgress

    @define_app
    class ac_prog:
        def main(self, val: str) -> str:
            return val.upper()

    app = ac_prog()
    results = list(app.as_completed(["a", "b"], show_progress=NoProgress()))
    objs = [r.obj if isinstance(r, source_proxy) else r for r in results]
    assert len(objs) == 2
    assert "A" in objs


def test_as_completed_with_tqdm_progress():
    from scinexus.progress import TqdmProgress

    @define_app
    class ac_tqdm:
        def main(self, val: int) -> int:
            return val * 2

    app = ac_tqdm()
    results = list(
        app.as_completed([1, 2, 3], show_progress=TqdmProgress(disable=True))
    )
    objs = [r.obj if isinstance(r, source_proxy) else r for r in results]
    assert len(objs) == 3


def test_writer_set_logger_default(tmp_path):
    @define_app(app_type=WRITER)
    class w_logger:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: int, identifier: str = "") -> int:
            return data

    ds = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")
    app = w_logger(data_store=ds)
    app.set_logger()
    assert app.logger is not None
    app.logger.shutdown()


def test_writer_set_logger_false(tmp_path):
    @define_app(app_type=WRITER)
    class w_logger2:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: int, identifier: str = "") -> int:
            return data

    ds = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")
    app = w_logger2(data_store=ds)
    app.set_logger(logger=False)
    assert app.logger is None


def test_writer_set_logger_invalid_type(tmp_path):
    @define_app(app_type=WRITER)
    class w_logger3:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: int, identifier: str = "") -> int:
            return data

    ds = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")
    app = w_logger3(data_store=ds)
    with pytest.raises(TypeError, match="CachingLogger"):
        app.set_logger(logger="not a logger")


def test_writer_apply_to(tmp_path):
    from scinexus.data_store import DataMember

    src = tmp_path / "src"
    src.mkdir()
    for i in range(3):
        (src / f"item_{i}.txt").write_text(f"data {i}")

    dstore = DataStoreDirectory(src, suffix="txt")

    out = tmp_path / "out"
    out_dstore = DataStoreDirectory(out, mode=Mode.w, suffix="txt")

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: DataMember) -> str:
            return val.read()

    @define_app(app_type=WRITER)
    class writer:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            return self.data_store.write(unique_id=identifier, data=data)

    process = reader() + writer(data_store=out_dstore)
    result = process.apply_to(dstore, logger=False, show_progress=False)
    assert len(result) == 3


def test_writerapp_subclass_apply_to_writes_once(tmp_path) -> None:
    """An inheritance-defined writer runs main() exactly once per input.

    Regression test for the original bug where ``class X(WriterApp)``
    ended up with ``app_type=GENERIC``, causing ``apply_to`` to feed
    the writer's own output back into ``main`` on a second pass.
    """
    src = tmp_path / "src"
    src.mkdir()
    for i in range(3):
        (src / f"item_{i}.txt").write_text(f"data {i}")

    dstore = DataStoreDirectory(src, suffix="txt")
    out_dstore = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")
    call_count: list[str] = []

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: DataMember) -> str:
            return val.read()

    class inherited_writer(WriterApp):
        def __init__(self, data_store: DataStoreDirectory) -> None:
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            call_count.append(identifier)
            return self.data_store.write(unique_id=identifier, data=data)

    assert inherited_writer.app_type is WRITER
    process = reader() + inherited_writer(data_store=out_dstore)
    result = process.apply_to(dstore, logger=False, show_progress=False)
    assert len(result) == 3
    assert len(call_count) == 3


def test_apply_to_uses_registered_id_from_source(
    tmp_path,
    reset_id_from_source: None,
) -> None:
    """`apply_to` consults the globally registered ID extractor by default."""
    src = tmp_path / "src"
    src.mkdir()
    for i in range(3):
        (src / f"item_{i}.txt").write_text(f"data {i}")

    dstore = DataStoreDirectory(src, suffix="txt")
    out_dstore = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")
    seen_ids: list[str | None] = []

    def custom(obj: object) -> str | None:
        result = get_unique_id(obj)
        seen_ids.append(result)
        return result

    set_id_from_source(custom)

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: DataMember) -> str:
            return val.read()

    @define_app(app_type=WRITER)
    class writer:
        def __init__(self, data_store: DataStoreDirectory) -> None:
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            return self.data_store.write(unique_id=identifier, data=data)

    process = reader() + writer(data_store=out_dstore)
    process.apply_to(dstore, logger=False, show_progress=False)

    # `apply_to`'s input-id loop calls `custom` directly with input paths,
    # so the seen IDs include the input filenames (suffix-stripped).
    assert seen_ids
    assert any(sid is not None and sid.startswith("item_") for sid in seen_ids)


def test_apply_to_explicit_id_from_source_overrides_registered(
    tmp_path,
    reset_id_from_source: None,
) -> None:
    """An explicit ``id_from_source=`` argument wins over the registered func."""
    registered_calls: list[object] = []
    explicit_calls: list[object] = []

    def registered(obj: object) -> str | None:
        registered_calls.append(obj)
        return get_unique_id(obj)

    def explicit(obj: object) -> str | None:
        explicit_calls.append(obj)
        return get_unique_id(obj)

    set_id_from_source(registered)

    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("data")
    dstore = DataStoreDirectory(src, suffix="txt")
    out_dstore = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: DataMember) -> str:
            return val.read()

    @define_app(app_type=WRITER)
    class writer:
        def __init__(self, data_store: DataStoreDirectory) -> None:
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            return self.data_store.write(unique_id=identifier, data=data)

    process = reader() + writer(data_store=out_dstore)
    process.apply_to(
        dstore,
        id_from_source=explicit,
        logger=False,
        show_progress=False,
    )

    assert explicit_calls, "explicit override should have been called"
    assert not registered_calls, (
        "registered func must not be called when explicit override is supplied"
    )


def test_not_completed_uses_registered_id_from_source(
    reset_id_from_source: None,
) -> None:
    """`NotCompleted` normalises its source via the registered extractor."""

    class WithSource:
        source = "path/to/file.fasta"

    def custom(obj: object) -> str | None:
        return f"custom::{obj!r}"

    set_id_from_source(custom)
    nc = NotCompleted(NotCompletedType.ERROR, "test", "msg", source=WithSource())
    assert nc.source is not None
    assert nc.source.startswith("custom::")


def test_not_completed_default_source_is_unique_id(
    reset_id_from_source: None,
) -> None:
    """Without a registration, `NotCompleted.source` is the unique ID form.

    Locks in the behaviour change: the default extractor strips file-format
    suffixes (``seqs.fasta`` → ``seqs``), where the prior implementation
    (calling ``get_data_source`` directly) returned the un-stripped form.
    """

    class WithSource:
        source = "path/to/seqs.fasta"

    nc = NotCompleted(NotCompletedType.ERROR, "test", "msg", source=WithSource())
    assert nc.source == "seqs"


def test_get_main_hints_no_main():
    from scinexus.composable import _get_main_hints

    class NoMain:
        pass

    with pytest.raises(ValueError, match="main"):
        _get_main_hints(NoMain)


def test_source_proxy_hash():
    sp = source_proxy("hello")
    assert isinstance(hash(sp), int)
    sp2 = source_proxy("hello")
    assert hash(sp) != hash(sp2)


def test_init_subclass_slots():
    with pytest.raises(NotImplementedError, match="slots"):

        class BadApp(ComposableApp[int, int]):
            __slots__ = ("x",)

            def main(self, val: int) -> int:
                return val


def test_writerapp_subclass_implies_writer() -> None:
    """Inheriting WriterApp yields a WRITER without any class kwarg."""

    class my_writer(WriterApp):
        def __init__(self, data_store: DataStoreDirectory) -> None:
            self.data_store = data_store

        def main(self, data: int, identifier: str = "") -> int:
            return data

    assert my_writer.app_type is WRITER


def test_loaderapp_subclass_implies_loader() -> None:
    """Inheriting LoaderApp yields a LOADER and is not stripped of input."""

    class my_loader(LoaderApp):
        def main(self, path: str) -> str:
            return path

    assert my_loader.app_type is LOADER
    # _init_subclass_setup only clears `cls.input` for non-LOADER apps.
    assert "input" not in my_loader.__dict__


def test_noncomposableapp_subclass_implies_non_composable() -> None:
    """Inheriting NonComposableApp yields a NON_COMPOSABLE."""

    class my_app(NonComposableApp[int, int]):
        def main(self, val: int) -> int:
            return val * 2

    assert my_app.app_type is NON_COMPOSABLE


def test_explicit_composableapp_subclass_is_generic() -> None:
    """Direct ComposableApp subclasses still resolve to GENERIC."""

    class my_app(ComposableApp[int, int]):
        def main(self, val: int) -> int:
            return val

    assert my_app.app_type is GENERIC


def test_validate_data_type_not_completed_skip_true():
    @define_app
    class skip_app:
        def main(self, val: int) -> int:
            return val

    app = skip_app()
    nc = NotCompleted(NotCompletedType.ERROR, "test", "msg")
    got = app._validate_data_type(nc)  # noqa: SLF001
    assert isinstance(got, NotCompleted)


def test_as_completed_with_datastore(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    for i in range(2):
        (src / f"f_{i}.txt").write_text(f"content {i}")

    dstore = DataStoreDirectory(src, suffix="txt")

    @define_app
    class read_app:
        def main(self, val: str) -> str:
            return val

    app = read_app()
    results = list(app.as_completed(dstore, show_progress=False))
    assert len(results) == 2


def test_as_completed_parallel():
    app = func2app(exponent=2)
    results = list(app.as_completed([1, 2, 3], parallel=True, show_progress=False))
    objs = [r.obj if isinstance(r, source_proxy) else r for r in results]
    assert sorted(objs) == [1, 4, 9]


def test_writer_apply_to_no_input(tmp_path):
    @define_app(app_type=WRITER)
    class lone_writer:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: int, identifier: str = "") -> int:
            return data

    ds = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")
    app = lone_writer(data_store=ds)
    with pytest.raises(RuntimeError, match="no composed input"):
        app.apply_to(["something"], logger=False)


def test_apply_to_empty_dstore(tmp_path):
    from scinexus.data_store import DataMember

    src = tmp_path / "empty"
    src.mkdir()
    dstore = DataStoreDirectory(src, suffix="txt")

    out_dstore = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: DataMember) -> str:
            return val.read()

    @define_app(app_type=WRITER)
    class writer:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            return self.data_store.write(unique_id=identifier, data=data)

    process = reader() + writer(data_store=out_dstore)
    with pytest.raises(ValueError, match="empty"):
        process.apply_to(dstore, logger=False, show_progress=False)


def test_apply_to_skip_existing(tmp_path):
    from scinexus.data_store import DataMember

    src = tmp_path / "src"
    src.mkdir()
    for i in range(3):
        (src / f"item_{i}.txt").write_text(f"data {i}")
    dstore = DataStoreDirectory(src, suffix="txt")

    out = tmp_path / "out"
    out_dstore = DataStoreDirectory(out, mode=Mode.w, suffix="txt")

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: DataMember) -> str:
            return val.read()

    @define_app(app_type=WRITER)
    class writer:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            return self.data_store.write(unique_id=identifier, data=data)

    process = reader() + writer(data_store=out_dstore)
    process.apply_to(dstore, logger=False, show_progress=False)
    assert len(out_dstore) == 3
    # run again — existing items should be skipped
    result = process.apply_to(dstore, logger=False, show_progress=False)
    assert len(result) == 3


def test_apply_to_with_logging(tmp_path):
    from scinexus.data_store import DataMember

    src = tmp_path / "src"
    src.mkdir()
    (src / "item.txt").write_text("data")
    dstore = DataStoreDirectory(src, suffix="txt")

    out = tmp_path / "out"
    out_dstore = DataStoreDirectory(out, mode=Mode.w, suffix="txt")

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: DataMember) -> str:
            return val.read()

    @define_app(app_type=WRITER)
    class writer:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            return self.data_store.write(unique_id=identifier, data=data)

    process = reader() + writer(data_store=out_dstore)
    result = process.apply_to(dstore, show_progress=False)
    assert len(result) == 1
    # log file should have been written to the data store
    assert any("log" in str(m) for m in out_dstore.logs)


def test_not_completed_to_json():
    """NotCompleted.to_json returns valid JSON"""
    import json

    nc = NotCompleted(NotCompletedType.ERROR, "origin", "a message")
    result = json.loads(nc.to_json())
    assert result["type"] == "scinexus.composable.NotCompleted"


def test_deserialise_not_completed():
    """roundtrip NotCompleted through JSON deserialisation"""
    nc = NotCompleted(NotCompletedType.ERROR, "origin", "msg")
    data = nc.to_rich_dict()
    result = deserialise_object(data)
    assert isinstance(result, NotCompleted)
    assert result.message == "msg"


def test_app_main_exception_returns_not_completed():
    """exception in main() returns NotCompleted instead of raising"""

    @define_app
    class raises_app:
        def main(self, val: int) -> int:
            msg = "boom"
            raise ValueError(msg)

    app = raises_app()
    result = app(1)
    assert isinstance(result, NotCompleted)
    assert result.type is NotCompletedType.ERROR
    assert "boom" in result.message


def test_not_completed_source_raises():
    """source that raises in get_data_source sets source to None"""

    class BadSource:
        @property
        def source(self):
            msg = "broken"
            raise RuntimeError(msg)

    nc = NotCompleted(NotCompletedType.ERROR, "origin", "msg", source=BadSource())
    assert nc.source is None


def test_composed_skip_not_completed_input():
    """NotCompleted passed as first input to composed pipeline is returned"""

    @define_app
    class first:
        def main(self, val: int) -> int:
            return val + 1

    @define_app
    class second:
        def main(self, val: int) -> int:
            return val * 2

    composed = first() + second()
    nc = NotCompleted(NotCompletedType.ERROR, "test", "fail")
    result = composed(nc)
    assert isinstance(result, NotCompleted)
    assert result is nc


def test_apply_to_string_input(tmp_path):
    """apply_to accepts a single string path as dstore"""
    src = tmp_path / "src"
    src.mkdir()
    (src / "item.txt").write_text("data")
    out_dstore = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: str) -> str:
            from pathlib import Path

            return Path(val).read_text()

    @define_app(app_type=WRITER)
    class writer:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            return self.data_store.write(unique_id=identifier, data=data)

    process = reader() + writer(data_store=out_dstore)
    result = process.apply_to(str(src / "item.txt"), logger=False, show_progress=False)
    assert len(result) == 1


def test_apply_to_duplicate_id(tmp_path):
    """apply_to raises ValueError for duplicate identifiers"""
    src = tmp_path / "src"
    src.mkdir()
    for i in range(2):
        (src / f"item_{i}.txt").write_text(f"data {i}")
    dstore = DataStoreDirectory(src, suffix="txt")
    out_dstore = DataStoreDirectory(tmp_path / "out", mode=Mode.w, suffix="txt")

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: DataMember) -> str:
            return val.read()

    @define_app(app_type=WRITER)
    class writer:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            return self.data_store.write(unique_id=identifier, data=data)

    process = reader() + writer(data_store=out_dstore)
    with pytest.raises(ValueError, match="non-unique identifier"):
        process.apply_to(
            dstore,
            id_from_source=lambda _: "same_id",
            logger=False,
            show_progress=False,
        )


def test_apply_to_skips_existing_items(tmp_path):
    """apply_to skips items already in the data store"""
    src = tmp_path / "src"
    src.mkdir()
    for i in range(3):
        (src / f"item_{i}.txt").write_text(f"data {i}")
    dstore = DataStoreDirectory(src, suffix="txt")
    out = tmp_path / "out"
    out_dstore = DataStoreDirectory(out, mode=Mode.w, suffix="txt")

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: DataMember) -> str:
            return val.read()

    @define_app(app_type=WRITER)
    class writer:
        def __init__(self, data_store):
            self.data_store = data_store

        def main(self, data: str, identifier: str = "") -> DataMember:
            return self.data_store.write(unique_id=identifier, data=data)

    process = reader() + writer(data_store=out_dstore)
    # pre-write one item so it already exists
    out_dstore.write(unique_id="item_0", data="existing")
    process.apply_to(dstore, logger=False, show_progress=False)
    # item_0 skipped, items 1 and 2 written → total 3
    assert len(out_dstore.completed) == 3


def test_triggers_bugcatcher():
    """a composable that does not trap failures returns NotCompletedResult
    requesting bug report"""

    @define_app(app_type=LOADER)
    class reader:
        def main(self, val: str) -> str:
            return val.read()

    read = reader()
    read.main = lambda x: None
    got = read("somepath.fasta")  # pylint: disable=not-callable
    assert isinstance(got, NotCompleted)
    assert got.type is NotCompletedType.BUG


@pytest.mark.parametrize("func", [str, repr])
def test_user_function_repr(func):
    @define_app
    def bar(val: float, num=3) -> float:
        return val * num

    got = func(bar(num=3))
    assert got == "bar(num=3)"


@pytest.fixture(params=[Path, None, str])
def source_type(DATA_DIR, request):
    fname = "brca1.fasta"
    dstore = open_data_store(DATA_DIR, suffix="fasta")
    if request.param is not None:
        return request.param(DATA_DIR / fname)
    return next((m for m in dstore if m.unique_id == fname), None)


@pytest.mark.cogent3
def test_composable_unwraps_source_proxy_as_completed(source_type):
    app = c3.get_app("load_unaligned", format_name="fasta", moltype="dna")
    result = next(iter(app.as_completed([source_type], show_progress=False)))
    got = result.source
    assert got.endswith("brca1.fasta")
    assert not isinstance(got, source_proxy)


@pytest.mark.cogent3
def test_composable_unwraps_source_proxy_call(source_type):
    app = c3.get_app("load_unaligned", format_name="fasta", moltype="dna")
    result = app(source_type)
    got = result.source
    assert got.endswith("brca1.fasta")
    assert not isinstance(got, source_proxy)


@pytest.mark.cogent3
def test_as_completed(DATA_DIR):
    """correctly applies iteratively"""
    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=3)
    reader = c3.get_app("load_unaligned", format_name="fasta", moltype="dna")
    got = list(reader.as_completed(dstore, show_progress=False))
    assert len(got) == len(dstore)
    # should also be able to apply the results to another composable func
    min_length = c3.get_app("sample.min_length", 10)
    got = list(min_length.as_completed(got, show_progress=False))
    assert len(got) == len(dstore)
    # should work on a chained function
    proc = reader + min_length
    got = list(proc.as_completed(dstore, show_progress=False))
    assert len(got) == len(dstore)
    # and works on a list of just strings
    got = list(proc.as_completed([str(m) for m in dstore], show_progress=False))
    assert len(got) == len(dstore)
    # or a single string
    path = str(Path(dstore[0].data_store.source) / dstore[0].unique_id)
    got = list(proc.as_completed(path, show_progress=False))
    assert len(got) == 1
    assert got[0].__class__.__name__.endswith("SequenceCollection")


@pytest.mark.cogent3
@pytest.mark.parametrize("data", [(), ("", "")])
def test_as_completed_empty_data(data):
    """correctly applies iteratively"""
    reader = c3.get_app("load_unaligned", format_name="fasta", moltype="dna")
    min_length = c3.get_app("sample.min_length", 10)
    proc = reader + min_length

    # returns empty input
    got = list(proc.as_completed(data))
    assert got == []


_as_completed_w_wout_source_params = [{"a": 2}]
if c3 is not None:
    _as_completed_w_wout_source_params.extend(
        [
            UnionDict(a=2, source="blah.txt"),
            c3.make_aligned_seqs(
                {"a": "ACGT"},
                info={"source": "blah.txt"},
                moltype="dna",
            ),
        ],
    )


@pytest.mark.cogent3
@pytest.mark.parametrize("data", _as_completed_w_wout_source_params)
def test_as_completed_w_wout_source(data):
    from cogent3.app.typing import AlignedSeqsType

    @define_app
    def pass_through(val: dict | UnionDict | AlignedSeqsType) -> dict:
        return val

    app = pass_through()  # pylint: disable=not-callable,no-value-for-parameter
    got = list(app.as_completed([data], show_progress=False))
    assert bool(got), got


@pytest.mark.cogent3
@pytest.mark.parametrize("klass", [DataStoreDirectory, DataStoreSqlite])
@pytest.mark.parametrize("cast", [str, Path])
def test_apply_to_strings(DATA_DIR, tmp_dir, klass, cast):
    """apply_to handles non-DataMember"""
    dname = "test_apply_to_strings"
    outpath = tmp_dir / dname

    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=3)
    dstore = [cast(str(m)) for m in dstore]
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    min_length = c3.get_app("min_length", 10)
    kwargs = {"suffix": "fasta"} if klass == DataStoreDirectory else {}
    writer = c3.get_app("write_seqs", data_store=klass(outpath, mode="w", **kwargs))
    process = reader + min_length + writer
    # create paths as strings
    _ = process.apply_to(dstore, show_progress=False)
    assert len(process.data_store.logs) == 1


@pytest.mark.cogent3
@pytest.mark.parametrize("klass", [DataStoreDirectory, DataStoreSqlite])
@pytest.mark.parametrize("cast", [str, Path])
def test_as_completed_strings(DATA_DIR, tmp_dir, klass, cast):
    """as_completed handles non-DataMember"""
    dname = "test_apply_to_strings"
    outpath = tmp_dir / dname

    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=3)
    dstore = [cast(str(m)) for m in dstore]
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    min_length = c3.get_app("min_length", 10)
    if klass == DataStoreDirectory:
        writer = c3.get_app("write_seqs", klass(outpath, mode="w", suffix="fasta"))
    else:
        writer = c3.get_app("write_seqs", klass(outpath, mode="w"))
    orig_length = len(writer.data_store)
    process = reader + min_length + writer
    # create paths as strings
    got = list(process.as_completed(dstore, show_progress=False))
    assert len(got) > orig_length


@pytest.mark.cogent3
def test_apply_to_non_unique_identifiers(tmp_dir):
    """should fail if non-unique names"""
    dstore = [
        "brca1.fasta",
        "brca1.fasta",
    ]
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    min_length = c3.get_app("min_length", 10)
    outpath = tmp_dir / "test_apply_to_non_unique_identifiers"
    writer = c3.get_app(
        "write_seqs",
        DataStoreDirectory(outpath, mode="w", suffix="fasta"),
    )
    process = reader + min_length + writer
    with pytest.raises(ValueError):
        process.apply_to(dstore)


@pytest.mark.cogent3
def test_apply_to_logging(DATA_DIR, tmp_dir):
    """correctly creates log file"""
    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=3)
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    min_length = c3.get_app("min_length", 10)
    out_dstore = open_data_store(tmp_dir / "delme.sqlitedb", mode="w")
    writer = c3.get_app("write_db", out_dstore)
    process = reader + min_length + writer
    process.apply_to(dstore, show_progress=False)
    # always creates a log
    assert len(process.data_store.logs) == 1


@pytest.mark.cogent3
def test_apply_to_logger(DATA_DIR, tmp_dir):
    """correctly uses user provided logger"""
    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=3)
    LOGGER = CachingLogger()
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    min_length = c3.get_app("min_length", 10)
    out_dstore = open_data_store(tmp_dir / "delme.sqlitedb", mode="w")
    writer = c3.get_app("write_db", out_dstore)
    process = reader + min_length + writer
    process.apply_to(dstore, show_progress=False, logger=LOGGER)
    assert len(process.data_store.logs) == 1


@pytest.mark.cogent3
def test_apply_to_no_logger(DATA_DIR, tmp_dir):
    """correctly uses user provided logger"""
    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=3)
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    min_length = c3.get_app("min_length", 10)
    out_dstore = open_data_store(tmp_dir / "delme.sqlitedb", mode="w")
    writer = c3.get_app("write_db", out_dstore)
    process = reader + min_length + writer
    process.apply_to(dstore, show_progress=False, logger=False)
    assert len(process.data_store.logs) == 0
    assert process.logger is None


@pytest.mark.cogent3
@pytest.mark.parametrize("logger_val", [True, "somepath.log"])
def test_apply_to_invalid_logger(DATA_DIR, tmp_dir, logger_val):
    """incorrect logger value raises TypeError"""
    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=3)
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    min_length = c3.get_app("min_length", 10)
    out_dstore = open_data_store(tmp_dir / "delme.sqlitedb", mode="w")
    writer = c3.get_app("write_db", out_dstore)
    process = reader + min_length + writer
    with pytest.raises(TypeError):
        process.apply_to(dstore, show_progress=False, logger=logger_val)


@pytest.fixture
def fasta_dir(DATA_DIR, tmp_dir):
    filenames = DATA_DIR.glob("*.fasta")
    fasta_dir = tmp_dir / "fasta"
    fasta_dir.mkdir(parents=True, exist_ok=True)
    for fn in filenames:
        dest = fasta_dir / fn.name
        dest.write_text(fn.read_text())
    return fasta_dir


@pytest.fixture
def log_data(DATA_DIR):
    path = DATA_DIR / "scitrack.log"
    return path.read_text()


@pytest.fixture
def ro_dstore(fasta_dir):
    return DataStoreDirectory(fasta_dir, suffix="fasta", mode="r")


@pytest.fixture
def completed_objects(ro_dstore):
    return {f"{Path(m.unique_id).stem}": m.read() for m in ro_dstore}


@pytest.fixture
def write_dir1(tmp_dir):
    write_dir1 = tmp_dir / "write1"
    write_dir1.mkdir(parents=True, exist_ok=True)
    yield write_dir1
    shutil.rmtree(write_dir1)


@pytest.fixture
def write_dir2(tmp_dir):
    write_dir2 = tmp_dir / "write2"
    write_dir2.mkdir(parents=True, exist_ok=True)
    yield write_dir2
    shutil.rmtree(write_dir2)


@pytest.fixture
def nc_objects():
    return {
        f"id_{i}": NotCompleted("ERROR", "location", "message", source=f"id_{i}")
        for i in range(3)
    }


@pytest.fixture
def nc_dstore(tmp_dir, nc_objects):
    dstore = DataStoreDirectory(tmp_dir, suffix="fasta", mode="w")
    for id_, data in nc_objects.items():
        dstore.write_not_completed(unique_id=id_, data=data.to_json())

    return dstore


@pytest.mark.cogent3
def test_apply_to_input_only_not_completed(DATA_DIR, nc_dstore, tmp_dir):
    """correctly skips notcompleted"""
    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=3)
    # trigger creation of notcompleted
    outpath = tmp_dir / "delme.sqlitedb"
    out_dstore = open_data_store(outpath, mode="w")
    writer = c3.get_app("write_db", out_dstore)
    process = (
        c3.get_app("load_aligned", format_name="fasta", moltype="dna")
        + c3.get_app("min_length", 3000)
        + writer
    )
    process.apply_to(dstore, show_progress=False)
    assert len(out_dstore.not_completed) == len(nc_dstore)


@pytest.mark.cogent3
def test_apply_to_makes_not_completed(DATA_DIR, tmp_dir):
    """correctly creates notcompleted"""
    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=3)
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    # trigger creation of notcompleted
    min_length = c3.get_app("min_length", 3000)
    out_dstore = open_data_store(tmp_dir / "delme.sqlitedb", mode="w")
    writer = c3.get_app("write_db", out_dstore)
    process = reader + min_length + writer
    process.apply_to(dstore, show_progress=False)
    assert len(out_dstore.not_completed) == 3


@pytest.mark.cogent3
def test_apply_to_not_partially_done(DATA_DIR, tmp_dir):
    """correctly applies process when result already partially done"""
    dstore = open_data_store(DATA_DIR, suffix="fasta")
    num_records = len(dstore)
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    out_dstore = open_data_store(tmp_dir / "delme.sqlitedb", mode="w")
    writer = c3.get_app("write_db", out_dstore)
    # doing the first one
    # turning off warning as apps are callable
    _ = writer(reader(dstore[0]))  # pylint: disable=not-callable
    writer.data_store.close()

    out_dstore = open_data_store(tmp_dir / "delme.sqlitedb", mode="a")
    writer = c3.get_app("write_db", out_dstore)
    process = reader + writer
    _ = process.apply_to(dstore, show_progress=False)
    assert len(out_dstore) == num_records


@pytest.fixture
def full_dstore(write_dir1, nc_objects, completed_objects, log_data):
    dstore = DataStoreDirectory(write_dir1, suffix="fasta", mode="w")
    for id_, data in nc_objects.items():
        dstore.write_not_completed(unique_id=id_, data=data.to_json())

    for id_, data in completed_objects.items():
        dstore.write(unique_id=id_, data=data)

    dstore.write_log(unique_id="scitrack.log", data=log_data)
    return dstore


# @pytest.mark.xfail(reason="passes except when run in full test suite")
@pytest.mark.cogent3
@pytest.mark.parametrize("show", [True, False])
def test_as_completed_progress(full_dstore, capsys, show):
    loader = c3.get_app("load_unaligned", format_name="fasta", moltype="dna")
    omit = c3.get_app("omit_degenerates")
    app = loader + omit
    list(app.as_completed(full_dstore.completed, show_progress=show))
    result = capsys.readouterr().err.splitlines()
    if show:
        assert len(result) > 0
        assert "100%" in result[-1]
    else:
        assert len(result) == 0


@pytest.mark.cogent3
def test_composite_pickleable():
    """composable functions should be pickleable"""

    read = c3.get_app("load_aligned", moltype="dna")
    dumps(read)
    trans = c3.get_app("select_translatable")
    dumps(trans)
    aln = c3.get_app("progressive_align", "nucleotide")
    dumps(aln)
    just_nucs = c3.get_app("omit_degenerates", moltype="dna")
    dumps(just_nucs)
    limit = c3.get_app("fixed_length", 1000, random=True)
    dumps(limit)
    mod = c3.get_app("model", "HKY85")
    dumps(mod)
    qt = c3.get_app("quick_tree")
    dumps(qt)
    proc = read + trans + aln + just_nucs + limit + mod
    dumps(proc)


@pytest.mark.cogent3
def test_user_function():
    """composable functions should be user definable"""

    @define_app
    def foo(val: c3types.AlignedSeqsType, *args, **kwargs) -> c3types.AlignedSeqsType:
        return val[:4]

    u_function = foo()

    aln = c3.make_aligned_seqs(
        [("a", "GCAAGCGTTTAT"), ("b", "GCTTTTGTCAAT")],
        moltype="dna",
    )
    got = u_function(aln)

    assert got.to_dict() == {"a": "GCAA", "b": "GCTT"}


@pytest.mark.cogent3
def test_user_function_without_arg_kwargs():
    """composable functions should be user definable"""

    @define_app
    def foo_without_arg_kwargs(
        val: c3types.AlignedSeqsType,
    ) -> c3types.AlignedSeqsType:
        return val[:4]

    u_function = foo_without_arg_kwargs()

    aln = c3.make_aligned_seqs(
        [("a", "GCAAGCGTTTAT"), ("b", "GCTTTTGTCAAT")],
        moltype="dna",
    )
    got = u_function(aln)

    assert got.to_dict() == {"a": "GCAA", "b": "GCTT"}


@pytest.mark.cogent3
def test_user_function_multiple():
    """user defined composable functions should not interfere with each other"""

    @define_app
    def foo(val: c3types.AlignedSeqsType, *args, **kwargs) -> c3types.AlignedSeqsType:
        return val[:4]

    @define_app
    def bar(val: c3types.AlignedSeqsType, num=3) -> c3types.PairwiseDistanceType:
        return val.distance_matrix(calc="hamming")

    u_function_1 = foo()
    u_function_2 = bar()

    aln_1 = c3.make_aligned_seqs(
        [("a", "GCAAGCGTTTAT"), ("b", "GCTTTTGTCAAT")],
        moltype="dna",
    )
    data = {"s1": "ACGTACGTA", "s2": "GTGTACGTA"}
    aln_2 = c3.make_aligned_seqs(data, moltype="dna")

    got_1 = u_function_1(aln_1)
    got_2 = u_function_2(aln_2)
    assert got_1.to_dict() == {"a": "GCAA", "b": "GCTT"}
    assert got_2 == {("s1", "s2"): 2.0, ("s2", "s1"): 2.0}


@pytest.mark.cogent3
def test_concat_not_composable():
    concat = c3.get_app("concat")
    assert not is_app_composable(concat)


@pytest.mark.cogent3
def test_cogent3_serialisable_compatible_with_serialisabletype(tmp_path):

    @define_app
    def foo(arg: str) -> c3types.TreeType:
        # we're checking composability only
        return c3.make_tree(arg, source="blah")

    foo_instance = foo()
    out = open_data_store(tmp_path / "out.sqlitedb", mode="w")
    writer = c3.get_app("write_db", data_store=out)
    loader = c3.get_app("load_db")
    app = foo_instance + writer
    result = app("(A:0.1,B:0.2);")  # pylint: disable=not-callable
    got = loader(result)
    assert not isinstance(got, NotCompleted)


@pytest.mark.cogent3
@pytest.mark.parametrize("klass", [DataStoreDirectory, DataStoreSqlite])
def test_apply_to_writes_citations(DATA_DIR, tmp_dir, klass):
    """apply_to writes citations to the data store"""
    cite = _make_cite(title="Test Citation")

    dstore = open_data_store(DATA_DIR, suffix="fasta", limit=2)
    reader = c3.get_app("load_aligned", format_name="fasta", moltype="dna")

    @define_app(cite=cite)
    class cited_step:
        def main(self, val: c3types.AlignedSeqsType) -> c3types.AlignedSeqsType:
            return val

    outpath = (
        tmp_dir / "cite_out"
        if klass == DataStoreDirectory
        else tmp_dir / "cite_out.sqlitedb"
    )
    kwargs = {"suffix": "fasta"} if klass == DataStoreDirectory else {}
    out_dstore = open_data_store(outpath, mode="w", **kwargs)
    writer = c3.get_app("write_seqs", data_store=out_dstore)

    process = reader + cited_step() + writer
    result_dstore = process.apply_to(dstore, show_progress=False)
    loaded = result_dstore._load_citations()
    assert len(loaded) >= 1
    titles = [c.title for c in loaded]
    assert "Test Citation" in titles


@pytest.fixture
def half_dstore1(write_dir1, nc_objects, completed_objects, log_data):
    dstore = DataStoreDirectory(write_dir1, suffix="fasta", mode="w")
    i = 0
    for id_, data in nc_objects.items():
        dstore.write_not_completed(unique_id=id_, data=data.to_json())
        i += 1
        if i >= len(nc_objects.items()) / 2:
            break
    i = 0
    for id_, data in completed_objects.items():
        dstore.write(unique_id=id_, data=data)
        i += 1
        if i >= len(completed_objects.items()) / 2:
            break
    dstore.write_log(unique_id="scitrack.log", data=log_data)
    return dstore


@pytest.fixture
def half_dstore2(write_dir2, nc_objects, completed_objects, log_data):
    dstore = DataStoreDirectory(write_dir2, suffix="fasta", mode="w")
    for i, (id_, data) in enumerate(nc_objects.items()):
        if i >= len(nc_objects.items()) / 2:
            dstore.write_not_completed(unique_id=id_, data=data.to_json())
    for i, (id_, data) in enumerate(completed_objects.items()):
        if i >= len(completed_objects.items()) / 2:
            dstore.write(unique_id=id_, data=data)
    dstore.write_log(unique_id="scitrack.log", data=log_data)
    return dstore


@pytest.mark.cogent3
def test_apply_to_only_appends(half_dstore1, half_dstore2):
    half_dstore1 = open_data_store(
        half_dstore1.source,
        suffix=half_dstore1.suffix,
        mode="a",
    )
    reader1 = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    min_length1 = c3.get_app("min_length", length=10)
    writer1 = c3.get_app("write_seqs", data_store=half_dstore1)
    process1 = reader1 + min_length1 + writer1

    # create paths as strings
    dstore1 = [
        str(Path(m.data_store.source) / m.unique_id) for m in half_dstore1.completed
    ]
    # check does not modify dstore when applied to same records
    orig_members = {m.unique_id for m in half_dstore1.members}
    got = process1.apply_to(dstore1)
    assert {m.unique_id for m in got.members} == orig_members

    half_dstore2 = open_data_store(
        half_dstore2.source,
        suffix=half_dstore2.suffix,
        mode="a",
    )

    reader2 = c3.get_app("load_aligned", format_name="fasta", moltype="dna")
    min_length2 = c3.get_app("min_length", length=10)
    writer2 = c3.get_app("write_seqs", data_store=half_dstore2)
    process2 = reader2 + min_length2 + writer2
    # check not fail on append new records
    _ = process2.apply_to(dstore1)
