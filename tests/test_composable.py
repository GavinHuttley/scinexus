import inspect
import pickle
from copy import copy
from pickle import dumps, loads
from unittest.mock import Mock

import pytest
from citeable import Software
from numpy import array, ndarray

from scinexus import typing as snx_types
from scinexus.composable import (
    NON_COMPOSABLE,
    WRITER,
    ComposableApp,
    NotCompleted,
    NotCompletedType,
    _get_raw_hints,
    define_app,
    is_app,
)


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
    new = loads(p)  # noqa: S301
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
    u = pickle.loads(pickle.dumps(sqd))  # noqa: S301
    assert u(4) == 16


def test_roundtrip_composed_app():
    """composed app can be pickled/unpickled"""
    composed = func2app(exponent=2) + float2int()
    u = pickle.loads(pickle.dumps(composed))  # noqa: S301
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
