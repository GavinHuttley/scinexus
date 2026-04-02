from pathlib import Path
from typing import Any, ForwardRef, TypeVar, Union, get_args

import pytest

from scinexus.data_store import DataMemberABC
from scinexus.typing import (
    IdentifierType,
    SerialisableType,
    _resolve_name,
    check_type_compatibility,
    get_type_display_names,
    resolve_type_hint,
)


def test_resolve_type_hint_concrete_class():
    """Concrete classes pass through unchanged"""
    resolved = resolve_type_hint(int)
    assert resolved is int


def test_resolve_type_hint_protocol():
    """Protocol classes pass through unchanged"""
    resolved = resolve_type_hint(SerialisableType)
    assert resolved is SerialisableType


def test_resolve_type_hint_union():
    """Union types are resolved recursively"""
    resolved = resolve_type_hint(IdentifierType)
    from typing import get_args

    args = set(get_args(resolved))
    assert args == {str, Path, DataMemberABC}


def test_resolve_type_hint_unconstrained_typevar():
    """unconstrained TypeVar raises TypeError"""
    T = TypeVar("T")
    with pytest.raises(TypeError, match="unconstrained TypeVar"):
        resolve_type_hint(T)


def test_resolve_type_hint_unresolvable():
    """Unresolvable string raises TypeError"""
    with pytest.raises(TypeError, match="cannot resolve"):
        resolve_type_hint("NoSuchType")


def test_resolve_type_hint_user_module_globals():
    """module_globals are checked first for resolution"""

    class MyCustomType:
        pass

    resolved = resolve_type_hint("MyCustomType", {"MyCustomType": MyCustomType})
    assert resolved is MyCustomType


def test_serialisable_type_not_isinstance():
    """objects without to_rich_dict do not satisfy SerialisableType"""
    assert not isinstance("hello", SerialisableType)
    assert not isinstance(42, SerialisableType)


def test_serialisable_type_custom_class():
    """custom class with to_rich_dict satisfies SerialisableType"""

    class MyObj:
        def to_rich_dict(self) -> dict:
            return {}

    assert isinstance(MyObj(), SerialisableType)


def test_get_type_display_names_concrete():
    """concrete class returns its name"""
    names = get_type_display_names(int)
    assert names == frozenset({"int"})


def test_get_type_display_names_union():
    """Union returns names of all constituents"""
    names = get_type_display_names(Union[str, int])  # noqa: UP007
    assert names == frozenset({"str", "int"})


def test_get_type_display_names_protocol():
    """Protocol returns its own name"""
    names = get_type_display_names(SerialisableType)
    assert names == frozenset({"SerialisableType"})


def test_get_type_display_names_typevar_fallback():
    """unresolved TypeVar returns its __name__"""
    T = TypeVar("T")
    names = get_type_display_names(T)
    assert names == frozenset({"T"})


def test_check_type_compatibility_protocol_input():
    """Protocol on input side is lenient"""
    assert check_type_compatibility(int, SerialisableType) is True


def test_check_type_compatibility_protocol_return():
    """Protocol on return side is lenient"""
    assert check_type_compatibility(SerialisableType, int) is True


def test_check_type_compatibility_incompatible():
    """incompatible concrete types"""
    assert check_type_compatibility(int, str) is False


@pytest.fixture
def broken_subclasscheck():
    """a class whose metaclass makes issubclass() raise TypeError"""

    class BadMeta(type):
        def __subclasscheck__(cls, subclass: Any):
            msg = "broken"
            raise TypeError(msg)

    class Weird(metaclass=BadMeta):
        pass

    return Weird


def test_check_type_compatibility_issubclass_typeerror_same(broken_subclasscheck):
    """issubclass TypeError with identity match returns True"""
    assert check_type_compatibility(broken_subclasscheck, broken_subclasscheck) is True


def test_check_type_compatibility_issubclass_typeerror_diff(broken_subclasscheck):
    """issubclass TypeError with different classes returns False"""
    assert check_type_compatibility(int, broken_subclasscheck) is False


def test_resolve_name_non_type_in_globals():
    """non-type value in module_globals falls through to TypeError"""
    with pytest.raises(TypeError, match="cannot resolve"):
        _resolve_name("foo", {"foo": 42})


def test_resolve_type_hint_typevar_bound():
    """TypeVar with bound resolves to the bound type"""
    T = TypeVar("T", bound=int)
    assert resolve_type_hint(T) is int


def test_resolve_type_hint_typevar_bound_str():
    """TypeVar with string bound resolved via module_globals"""

    class Custom:
        pass

    T = TypeVar("T", bound="Custom")
    resolved = resolve_type_hint(T, {"Custom": Custom})
    assert resolved is Custom


def test_resolve_type_hint_typevar_bound_forwardref():
    """TypeVar with ForwardRef bound resolved via module_globals"""

    class Custom:
        pass

    T = TypeVar("T", bound=ForwardRef("Custom"))
    resolved = resolve_type_hint(T, {"Custom": Custom})
    assert resolved is Custom


def test_resolve_type_hint_typevar_constraints():
    """TypeVar with constraints resolves to Union of constraints"""
    T = TypeVar("T", int, str)
    resolved = resolve_type_hint(T)
    assert set(get_args(resolved)) == {int, str}


def test_resolve_type_hint_union_type():
    """PEP 604 X | Y syntax resolves correctly"""
    resolved = resolve_type_hint(int | str)
    assert set(get_args(resolved)) == {int, str}


def test_resolve_type_hint_list():
    """list[int] resolves correctly"""
    resolved = resolve_type_hint(list[int])
    assert get_args(resolved) == (int,)


def test_resolve_type_hint_tuple():
    """tuple[str, int] resolves correctly"""
    resolved = resolve_type_hint(tuple[str, int])
    assert get_args(resolved) == (str, int)


def test_resolve_type_hint_set():
    """set[int] resolves correctly"""
    resolved = resolve_type_hint(set[int])
    assert get_args(resolved) == (int,)


def test_resolve_type_hint_forwardref():
    """ForwardRef resolves via module_globals"""
    resolved = resolve_type_hint(ForwardRef("int"), {"int": int})
    assert resolved is int


def test_get_type_display_names_list():
    """list[int] returns inner type names"""
    names = get_type_display_names(list[int])
    assert names == frozenset({"int"})


def test_get_type_display_names_tuple():
    """tuple[str, int] returns all inner type names"""
    names = get_type_display_names(tuple[str, int])
    assert names == frozenset({"str", "int"})


def test_check_type_compatibility_any_return():
    """Any as return type is compatible with anything"""
    assert check_type_compatibility(Any, int) is True


def test_check_type_compatibility_any_input():
    """Any as input type is compatible with anything"""
    assert check_type_compatibility(int, Any) is True


def test_check_type_compatibility_subclass():
    """bool is subclass of int, so they are compatible"""
    assert check_type_compatibility(bool, int) is True


def test_check_type_compatibility_union_subclass():
    """Union types with subclass relationship are compatible"""
    assert check_type_compatibility(Union[bool, str], int) is True


def test_check_type_compatibility_protocol_in_union():
    """Union containing a Protocol is lenient"""
    assert check_type_compatibility(Union[SerialisableType, int], str) is True
