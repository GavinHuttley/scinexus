from pathlib import Path
from typing import TypeVar, Union

import pytest

from scinexus.data_store import DataMemberABC
from scinexus.typing import (
    IdentifierType,
    SerialisableType,
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
        def __subclasscheck__(cls, subclass):
            raise TypeError("broken")

    class Weird(metaclass=BadMeta):
        pass

    return Weird


def test_check_type_compatibility_issubclass_typeerror_same(broken_subclasscheck):
    """issubclass TypeError with identity match returns True"""
    assert check_type_compatibility(broken_subclasscheck, broken_subclasscheck) is True


def test_check_type_compatibility_issubclass_typeerror_diff(broken_subclasscheck):
    """issubclass TypeError with different classes returns False"""
    assert check_type_compatibility(int, broken_subclasscheck) is False
