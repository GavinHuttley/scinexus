"""defined type hints for app composability"""

# TODO write more extensive docstring explaining limited use of these types
from __future__ import annotations

from pathlib import Path
from types import UnionType
from typing import (
    Any,
    ForwardRef,
    Protocol,
    TypeVar,
    Union,
    get_args,
    get_origin,
    runtime_checkable,
)

from scinexus.data_store import DataMemberABC

NESTED_HINTS = (Union, UnionType, list, tuple, set)


@runtime_checkable
class HasSource(Protocol):
    @property
    def source(self) -> Any: ...


@runtime_checkable
class HasInfo(Protocol):
    @property
    def info(self) -> HasSource: ...


@runtime_checkable
class SerialisableType(Protocol):
    def to_rich_dict(self) -> dict: ...


IdentifierType = Union[str, Path, DataMemberABC]


def _resolve_name(name: str, module_globals: dict | None = None) -> type:
    """resolves a string name to a type, checking module_globals"""
    if module_globals and name in module_globals:
        result = module_globals[name]
        if isinstance(result, type):
            return result

    msg = f"cannot resolve type name {name!r}"
    raise TypeError(msg)


def resolve_type_hint(hint, module_globals=None):
    """walks a type hint tree and resolves all forward references to classes

    Parameters
    ----------
    hint
        a type hint (TypeVar, Union, ForwardRef, str, or concrete class)
    module_globals
        optional dict of the module where the hint was defined, used
        to resolve forward references from user code
    """
    # Protocol classes (like SerialisableType) -- return as-is
    if getattr(hint, "_is_protocol", False) and hint is not Protocol:
        return hint

    # TypeVar with __bound__ -> resolve bound class
    if isinstance(hint, TypeVar):
        if hint.__bound__:
            bound = hint.__bound__
            if isinstance(bound, str):
                bound = _resolve_name(bound, module_globals)
            elif isinstance(bound, ForwardRef):
                bound = _resolve_name(bound.__forward_arg__, module_globals)
            return bound
        if hint.__constraints__:
            resolved = tuple(
                resolve_type_hint(c, module_globals) for c in hint.__constraints__
            )
            return Union[resolved]  # type: ignore
        msg = f"unconstrained TypeVar {hint!r} cannot be resolved"
        raise TypeError(msg)

    # Union / UnionType -> recurse
    origin = get_origin(hint)
    if origin is Union or isinstance(hint, UnionType):
        args = tuple(resolve_type_hint(a, module_globals) for a in get_args(hint))
        return Union[args]  # type: ignore

    # Container types (list[X], tuple[X,Y], set[X])
    if origin in (list, tuple, set):
        args = tuple(resolve_type_hint(a, module_globals) for a in get_args(hint))
        return origin[args] if args else hint

    # ForwardRef
    if isinstance(hint, ForwardRef):
        return _resolve_name(hint.__forward_arg__, module_globals)

    # plain str
    return _resolve_name(hint, module_globals) if isinstance(hint, str) else hint


def get_type_display_names(hint) -> frozenset[str]:
    """extracts human-readable class names from a resolved type hint

    Parameters
    ----------
    hint
        a resolved type hint (one that has been through resolve_type_hint)
    """
    names: set[str] = set()
    origin = get_origin(hint)

    if origin is Union or isinstance(hint, UnionType) or origin in (list, tuple, set):
        for arg in get_args(hint):
            names |= get_type_display_names(arg)
    elif isinstance(hint, type):
        names.add(hint.__name__)
    elif isinstance(hint, TypeVar):
        # fallback for unresolved TypeVars -- shouldn't normally happen
        names.add(hint.__name__)

    return frozenset(names)


def _get_concrete_classes(hint) -> set[type]:
    """extracts concrete classes from a resolved type hint, walking Unions"""
    classes = set()
    origin = get_origin(hint)

    if origin is Union or isinstance(hint, UnionType):
        for arg in get_args(hint):
            classes |= _get_concrete_classes(arg)
    elif isinstance(hint, type):
        classes.add(hint)

    return classes


def _is_protocol(hint) -> bool:
    """checks if a type hint is or contains a runtime-checkable Protocol"""
    if getattr(hint, "_is_protocol", False) and hint is not Protocol:
        return True

    origin = get_origin(hint)
    if origin is Union or isinstance(hint, UnionType):
        return any(_is_protocol(a) for a in get_args(hint))

    return False


def check_type_compatibility(return_hint, input_hint) -> bool:
    """composition-time check: is the return type compatible with the input type?

    Parameters
    ----------
    return_hint
        resolved return type of the upstream app
    input_hint
        resolved input type of the downstream app

    Returns
    -------
    True if the types are compatible, False otherwise
    """
    # typing.Any is compatible with everything
    if return_hint is Any or input_hint is Any:
        return True

    # If either side is or contains a Protocol, be lenient -- runtime check_type
    # provides the real safety net
    if _is_protocol(return_hint) or _is_protocol(input_hint):
        return True

    return_classes = _get_concrete_classes(return_hint)
    input_classes = _get_concrete_classes(input_hint)

    # Check if any return class is a subclass of any input class (or vice versa)
    for ret_cls in return_classes:
        for inp_cls in input_classes:
            try:
                if issubclass(ret_cls, inp_cls) or issubclass(inp_cls, ret_cls):
                    return True
            except TypeError:
                # issubclass can fail for some types
                if ret_cls is inp_cls:
                    return True

    return False
