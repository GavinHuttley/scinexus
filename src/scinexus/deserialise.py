import json
import re
from collections.abc import Callable
from importlib import import_module
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

if TYPE_CHECKING:  # pragma: no cover
    from scinexus.io_util import PathType

P = ParamSpec("P")
R = TypeVar("R")

_deserialise_func_map: dict[str, Callable[..., Any]] = {}


class register_deserialiser:
    """
    registration decorator for functions to inflate objects that were
    serialised using json.

    Functions are added to a dict which is used by the deserialise_object()
    function. The type string(s) must uniquely identify the appropriate
    value for the dict 'type' entry, e.g. 'scinexus.composable.NotCompleted'.

    Parameters
    ----------
    args: str or sequence of str
        must be unique
    """

    def __init__(self, *args: str) -> None:
        for type_str in args:
            if not isinstance(type_str, str):
                msg = f"{type_str!r} is not a string"
                raise TypeError(msg)
            if type_str in _deserialise_func_map:
                msg = f"{type_str!r} already in {list(_deserialise_func_map)}"
                raise ValueError(msg)
        self._type_str = args

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        for type_str in self._type_str:
            _deserialise_func_map[type_str] = func
        return func


def get_class(provenance: str) -> type:
    index = provenance.rfind(".")
    if index <= 0:
        msg = (
            f"invalid provenance string {provenance!r}, expected 'module.class' format"
        )
        raise ValueError(msg)
    klass = provenance[index + 1 :]
    nc = "NotCompleted"
    klass = nc if nc in klass else klass
    mod = import_module(provenance[:index])
    return getattr(mod, klass)


_pat = re.compile("[a-z]")


def str_to_version(v: str) -> tuple[str, ...]:
    if letter := _pat.search(v):
        return tuple(
            f"{v[: letter.start()]}.{letter.group()}.{letter.end():}".split(".")
        )
    return ()


def deserialise_object(data: "PathType | str | dict[str, Any]") -> Any:
    """
    deserialises from json

    Parameters
    ----------
    data
        path to json file, json string or a dict

    Returns
    -------
    If the dict from json.loads does not contain a "type" key, the object will
    be returned as is. Otherwise, it will be deserialised to the original object.

    Notes
    -----
    The value of the "type" key is used to identify the specific function for recreating
    the original instance.
    """
    from scinexus.io_util import open_, path_exists

    if path_exists(path := cast("PathType", data)):
        with open_(path) as infile:
            data = json.load(infile)

    if isinstance(data, str):
        data = json.loads(str(data))

    data = cast("dict[str, Any]", data)
    type_ = data.get("type", None) if hasattr(data, "get") else None
    if type_ is None:
        return data

    for type_str, func in _deserialise_func_map.items():
        if type_str in type_:
            break
    else:
        msg = f"deserialising '{type_}' from json"
        raise NotImplementedError(msg)

    return func(data)
