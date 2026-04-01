"""Selected utility functions."""

import inspect
import re
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

_wout_period = re.compile(r"^\.")

P = ParamSpec("P")
R = TypeVar("R")


def get_object_provenance(obj: object) -> str:
    """returns string of complete object provenance"""
    # algorithm inspired by Greg Baacon's answer to
    # https://stackoverflow.com/questions/2020014/get-fully-qualified-class
    # -name-of-an-object-in-python
    if isinstance(obj, type) or inspect.isfunction(obj):
        mod = obj.__module__
        name = obj.__name__
    else:
        mod = obj.__class__.__module__
        name = obj.__class__.__name__

    return name if mod is None or mod == "builtins" else f"{mod}.{name}"


def extend_docstring_from(
    source: object, pre: bool = False
) -> "Callable[[Callable[P, R]], Callable[P, R]]":
    def docstring_inheriting_decorator(dest):
        parts = [source.__doc__ or "", dest.__doc__ or ""]
        # trim leading/trailing blank lines from parts
        for i, part in enumerate(parts):
            part = part.split("\n")
            if not part[0].strip():
                part.pop(0)
            if part and not part[-1].strip():
                part.pop(-1)

            parts[i] = "\n".join(part)

        if pre:
            parts.reverse()
        dest.__doc__ = "\n".join(parts)
        return dest

    return docstring_inheriting_decorator


_doc_block = re.compile(
    r"^\s*(Parameters|Notes|Raises)",
    flags=re.IGNORECASE | re.MULTILINE,
)


def docstring_to_summary_rest(text: str) -> tuple[str, str]:
    """separates the summary at the start of a docstring from the rest

    Notes
    -----
    Assumes numpydoc style.
    """
    if not text:
        return "", ""

    pos = _doc_block.search(text)
    if pos is None:
        return text, ""

    summary = text[: pos.start()].rstrip()
    text = text[pos.start() :]
    return summary, text.lstrip("\n").rstrip(" ")


def in_jupyter() -> bool:
    """whether code is being executed within a jupyter notebook"""
    val = True
    try:
        # primitive approach, just check whether the following function
        # is in the namespace
        get_ipython  # type: ignore[name-defined]  # noqa: B018
    except NameError:
        val = False

    return val
