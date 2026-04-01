from gzip import GzipFile, compress

import pytest

import scinexus.misc as misc_module
from scinexus.misc import (
    docstring_to_summary_rest,
    extend_docstring_from,
    get_object_provenance,
    in_jupyter,
)


def test_not_in_jupyter():
    assert not in_jupyter()


def test_is_in_jupyter():
    misc_module.get_ipython = lambda: None
    try:
        assert in_jupyter()
    finally:
        del misc_module.get_ipython


def test_get_object_provenance_builtin_instance():
    assert get_object_provenance("abc") == "str"
    assert get_object_provenance({"a": 1}) == "dict"


def test_get_object_provenance_builtin_type():
    assert get_object_provenance(dict) == "dict"
    assert get_object_provenance(str) == "str"


def test_get_object_provenance_function():
    assert get_object_provenance(compress) == "gzip.compress"


def test_get_object_provenance_class():
    assert get_object_provenance(GzipFile) == "gzip.GzipFile"


def _source():
    """This is a source docstring."""


def test_extend_docstring_from_append():
    @extend_docstring_from(_source)
    def target():
        """I am target."""

    assert target.__doc__ == "This is a source docstring.\nI am target."


def test_extend_docstring_from_prepend():
    @extend_docstring_from(_source, pre=True)
    def target():
        """I am target."""

    assert target.__doc__ == "I am target.\nThis is a source docstring."


def test_extend_docstring_from_no_dest_doc():
    @extend_docstring_from(_source)
    def target():
        pass

    assert target.__doc__ == "This is a source docstring.\n"


def _foo1():
    """some text"""


def _foo2():
    """some text

    Notes
    -----
    body
    """


def _foo3():
    """
    Notes
    -----
    body
    """


def _foo4(): ...


@pytest.mark.parametrize(
    ("func", "sum_exp", "body_exp"),
    [
        (_foo1, "some text", []),
        (_foo2, "some text", ["Notes", "-----", "body"]),
        (_foo3, "", ["Notes", "-----", "body"]),
        (_foo4, "", []),
    ],
)
def test_docstring_to_summary_rest(func, sum_exp, body_exp):
    summary, body = docstring_to_summary_rest(func.__doc__)
    assert summary == sum_exp
    assert body.split() == body_exp
