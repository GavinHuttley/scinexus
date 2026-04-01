import json

import pytest

from scinexus.composable import NotCompleted
from scinexus.deserialise import (
    deserialise_object,
    get_class,
    register_deserialiser,
    str_to_version,
)


def test_deserialise_python_builtins():
    """any object that does not contain a type key is returned as is"""
    data = {"a": 123, "b": "text"}
    jdata = json.dumps(data)
    got = deserialise_object(jdata)
    assert got == data
    data = range(4)
    got = deserialise_object(data)
    assert got is data


def test_custom_deserialiser():
    """correctly registers a function to inflate a custom object"""

    @register_deserialiser("test_myfunkydata")
    def astuple(data):
        data.pop("type")
        return tuple(data["data"])

    orig = {"type": "test_myfunkydata", "data": [1, 2, 3]}
    txt = json.dumps(orig)
    got = deserialise_object(txt)
    assert got == (1, 2, 3)
    assert isinstance(got, tuple)

    with pytest.raises(TypeError):

        @register_deserialiser(42)
        def bad(data):
            return data


def test_register_deserialiser_duplicate():
    """raises ValueError for duplicate type strings"""
    register_deserialiser("test_unique_type_xyz")(lambda d: d)
    with pytest.raises(ValueError, match="already in"):
        register_deserialiser("test_unique_type_xyz")(lambda d: d)


def test_not_completed_deserialise():
    """correctly reconstructs a NotCompleted object"""
    val = NotCompleted("ERROR", "nothing", "some error", source="here")
    expect = val.to_rich_dict()
    j = val.to_json()
    got = deserialise_object(j)
    assert got.to_rich_dict() == expect


def test_deserialise_from_file(tmp_path):
    """correctly deserialises from a json file"""
    val = NotCompleted("ERROR", "nothing", "some error", source="here")
    j = val.to_json()
    outpath = tmp_path / "test.json"
    outpath.write_text(j)
    got = deserialise_object(outpath)
    assert got.to_rich_dict() == val.to_rich_dict()


def test_deserialise_unknown_type():
    """raises NotImplementedError for unknown type"""
    data = {"type": "completely.unknown.Type", "data": 42}
    with pytest.raises(NotImplementedError, match="completely.unknown.Type"):
        deserialise_object(data)


def test_get_class():
    """correctly imports a class from provenance string"""
    klass = get_class("scinexus.composable.NotCompleted")
    assert klass is NotCompleted


def test_get_class_invalid():
    """raises ValueError for invalid provenance string"""
    with pytest.raises(ValueError, match="invalid provenance"):
        get_class("nodotshere")


def test_str_to_version():
    """correctly parses version strings"""
    got = str_to_version("2024.5.8a9")
    assert isinstance(got, tuple)
    assert len(got) > 0

    got = str_to_version("1.2.3")
    assert got == ()
