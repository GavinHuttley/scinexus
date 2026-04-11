import pytest

import scinexus
from scinexus.progress import NoProgress


def test_get_progress():
    result = scinexus.get_progress()
    assert isinstance(result, NoProgress)


def test_set_default_progress():
    scinexus.set_default_progress(None)


def test_lazy_import_open_data_store():
    assert scinexus.open_data_store is not None


def test_lazy_import_set_summary_display():
    assert callable(scinexus.set_summary_display)


def test_lazy_import_get_summary_display():
    assert callable(scinexus.get_summary_display)


def test_lazy_import_nonexistent():
    with pytest.raises(AttributeError, match="no attribute"):
        scinexus.no_such_attribute  # noqa: B018


def test_lazy_open_data_store():
    f = scinexus.open_data_store
    assert callable(f)
