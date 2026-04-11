from collections.abc import Iterator
from pathlib import Path

import pytest
from cogent3.app.typing import _get_resolution_namespace

from scinexus.data_store import set_id_from_source
from scinexus.typing import register_type_namespace

register_type_namespace(_get_resolution_namespace)


@pytest.fixture
def tmp_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("scinexus_tests")


@pytest.fixture(scope="session")
def DATA_DIR():
    return Path(__file__).parent / "data"


@pytest.fixture
def HOME_TMP_DIR(tmp_path):
    return tmp_path


@pytest.fixture
def reset_id_from_source() -> Iterator[None]:
    """Restore the default ID extractor after the test."""
    yield
    set_id_from_source(None)
