from collections.abc import Iterator
from pathlib import Path

import pytest

from scinexus.data_store import set_id_from_source


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
