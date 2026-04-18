from collections.abc import Iterator
from pathlib import Path

import pytest

from scinexus.data_store import set_id_from_source
from scinexus.parallel import set_parallel_backend
from scinexus.typing import register_type_namespace

try:
    from cogent3.app.typing import _get_resolution_namespace

    register_type_namespace(_get_resolution_namespace)
except ImportError:
    pass


@pytest.fixture
def tmp_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("scinexus_tests")


@pytest.fixture(scope="session")
def DATA_DIR():
    return Path(__file__).parent / "data"


@pytest.fixture
def HOME_TMP_DIR() -> Iterator[Path]:
    """makes a temporary directory"""
    import tempfile

    home = Path("~")
    with tempfile.TemporaryDirectory(dir=home.expanduser()) as dn:
        yield home / dn


@pytest.fixture
def reset_id_from_source() -> Iterator[None]:
    """Restore the default ID extractor after the test."""
    yield
    set_id_from_source(None)


@pytest.fixture
def reset_parallel_backend() -> Iterator[None]:
    """Restore the default parallel backend after the test."""
    yield
    set_parallel_backend(None)
