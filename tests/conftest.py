from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def DATA_DIR():
    return Path(__file__).parent / "data"


@pytest.fixture
def HOME_TMP_DIR(tmp_path):
    return tmp_path
