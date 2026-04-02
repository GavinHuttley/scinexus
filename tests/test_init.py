import scinexus
from scinexus.progress import NoProgress


def test_get_progress():
    result = scinexus.get_progress()
    assert isinstance(result, NoProgress)


def test_set_default_progress():
    scinexus.set_default_progress(None)
