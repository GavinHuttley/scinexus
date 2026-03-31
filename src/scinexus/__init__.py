"""scinexus: composable app infrastructure for scientific computing."""

from scinexus._version import __version__
from scinexus.composable import (
    NotCompleted,
    define_app,
    is_app,
    is_app_composable,
)


def open_data_store(*args, **kwargs):
    from scinexus.io import open_data_store as _open_data_store

    return _open_data_store(*args, **kwargs)
