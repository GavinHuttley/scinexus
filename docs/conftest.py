from scinexus.typing import register_type_namespace

try:
    from cogent3.app.typing import _get_resolution_namespace

    register_type_namespace(_get_resolution_namespace)
except ImportError:
    pass
