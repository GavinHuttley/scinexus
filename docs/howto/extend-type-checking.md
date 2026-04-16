# Extend type checking

*How to use `register_type_namespace` to make forward references from downstream packages resolvable at composition time, enabling third-party types in app pipelines.*

## The problem

When you compose apps with `+`, `scinexus` checks that the output type of the left app is compatible with the input type of the right app. Type hints are often written as forward references — strings like `"Alignment"` or `"PhyloNode"` — to avoid circular imports. At composition time `scinexus` must resolve these strings to actual classes, but it only knows about its own types by default. If your package defines custom types used in app hints, `scinexus` cannot resolve them without help.

## The solution

`register_type_namespace` lets a downstream package register a **lazy namespace provider** — a zero-argument callable that returns a `dict[str, type]`. When `scinexus` encounters an unresolved forward reference, it queries each registered provider in order until it finds a match.

```python { notest }
from scinexus.typing import register_type_namespace

register_type_namespace(my_provider)
```

The provider is called lazily each time a name needs resolving, so the package can defer heavy imports. Providers are responsible for their own caching. Registration is idempotent: re-registering the same callable is a no-op.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus.typing import register_type_namespace

_resolution_ns = None

def _get_resolution_namespace():
    global _resolution_ns
    if _resolution_ns is not None:
        return _resolution_ns

    from cogent3.core.alignment import Alignment, SequenceCollection  # (1)!
    from cogent3.core.tree import PhyloNode
    # ... other imports ...

    _resolution_ns = {
        "Alignment": Alignment,
        "SequenceCollection": SequenceCollection,
        "PhyloNode": PhyloNode,  # (3)!
        # ... other types ...
    }
    return _resolution_ns

register_type_namespace(_get_resolution_namespace)  # (2)!
"""
exec_codeblock(src=src, admonition='???- example "How `cogent3` does it"',
annotations=["`cogent3` defines many types (`Alignment`, `PhyloNode`, `Table`, etc.) that are used as forward references in app type hints.",
"In `cogent3/app/typing.py`, a resolution namespace is built lazily and registered with `scinexus`",
'With this registration, any `scinexus` app that uses `"PhyloNode"` as a type hint will resolve correctly at composition time without the user importing `PhyloNode` explicitly.'
])
  ]]] -->
???- example "How `cogent3` does it"

    ```python { linenums="1" notest }
    from scinexus.typing import register_type_namespace

    _resolution_ns = None

    def _get_resolution_namespace():
        global _resolution_ns
        if _resolution_ns is not None:
            return _resolution_ns

        from cogent3.core.alignment import Alignment, SequenceCollection  # (1)!
        from cogent3.core.tree import PhyloNode
        # ... other imports ...

        _resolution_ns = {
            "Alignment": Alignment,
            "SequenceCollection": SequenceCollection,
            "PhyloNode": PhyloNode,  # (3)!
            # ... other types ...
        }
        return _resolution_ns

    register_type_namespace(_get_resolution_namespace)  # (2)!
    ```

    1. `cogent3` defines many types (`Alignment`, `PhyloNode`, `Table`, etc.) that are used as forward references in app type hints.
    2. In `cogent3/app/typing.py`, a resolution namespace is built lazily and registered with `scinexus`
    3. With this registration, any `scinexus` app that uses `"PhyloNode"` as a type hint will resolve correctly at composition time without the user importing `PhyloNode` explicitly.
<!-- [[[end]]] -->


## Registering your own package's types

Follow the same pattern: define a lazy provider function that imports and caches your types, then register it at module level.

```python { notest }
from scinexus.typing import register_type_namespace

_ns = None


def _get_my_types():
    global _ns
    if _ns is not None:
        return _ns

    from my_package.core import MyDataType, MyResultType

    _ns = {
        "MyDataType": MyDataType,
        "MyResultType": MyResultType,
    }
    return _ns


register_type_namespace(_get_my_types)
```

Place this in a module that is imported early (e.g. your package's `typing.py` or `__init__.py`). Once registered, apps using `"MyDataType"` as a forward reference will resolve correctly when composed with other apps.
