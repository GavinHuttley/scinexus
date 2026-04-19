# Write a class app

!!! abstract ""

    How to inherit from `scinexus` app base classes, or using the `define_app` decorator, specifying input/output type hints.

## Using inheritance from a base class

```python linenums="1"
from collections.abc import Callable
from citeable import Software
from scinexus import ComposableApp

my_cite = Software(
    author=["Doe, J", "Smith, A"],
    title="My Sequence Filter",
    year=2025,
    url="https://example.com/my-filter",
    version="0.1.0",
)


class my_app(  # (1)!
    ComposableApp[str, str],  # (2)!
    cite=my_cite,  # (3)!
):
    def __init__(self, convert: Callable[[str], str]):
        self.convert = convert

    def main(self, val: str) -> str:  # (4)!
        return self.convert(val)
```

1. We suggest naming your apps using the PEP8 naming style for functions (lowercase separated by underscores) because the instances will be used like functions.
2. We type hint the input / output types with the base class.
3. We assign the citation in the class definition.
4. The class has a `main()` method with type hints specified for its first argument and its return type.

## Using the `define_app` decorator

How to use `@define_app` on a class with a `main()` method, configure it via `__init__` parameters, and control behaviour with the `app_type` parameter.

```python { linenums="1" notest }
from collections.abc import Callable
from citeable import Software
from scinexus import ComposableApp

my_cite = Software(
    author=["Doe, J", "Smith, A"],
    title="My Sequence Filter",
    year=2025,
    url="https://example.com/my-filter",
    version="0.1.0",
)


@define_app(cite=my_cite)  # (1)!
class my_app:
    def __init__(self, convert: Callable[[str], str]):
        self.convert = convert

    def main(self, val: str) -> str:
        return self.convert(val)
```

1. The `define_app` decorator is used (line 5).
2. The class has a `main()` method (line 10).
3. Type hints are specified for `main()`'s first argument and its return type (line 10).

Instantiate with parameters (line 13), then call on data (line 17).

### Specifying the app type

The `define_app` decorator has a default `app_type` of `"generic"`. This means the app does data transformation and does not load or write data. The supported app types are indicated by the `AppType` enum:

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus.composable import AppType

print(list(AppType))
"""
exec_codeblock(src=src)
  ]]] -->
```python { linenums="1" notest }
from scinexus.composable import AppType

print(list(AppType))

# [<AppType.LOADER: 'loader'>, <AppType.WRITER: 'writer'>, <AppType.GENERIC:
# 'generic'>, <AppType.NON_COMPOSABLE: 'non_composable'>]
```
<!-- [[[end]]] -->

If your app is not intended to be composed sequentially with other apps, set it to non-composable:

```python
from scinexus import define_app, AppType


@define_app(app_type=AppType.NON_COMPOSABLE)
class my_standalone_app:
    def main(self, val: str) -> str:
        return val.upper()
```

## Handling `NotCompleted` values

By default, apps skip `NotCompleted` inputs — they propagate through the pipeline without calling `main()`. If your app needs access to `NotCompleted` instances (e.g. you are developing a writer that records failures), set `skip_not_completed=False`:

```python
from scinexus import define_app, NotCompleted


@define_app(skip_not_completed=False)
class my_writer:
    def main(self, val: str) -> str:
        if isinstance(val, NotCompleted):
            # handle the failure
            ...
        return val
```
