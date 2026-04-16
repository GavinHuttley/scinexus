# Write a function app

*How to use `@define_app` on a plain function, specifying input/output type hints and turning function parameters into constructor arguments.*

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from collections.abc import Callable
from citeable import Software
from scinexus import define_app, AppType

my_cite = Software(
    author=["Doe, J", "Smith, A"],
    title="My Sequence Filter",
    year=2025,
    url="https://example.com/my-filter",
    version="0.1.0",
)

@define_app(
    app_type=AppType.GENERIC,  # (1)!
    cite=my_cite,  # (2)!
)
def my_app(val: str, convert: Callable[[str], str]) -> str:  # (4)!
    return convert(val)

app = my_app(str.upper)
print(app("hello world"))
"""
exec_codeblock(src=src, annotations=[
"We specify the `app_type` explicitly here.",
"We assign the citation in the class definition.",
"The function definition has type hints for it's first argument and its return type."
])
  ]]] -->
```python { linenums="1" notest }
from collections.abc import Callable
from citeable import Software
from scinexus import define_app, AppType

my_cite = Software(
    author=["Doe, J", "Smith, A"],
    title="My Sequence Filter",
    year=2025,
    url="https://example.com/my-filter",
    version="0.1.0",
)

@define_app(
    app_type=AppType.GENERIC,  # (1)!
    cite=my_cite,  # (2)!
)
def my_app(val: str, convert: Callable[[str], str]) -> str:  # (4)!
    return convert(val)

app = my_app(str.upper)
print(app("hello world"))

# HELLO WORLD
```

1. We specify the `app_type` explicitly here.
2. We assign the citation in the class definition.
3. The function definition has type hints for it's first argument and its return type.
<!-- [[[end]]] -->

!!! note

    Your function can only have one required argument. It can have any number of optional arguments.

    Pay attention to the order of arguments for the function! Every call to the app provides a new instance of `val`. Whereas `str.upper` is assigned to the variable `convert`. You can think of all of the other arguments as being arguments to a class constructor. Under the hood, `scinexus` caches these and injects them into each call of your function with new values of `val`.

