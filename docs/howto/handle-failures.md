# Handle failures

*How to create `NotCompleted` values, check their truthiness, inspect their attributes, and control propagation with `skip_not_completed=False`.*

## `NotCompleted` FALSE type

A FALSE type is returned when a condition is not met. For example, below we create an app that selects 2 specific sequences from an alignment. Applying this to a data set where a "Mouse" sequence does not exist produces a FALSE type.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
import cogent3 as c3

aln = c3.get_dataset("primate-brca1")
select_seqs = c3.get_app("take_named_seqs", "Mouse", "Human")
result = select_seqs(aln)
assert result == False
print(result)
"""
exec_codeblock(src=src)
  ]]] -->
```python { linenums="1" notest }
import cogent3 as c3

aln = c3.get_dataset("primate-brca1")
select_seqs = c3.get_app("take_named_seqs", "Mouse", "Human")
result = select_seqs(aln)
assert result == False
print(result)

# NotCompleted(type=FAIL, origin=take_named_seqs, source="brca1", message="named
# seq(s) {'Mouse'} not in ('FlyingLem', 'TreeShrew', 'Galago', 'HowlerMon',
# 'Rhesus', 'Orangutan', 'Gorilla', 'Chimpanzee', 'Human')")
```
<!-- [[[end]]] -->

## Inspecting `NotCompleted` attributes

The `NotCompleted` instance has attributes identifying what data failed:

```python { notest }
result.source
```

Where the failure occurred:

```python { notest }
result.origin
```

And the reason for the failure:

```python { notest }
result.message
```

The `.type` attribute is the `NotCompletedType` enum value (e.g. `NotCompletedType.FALSE`, `NotCompletedType.ERROR`, or `NotCompletedType.BUG`).

## `NotCompleted` ERROR type

An ERROR type is returned if an unexpected condition occurs, such as an exception raised during execution. Here we illustrate this by trying to open a file with an incorrect path.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
import cogent3 as c3

reader = c3.get_app("load_aligned", moltype="dna")
result = reader("primate_brca1.fasta")
print(result)
"""
exec_codeblock(src=src, max_lines=4, admonition='???- example "Example"')
]]] -->
???- example "Example"

    ```python { linenums="1" notest }
    import cogent3 as c3

    reader = c3.get_app("load_aligned", moltype="dna")
    result = reader("primate_brca1.fasta")
    print(result)

    # NotCompleted(type=ERROR, origin=load_aligned, source="primate_brca1",
    # message="Traceback (most recent call last):   File
    # "/Users/gavin/repos/SciNexus/src/scinexus/composable.py", line 525, in __call__
    # result = self.main(val, *args, **kwargs)   File [...]
    ```
<!-- [[[end]]] -->

## Composed functions propagate `NotCompleted` results

If you have a composed function with multiple steps and a failure occurs, the resulting `NotCompleted` is returned without any of the subsequent steps being applied. For example, we make a composed app from both of the above apps:

<!-- [[[cog
from cog_utils import exec_codeblock, DOCS_DIR
import cogent3 as c3

aln = c3.get_dataset("primate-brca1")
aln.write(DOCS_DIR / "data/primate_brca1.fasta")

src = \
"""
import cogent3 as c3

reader = c3.get_app("load_aligned", moltype="dna")
select_seqs = c3.get_app("take_named_seqs", "Mouse", "Human")
app = reader + select_seqs
result = app("data/primate_brca1.fasta")
print(result)
"""
exec_codeblock(src=src, max_lines=4, admonition='???- example "Example"')
]]] -->
???- example "Example"

    ```python { linenums="1" notest }
    import cogent3 as c3

    reader = c3.get_app("load_aligned", moltype="dna")
    select_seqs = c3.get_app("take_named_seqs", "Mouse", "Human")
    app = reader + select_seqs
    result = app("data/primate_brca1.fasta")
    print(result)

    # NotCompleted(type=FAIL, origin=take_named_seqs, source="primate_brca1",
    # message="named seq(s) {'Mouse'} not in ('FlyingLem', 'TreeShrew', 'Galago',
    # 'HowlerMon', 'Rhesus', 'Orangutan', 'Gorilla', 'Chimpanzee', 'Human')")
    ```
<!-- [[[end]]] -->

The failure originated in `select_seqs` (an instance of `take_named_seqs`), and `reader` ran successfully — but the `NotCompleted` propagated through the rest of the pipeline.

<!-- [[[cog
from cog_utils import exec_codeblock
src = \
"""
import cogent3 as c3

reader = c3.get_app("load_aligned", moltype="dna")
select_seqs = c3.get_app("take_named_seqs", "Mouse", "Human")
app = reader + select_seqs
result = app("primate_brca1.fasta")
print(result)
"""
exec_codeblock(src=src, max_lines=1, admonition='???- example "Example"')
]]] -->
???- example "Example"

    ```python { linenums="1" notest }
    import cogent3 as c3

    reader = c3.get_app("load_aligned", moltype="dna")
    select_seqs = c3.get_app("take_named_seqs", "Mouse", "Human")
    app = reader + select_seqs
    result = app("primate_brca1.fasta")
    print(result)

    # NotCompleted(type=ERROR, origin=load_aligned, source="primate_brca1", [...]
    ```
<!-- [[[end]]] -->

Here the failure originated in `reader` (bad path), and `select_seqs` was never called.

## Creating `NotCompleted` in your own apps

You can return a `NotCompleted` from your own app to signal that a particular input cannot be processed:

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus import define_app, NotCompleted, NotCompletedType


@define_app
def require_min_length(val: str, min_length: int = 10) -> str:
    if len(val) < min_length:
        return NotCompleted(
            NotCompletedType.FALSE,
            "require_min_length",
            val,
            message=f"too short: {len(val)} < {min_length}",
        )
    return val
"""
exec_codeblock(src=src)
  ]]] -->
```python { linenums="1" notest }
from scinexus import define_app, NotCompleted, NotCompletedType


@define_app
def require_min_length(val: str, min_length: int = 10) -> str:
    if len(val) < min_length:
        return NotCompleted(
            NotCompletedType.FALSE,
            "require_min_length",
            val,
            message=f"too short: {len(val)} < {min_length}",
        )
    return val
```
<!-- [[[end]]] -->

## Receiving `NotCompleted` with `skip_not_completed=False`

By default, apps skip `NotCompleted` inputs — they propagate without calling `main()`. If your app needs to see `NotCompleted` values (e.g. a writer that records failures), set `skip_not_completed=False`:

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus import define_app, NotCompleted


@define_app(skip_not_completed=False)
def log_failures(val: str) -> str:
    if isinstance(val, NotCompleted):
        print(f"Failure: {val.message}")
        return val
    return val
"""
exec_codeblock(src=src)
  ]]] -->
```python { linenums="1" notest }
from scinexus import define_app, NotCompleted


@define_app(skip_not_completed=False)
def log_failures(val: str) -> str:
    if isinstance(val, NotCompleted):
        print(f"Failure: {val.message}")
        return val
    return val
```
<!-- [[[end]]] -->
