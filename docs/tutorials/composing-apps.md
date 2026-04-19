# Composing apps

!!! abstract ""

    Compose multiple apps into a single pipeline using the `+` operator, see what happens when types don't match, and observe how `NotCompleted` propagates through a pipeline without raising exceptions.

## Why compose?

Consider an app that performs a molecular evolutionary analysis (`fit_model`) and another that extracts statistics from the result (`extract_stats`). You could apply them sequentially:

```python { notest }
fitted = fit_model(alignment)
stats = extract_stats(fitted)
```

Composability simplifies this into a single callable:

```python { notest }
app = fit_model + extract_stats
stats = app(alignment)
```

You can have many more apps in a composed function than just two.

## A worked example

We compose three apps: a loader, a processor, and a writer.

```python { notest }
from cogent3 import get_app
from scinexus import open_data_store

out_dstore = open_data_store(path_to_dir, suffix="fa", mode="w")

loader = get_app("load_aligned", format_name="fasta", moltype="dna")
cpos3 = get_app("take_codon_positions", 3)
writer = get_app("write_seqs", out_dstore, format_name="fasta")
```

### Using apps sequentially

```python { notest }
data = loader("data/primate_brca1.fasta")
just3rd = cpos3(data)
m = writer(just3rd)
```

### Composing into a single pipeline

```python { notest }
process = loader + cpos3 + writer
m = process("data/primate_brca1.fasta")
```

The result is identical, but the composed form is more concise and enables batch processing via `apply_to()`.

## Composability rules

### App type ordering

Loaders and writers are special cases. If included, a loader must always be first:

```python { notest }
app = a_loader + a_generic
```

If included, a writer must always be last:

```python { notest }
app = a_generic + a_writer
```

Changing the order for either will raise a `TypeError`.

### Type compatibility

Apps define the type of input they accept and the type of output they produce. For two apps to be composed, the output type of the app on the left must overlap with the input type of the app on the right. If they don't match, a `TypeError` is raised.

## `NotCompleted` propagation

If any step in a composed pipeline returns a `NotCompleted`, subsequent steps are skipped and the `NotCompleted` is returned as the final result.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from cogent3 import get_app

reader = get_app("load_aligned", format_name="fasta")
select_seqs = get_app("take_named_seqs", "Mouse", "Human")
app = reader + select_seqs

result = app("data/primate_brca1.fasta")
print(result)  # (1)!
"""
exec_codeblock(src=src,
admonition='???+ example "Condition not satisfied"',
max_lines=4,
annotations=[
"A successful load but a failed selection — the `NotCompleted` from `select_seqs` is returned "
]
)
  ]]] -->
???+ example "Condition not satisfied"

    ```python { linenums="1" notest }
    from cogent3 import get_app

    reader = get_app("load_aligned", format_name="fasta")
    select_seqs = get_app("take_named_seqs", "Mouse", "Human")
    app = reader + select_seqs

    result = app("data/primate_brca1.fasta")
    print(result)  # (1)!

    # NotCompleted(type=FAIL, origin=take_named_seqs, source="primate_brca1",
    # message="named seq(s) {'Mouse'} not in ('FlyingLem', 'TreeShrew', 'Galago',
    # 'HowlerMon', 'Rhesus', 'Orangutan', 'Gorilla', 'Chimpanzee', 'Human')")
    ```

    1. A successful load but a failed selection — the `NotCompleted` from `select_seqs` is returned 
<!-- [[[end]]] -->


<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from cogent3 import get_app

reader = get_app("load_aligned", format_name="fasta")
select_seqs = get_app("take_named_seqs", "Mouse", "Human")
app = reader + select_seqs

result = app("primate_brca1.fasta")
print(result)  # (1)!
"""
exec_codeblock(src=src,
admonition='???+ example "Caught an exception"',
annotations=[
"An error during load — `select_seqs` is never called."
]
)
  ]]] -->
???+ example "Caught an exception"

    ```python { linenums="1" notest }
    from cogent3 import get_app

    reader = get_app("load_aligned", format_name="fasta")
    select_seqs = get_app("take_named_seqs", "Mouse", "Human")
    app = reader + select_seqs

    result = app("primate_brca1.fasta")
    print(result)  # (1)!

    # NotCompleted(type=ERROR, origin=load_aligned, source="primate_brca1",
    # message="Traceback (most recent call last):   File
    # "/Users/gavin/repos/SciNexus/src/scinexus/composable.py", line 525, in __call__
    # result = self.main(val, *args, **kwargs)   File
    # "/Users/gavin/repos/Cogent3/src/cogent3/app/io.py", line 334, in main     return
    # _load_seqs(path, cogent3.make_aligned_seqs, self._parser, self.moltype)   File
    # "/Users/gavin/repos/Cogent3/src/cogent3/app/io.py", line 294, in _load_seqs
    # data = _read_it(path)   File "/opt/homebrew/Cellar/python@3.14/3.14.3_1/Framewor
    # ks/Python.framework/Versions/3.14/lib/python3.14/functools.py", line 982, in
    # wrapper     return dispatch(args[0].__class__)(*args, **kw) [...]
    ```

    1. An error during load — `select_seqs` is never called.
<!-- [[[end]]] -->
