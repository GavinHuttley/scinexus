# `NotCompleted` design

*Why `scinexus` uses a sentinel object instead of exceptions for handling failures in batch processing.*

## The problem with exceptions in pipelines

When applying an algorithm to hundreds or thousands of data records, some records will inevitably fail — bad data, missing fields, violated preconditions. If failures raise exceptions, you face an unpleasant choice:

- **Let it crash.** You lose all progress and must restart from scratch.
- **Wrap everything in try/except.** Your pipeline logic becomes cluttered with error-handling boilerplate, and you must decide at every step what to catch and what to re-raise.

Neither approach scales well. You want failures to be recorded and the pipeline to continue processing the remaining records.

## The sentinel pattern

`NotCompleted` is `scinexus`'s answer: a sentinel return value that signals "this record could not be processed" without raising an exception. It carries structured information about the failure:

- **`.type`** — `FALSE` (a condition was not met) or `ERROR` (an unexpected exception occurred)
- **`.origin`** — which app produced the failure
- **`.source`** — which input data failed
- **`.message`** — a human-readable explanation

Because `NotCompleted` is a regular return value, it flows through the same code paths as successful results.

## Why it subclasses `int` and is falsy

`NotCompleted` subclasses `int` with a value of `0`, making it evaluate to `False` in boolean contexts. This means you can check for failure with a simple truthiness test:

```python { notest }
result = my_app(data)
if not result:
    print(f"Failed: {result.message}")
```

Subclassing `int` rather than defining `__bool__` alone ensures consistent behaviour with Python's truth-testing protocol across all contexts (including NumPy arrays and other libraries that inspect types).

## Automatic propagation through pipelines

When apps are composed with `+`, the resulting pipeline checks each intermediate result. If any step returns a `NotCompleted`, subsequent steps are skipped and the `NotCompleted` is returned as the final result. This means:

- A single failure does not corrupt downstream steps.
- The failure's `.origin` accurately records where the problem occurred, not where it was finally caught.
- No try/except scaffolding is needed in pipeline code.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
import cogent3 as c3

aln = c3.get_dataset("primate-brca1")
select_seqs = c3.get_app("take_named_seqs", "Mouse", "Human")
min_length = c3.get_app("min_length", 300)
app = select_seqs + min_length
result = app(aln)
print(result)
"""

exec_codeblock(src=src)
]]] -->
```python { linenums="1" notest }
import cogent3 as c3

aln = c3.get_dataset("primate-brca1")
select_seqs = c3.get_app("take_named_seqs", "Mouse", "Human")
min_length = c3.get_app("min_length", 300)
app = select_seqs + min_length
result = app(aln)
print(result)

# NotCompleted(type=FAIL, origin=take_named_seqs, source="brca1", message="named
# seq(s) {'Mouse'} not in ('FlyingLem', 'TreeShrew', 'Galago', 'HowlerMon',
# 'Rhesus', 'Orangutan', 'Gorilla', 'Chimpanzee', 'Human')")
```
<!-- [[[end]]] -->


## Recording failures in data stores

When a pipeline is run via `apply_to()` on a data store, `NotCompleted` results are automatically written to a separate area (the `not_completed/` subdirectory or SQL table). This gives you a complete audit trail: you can inspect which records failed, which app was responsible, and why — all without interrupting the processing of successful records.

See [Handle failures](../howto/handle-failures.md) for usage examples.
