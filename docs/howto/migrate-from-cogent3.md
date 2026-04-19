# Migrate from cogent3

!!! abstract ""

    A guide for users moving from `cogent3.app` to `scinexus`.

## Update imports

Replace `cogent3.app` imports with their `scinexus` equivalents:

```python { notest }
# before
from cogent3.app.composable import define_app, NotCompleted

# after
from scinexus import define_app, NotCompleted
```

## Summary methods return plain Python objects

Data store summary methods (`describe`, `summary_logs`, `summary_not_completed`, `summary_citations`, `validate`) now return `list[dict]` or `dict` instead of cogent3 `Table` objects.

To restore `Table`-based display, register a converter with `set_summary_display()`:

```python { notest }
from cogent3.core.table import Table
from scinexus.data_store import set_summary_display


def _summary_to_table(data, *, name):
    if isinstance(data, dict):
        rows = [[k, v] for k, v in data.items()]
        return Table(header=["Condition", "Value"], data=rows, title=name)
    if isinstance(data, list) and data:
        header = list(data[0].keys())
        rows = [list(row.values()) for row in data]
        return Table(header=header, data=rows, title=name)
    return data


set_summary_display(_summary_to_table)
```

## NotCompletedType is now an enum

Failure types are categorised using the `NotCompletedType` enum rather than bare strings:

```python { notest }
from scinexus.composable import NotCompletedType

# before
nc.type == "ERROR"

# after
nc.type == NotCompletedType.ERROR
```

The three values are `ERROR`, `FAIL`, and `BUG`.

## App composition uses shallow copies

Composing apps with `+` now creates a shallow copy of the right-hand operand. Composed pipelines no longer share mutable state, so `ComposableApp.disconnect()` is no longer needed and is deprecated.

## New features

These capabilities are new in `scinexus` and were not available in `cogent3.app`:

- **`check_data_type` property** -- toggle runtime input type checking on or off. See [Runtime type checking](../explanation/type-system.md#runtime-type-checking).
- **Better IDE integration through static typing support** -- `AppBase[T, R]`, `ComposableApp[T, R]`, and `WriterApp[T, R]` can be inherited from directly as an alternative to `@define_app`.
- **Pluggable parallel backends** -- choose between stdlib multiprocessing, loky, or MPI backends. See [Run in parallel](run-in-parallel.md).
- **Pluggable progress bars** -- use `tqdm` or `rich` for progress display. See [Track progress](track-progress.md).
- **Custom identifier extraction** -- register a custom function for extracting storage identifiers from data via `set_id_from_source()`. See [Customise display and IDs](customise-display-and-ids.md).
- **Logging can be disabled** -- pass `logger=False` to `apply_to()` to skip log file creation.

## Full changelog

See the [changelog](https://github.com/cogent3/scinexus/blob/main/changelog.md) for a complete list of changes from the cogent3 app infrastructure.
