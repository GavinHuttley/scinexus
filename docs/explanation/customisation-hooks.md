# Customisation hooks

!!! abstract ""

    How `scinexus` uses module-level registry functions to let downstream packages customise summary display and identifier extraction without subclassing.

## The pattern

`scinexus` uses module-level registry functions to let downstream packages customise behaviour without subclassing or monkey-patching. Each hook follows the same shape:

- **`set_*(func)`** registers a callable (or `None` to clear)
- **`get_*()`** returns the currently registered callable (or the default)

This keeps `scinexus` free of dependencies on downstream packages while still allowing them to integrate deeply.

## `set_summary_display` — transforming summary output

Data store summary properties (`.describe`, `.summary_logs`, `.summary_not_completed`, `.summary_citations`, `.validate()`) collect their data as plain Python dicts or lists of dicts. By default these are returned as-is.

A downstream package can register a display function that transforms these raw structures into richer objects. The function must accept `(data, *, name)` where `data` is the raw dict or list and `name` is the summary method name (e.g. `"describe"`).

### How `cogent3` uses this

When `cogent3.app` is imported, it registers a function that converts summaries into `cogent3.core.table.Table` objects:

```python { notest }
from scinexus.data_store import set_summary_display
from cogent3.core.table import Table


def _summary_to_table(data, *, name):
    if isinstance(data, dict):
        title = data.pop("title", name)
        rows = [[k, v] for k, v in data.items()]
        return Table(
            header=["Condition", "Value"],
            data=rows,
            title=title,
        )

    if isinstance(data, list):
        if not data:
            return Table(header=[], data=[], title=name)
        header = list(data[0].keys())
        rows = [list(row.values()) for row in data]
        return Table(header=header, data=rows, title=name)

    return data


set_summary_display(_summary_to_table)
```

After this registration, every call to `dstore.describe` or `dstore.summary_not_completed` returns a `Table` with a rich notebook repr, rather than a plain dict.

## `set_id_from_source` — customising unique ID extraction

When `apply_to()` or `as_completed()` processes a data store, each result needs a unique identifier so the writer can store it and skip already-processed inputs on subsequent runs. By default, `scinexus` extracts this ID using `get_unique_id`, which strips format suffixes from file names:

```
"gene_001.fasta.gz"  →  "gene_001"
```

If your data uses a different naming convention — for example, IDs embedded in the file content or in a metadata field — you can register a custom extractor:

```python { notest }
from scinexus.data_store import set_id_from_source


def my_id_extractor(data):
    """Extract ID from a metadata dict."""
    return data.info.source.split("/")[-1].split("_")[0]


set_id_from_source(my_id_extractor)
```

The registered function is consulted by:

- `WriterApp.apply_to()` — to derive output record keys
- `AppBase.as_completed()` — to identify results
- `NotCompleted` — to normalise the `source=` attribute on error records

Pass `None` to restore the default:

```python { notest }
set_id_from_source(None)  # back to get_unique_id
```

Per-call overrides via the `id_from_source` keyword on `apply_to()` and `as_completed()` still take precedence over the registered function.

## The default ID pipeline: `get_data_source` → `get_unique_id`

The default extractor, `get_unique_id`, works in two steps:

1. **`get_data_source(data)`** extracts a source string from the input. This is a singledispatch function that handles:
    - `str` / `Path` → the file name
    - `dict` → looks for `data["info"]["source"]` or `data["source"]`
    - `DataMemberABC` → the member's `unique_id`
    - Any object with a `.source` attribute → recurses on that attribute

2. **`get_unique_id(name)`** strips format suffixes (e.g. `.fasta`, `.gz`) from the source string returned by `get_data_source`.

Together they turn inputs like `DataMember(unique_id="gene_001.fasta.gz")` into the key `"gene_001"`.
