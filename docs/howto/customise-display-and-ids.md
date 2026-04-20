# Customise display and IDs

!!! abstract ""

    How to use `set_summary_display` to transform data store summary output into richer objects, and `set_id_from_source` to control how unique identifiers are extracted from data.

## Summary display default

By default, summary properties like `.describe` return Python primitive types like `dict` and `list`.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
r"""
from scinexus import open_data_store

dstore = open_data_store("data/raw.zip", suffix="fa", mode="r")
print(type(dstore.describe), "", dstore.describe, sep="\n")
"""
exec_codeblock(src=src, use_wrap=False)
  ]]] -->
```python { linenums="1" notest }
from scinexus import open_data_store

dstore = open_data_store("data/raw.zip", suffix="fa", mode="r")
print(type(dstore.describe), "", dstore.describe, sep="\n")

<class 'dict'>

{'completed': 1035, 'not_completed': 0, 'logs': 0}
```
<!-- [[[end]]] -->

## Customising summary display

You can register a customised display function for your project. For `cogent3`, it converts them into `cogent3` `Table` objects:

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
r"""
from scinexus.data_store import set_summary_display
from cogent3.core.table import Table


def summary_to_table(data, *, name):
    if isinstance(data, dict):
        title = data.pop("title", name)
        rows = [[k, v] for k, v in data.items()]
        return Table(header=["Condition", "Value"], data=rows, title=title)
    if isinstance(data, list):
        if not data:
            return Table(header=[], data=[], title=name)
        header = list(data[0].keys())
        rows = [list(row.values()) for row in data]
        return Table(header=header, data=rows, title=name)
    return data


set_summary_display(summary_to_table)
"""
exec_codeblock(src=src)
  ]]] -->
```python { linenums="1" notest }
from scinexus.data_store import set_summary_display
from cogent3.core.table import Table


def summary_to_table(data, *, name):
    if isinstance(data, dict):
        title = data.pop("title", name)
        rows = [[k, v] for k, v in data.items()]
        return Table(header=["Condition", "Value"], data=rows, title=title)
    if isinstance(data, list):
        if not data:
            return Table(header=[], data=[], title=name)
        header = list(data[0].keys())
        rows = [list(row.values()) for row in data]
        return Table(header=header, data=rows, title=name)
    return data


set_summary_display(summary_to_table)
```
<!-- [[[end]]] -->

This results in the following:

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
r"""
from scinexus import open_data_store
from scinexus.data_store import set_summary_display
from cogent3.core.table import Table


def summary_to_table(data, *, name):
    if isinstance(data, dict):
        title = data.pop("title", name)
        rows = [[k, v] for k, v in data.items()]
        return Table(header=["Condition", "Value"], data=rows, title=title)
    if isinstance(data, list):
        if not data:
            return Table(header=[], data=[], title=name)
        header = list(data[0].keys())
        rows = [list(row.values()) for row in data]
        return Table(header=header, data=rows, title=name)
    return data


set_summary_display(summary_to_table)

dstore = open_data_store("data/raw.zip", suffix="fa", mode="r")
print(type(dstore.describe), "", dstore.describe, sep="\n")
"""
exec_codeblock(src=src, use_wrap=False, display_src=False)
  ]]] -->
```python { linenums="1" notest }

<class 'cogent3.core.table.Table'>

describe
======================
Condition        Value
----------------------
completed         1035
not_completed        0
logs                 0
----------------------
```
<!-- [[[end]]] -->


!!! note
    `cogent3` registers this transformation automatically when you `import cogent3.app`, so you get `Table` output without any setup in cogent3 projects.

### Unsetting the display function

Reset the display function and revert to the default `scinexus` behaviour as follows:

```python { notest }
set_summary_display(None)
```

## Default unique ID extraction

Being able to extract unique identifiers for individual data objects is fundamental to the ability of scinexus to track provenance of individual results. Because of its roots from `cogent3`, the `scinexus` default `get_unique_id` function extracts this information from a `.source` attribute. That function,  strips format suffixes from file names to derive unique keys for data store records.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
r"""
from scinexus import get_id_from_source

func = get_id_from_source()

print(func("gene_001.fasta.gz"), func("sample.txt"))
"""
exec_codeblock(src=src)
  ]]] -->
```python { linenums="1" notest }
from scinexus import get_id_from_source

func = get_id_from_source()

print(func("gene_001.fasta.gz"), func("sample.txt"))

# gene_001 sample
```
<!-- [[[end]]] -->

## Customising unique ID extraction

Register a custom extractor when your naming convention differs:

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
r"""
from scinexus.data_store import set_id_from_source, get_id_from_source

def extract_ensembl_id(data):
    name = str(data)
    if name.startswith("ENSG"):
        return name.split(".")[0]
    return name

set_id_from_source(extract_ensembl_id)

func = get_id_from_source()

# Now the registered function is used as the default
print(func("ENSG00000157184.fa"), func("gene_001.fasta.gz"))
"""
exec_codeblock(src=src)
  ]]] -->
```python { linenums="1" notest }
from scinexus.data_store import set_id_from_source, get_id_from_source

def extract_ensembl_id(data):
    name = str(data)
    if name.startswith("ENSG"):
        return name.split(".")[0]
    return name

set_id_from_source(extract_ensembl_id)

func = get_id_from_source()

# Now the registered function is used as the default
print(func("ENSG00000157184.fa"), func("gene_001.fasta.gz"))

# ENSG00000157184 gene_001.fasta.gz
```
<!-- [[[end]]] -->

## Reset to default

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
r"""
from scinexus.data_store import set_id_from_source

set_id_from_source(None)
"""
exec_codeblock(src=src)
  ]]] -->
```python { linenums="1" notest }
from scinexus.data_store import set_id_from_source

set_id_from_source(None)
```
<!-- [[[end]]] -->

## Over-riding the default per-call

You can also override per-call without affecting the global default:

```python { notest }
result = app.apply_to(dstore, id_from_source=extract_ensembl_id)
```
